# SVS Integration Specification

**Audience:** Intel SVS runtime engineers
**Author:** valkey-search team (`izaakk/valkey-search`, branch `svs-integration-spec`)
**Scope:** How valkey-search integrates a vector index today, how the current SVS prototype differs from HNSW, and what we need from the next SVS runtime to reach parity.

---

## 1. Why this document exists

valkey-search uses a vector index through an abstract C++ class (`VectorBase`). Two concrete implementations exist today: `VectorHNSW` (wraps hnswlib) and `VectorSVS` (wraps the SVS runtime at `libsvs_runtime.so.0.2.0`).

The HNSW implementation hits valkey-search's threading requirements naturally: readers run concurrently, writers run concurrently, and the only serialization point is a rare index resize. hnswlib does the locking itself with per-label mutexes (`link_list_locks_` and `label_lookup_lock` in `hnswalg.h`), so the valkey-search wrapper takes no lock at all on the search path, and takes a *shared* lock only for mutations so the resize writer can shoot down all in-flight mutations briefly while it grows the backing arrays.

The SVS prototype cannot match this yet. Our wrapper takes an **exclusive mutex** on every mutation, buffers inserts in a 10 K-entry queue (`pending_buffer_`), and flushes the buffer inside the same lock — blocking searches for 0.5-1 s on each flush. It also keeps a full FP32 shadow copy of every vector (`raw_vectors_`) because the SVS runtime does not expose the retrieval/distance APIs we need. These workarounds exist because the current SVS runtime API and threading model don't support the access pattern valkey-search needs.

This document walks through each `VectorBase` virtual method, shows how hnswlib supports it today, shows the SVS workaround, and states the concrete ask on the SVS runtime. Every code claim is cited with a file path and line range in `izaakk/valkey-search` (branch `svs-integration-spec`). Each method section ends with a pointer to a **pair** of standalone C++ tests in [`svs_integration_tests/`](./svs_integration_tests/): an `hnsw/test_0N_*.cc` baseline that runs against vendored upstream hnswlib, and a direct twin `svs/test_0N_*.cc` that runs the same scenario against `libsvs_runtime.so`. Read them side by side.

---

## 2. valkey-search threading model (1-page primer)

valkey-search runs three worker thread pools, created at module startup in `src/valkey_search.cc:1236-1248` (reader/writer sizes default to the physical CPU count; utility pool defaults to 1 thread):

```
reader_thread_pool_  ← FT.SEARCH commands dispatch here
writer_thread_pool_  ← HSET / HDEL / FT.CREATE / FT.DROPINDEX dispatch here
utility_thread_pool_ ← background cleanup
```

Each `VectorBase` instance owns a single `absl::Mutex` (the "resize mutex" in HNSW, the "index mutex" in SVS). The lock is used only on *mutation* paths — the HNSW search path doesn't take it at all. SVS does take a reader lock on it in Search (but that's a SVS-specific workaround; see §3.1.4).

**What we require from the underlying index, per method:**

| Method called from | valkey-search locking | Index impl must support |
|---|---|---|
| `Search()` (reader pool) | **no lock at all** (HNSW); reader lock (SVS today as a workaround) | fully concurrent calls, safely re-entrant |
| `AddRecordImpl()` (writer pool) | shared on `resize_mutex_` (HNSW) / exclusive on `index_mutex_` (SVS today) | safely concurrent with searches and other adds |
| `RemoveRecordImpl()` (writer pool) | shared (HNSW) / exclusive (SVS today) | safely concurrent with searches |
| `ModifyRecordImpl()` (writer pool) | shared (HNSW) / exclusive (SVS today) | safely concurrent with searches |

**HNSW passes this contract**: hnswlib's `searchKnn` / `addPoint` / `markDelete` are safe to call concurrently (from unlocked readers and shared-lock writers) because hnswlib has its own per-label locks — `link_list_locks_[node]` protecting each node's outgoing edges and `label_lookup_lock` protecting the label→id map (see `hnswalg.h:43`, `hnswalg.h:59`, and the locking in `addPoint` at `hnswalg.h:954-992`). The wrapper's `resize_mutex_` reader lock on mutations exists solely so that the rare `ResizeIfFull` exclusive-lock writer can drain all in-flight adds before growing the arrays (`src/indexes/vector_hnsw.cc:244-276`). Search itself takes no lock on the wrapper side (`src/indexes/vector_hnsw.cc:320-361`, with `ABSL_NO_THREAD_SAFETY_ANALYSIS` on the inner lambda).

**SVS fails this contract today**: the prototype takes exclusive locks on every mutation and a reader lock on every search. Searches can become writers (auto-flush under exclusive lock). We're asking the SVS runtime to close that gap so the wrapper can match HNSW's locking discipline — see §3.1 for the detailed view of the Search path specifically.

---

## 3. Per-method specification

Every section below has five parts:

1. **Contract** — what the method must do
2. **HNSW today** — code snippet and line range from `src/indexes/vector_hnsw.cc`
3. **SVS prototype today** — code snippet and line range from `src/indexes/vector_svs.cc`
4. **Ask on new SVS runtime** — concrete bullets
5. **Validation** — path to a reproducible test

### 3.1 `Search()`

This section is deliberately long. `Search()` is the method valkey-search
cares about most — every read-path command (`FT.SEARCH`, filter
evaluation, KNN-within-predicate) ultimately lands here, and the
concurrency model around it determines whether valkey-search can serve
queries at core-count throughput.

#### 3.1.1 Contract

```cpp
absl::StatusOr<std::vector<Neighbor>> Search(
    absl::string_view query, uint64_t count,
    cancel::Token& cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
    /* impl-specific runtime tuning parameter: ef_runtime for HNSW,
       search_window_size for SVS */);
```

- Return the top-`count` nearest neighbors to `query`, ordered by distance using the index's configured metric.
- Called on a **reader-pool thread** (`reader_thread_pool_`, set up in `src/valkey_search.cc:1236-1239`). Many calls may be in-flight concurrently on different reader threads.
- `filter` is optional and can cause the call to evaluate a pre-filter predicate against each candidate (extra distance computations against stored vectors).
- `cancellation_token` lets the caller abort a search mid-traversal (FT.SEARCH timeout).
- **valkey-search does not wrap this call in any lock** (neither `VectorBase` nor the query layer take a lock before calling Search — see §3.1.3 below). All thread-safety is delegated to the index implementation.

#### 3.1.2 How FT.SEARCH reaches the index

1. `FT.SEARCH` arrives on Valkey's main thread.
2. The command handler at `src/commands/commands.cc:196-198` calls `query::SearchAsync(..., ValkeySearch::Instance().GetReaderThreadPool(), ...)`, which enqueues a task on the reader pool and returns.
3. A reader-pool worker picks up the task and calls into `src/query/search.cc:121-149`, which dispatches to `VectorHNSW::Search(...)` or `VectorSVS::Search(...)` based on the index type.
4. Neither step takes a lock on the vector index itself. Whatever synchronization is needed must live inside the implementation.

This matters because it means **concurrent `Search()` calls *must* be safe by themselves, without the caller doing anything special**. The reader pool can issue as many parallel searches as it has threads, and each of them lands straight on the index.

#### 3.1.3 HNSW behavior (what works today)

**Entry point: `VectorHNSW::Search` (`src/indexes/vector_hnsw.cc:320-361`)**

```cpp
template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorHNSW<T>::Search(…) {
  auto perform_search = [this, count, &filter, enable_partial_results,
                         &ef_runtime, &cancellation_token]
      (absl::string_view query) ABSL_NO_THREAD_SAFETY_ANALYSIS
      -> absl::StatusOr<std::priority_queue<std::pair<T, hnswlib::labeltype>>> {
    try {
      CancelCondition cancel_condition(cancellation_token);
      auto res = algo_->searchKnn((T *)query.data(), count, ef_runtime,
                                  filter.get(), &cancel_condition);
      …
    } catch (…) { … }
  };
  …
}
```

Observations:

- **No `resize_mutex_` is acquired.** The lambda is annotated `ABSL_NO_THREAD_SAFETY_ANALYSIS`, i.e. the wrapper intentionally bypasses Abseil's lock checker. `algo_->searchKnn(...)` is called with zero synchronization from the valkey-search side.
- This is safe because hnswlib's `searchKnn` is a `const` method and reads the graph with fine-grained internal locking only when strictly needed.

**What hnswlib does internally to stay safe:**

- `HierarchicalNSW::searchKnn` (upstream hnswlib `hnswalg.h`, `svs_integration_tests/hnsw/third_party/hnswlib/hnswalg.h:1271-1324` in the vendored copy) is `const`. It descends the graph from `enterpoint_node_`, at each hop reading a node's link list via `get_linklist(currObj, level)` and calling `fstdistfunc_(query, getDataByInternalId(cand), …)`.
- Link lists and vector storage are bare memory (no per-hop locking during search). What keeps this safe under concurrent mutation:
  - Per-node link-list locks: `std::vector<std::mutex> link_list_locks_` (`hnswalg.h:43`). `addPoint` / `markDelete` / graph-repair paths take `link_list_locks_[node]` before mutating that node's link list. Readers do not take these locks, but mutations are short — the write is a list overwrite, and readers that happen to traverse a node mid-write observe either the old or new list, not torn state (atomic pointer writes of pre-sized buffers).
  - A label→internal-id table guarded by `label_lookup_lock` (`hnswalg.h:59`). Used by `addPoint` / `markDelete` / `getExternalLabel`. Readers on the search path consult it via `getExternalLabel(internalId)` after the graph traversal completes.
  - A delete bit in each node's link-list header, tested by `isMarkedDeleted(internalId)` (`hnswalg.h:934-937`). Set with a per-node lock; reads are unlocked byte reads. A just-deleted node may still be traversed, but is filtered out of the result set.

**In valkey-search terms:**
- Multiple `VectorHNSW::Search` calls run in parallel with no serialization between them.
- A `VectorHNSW::AddRecordImpl` running concurrently (also with no `resize_mutex_` contention — shared lock only, see §3.2) cannot corrupt a search. At worst a brand-new node becomes visible mid-traversal.
- The only exclusive lock on `resize_mutex_` is taken by `ResizeIfFull` (`src/indexes/vector_hnsw.cc:244-276`). Writers take reader locks on `resize_mutex_` specifically to let the resize-writer block *everyone* when it needs to enlarge the backing arrays. Searches do **not** take that reader lock — the search path is genuinely unlocked.

**Net effect:** on 8 reader cores, hnswlib scales to ~5× on our test (`svs_integration_tests/hnsw/test_02_concurrent_search.cc` reports 14k qps → 71k qps from N=1 to N=8).

#### 3.1.4 SVS prototype behavior today

**Entry point: `VectorSVS::Search` (`src/indexes/vector_svs.cc:459-610`)**

The full path (abridged; comments removed for density):

```cpp
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(…) {
  // 1. Record a total-search stopwatch for the Layer-1 histogram.
  vmsdk::StopWatch total_search_timer;
  Metrics::GetStats().svs_search_cnt.fetch_add(1, std::memory_order_relaxed);

  auto perform_search = [this, count, &filter, &search_window_size,
                         &cancellation_token](absl::string_view q) {
    // 2. Auto-flush: if pending_buffer_ is non-empty, the search thread
    //    becomes a *writer*, grabs an exclusive lock, and flushes.
    bool needs_flush = false;
    bool was_flushing = false;
    { absl::ReaderMutexLock check_lock(&index_mutex_);
      needs_flush   = !pending_buffer_.empty() && !buffer_flushing_;
      was_flushing  = buffer_flushing_;
    }
    if (was_flushing) { Metrics::GetStats().svs_searches_during_flush_cnt.fetch_add(1, …); }
    if (needs_flush) {
      absl::MutexLock flush_lock(&index_mutex_);       // ← EXCLUSIVE LOCK
      if (!pending_buffer_.empty()) {
        VMSDK_RETURN_IF_ERROR(FlushBuffer());          // ← 500-1000 ms stall
      }
    }

    // 3. Wait for the reader lock. When we were racing with FlushBuffer()
    //    from step 2, this is the blackout window.
    vmsdk::StopWatch lock_wait_timer;
    absl::ReaderMutexLock lock(&index_mutex_);         // ← SHARED LOCK
    auto lock_wait = lock_wait_timer.Duration();
    Metrics::GetStats().svs_search_lock_wait_latency.SubmitSample(lock_wait);

    // 4. Per-thread OMP pin. omp_set_num_threads is a thread-local ICV —
    //    the value set during Create() on the main thread does NOT
    //    propagate to reader-pool threads, so we re-apply it here.
#ifdef _OPENMP
    long long omp_threads = options::GetSVSOmpThreads().GetValue();
    if (omp_threads > 0) { omp_set_num_threads((int)omp_threads); }
#endif

    // 5. The actual SVS call. Goes through GOMP_parallel internally,
    //    even with a team size of 1.
    vmsdk::StopWatch core_search_timer;
    auto status = svs_index_->search(1, q.data(), k,
                                      distances.data(), labels.data(),
                                      params_ptr, svs_filter.get());
    Metrics::GetStats().svs_search_core_latency.SubmitSample(
        core_search_timer.Duration());
    …
  };
  …
}
```

**Five structural differences vs. HNSW:**

1. **A search can become a writer.** If `pending_buffer_` is non-empty when a search arrives, that search thread grabs the exclusive lock and runs `FlushBuffer()` — a blocking call that takes ~500-1000 ms for a 10 K-vector batch at 1 M scale (observed in `INFO search_svs`, spec §3.2). During that window, all other concurrent searches on the same index block on the writer lock.

2. **The search holds a reader lock.** Every call wraps `svs_index_->search(...)` in `absl::ReaderMutexLock lock(&index_mutex_)`. Concurrent searches share the lock (they don't serialize on each other), but any concurrent writer blocks all of them.

3. **Per-call OMP re-pin.** `omp_set_num_threads(N)` is a per-thread ICV in libgomp. Setting it in `Create()` on the main thread does not carry over to reader-pool threads. We re-apply it on every search to make the pin stick. The default value is `1` (spec §3.10) — controllable via the `svs-omp-threads` module option.

4. **`svs_index_->search(...)` always dispatches through `GOMP_parallel`.** In the vendored `libsvs_runtime.so.0.2.0` this is unconditional — even with a team size of 1, every call pays the libgomp fork/join bookkeeping (~40-70 µs per call on our box, measured in `SVS_OMP_PERF_ANALYSIS.md`). The SVS runtime is nominally thread-safe (`search()` is `const noexcept` in SVS runtime header `svs/runtime/vamana_index.h:51-59`), but the throughput cost of the dispatch compounds at high concurrency.

5. **Oversubscription at OMP > 1.** If the caller sets `omp_set_num_threads(N)` with N > 1 and valkey-search has R reader threads, the effective thread count is `N × R`. On 8 cores with R = 8 and N = 4, that's 32 threads competing for 8 cores — profiling shows kernel-space CPU rising from 2.7 % to 17.6 %, dominated by `native_queued_spin_lock_slowpath` (details in `SVS_OMP_PERF_ANALYSIS.md`). Today we avoid this by hard-pinning OMP = 1.

#### 3.1.5 Observability (Layer 1 metrics exposed by `INFO search_svs`)

We added `INFO` fields (definitions in `src/valkey_search.cc:878-965`) so anyone running the prototype can see the cost directly:

| `INFO` key (section → field) | Meaning |
|---|---|
| `INFO search_svs` → `search_svs_search_count` | cumulative Search calls |
| `INFO search_svs` → `search_svs_searches_during_flush` | searches that arrived while `buffer_flushing_ == true` |
| `INFO search_svs` → `search_svs_search_blackout_us_total` | cumulative microseconds searches spent blocked on writer-held exclusive lock |
| `INFO search_latency` → `search_svs_search_lock_wait_latency_usec` | HDR histogram of reader-lock acquisition time |
| `INFO search_latency` → `search_svs_search_core_latency_usec` | HDR histogram of `svs_index_->search()` alone |
| `INFO search_latency` → `search_svs_vector_index_search_latency_usec` | HDR histogram of the whole `Search()` (lock wait + core + post-processing) |

The gap between `search_core_latency` and `vector_index_search_latency` shows post-processing overhead (priority-queue building, result assembly). Non-zero `search_lock_wait_latency` indicates contention with writers — today, almost entirely from `FlushBuffer()`.

#### 3.1.6 Ask on new SVS runtime

Concretely, in order of importance to us:

1. **Concurrent-safe `search()` without internal OS-thread fan-out.** The ideal: `svs_index_->search()` runs entirely on the calling thread, uses no libgomp, and is safe to call from any number of threads simultaneously. valkey-search already multithreads at the reader-pool level; intra-search parallelism is the wrong axis when every sibling search also wants a core.
2. **A build option or a per-index config that turns off the `GOMP_parallel` dispatch on the search path.** If SVS must ship the parallel-search code path, fine — but let us opt out at build time (e.g. `-DSVS_ENABLE_OMP=OFF`) or per index (`VamanaIndex::SearchParams::threads = 1` with guaranteed-serial semantics). Today's `omp_set_num_threads(1)` workaround is brittle because it depends on a thread-local ICV we have to re-apply on every call.
3. **Deterministic per-call thread behavior.** When `threads = 1`, the code path must be identical to `threads = 4` with only team size different — no "skip the parallel region entirely if N == 1" or "start extra helpers if N > 1." This way, correctness bugs don't hide behind OMP configuration.
4. **Concurrent-safe with concurrent `add()` / `remove()` / `update()`.** Today we serialize all mutations with an exclusive lock because we don't know what the SVS graph layout looks like mid-insert. If SVS uses per-partition or per-label locking internally, expose a guarantee that `search()` can run concurrently with one mutating call.
5. **Cancellation.** Our wrapper checks the cancel token after `svs_index_->search()` returns (`src/indexes/vector_svs.cc:567-572`) because SVS has no mid-traversal cancel hook. For long searches this wastes CPU on a query we're going to discard. A `SearchParams::cancel_callback` (equivalent of hnswlib's `BaseCancellationFunctor`) would let us kill an in-flight search on timeout.

**Non-goals for SVS:** we do *not* need SVS to spawn its own threads, run a background search pool, or manage request queueing — that's valkey-search's job.

#### 3.1.7 Validation tests

Test pair **02** — concurrent search scaling:
- [`svs_integration_tests/hnsw/test_02_concurrent_search.cc`](./svs_integration_tests/hnsw/test_02_concurrent_search.cc) — baseline. Observed on 8 vCPU: ~14 k qps at N=1, ~71 k qps at N=8 (scale 5.1×).
- [`svs_integration_tests/svs/test_02_concurrent_search.cc`](./svs_integration_tests/svs/test_02_concurrent_search.cc) — same scenario on SVS, run twice: once with `omp_set_num_threads(1)` and once with `4`. At OMP=1, scaling matches HNSW (≈ 4.9×). At OMP=4, single-thread QPS collapses to ~3 k (5× worse than OMP=1) and aggregate scaling is non-linear — exactly the oversubscription described in §3.1.4 point 5.

Test pair **04** — search during add (demonstrates §3.1.4 point 1):
- [`svs_integration_tests/hnsw/test_04_search_during_add.cc`](./svs_integration_tests/hnsw/test_04_search_during_add.cc) — baseline. Reader p99 stays within 10% of the no-writer case.
- [`svs_integration_tests/svs/test_04_search_during_add.cc`](./svs_integration_tests/svs/test_04_search_during_add.cc) — same scenario on SVS. p99 spikes are observable but currently masked by the load-gen pattern; the real-world blackout is better seen through `INFO search_svs_search_blackout_us_total` under a mixed read/write workload.

What "pass" looks like on the new SVS runtime:
- test_02 at OMP=any runs without oversubscription — no collapse at high thread counts.
- test_04 shows a stall-free p99 even while the writer thread is running.
- `search_svs_search_blackout_us_total` stays near zero under mixed load.

---

### 3.2 `AddRecordImpl()`

#### Contract

```cpp
virtual absl::Status AddRecordImpl(uint64_t internal_id,
                                   absl::string_view record) = 0;
```

Insert one vector with a caller-chosen integer label. Called on a **writer-pool thread**. Many `AddRecordImpl` calls may run concurrently on different threads with different `internal_id` values. Must be safe to run while readers are actively searching.

#### HNSW today (`src/indexes/vector_hnsw.cc:182-204`)

```cpp
absl::Status VectorHNSW<T>::AddRecordImpl(uint64_t internal_id,
                                          absl::string_view record) {
  do {
    try {
      absl::ReaderMutexLock lock(&resize_mutex_);   // ← SHARED lock
      algo_->addPoint((T *)record.data(), internal_id);
      return absl::OkStatus();
    } catch (…) {
      if (absl::StrContains(error_msg,
              "The number of elements exceeds the specified limit")) {
        VMSDK_RETURN_IF_ERROR(ResizeIfFull());       // upgrades to exclusive
        continue;
      }
      return absl::InternalError(…);
    }
  } while (true);
}
```

Two important properties:

- **Shared lock**: concurrent `AddRecordImpl` calls don't serialize on each other. hnswlib uses per-label locks (`link_list_locks_[internal_id]` and the `label_lookup_lock` guarding the label→id map) to protect the graph structure.
- **Exclusive lock only on resize**: if `addPoint` throws "exceeds limit", the wrapper calls `ResizeIfFull()` which grabs a writer lock, grows the backing arrays by a fixed `hnsw-block-size` (default 10 240, see `src/valkey_search_options.cc:75`), and retries. Resize is linear in N — happens every `block_size` inserts — but each one completes in milliseconds.

#### SVS prototype today (`src/indexes/vector_svs.cc:230-274`)

```cpp
absl::Status VectorSVS<T>::AddRecordImpl(uint64_t internal_id,
                                          absl::string_view record) {
  try {
    absl::MutexLock lock(&index_mutex_);              // ← EXCLUSIVE lock
    pending_buffer_.push_back({ .internal_id = internal_id,
                                .data = std::vector<char>(record.begin(),
                                                          record.end()) });
    raw_vectors_[internal_id] = pending_buffer_.back().data;  // FP32 shadow
    Metrics::GetStats().svs_pending_buffer_vectors.fetch_add(1, …);
    if (pending_buffer_.size() >= kBufferSize) {     // 10 000
      VMSDK_RETURN_IF_ERROR(FlushBuffer());          // ← 0.5-1 s stall
    }
    ++num_elements_;
    return absl::OkStatus();
  } catch (…) { … }
}
```

Two reasons for the workaround (both root-causable to the SVS runtime):

1. Per-call `svs_index_->add(1, &label, data)` has high variance and a long tail (average ~260 µs but p99 > 2 ms at just 5 K vectors, see test pair 05; this grows substantially with N). In earlier measurements at 1 M scale with unfavourable parameters we saw sub-1-vec/s throughput. Batching 10 000 vectors and calling `add(10000, …)` once amortizes the per-call overhead.
2. The buffer + flush is done under an **exclusive** lock because we can't safely mix `add()` with concurrent `search()` calls on the same index.

Consequences: every 10 000th insert blocks every reader for ~0.5-1 s (the "flush blackout"). See `src/indexes/vector_svs.cc:277-338` for `FlushBuffer`.

#### Ask on new SVS runtime

- **Make per-vector `add()` fast** (single-digit milliseconds for a 768-dim vector at 1 M scale), so valkey-search doesn't need to buffer client-side.
- **Allow `add()` to run concurrently with `search()`** on the same index. Internal locking should be per-label or per-partition, not global.
- **Allow concurrent `add()` from multiple threads**, each inserting disjoint labels, without the caller serializing them.

If the per-vector latency target isn't achievable, a secondary ask is an **asynchronous `add()`** that enqueues work on an SVS-internal thread and returns immediately (with a completion notification); valkey-search can then drop its own buffer.

#### Validation

Test pair **05** — per-vector add latency:
- [`svs_integration_tests/hnsw/test_05_incremental_add_latency.cc`](./svs_integration_tests/hnsw/test_05_incremental_add_latency.cc) — baseline ~330 µs/vec at N=20k.
- [`svs_integration_tests/svs/test_05_incremental_add_latency.cc`](./svs_integration_tests/svs/test_05_incremental_add_latency.cc) — ~260 µs avg / p99 >2 ms at N=5k (high variance).

Test pair **03** — concurrent add with disjoint IDs:
- [`svs_integration_tests/hnsw/test_03_concurrent_add.cc`](./svs_integration_tests/hnsw/test_03_concurrent_add.cc) — baseline scales 2.5× at n=8.
- [`svs_integration_tests/svs/test_03_concurrent_add.cc`](./svs_integration_tests/svs/test_03_concurrent_add.cc) — **SEGFAULTs on 0.2.0**; the crash is the ask.

Test pair **04** — search during add (see §3.1).

---

### 3.3 `RemoveRecordImpl()`

#### Contract

```cpp
virtual absl::Status RemoveRecordImpl(uint64_t internal_id) = 0;
```

Logically delete one vector. Called on a **writer-pool thread**, possibly concurrent with readers and other writers. valkey-search is fine with "mark-deleted" semantics — no physical reclaim required.

#### HNSW today (`src/indexes/vector_hnsw.cc:296-307`)

```cpp
absl::Status VectorHNSW<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);   // ← SHARED lock
    algo_->markDelete(internal_id);
  } catch (…) { … }
  return absl::OkStatus();
}
```

`markDelete` flips a bit in hnswlib under its own per-label lock. Constant-time, concurrent-safe.

#### SVS prototype today (`src/indexes/vector_svs.cc:341-367`)

```cpp
absl::Status VectorSVS<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::MutexLock lock(&index_mutex_);          // ← EXCLUSIVE lock
    if (!pending_buffer_.empty()) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());        // ← stall
    }
    size_t label = static_cast<size_t>(internal_id);
    auto status = svs_index_->remove(1, &label);
    raw_vectors_.erase(internal_id);
    …
  } catch (…) { … }
}
```

We force a flush before remove because `remove()` needs the vector to be in the SVS graph (not still pending in our buffer). This amplifies the blackout.

#### Ask on new SVS runtime

- **Concurrent-safe `remove()`** under a reader lock (same contract as `search()`).
- **`remove()` on a label that was just `add()`-ed must succeed** without requiring an intermediate flush/sync. The ordering guarantee valkey-search needs is: "after `add(id)` returns, `remove(id)` and `search()` both see the vector."

#### Validation

Test pair **01** — basic call pattern `add → search → modify → remove → search`:
- [`svs_integration_tests/hnsw/test_01_call_pattern.cc`](./svs_integration_tests/hnsw/test_01_call_pattern.cc)
- [`svs_integration_tests/svs/test_01_call_pattern.cc`](./svs_integration_tests/svs/test_01_call_pattern.cc)

---

### 3.4 `ModifyRecordImpl()`

#### Contract

```cpp
virtual absl::Status ModifyRecordImpl(uint64_t internal_id,
                                      absl::string_view record) = 0;
```

Replace the vector stored at `internal_id`. Called on writer-pool thread.

#### HNSW today (`src/indexes/vector_hnsw.cc:278-294`)

```cpp
absl::Status VectorHNSW<T>::ModifyRecordImpl(uint64_t internal_id,
                                             absl::string_view record) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);   // ← SHARED lock
    algo_->markDelete(internal_id);
    algo_->addPoint((T *)record.data(), internal_id);
  } catch (…) { … }
  return absl::OkStatus();
}
```

Mark-delete old + add new. Both under shared lock; hnswlib handles per-label safety.

#### SVS prototype today (`src/indexes/vector_svs.cc:370-441`)

Forces a flush, then calls `svs_index_->remove()` + `svs_index_->add()` under an exclusive lock, and updates the FP32 shadow in `raw_vectors_`. Same performance problems as `AddRecordImpl` + `RemoveRecordImpl` combined.

#### Ask on new SVS runtime

Same as `AddRecordImpl` + `RemoveRecordImpl`. If SVS provides a single `update(id, new_vector)` primitive, valkey-search will use it; otherwise remove+add is fine.

#### Validation

Test pair **01** (see §3.3) exercises the modify path.

---

### 3.5 `GetValueImpl()`

#### Contract

```cpp
virtual char* GetValueImpl(uint64_t internal_id) const = 0;
```

Return a pointer to the stored raw vector bytes. Called when valkey-search needs to hand back a vector to the client (`FT.SEARCH ... RETURN vector` in `src/query/search.cc:525-545`) and during a few internal paths.

Ideal semantics: return exactly what was inserted (bit-identical FP32). For lossy compression (LVQ*, LeanVec) a best-effort reconstruction is still useful to us and preferable to not having the method at all — we'd rather return an approximate vector to the client than refuse the read — but if the SVS team can guarantee bit-identity for StorageKind::FP32, that's the preferred behavior.

#### HNSW today (`src/indexes/vector_hnsw.h:92-95`)

```cpp
char* GetValueImpl(uint64_t internal_id) const override
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  return algo_->getPoint(internal_id);
}
```

hnswlib stores raw vectors inline in its graph node storage — free.

#### SVS prototype today (`src/indexes/vector_svs.cc:661-673`)

```cpp
char* VectorSVS<T>::GetValueImpl(uint64_t internal_id) const {
  absl::ReaderMutexLock lock(&index_mutex_);
  auto it = raw_vectors_.find(internal_id);
  if (it == raw_vectors_.end()) return nullptr;
  // Return pointer into raw_vectors_ map (unsafe if mutated during use!)
  return const_cast<char*>(it->second.data());
}
```

We keep `raw_vectors_` — a parallel `hash_map<uint64_t, std::vector<char>>` of every vector ever inserted, uncompressed, guarded by `index_mutex_`. This **doubles memory cost** (full FP32 copy + compressed graph). It exists entirely because the SVS runtime has no way to read back a vector that's already in the graph.

#### Ask on new SVS runtime

Add **`reconstruct(size_t label, float* out)`** to `DynamicVamanaIndex`. Semantics:
- Return the stored vector for `label`, decompressed to FP32 if needed.
- For lossy compression (LVQ, LeanVec), an approximate reconstruction is acceptable — valkey-search does not require bit-exact round-trip.
- Safe to call concurrently with `search()` and `add()`.
- O(1) or O(log N) ideally; not a full graph traversal.

With `reconstruct` available, valkey-search can delete `raw_vectors_` entirely. Impact on memory: roughly halves the footprint for an uncompressed SVS index and is even larger savings proportionally for compressed indexes (because the LVQ4X8 graph itself is small, so the FP32 shadow dominates).

#### Validation

Test pair **07** — reconstruct vector by label:
- [`svs_integration_tests/hnsw/test_07_reconstruct.cc`](./svs_integration_tests/hnsw/test_07_reconstruct.cc) — baseline via `algo_->getDataByInternalId()`.
- [`svs_integration_tests/svs/test_07_reconstruct.cc`](./svs_integration_tests/svs/test_07_reconstruct.cc) — **fails to link on 0.2.0** with `undefined reference to svs::runtime::v0::dynamic_vamana_reconstruct`. That link error is the ask.

---

### 3.6 `ComputeDistanceFromRecordImpl()`

#### Contract

```cpp
virtual absl::StatusOr<std::pair<float, hnswlib::labeltype>>
ComputeDistanceFromRecordImpl(uint64_t internal_id,
                              absl::string_view query) const = 0;
```

Return the distance between the query vector and the stored vector at `internal_id`, using the same metric the index was built with. Called during pre-filter scoring.

#### HNSW today (`src/indexes/vector_hnsw.cc:383-397`)

```cpp
absl::StatusOr<std::pair<float, hnswlib::labeltype>>
VectorHNSW<T>::ComputeDistanceFromRecordImpl(uint64_t internal_id,
                                             absl::string_view query) const {
  auto id = hnswlib_helpers::GetInternalIdDuringSearch(algo_.get(), internal_id);
  …
  return {algo_->fstdistfunc_((T *)query.data(),
                              algo_->getDataByInternalId(*id),
                              algo_->dist_func_param_), internal_id};
}
```

One distance-function call against the stored vector. Fast.

#### SVS prototype today (`src/indexes/vector_svs.cc:640-658`)

```cpp
absl::StatusOr<std::pair<float, hnswlib::labeltype>>
VectorSVS<T>::ComputeDistanceFromRecordImpl(uint64_t internal_id,
                                            absl::string_view query) const {
  absl::ReaderMutexLock lock(&index_mutex_);
  auto it = raw_vectors_.find(internal_id);
  if (it == raw_vectors_.end()) { … }
  // Use the hnswlib space interface to compute distance on the raw FP32 copy.
  auto dist = space_->get_dist_func()(query.data(), it->second.data(),
                                      space_->get_dist_func_param());
  return {dist, internal_id};
}
```

We fall back to `raw_vectors_` and an **hnswlib** `SpaceInterface`, because the SVS runtime has no way to compute a distance against a stored label. This works but ties us to keeping the FP32 shadow *and* compiling hnswlib's space interface into the SVS wrapper — neither of which should be necessary once SVS exposes the API.

#### Ask on new SVS runtime

Add **`compute_distance(size_t label, const float* query, float* out)`** to `DynamicVamanaIndex`. Semantics:
- Return the distance between `query` and the stored vector at `label`, using the index's metric (and, for compressed indexes, the asymmetric distance over the compressed form — same function `search()` uses internally).
- Safe for concurrent calls.

This lets us drop both `raw_vectors_` and the hnswlib space-interface dependency.

#### Validation

Test pair **08** — compute distance by label:
- [`svs_integration_tests/hnsw/test_08_compute_distance.cc`](./svs_integration_tests/hnsw/test_08_compute_distance.cc) — baseline via `algo_->fstdistfunc_()`.
- [`svs_integration_tests/svs/test_08_compute_distance.cc`](./svs_integration_tests/svs/test_08_compute_distance.cc) — **fails to link on 0.2.0** with `undefined reference to svs::runtime::v0::dynamic_vamana_compute_distance`. That link error is the ask.

---

### 3.7 `TrackVector()` / `IsVectorMatch()` / `UnTrackVector()`

#### Contract

These three methods are used by valkey-search's deduplication / reindex logic. They let the index compare a newly-arriving vector against the stored one for a given id, so valkey-search can skip no-op updates.

#### HNSW today (`src/indexes/vector_hnsw.cc:110-137`)

`TrackVector` pushes the `InternedStringPtr` into a tracked-vectors deque (under `tracked_vectors_mutex_`). `IsVectorMatch` reads back the raw vector via `algo_->getDataByInternalId(...)` and compares bytes. `UnTrackVector` is a no-op (HNSW never physically removes).

#### SVS prototype today (`src/indexes/vector_svs.cc:615-636`)

Same general pattern, but `IsVectorMatch` reads from `raw_vectors_` (not from the SVS graph) because we still can't read vectors back out of SVS. When `reconstruct()` exists (§3.5), this is rewritten to use it and `raw_vectors_` goes away.

#### Ask on new SVS runtime

Once `reconstruct()` is available (§3.5), these have no additional asks.

---

### 3.8 `ToProtoImpl()` / `RespondWithInfoImpl()`

Metadata-only methods, called on the main Redis thread for `FT.INFO` / RDB metadata serialization. No threading concerns. Current SVS implementation is correct (`src/indexes/vector_svs.cc:678-735`).

---

### 3.9 `SaveIndexImpl()` / `LoadFromRDB()`

#### Contract

Serialize the index to an output stream (RDB persistence); load an index from a stream.

#### HNSW today (`src/indexes/vector_hnsw.cc:237-241` for save, `:140-173` for load)

```cpp
absl::Status VectorHNSW<T>::SaveIndexImpl(RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&resize_mutex_);
  return algo_->SaveIndex(chunked_out);
}
```

hnswlib has a `SaveIndex` / `LoadIndex` pair that serializes the full graph and all vectors.

#### SVS prototype today (`src/indexes/vector_svs.cc:738-742`)

```cpp
absl::Status VectorSVS<T>::SaveIndexImpl(…) const {
  return absl::UnimplementedError(
      "SVS index RDB persistence is not yet implemented");
}
```

**Not implemented.** Every RDB save logs a warning and drops the SVS index.

The SVS runtime already has `virtual Status save(std::ostream& out) const noexcept` and a static `load(...)` (see SVS runtime header `svs/runtime/dynamic_vamana_index.h:68-75`). We just haven't wired them up to valkey-search's streaming RDB API. That's on us — but we'd like the SVS team to confirm the save/load round-trip is stable across runtime versions (i.e. a snapshot written by SVS 0.2 can be read by SVS 0.3).

#### Ask on new SVS runtime

- **Version-stable on-disk format.** A snapshot written by one SVS release should be readable by at least the next minor release. If this isn't guaranteed, expose a version tag in the output so valkey-search can refuse to load incompatible snapshots.
- **Concurrent-safe `save()`** under a reader lock — so an RDB snapshot can be taken without blocking all mutations.

#### Validation

Test pair **06** — save/load round-trip:
- [`svs_integration_tests/hnsw/test_06_save_load.cc`](./svs_integration_tests/hnsw/test_06_save_load.cc) — baseline passes 10/10 top-K match.
- [`svs_integration_tests/svs/test_06_save_load.cc`](./svs_integration_tests/svs/test_06_save_load.cc) — **FAILs on 0.2.0** with ~2/10 top-K match after reload, i.e. save and load both return OK but the reloaded index behaves differently. Surfacing this is the point.

---

### 3.10 Index build / `Create()`

#### Contract

Build a new empty index with the given parameters.

#### HNSW today (`src/indexes/vector_hnsw.cc:82-108`)

```cpp
index->algo_ = std::make_unique<hnswlib::HierarchicalNSW<T>>(
    index->space_.get(), initial_cap, M, ef_construction);
```

Synchronous, O(1).

#### SVS prototype today (`src/indexes/vector_svs.cc:182-188` for the OMP pin)

```cpp
// Configure OpenMP threads used by the SVS runtime.
#ifdef _OPENMP
  long long omp_threads = options::GetSVSOmpThreads().GetValue();
  if (omp_threads > 0) {
    omp_set_num_threads(static_cast<int>(omp_threads));
  }
#endif
```

We pin `omp_set_num_threads(1)` here, and again on every search (`src/indexes/vector_svs.cc:540-546`). This is a workaround for the oversubscription issue described in §3.1. It's brittle because `omp_set_num_threads` is a per-thread ICV — setting it during `Create()` doesn't propagate to reader-pool threads that later call `search()`.

#### Ask on new SVS runtime

- **Expose the thread count as an SVS API parameter**, not a libgomp ICV. Ideally something like `VamanaIndex::SearchParams::search_threads = 1` so valkey-search can set it per call without touching global state.
- **Or** provide a build variant of `libsvs_runtime` that doesn't link libgomp at all (purely single-threaded per-call search). valkey-search will multithread externally via its reader pool.

---

## 4. Ancillary asks

### 4.1 Replace `pending_buffer_` with first-class incremental `add()`

Currently: `src/indexes/vector_svs.h` defines `static constexpr size_t kBufferSize = 10000;` and `std::vector<PendingInsert> pending_buffer_`. Every 10 000th insert triggers a ~0.5-1 s flush inside an exclusive lock.

If per-vector `add()` is fast enough (§3.2), we delete all of this. If not, the secondary ask is an **async `add()`** that the SVS runtime completes on a background thread.

Companion tests: [`hnsw/test_05_incremental_add_latency.cc`](./svs_integration_tests/hnsw/test_05_incremental_add_latency.cc) + [`svs/test_05_incremental_add_latency.cc`](./svs_integration_tests/svs/test_05_incremental_add_latency.cc).

### 4.2 Replace `raw_vectors_` with `reconstruct()` + `compute_distance()`

Currently: `src/indexes/vector_svs.h` defines `absl::flat_hash_map<uint64_t, std::vector<char>> raw_vectors_`. Roughly doubles the memory footprint of an SVS index.

Once §3.5 and §3.6 are delivered, we delete `raw_vectors_`.

### 4.3 OMP thread management

Currently: we pin `omp_set_num_threads(1)` twice (once in `Create()`, once per-search) as a workaround for oversubscription.

See `SVS_OMP_PERF_ANALYSIS.md` for the profiling data that motivated this. The request is §3.10: make thread count an API parameter, not a libgomp ICV.

---

## 5. Summary of asks

| # | Ask | Motivation | Test pair |
|---|---|---|---|
| 1 | Concurrent-safe `search()` with no OMP oversubscription | §3.1 | 02 |
| 2 | Fast per-vector `add()` **or** async batch `add()` | §3.2 | 05 |
| 3 | Thread-safe concurrent `add()` | §3.2 | 03 |
| 4 | `add()` doesn't block concurrent searches | §3.1, §3.2 | 04 |
| 5 | `reconstruct(label, float* out)` API | §3.5 | 07 |
| 6 | `compute_distance(label, query, float* out)` API | §3.6 | 08 |
| 7 | Version-stable save/load format | §3.9 | 06 |
| 8 | Per-index thread count via SVS API (not libgomp ICV) | §3.10 | 02 |

Validation pattern: each scenario has an **`hnsw/test_0N_X.cc`** baseline (vendored upstream hnswlib) and an **`svs/test_0N_X.cc`** twin (against `libsvs_runtime.so`). Read them side by side. The SVS twin is expected to fail (perf, correctness, crash, or link error) on the current 0.2.0 runtime and pass on the new runtime — `svs_integration_tests/README.md` has the observed failure modes on 0.2.0.

---

## 6. How to use the tests

Each test under `svs_integration_tests/hnsw/` or `svs_integration_tests/svs/` has a header comment with copy-pasteable reproduction steps. The general workflow:

```bash
git clone https://github.com/izaakk/valkey-search.git
cd valkey-search && git checkout svs-integration-spec
cmake -S . -B .build-release -DCMAKE_BUILD_TYPE=Release -DENABLE_SVS=ON -G Ninja
ninja -C .build-release libsearch.so      # downloads libsvs_runtime.so via FetchContent
cd svs_integration_tests

# Build and run one pair (example: concurrent search):
./build_test.sh hnsw/test_02_concurrent_search && ./hnsw/test_02_concurrent_search
./build_test.sh svs/test_02_concurrent_search  && ./svs/test_02_concurrent_search

# Or build everything at once:
./build_test.sh all
```

Build details, the full test index, and observed failure modes on SVS 0.2.0: [`svs_integration_tests/README.md`](./svs_integration_tests/README.md).

---

## 7. Cross-references

- `SVS_OMP_PERF_ANALYSIS.md` — `perf record` data behind the OMP oversubscription story (§3.1, §3.10).
- `SVS_BENCHMARKING_METHODOLOGY.md` — end-to-end benchmark setup; useful context for the VectorDBBench numbers we quote elsewhere.
- `VALKEY_SEARCH_INTERNALS_FOR_SVS.md` — longer-form walk of valkey-search internals; overlaps in parts with §2 but from the SVS team's previous onboarding session.
