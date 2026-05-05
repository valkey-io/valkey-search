# SVS Integration Specification

**Audience:** Intel SVS runtime engineers
**Author:** valkey-search team (`izaakk/valkey-search`, branch `svs-integration-spec`)
**Scope:** How valkey-search integrates a vector index today, how the current SVS prototype differs from HNSW, and what we need from the next SVS runtime to reach parity.

---

## 1. Why this document exists

valkey-search uses a vector index through an abstract C++ class (`VectorBase`). Two concrete implementations exist today: `VectorHNSW` (wraps hnswlib) and `VectorSVS` (wraps the SVS runtime at `libsvs_runtime.so.0.2.0`).

The HNSW implementation hits valkey-search's threading requirements naturally: readers run concurrently, writers run concurrently, and the only serialization point is a rare index resize. hnswlib does the locking itself with per-label mutexes, so valkey-search's wrapper can use a shared reader lock for everything except resize.

The SVS prototype cannot match this yet. Our wrapper takes an **exclusive mutex** on every mutation, buffers inserts in a 10 K-entry queue (`pending_buffer_`), and flushes the buffer inside the same lock — blocking searches for 0.5-1 s on each flush. It also keeps a full FP32 shadow copy of every vector (`raw_vectors_`) because the SVS runtime does not expose the retrieval/distance APIs we need. These workarounds exist because the current SVS runtime API and threading model don't support the access pattern valkey-search needs.

This document walks through each `VectorBase` virtual method, shows how hnswlib supports it today, shows the SVS workaround, and states the concrete ask on the SVS runtime. Every code claim is cited with a file path and line range in `izaakk/valkey-search` (branch `svs-integration-spec`). Each method section ends with a pointer to a **pair** of standalone C++ tests in [`svs_integration_tests/`](./svs_integration_tests/): an `hnsw/test_0N_*.cc` baseline that runs against vendored upstream hnswlib, and a direct twin `svs/test_0N_*.cc` that runs the same scenario against `libsvs_runtime.so`. Read them side by side.

---

## 2. valkey-search threading model (1-page primer)

valkey-search runs three worker thread pools, created at module startup in `src/valkey_search.cc:1236-1240` (sizes default to the physical CPU count):

```
reader_thread_pool_  ← FT.SEARCH commands dispatch here
writer_thread_pool_  ← HSET / HDEL / FT.CREATE / FT.DROPINDEX dispatch here
utility_thread_pool_ ← background cleanup
```

Each `VectorBase` instance owns a single `absl::Mutex` (the "resize mutex" in HNSW, the "index mutex" in SVS). All method calls to the index take either a **shared** (reader) lock or an **exclusive** (writer) lock on this mutex.

**What we require from the underlying index:**

| Method called from | Lock valkey-search holds | Expected to be |
|---|---|---|
| `Search()` (reader pool) | shared | safely concurrent with other shared-lock calls |
| `AddRecordImpl()` (writer pool) | shared (HNSW) / exclusive (SVS today) | safely concurrent with searches |
| `RemoveRecordImpl()` (writer pool) | shared (HNSW) / exclusive (SVS today) | safely concurrent with searches |
| `ModifyRecordImpl()` (writer pool) | shared (HNSW) / exclusive (SVS today) | safely concurrent with searches |

**HNSW passes this contract**: under a shared lock, hnswlib's `searchKnn` / `addPoint` / `markDelete` are safe because hnswlib internally locks per-label (`src/indexes/vector_hnsw.cc:117-133`). Only `resizeIndex` needs an exclusive lock — hence the name `resize_mutex_`. See `src/indexes/vector_hnsw.cc:244-276`.

**SVS fails this contract today**: the prototype takes exclusive locks everywhere because the SVS runtime doesn't guarantee safety under concurrent mutation. This is the root cause the SVS team is being asked to address.

---

## 3. Per-method specification

Every section below has five parts:

1. **Contract** — what the method must do
2. **HNSW today** — code snippet and line range from `src/indexes/vector_hnsw.cc`
3. **SVS prototype today** — code snippet and line range from `src/indexes/vector_svs.cc`
4. **Ask on new SVS runtime** — concrete bullets
5. **Validation** — path to a reproducible test

### 3.1 `Search()`

#### Contract

```cpp
absl::StatusOr<std::vector<Neighbor>> Search(
    absl::string_view query, uint64_t count,
    cancel::Token& cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
    /* impl-specific runtime tuning parameter */
);
```

Find the top-`count` nearest neighbors to `query`. Called on a **reader-pool thread**. Many calls run concurrently. valkey-search expects the index to be safely re-entrant under a shared lock.

#### HNSW today (`src/indexes/vector_hnsw.cc:320-361`)

```cpp
absl::StatusOr<std::vector<Neighbor>> VectorHNSW<T>::Search(…) {
  auto perform_search = [&](absl::string_view query) { … 
    CancelCondition cancel_condition(cancellation_token);
    auto res = algo_->searchKnn((T *)query.data(), count, ef_runtime,
                                filter.get(), &cancel_condition);
    …
  };
  …
}
```

No lock is taken here — `algo_->searchKnn` is read-only and safe for concurrent calls because hnswlib's graph traversal is immutable during search. The reader lock that *does* wrap this call is held at the `VectorBase` level (not shown).

#### SVS prototype today (`src/indexes/vector_svs.cc:459-610`)

```cpp
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(…) {
  // 1. Check if buffer needs flushing; possibly upgrade to exclusive lock.
  bool needs_flush = false;
  {
    absl::ReaderMutexLock check_lock(&index_mutex_);
    needs_flush = !pending_buffer_.empty() && !buffer_flushing_;
  }
  if (needs_flush) {
    absl::MutexLock flush_lock(&index_mutex_);       // ← EXCLUSIVE lock
    if (!pending_buffer_.empty()) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());          // ← 0.5-1 s stall
    }
  }
  absl::ReaderMutexLock lock(&index_mutex_);

#ifdef _OPENMP
  long long omp_threads = options::GetSVSOmpThreads().GetValue();
  if (omp_threads > 0) omp_set_num_threads((int)omp_threads);   // pin to 1
#endif

  auto status = svs_index_->search(1, query_data, k,
                                   distances.data(), labels.data(),
                                   params_ptr, svs_filter.get());
  …
}
```

The SVS search path is much heavier than HNSW's:

- It may upgrade to an **exclusive** lock and run `FlushBuffer()` before searching, blocking every concurrent reader for the flush duration.
- It re-applies `omp_set_num_threads(1)` on every search (see `src/indexes/vector_svs.cc:540-546`) because SVS's internal `GOMP_parallel` would otherwise oversubscribe the host's threads. Profiling showed OMP=8 raises kernel-space CPU from 2.7 % → 17.6 %, mostly in `native_queued_spin_lock_slowpath` (see `SVS_OMP_PERF_ANALYSIS.md`).
- `svs_index_->search(…)` is already `const noexcept` in the SVS runtime (`include/svs/runtime/vamana_index.h:50-58`), i.e. nominally thread-safe, but it internally dispatches through `GOMP_parallel` for every call — a per-query fork/join even when no parallelism is wanted.

#### Ask on new SVS runtime

- Make `search()` safe for **many concurrent callers** with no internal oversubscription of OS threads. If parallelism is desired inside a single search, control it via a per-call parameter or a per-index configuration, not a global OMP setting.
- Provide a build option **or** a runtime switch to run `search()` without dispatching through `GOMP_parallel` at all (serial path). Rationale: when valkey-search has many reader threads each issuing a search, intra-search parallelism is the *wrong* axis — it steals cores from sibling searches.
- Make per-call thread behavior deterministic (no "fork a team of size N only if N>1" — we want the same code path for N=1 and N>N).

#### Validation

Test pair **02** — concurrent search scaling:
- [`svs_integration_tests/hnsw/test_02_concurrent_search.cc`](./svs_integration_tests/hnsw/test_02_concurrent_search.cc) — baseline. On 8 vCPU: ~14k qps at N=1, ~71k qps at N=8 (scale 5.1×).
- [`svs_integration_tests/svs/test_02_concurrent_search.cc`](./svs_integration_tests/svs/test_02_concurrent_search.cc) — same scenario on SVS, run twice: once with `omp_set_num_threads(1)` and once with `4`. At OMP=1, scaling matches HNSW (~5×). At OMP=4, single-thread QPS collapses to ~3k — exactly the oversubscription the spec describes.

Test pair **04** — search during add:
- [`svs_integration_tests/hnsw/test_04_search_during_add.cc`](./svs_integration_tests/hnsw/test_04_search_during_add.cc) — baseline. Reader p99 stays within 10% when a writer runs.
- [`svs_integration_tests/svs/test_04_search_during_add.cc`](./svs_integration_tests/svs/test_04_search_during_add.cc) — same scenario on SVS.

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

- **Shared lock**: concurrent `AddRecordImpl` calls don't serialize on each other. hnswlib internally per-label locks (`label_lookup_lock` / `link_list_locks_`) to protect the graph structure.
- **Exclusive lock only on resize**: if `addPoint` throws "exceeds limit", the wrapper calls `ResizeIfFull()` which grabs a writer lock, doubles capacity, and retries. This happens O(log N) times per index lifetime.

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

1. `svs_index_->add(1, &label, data)` is prohibitively slow per call (≈ 0.1 vec/s at 128-dim in our original measurements) because the SVS runtime treats every `add()` as a batch and spins up GOMP threads. So we batch 10 000 vectors ourselves and call `add(10000, …)` once per buffer-fill.
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

Return a pointer to the stored raw vector bytes. Called during pre-filter evaluation, `DUMP`-style reads, and some cancellation paths. Returning the original FP32 bytes is fine; an approximate reconstruction from compression is also acceptable (the caller uses this for tie-breaking and display, not for correctness-critical distance computations).

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

With `reconstruct` available, valkey-search can delete `raw_vectors_` entirely — halving the memory footprint of an SVS index at 1M+ scale.

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

The SVS runtime already has `virtual Status save(std::ostream& out) const noexcept` and a static `load(...)` (see `include/svs/runtime/dynamic_vamana_index.h:68-75`). We just haven't wired them up to valkey-search's streaming RDB API. That's on us — but we'd like the SVS team to confirm the save/load round-trip is stable across runtime versions (i.e. a snapshot written by SVS 0.2 can be read by SVS 0.3).

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
