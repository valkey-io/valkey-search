# SVS Iteration 0: Learnings & Findings

## Executive Summary

We integrated Intel's SVS (Scalable Vector Search) Vamana graph index as a third vector backend in valkey-search, alongside HNSW and FLAT. The integration is **functionally complete**: `FT.CREATE`, `HSET`, `DEL`, `FT.SEARCH` with KNN all work correctly across L2, IP, and COSINE metrics, including LVQ compression and per-query `SEARCH_WINDOW_SIZE` tuning.

However, **SVS per-vector insertion is orders of magnitude slower than HNSW**, making it impossible to benchmark meaningful datasets (1K+ vectors) in the current architecture. This is the critical blocker for Iteration 1 and must be solved before any real performance comparison can be made.

---

## Critical Finding: Per-Vector Insertion Is Too Slow

### The Problem

SVS `DynamicVamanaIndex::add()` processes each vector insertion by rebuilding graph connections, which takes ~8 seconds per vector at 128 dimensions with `omp_set_num_threads(1)`. Even at 4 dimensions, insertion rate is only ~22 vec/s.

| Dimension | SVS Insert Rate | HNSW Insert Rate | Ratio |
|-----------|----------------|-------------------|-------|
| 4-dim | ~22 vec/s | ~25 vec/s | 0.88x |
| 128-dim | ~0.1 vec/s | ~5 vec/s* | 0.02x |

*HNSW 128-dim with ef_construction=200 is also slow on single thread, but SVS is 50x slower.

At 0.1 vec/s, inserting 10K vectors takes **28 hours**. This makes benchmarks with standard datasets (SIFT 10K, OpenAI 50K) completely impractical.

### Root Cause

1. **Single-threaded OMP**: We pin `omp_set_num_threads(1)` to prevent SVS from spawning uncontrolled thread pools. SVS's graph construction is designed to parallelize across OMP threads.

2. **Per-vector graph rebuild**: `DynamicVamanaIndex::add()` performs full neighbor selection and pruning for each inserted vector. Unlike HNSW (which uses a fast greedy insert), Vamana's pruning rule evaluates `alpha * dist(selected, candidate) < dist(node, candidate)` for all candidates — an O(degree²) operation per insert.

3. **No bulk insert API**: The SVS runtime's `add(n, labels, data)` accepts multiple vectors but processes them sequentially internally. There is no bulk-build-then-serve mode in the dynamic index.

### Why This Must Be Solved in Iteration 1

Without solving insertion performance, we cannot:
- Run VectorDBBench standard benchmarks (minimum 50K vectors)
- Compare recall-vs-latency tradeoffs at meaningful scale
- Evaluate LVQ compression benefits (compression shines at scale, not 50 vectors)
- Demo SVS to stakeholders with realistic workloads

### Proposed Solutions for Iteration 1

**Option A: Bulk Loading (Recommended)**
Build the SVS index from a batch of vectors rather than inserting one at a time:
1. Buffer incoming vectors in memory during a "loading" phase
2. Call `svs::runtime::v0::DynamicVamanaIndex::build()` with all vectors at once
3. Transition to per-vector `add()` only for incremental updates after initial load

This matches how SVS is designed to be used — bulk build then dynamic updates. The SVS runtime already supports this: `build()` accepts initial data.

**Option B: Multi-threaded OMP**
Allow SVS to use multiple OMP threads during insertion. Requires:
- Understanding how valkey-search's thread model interacts with OMP
- Ensuring OMP threads don't interfere with valkey-search's background workers
- Possibly using `omp_set_num_threads(N)` only during mutation time-slices

**Option C: SVS Runtime API Enhancement**
Request from SVS team:
- A batched `add_batch()` that amortizes graph construction
- A `consolidate()` call that defers pruning until explicitly triggered
- Better parallelism control (thread pool API instead of global OMP)

---

## SVS Runtime API Observations

### What We Used

| API | Purpose | Behavior |
|-----|---------|----------|
| `DynamicVamanaIndex::build()` | Create empty index | Fast (~3ms), returns opaque handle |
| `DynamicVamanaIndex::destroy()` | Free index | Returns Status (must check) |
| `svs_index->add(n, labels, data)` | Insert vectors | **Extremely slow** per vector |
| `svs_index->remove(n, labels)` | Delete vectors | Physical removal from graph |
| `svs_index->search(n, x, k, dist, labels, params, filter)` | KNN search | Fast, fills flat arrays |

### What Worked Well

- **Search performance**: SVS search at p50 was 2-4x faster than HNSW for the same recall (0.21ms vs 0.83ms at 50 vectors, 4-dim)
- **LVQ compression**: `StorageKind::LVQ4x8` worked out of the box — pass at build time, no training needed
- **IDFilter interface**: Clean virtual interface, easy to adapt from hnswlib's `BaseFilterFunctor`
- **Error reporting**: `Status` with error codes and messages — clear and consistent

### API Gaps

| Missing API | Why We Need It | Current Workaround |
|------------|----------------|-------------------|
| `reconstruct(id)` | Return stored vector for a given ID | `raw_vectors_` FP32 copy (doubles memory) |
| `compute_distance(id, query)` | Distance on compressed data | hnswlib `space_` on FP32 copy |
| `save/load to stream` | RDB persistence | Returns `UnimplementedError` |
| `get_size()` | Actual element count | Maintain `num_elements_` counter |
| `cancel_search()` | Cooperative cancellation | Post-search token check only |
| `bulk_add(data)` | Fast initial load | Per-vector `add()` (unusable at scale) |

### Alpha Constraint

SVS requires `alpha <= 1.0` for Inner Product / COSINE metrics. The default alpha 1.2 (good for L2) causes `add()` to fail with:
```
For MIP/Cosine distance, alpha must be <= 1.0
```
We clamp alpha to 1.0 automatically and log a notice. This should be documented in SVS API docs.

---

## Architecture Decisions & Why

### raw_vectors_ Temporary Storage

We maintain a full FP32 copy of every vector in `absl::flat_hash_map<uint64_t, std::vector<char>>`. This doubles memory usage but is necessary because SVS doesn't expose `reconstruct()` or `compute_distance()`.

**Used for:**
- `GetValueImpl()` — returns vector data for content resolution
- `ComputeDistanceFromRecordImpl()` — pre-filter distance computation
- `IsVectorMatch()` — verify vector hasn't changed during mutation

**Must be removed in Iteration 1** when SVS provides `reconstruct()` and `compute_distance()`.

### Exclusive Lock for Mutations

HNSW uses a reader lock for mutations (hnswlib handles concurrent access internally). SVS requires an exclusive `absl::MutexLock` because:
- `remove()` + `add()` are separate API calls (no atomic modify)
- SVS `DynamicVamanaIndex` thread safety for concurrent mutation is undocumented
- Reader lock for search + exclusive for mutations correctly serializes access

### OpenMP Pinned to 1 Thread

`omp_set_num_threads(1)` prevents SVS from spawning thread pools. This is a process-global setting that affects all OpenMP users. Called per `Create()` (redundant but harmless).

**Impact**: All SVS operations are single-threaded, which is the primary cause of slow insertion.

---

## Benchmark Results

### Setup
- Valkey server with valkey-search module (libsearch.so)
- SVS runtime v0.2.0 linked as shared library
- Single machine, single-threaded SVS operations
- OMP threads pinned to 1

### Results (50 vectors, 4-dim, L2, K=5)

| Metric | HNSW | SVS FP32 | SVS LVQ4X8 |
|--------|------|----------|------------|
| Index create | 4.0ms | 2.6ms | 29.7ms |
| Insert rate | 25 vec/s | 22 vec/s | 22 vec/s |
| Search p50 | 0.83ms | **0.21ms** | 0.49ms |
| Search mean | 0.82ms | **0.38ms** | 0.74ms |
| QPS (serial) | ~1,220 | **~2,630** | ~1,350 |
| Recall@5 | 1.0000 | 1.0000 | 1.0000 |

### Interpretation

- **SVS FP32 search is 2-4x faster than HNSW** at p50 — promising signal even at tiny scale
- **LVQ4X8 adds compression overhead** on 4-dim vectors (quantization doesn't help with 4 dimensions; it's designed for 128+ dim)
- **Perfect recall** — dataset is too small to exercise ANN approximation
- **Insert rates are similar** only because 50 vectors doesn't stress graph construction
- **Cannot draw production conclusions** from 50 vectors — need 10K+ for meaningful comparison

### What We Could NOT Benchmark

| Dataset | Size | Why Blocked |
|---------|------|-------------|
| SIFT-128 | 10K | SVS insert: ~28 hours |
| OpenAI-1536 | 50K | SVS insert: ~140 hours + OOM |
| GIST-960 | 100K | SVS insert: ~280 hours |

---

## Known Limitations of Iteration 0

1. **`raw_vectors_` doubles memory** — every vector stored twice (SVS graph + FP32 map)
2. **RDB persistence not implemented** — server restart loses all SVS index data
3. **Single-threaded SVS operations** — `omp_set_num_threads(1)` limits all parallelism
4. **No BFLOAT16 support** — only FLOAT32 template instantiation
5. **No cancellation during search** — SVS search runs to completion regardless of timeout
6. **No metrics counters** — SVS operations not visible in monitoring
7. **Per-vector insert is unusably slow** at scale (critical blocker)

---

## Recommendations for Iteration 1

### Priority 1: Solve Bulk Loading (Blocks Everything Else)

Implement a two-phase loading model:
1. **Staging phase**: Buffer vectors in memory (or temp file), track count
2. **Build phase**: When triggered (explicit command or threshold), call `DynamicVamanaIndex::build()` with all buffered vectors
3. **Live phase**: Per-vector `add()`/`remove()` for incremental updates

This is the architectural equivalent of what LeanVec staging already does — extend it to all compression types.

**Without this, no meaningful benchmarks are possible.**

### Priority 2: Replace raw_vectors_

Once SVS exposes `reconstruct()` and `compute_distance()`:
- Remove `raw_vectors_` map (saves ~50% memory per vector)
- Replace `GetValueImpl()` with `reconstruct()` (approximate, acceptable)
- Replace `ComputeDistanceFromRecordImpl()` with `compute_distance()` on compressed data

### Priority 3: RDB Persistence

Implement `SaveIndexImpl()` and `LoadFromRDB()` using SVS save/load API (once available). Without this, any server restart or RDB snapshot loses all SVS index data.

### Priority 4: Multi-threaded SVS

Allow controlled parallelism for SVS operations:
- Multiple OMP threads during mutation time-slice
- Thread pool size configurable via FT.CREATE parameter
- Ensure compatibility with valkey-search's thread model

### Priority 5: Proper Benchmarking

Once bulk loading is solved:
- Run VectorDBBench with SIFT-128 10K (L2, with ground truth)
- Run with OpenAI-1536 50K (COSINE, standard benchmark)
- Compare recall-vs-latency curves at different search_window_size values
- Measure memory footprint: FP32 vs LVQ4x8 vs LVQ8x0

---

## Files Modified in Iteration 0

| File | Lines Changed | Purpose |
|------|--------------|---------|
| `src/index_schema.proto` | +25 | SVS protobuf schema |
| `src/indexes/index_base.h` | +8 | kSVS enum, IsVectorIndexType() |
| `src/indexes/vector_base.h` | +3 | SVS in kVectorAlgoByStr |
| `src/indexes/vector_svs.h` | +127 | **NEW** — VectorSVS class |
| `src/indexes/vector_svs.cc` | +500 | **NEW** — Full implementation |
| `src/indexes/CMakeLists.txt` | +30 | SVS build target |
| `src/CMakeLists.txt` | +4 | Link vector_svs |
| `src/index_schema.cc` | +25 | Factory, type checks |
| `src/commands/ft_create_parser.h` | +15 | SVSParameters |
| `src/commands/ft_create_parser.cc` | +80 | SVS parser |
| `src/commands/ft_search_parser.cc` | +16 | SWS validation |
| `src/query/search.h` | +3 | search_window_size field |
| `src/query/search.cc` | +30 | SVS dispatch, SWS parsing |
| `src/query/planner.cc` | +2 | kSVS pre-filter |
| `src/query/CMakeLists.txt` | +3 | Link vector_svs to search |
| `benchmark_svs.py` | +242 | **NEW** — Benchmark script |

**Total: ~1,100 lines of new C++ code, 2 new files, 15 existing files modified**
