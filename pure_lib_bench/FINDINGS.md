# Pure-library memory comparison: findings

## Setup

Two standalone C++ binaries — no Valkey, no module, no intern store, no
`raw_vectors_`, no keyspace notifications — each link against only the
library under test and index N random FP32 vectors.

- `bench_hnsw.cc` — upstream hnswlib (nmslib/hnswlib). The valkey-search fork
  differs only by storing pointers vs. memcpy; total process RSS is
  identical when no external backing store exists.
- `bench_svs.cc` — `libsvs_runtime.so` (Intel SVS) built from
  `ScalableVectorSearch/`, same library valkey-search uses.

## Headline numbers (100K × 768-d FP32, stage 4 = input buffer freed)

| Config | VmRSS | Justified | Amplification |
|---|---|---|---|
| **hnswlib** | **335 MiB** | ~318 MiB (vectors + graph) | **1.05×** |
| **libsvs FP32** | **846 MiB** | ~318 MiB | **2.66×** |
| **libsvs LVQ4x8** | **665 MiB** | ~135 MiB (codes + graph) | **4.93×** |
| **libsvs LVQ4x8, block_exp=22** | **665 MiB** | ~135 MiB | **4.93×** |

**Conclusion: the amplification is entirely inside libsvs.** hnswlib is
honest (1.05×). libsvs holds 2.7-4.9× more than its stored graph contents
justify, with nothing else in the process.

## Decomposition of the 846 MiB (libsvs FP32)

smaps at stage 4 shows two dominant anon-mmap regions:

```
region                 virt_MiB   rss_MiB
(anon)                    528.0     528.0    ← unaccounted
(anon)                    768.0     293.0    ← FP32 vector storage
```

The 293 MiB region is the expected vector storage
(100K × 768 × 4 B = 293 MiB). The 528 MiB region is the anomaly.

## Root cause: graph allocator's `prevpow2` minimum block size

Instrumented with an `LD_PRELOAD` mmap tracer (`mmap_trace.c`). Stack trace
of the first 528 MiB allocation resolves (via `addr2line`) to:

```
#0 svs::DenseArray<uint32, (size_t,size_t), HugepageAllocator<uint32>>::DenseArray
#1 svs::graphs::SimpleGraphBase<uint32, SimpleData<uint32, -1, Blocked<HugepageAllocator<uint32>>>>::SimpleGraphBase(num_nodes, max_degree)
#2 svs::index::vamana::MutableVamanaIndex<SimpleBlockedGraph<uint32>, ...>::MutableVamanaIndex
#3 svs::runtime::storage::dispatch_storage_kind<DynamicVamanaIndexImpl::init_impl>
#4 svs::runtime::DynamicVamanaIndexImpl::init_impl
#5 DynamicVamanaIndexManagerBase::add
#6 main → DynamicVamanaIndex::add
```

The graph's `BlockedData` sizing in
`ScalableVectorSearch/include/svs/core/data/simple.h:619-622`:

```cpp
SimpleData(size_t n_elements, size_t n_dimensions, const Blocked<Alloc>& alloc)
    : blocksize_{lib::prevpow2(
          alloc.parameters().blocksize_bytes.value() / (sizeof(T) * n_dimensions)
      )}
```

With default `blocksize_bytes.value() = 1 GiB = 2^30`:
- `sizeof(uint32) × (graph_max_degree + 1) = 4 × 33 = 132 B` per vertex row
- `2^30 / 132 = 8,134,519` vertices per block
- `prevpow2(8,134,519) = 4,194,304 = 2^22` vertices per block (rounded **down**)
- **One block = 4,194,304 × 132 = 528 MiB**

For any N ≤ 4,194,304 vertices, `div_round_up(N, 4,194,304) = 1` block
allocated. Because the graph allocator uses `MAP_POPULATE`
(`allocator.h:135`), the entire 528 MiB is pre-faulted and counts fully
toward RSS from the first `add()`.

Observed sizes vs N (from `traces/tr_*.log`):

| N | first graph mmap |
|---|---|
| 1,000 | 528 MiB |
| 5,000 | 528 MiB |
| 10,000 | 528 MiB |
| 50,000 | 528 MiB |
| 100,000 | 528 MiB |

**Graph size is invariant to N up to 4.2M vertices.** A user indexing 10K
vectors pays the same 528 MiB graph cost as a user indexing 4M.

## The second 528 MiB allocation

`MutableVamanaIndex` constructor
(`ScalableVectorSearch/include/svs/index/vamana/dynamic_index.h:216, 231`):

```cpp
: graph_(Graph{data.size(), parameters.graph_max_degree})          // line 216 — first
...
verify_and_set_default_index_parameters(build_parameters_, ...);
graph_ = Graph{data_.size(), build_parameters_.graph_max_degree};  // line 231 — second
```

The constructor allocates the graph, then after
`verify_and_set_default_index_parameters` (which may or may not have changed
`graph_max_degree`) reallocates a fresh graph, destroying the first.

The old graph's memory is released (second `MUNMAP` in traces), but during
the construction window both are live. Not a permanent cost, but at 1 GiB
transient peak it's visible in `VmHWM`.

## Why `blocksize_exp` doesn't help

The `DynamicIndexParams::blocksize_exp` API parameter (exposed via
`svs::runtime::v0::VamanaIndex::DynamicIndexParams`) only controls the
**vector storage** allocator. The graph is independently configured and
uses the default `BlockingParameters::default_blocksize_bytes{30}` with
no API override.

Trace evidence with `BLOCKSIZE_EXP=22` (4 MiB):

```
MMAP 0x... size=528 MiB prot=0x3 flags=0x8022    ← graph (still 1 GiB blocks)
MMAP 0x... size=528 MiB prot=0x3 flags=0x8022    ← graph realloc
```

Same 528 MiB allocations. The user's `blocksize_exp=22` request was
silently ignored for the graph, which is the single biggest memory
contributor.

## Why LVQ4x8 doesn't fix it

LVQ4x8 compresses the **vector storage** from 293 MiB (FP32) to ~110 MiB.
But the **graph** (528 MiB) is unaffected — it's integer adjacency lists,
not vector data. So for 100K × 768-d:

- FP32: 528 (graph) + 293 (vectors) = 821 MiB, measured 846 MiB
- LVQ4x8: 528 (graph) + 110 (codes) + extras = 665 MiB

LVQ compresses the smaller of the two components. At 100K scale, the graph
over-allocation dominates the compression savings.

## Scaling math: where the crossover is

The 528 MiB graph over-allocation is a fixed cost *up to 4.2M vertices*.
At scales below that, it dominates; at scales above it, per-vertex cost
takes over.

| N (vectors) | Graph RSS (libsvs) | Graph RSS (hnswlib) | Ratio |
|---|---|---|---|
| 10K | 528 MiB | ~2 MiB | 264× |
| 100K | 528 MiB | ~25 MiB | 21× |
| 1M | ~528 MiB (still one block) | ~250 MiB | 2.1× |
| 4.2M | 528 MiB (one block, fully used) | ~1 GiB | 0.5× |
| 10M | ~2 × 528 MiB = ~1.06 GiB (two blocks) | ~2.5 GiB | 0.4× |

**The amplification is dataset-size dependent.** At 10K vectors it's
catastrophic (264×). At 10M it's essentially zero. OpenSearch's 33%
savings claim is probably at scales where the fixed overhead has been
amortized.

## Recommendations for the SVS team

1. **Fix `prevpow2` → `ceil` in `SimpleData` block sizing** (simple.h:620).
   `prevpow2` is only there for cheap integer division (`resolve(i)` uses
   `i / blocksize_` and `i % blocksize_`). A power-of-2 blocksize is
   convenient but costs up to 2× memory. For small datasets, a right-sized
   first block matters more than cheap division.

2. **Plumb `dynamic_index_params_.blocksize_exp` through to the graph
   allocator.** Currently it's silently ignored; user-supplied values only
   affect vector storage.

3. **Eliminate the double-allocation in `MutableVamanaIndex` constructor**
   (dynamic_index.h:216 then 231). Call
   `verify_and_set_default_index_parameters` **before** the first graph
   construction, not after.

4. **Consider `MAP_POPULATE` carefully.** Using it for the graph guarantees
   worst-case RSS from first add. Dropping it would let unused pages stay
   unmapped until actually touched during graph construction. In exchange,
   on-demand paging during search may have minor latency impact on cold
   pages — worth measuring.

5. **Expose an `initial_capacity` or `expected_size` build parameter**
   so callers who know their dataset size can avoid over-allocation.

## What this means for valkey-search memory

The amplification at the scales customers use (100K-10M) is largely
**not valkey-search's problem to fix.** Ranking the contributors to the
valkey-search VDB-order 100K × 768-d LVQ4x8 measurement of 1,698 MiB:

| Layer | MiB | Who owns it |
|---|---|---|
| Valkey core hashes | 320 | Valkey core |
| Intern store (FP32) | 293 | valkey-search (hnswlib fork requires stable ptrs) |
| `raw_vectors_` shadow (FP32) | 293 | valkey-search (SVS lacks reconstruct/compute_distance) |
| **libsvs graph over-allocation** | **~528** | **libsvs (`prevpow2` bug)** |
| libsvs vector storage + misc | ~137 | libsvs |
| Fragmentation | 127 | glibc (workflow-dependent) |
| **Total** | **~1,698** | |

Biggest single item is the 528 MiB graph over-allocation, larger than the
two FP32 shadows combined. Fixing it in libsvs removes more memory than
every change we can make on the valkey-search side combined.

## Artifacts

- `bench_hnsw.cc`, `bench_svs.cc` — source for the pure-library benchmarks
- `common.h` — snapshot helper (in-process smaps capture)
- `mmap_trace.c` — `LD_PRELOAD` tracer that captured the stack traces
- `build.sh`, `run.sh` — compile and run helpers
- `traces/tr_*.log` — raw mmap trace logs at N=1K, 10K, 50K, and
  block_exp=22 variant
- `/tmp/pure_lib_bench/<N>/<config>/stage*_*/smaps.txt` — smaps per stage
