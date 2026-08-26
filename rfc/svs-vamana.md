---
RFC: (PR number)
Status: Proposed
---

# Scalable Vector Search (SVS) Integration for valkey-search

## Abstract

This RFC proposes integrating [Intel Scalable Vector Search (SVS)](https://github.com/intel/ScalableVectorSearch) into valkey-search as a new `ALGORITHM SVS_VAMANA` option alongside the existing HNSW and FLAT algorithms. SVS_VAMANA uses the DynamicVamana graph-based index for high-performance approximate nearest neighbor (ANN) search optimized for x86_64 platforms, with multiple compression backends (FP16, SQ8, LVQ 4/8-bit, LeanVec dimensionality reduction).

The RFC specifies the linking architecture using a **static submodule integration** model: SVS is compiled directly into `libsearch.so` as a git submodule (Apache-2.0), supporting FP32, FP16, and SQ8 compression. LVQ and LeanVec proprietary compression backends are out of scope for this RFC; distribution options for proprietary compression are captured in the Future Considerations section as a follow-on discussion item.

## Motivation

The valkey-search module currently provides two vector indexing algorithms: FLAT (brute-force exact search) and HNSW (Hierarchical Navigable Small World graph). While HNSW is effective for many workloads, there are scenarios where alternative graph-based algorithms offer better trade-offs:

1. **Memory efficiency at scale.** Large-scale vector datasets (millions to billions of vectors) benefit from advanced compression techniques. Intel SVS provides Locally-adaptive Vector Quantization (LVQ) and LeanVec dimensionality reduction, enabling significant memory reduction at tunable accuracy trade-offs. These backends outperform the scalar quantization approaches available in HNSW implementations; exact compression ratios depend on the backend chosen and the acceptable recall degradation.

2. **x86_64 hardware optimization.** SVS is purpose-built for Intel platforms, leveraging AVX-512 and AVX2 instruction sets for vectorized distance computations and graph traversal. Deployments running on Intel hardware can achieve higher throughput compared to platform-agnostic implementations.

3. **Cold-start problem.** Compressed indexes traditionally require a minimum dataset size for training (e.g., learning quantization codebooks or projection matrices). SVS's deferred compression model starts the index uncompressed and searchable immediately, then transparently transitions to the compressed backend once sufficient vectors are present. This eliminates the need for a separate "training" phase where the index is unavailable for queries.

4. **Algorithm diversity.** DynamicVamana uses a single-level graph with alpha-pruning as an alternative to HNSW's multi-layer graph. The memory efficiency advantage of SVS_VAMANA comes from its compression backends (LVQ, LeanVec) rather than from the graph algorithm itself -- HNSW's additional layers do not contribute significantly to memory overhead. Providing both algorithms gives operators access to Intel's hardware-optimized compression pipeline alongside the well-established HNSW implementation.

## Design Considerations

### Comparison with Existing Algorithms

| Property | FLAT | HNSW | SVS_VAMANA |
|----------|------|------|------------|
| Search complexity | O(n) exact | O(log n) approximate | O(log n) approximate |
| Graph structure | None | Multi-layer skip-list graph | Single-level Vamana graph with alpha-pruning |
| Compression | None | None | FP16, SQ8, LVQ (4/8-bit), LeanVec |
| Platform | Any | Any | x86_64 Linux (primary); ARM64: no planned Intel SVS optimization; submodule approach removes hard x86 binary constraint |
| Dynamic updates | Supported | Supported | In progress (thread-safe add/remove) |
| Memory overhead | Vectors only | Vectors + graph | Vectors + graph; LVQ/LeanVec compression backends reduce vector storage |

### Deployment Architecture and Linking Model

#### Prior Model: Pre-Built Runtime `.so`

```text
valkey-server
  \-- MODULE LOAD libsearch.so (73 MB, built by valkey-search)
         +-- statically links: gRPC, Abseil, Protobuf, hnswlib, ICU, snowball, ...
         \-- dynamically links: libsvs_runtime.so.0.4.0 (43 MB, pre-built by Intel)
```

The SVS nightly tarball ships a pre-compiled runtime with all algorithms (FP32, FP16, SQ8, LVQ, LeanVec) baked into a single shared library, exposing 55 C++ mangled symbols via vtable interface.

**Limitations of the prior model:**
- All-or-nothing: cannot ship open-source-only `libsearch.so` that later gains LVQ
- C++ vtable ABI is fragile across compiler versions
- Memory accounting required explicit contracts -- `malloc` interposition cannot intercept SVS allocations because the runtime has its own PLT entries; `get_memory_usage()` polling was required

#### Target Model: Static Submodule (Recommended)

```text
valkey-server
  \-- MODULE LOAD libsearch.so
         \-- statically compiled from source:
               +-- hnswlib, highwayhash, simsimd, ...
               \-- third_party/svs/ (Apache-2.0 git submodule)
                     \-- supports: FP32, FP16, SQ8
```

SVS is added as a git submodule under `third_party/svs/` and compiled as an object library into `libsearch.so` during the standard `cmake --build`. No pre-built artifact is needed. The resulting `libsearch.so` is fully self-contained and supports FP32, FP16, and SQ8 compression natively.

**Advantages over the prior pre-built runtime model:**

- **Automatic memory tracking:** All SVS C and C++ allocations route through `vmsdk::__wrap_malloc` and the global `operator new` overloads that valkey-search already maintains. These wrappers intercept at link time -- no wrapper boilerplate, no custom allocator interface. SVS memory is immediately visible in `used_memory` and `FT.INFO`.
- **Removes x86 binary constraint:** The pre-built runtime binary was x86-only by definition. Statically linking SVS from source removes this hard constraint -- ARM64 compatibility becomes architecturally possible via valkey-search's existing `simsimd` layer without requiring ARM-specific optimization work from the Intel.
- **Single self-contained binary:** `libsearch.so` distributes cleanly with no `LD_LIBRARY_PATH` dependencies or runtime `.so` discovery.
- **Hermetic symbol isolation:** Statically linked SVS symbols remain hidden via `-Wl,--version-script`, eliminating any risk of symbol collision with other Valkey modules.
- **Unified concurrency:** SVS indexing and search routines are scheduled on valkey-search's existing thread pools (see Thread Ownership section).
- **Full compiler optimization:** Enables full Link-Time Optimization (LTO), SIMD vectorization, and inlining across the vector search pipeline.

**Build/packaging:**
- SVS added as `git submodule` under `third_party/svs/` (same pattern as hnswlib)
- `cmake --build` compiles SVS sources alongside valkey-search; no pre-built artifact needed
- SVS submodule is built automatically on x86_64 Linux with no separate compile flag required
- LVQ/LeanVec proprietary compression: see Future Considerations section

#### Alternative Approaches Considered

| Dimension | Prior (Runtime .so) | Static Submodule (recommended) | Dynamic C API .so | SharedAPI Hot-plug |
|-----------|---------------------|-------------------------------|-------------------|---------------------|
| Deployment files | 2 | 1 | 2 | 2-3 |
| Operator complexity | Low | Low | Low | Medium |
| Hot-pluggable compression | No | No (module reload) | Yes (.so swap) | Yes |
| ABI stability | C++ vtable (fragile) | N/A (built together, same toolchain) | C stable (pure C ABI) | C stable |
| Memory accounting | `get_memory_usage()` polling | Automatic via `__wrap_malloc` | `get_memory_usage()` polling | Via C API + SharedAPI |
| LTO / inlining | None | Full (within single build) | None (.so boundary) | None (.so boundary) |
| Proprietary backend independence | None | Requires matching toolchain (see Future Considerations) | Intel builds independently | Intel builds independently |
| Hardware-specific variants | No | Requires full rebuild | Yes (.so swap) | Partial |
| ARM64 support | None (x86 binary) | Via simsimd | Independent build | Depends on build |
| Single binary | No | Yes | No | No |
| Existing precedent | Current model | hnswlib, highwayhash in valkey-search | SVS C API (PR #363) | JSON integration in valkey-search |

**Note -- static submodule and proprietary C++ ABI:** The ABI stability advantage of static linking applies specifically to the open-source build, where all SVS C++ objects are compiled from source in the same toolchain (gcc-13, Ubuntu 24.04, C++20). This guarantee does **not** extend to pre-built proprietary C++ objects added via the tarball path (Alternative A); those risks are documented in Future Considerations -- Alternative A.

**Why not SharedAPI hot-plug?** Significant engineering complexity: reference counting for in-use compression types, SharedAPI discovery and invalidation on module unload, and memory accounting limitations at `.so` PLT boundaries.

**LVQ/LeanVec distribution options** are a follow-on discussion; four approaches (valkey-bundle build flag, dynamic C API .so, separate image, standalone user build) are evaluated in the Future Considerations section.

### Deferred Compression

Deferred compression applies **only to LeanVec** compression types. For all other types the index is ready immediately:

| Compression | Activation | `state` in FT.INFO |
|-------------|-----------|---------------------|
| NONE | Immediate -- index ready at creation | Always `ready` |
| FP16 | Immediate -- index built with FP16 storage at creation | Always `ready` |
| SQ8 | Immediate -- index built with SQ8 storage at creation | Always `ready` |
| LVQ4/8/4X4/4X8 | Immediate -- index built with LVQ storage at creation (proprietary) | Always `ready` |
| LEANVEC* | Deferred -- accumulates vectors until `LEANVEC_TRAINING_THRESHOLD`, then trains projection and builds compressed index | `training` below threshold; `ready` after |

For LeanVec types, SVS requires a minimum corpus to train the projection matrices. Until `LEANVEC_TRAINING_THRESHOLD` vectors have been buffered, the index is in `state: training`, search returns an error, and modifications are queued. Once the threshold is crossed, valkey-search trains the LeanVec matrices on the buffered corpus, builds the compressed index, and transitions to `state: ready`.

The graph structure, ID translator, and entry point are preserved across the transition -- only the data storage layer changes.

#### Transition Mechanics

The compression transition uses **copy semantics** to avoid blocking concurrent searches:

1. Threshold crossed -- valkey-search detects the live vector count reaches `LEANVEC_TRAINING_THRESHOLD`.
2. Capability check -- valkey-search verifies the target compression type is built in:
   - FP16/SQ8: always available in the open-source submodule build (this RFC)
   - LVQ/LeanVec: require proprietary backends outside the scope of this RFC; `FT.CREATE` with these types returns an error unless a future proprietary build is loaded
3. Clone with new storage -- a compressed index is built from a snapshot of the source index. Mutations that complete on the source index after the snapshot is taken are journaled. Searches continue against the original uncompressed index during this phase.
4. Pre-lock catch-up -- without holding the exclusive lock, valkey-search drains the bulk of the journal into the compressed index. New mutations arriving during this phase continue to be journaled. This step repeats until the remaining journal tail is small enough that replaying it under the lock will complete within the latency bound.
5. Reconcile and swap -- valkey-search acquires the exclusive index lock, replays the bounded remaining journal tail into the compressed index, atomically swaps the index pointer, and releases the lock. The old uncompressed storage is freed.
6. Memory accounting update -- the freed memory is reflected in `FT.INFO` and per-index byte counters.

**Hard constraint:** Searches must never block for more than ~10ms during the transition. The pre-lock catch-up phase (step 4) ensures the journal tail replayed under the lock in step 5 is small enough to satisfy this bound. 2x peak memory during the overlap window is acceptable.

**Fallback behavior:** If the target compression is unavailable at `FT.CREATE` time (LVQ/LeanVec requested), the command returns an error immediately. Deferred compression transitions within the open-source build target FP16 or SQ8 only.

### Platform Requirements

- **x86_64 Linux** (current): submodule build. Optimal with AVX-512; functional with AVX2 at reduced throughput. All open-source compression backends available.
- **ARM64**: Intel SVS does not have planned ARM-specific optimization work. The submodule approach removes the hard x86 constraint of the prior pre-built binary -- ARM64 compatibility would depend on valkey-search's existing `simsimd` layer (NEON/SVE/DotProd) rather than SVS-specific ARM work, which is an improvement over the prior model where the pre-built binary was x86-only.

SVS is compiled into valkey-search unconditionally on x86_64 Linux. No separate compile flag is required.

### Comparison with Vector Search in Other Systems

| System | Vamana/DiskANN Support | Compression | Platform-Specific Optimizations |
|--------|----------------------|-------------|-------------------------------|
| RediSearch (>=2.8.10) | Yes (SVS_VAMANA) | LVQ + LeanVec | x86_64 (Intel optimized) |
| Milvus | DiskANN (Vamana family) | Scalar/Product quantization | Limited |
| Qdrant | No (HNSW only) | Scalar/Product quantization | No |
| Weaviate | No (HNSW only) | Product quantization | No |
| **valkey-search + SVS** | **Yes (SVS_VAMANA)** | **FP16, SQ8 (open-source); LVQ + LeanVec (future -- see Future Considerations)** | **x86_64 AVX-512/AVX2** |

## Specification

### FT.CREATE with ALGORITHM SVS_VAMANA

The `SVS_VAMANA` algorithm is selected via the `VECTOR` field specification of `FT.CREATE`:

```text
FT.CREATE <index> ... SCHEMA <field> VECTOR SVS_VAMANA <num_params>
    TYPE FLOAT32
    DIM <dimensions>
    DISTANCE_METRIC L2|IP|COSINE
    [INITIAL_CAP <capacity>]
    [GRAPH_MAX_DEGREE <degree>]
    [CONSTRUCTION_WINDOW_SIZE <size>]
    [SEARCH_WINDOW_SIZE <size>]
    [ALPHA <value>]
    [COMPRESSION NONE|FP16|SQ8|LVQ4|LVQ8|LVQ4X4|LVQ4X8|LEANVEC4X4|LEANVEC4X8|LEANVEC8X8]
    [LEANVEC_DIMS <dims>]
    [LEANVEC_TRAINING_THRESHOLD <count>]
    [RAW_VECTOR_STORAGE KEEP|DROP]
```

#### Parameter Reference

| Parameter | Type | Default | Constraints | Description |
|-----------|------|---------|-------------|-------------|
| TYPE | enum | -- | FLOAT32 | Vector element type (currently only FLOAT32 supported) |
| DIM | int | -- | Required | Vector dimensionality |
| DISTANCE_METRIC | enum | -- | L2, IP, COSINE | Distance function for similarity computation |
| INITIAL_CAP | int | 10240 | -- | Initial capacity hint for memory pre-allocation |
| GRAPH_MAX_DEGREE | int | 64 | >=2 | Maximum out-degree of each node in the Vamana graph |
| CONSTRUCTION_WINDOW_SIZE | int | 128 | >=1 | Candidate window size during graph construction |
| SEARCH_WINDOW_SIZE | int | 10 | >=1 | Beam width during greedy graph search |
| ALPHA | float | 1.2 | >0.0; <=1.0 for IP/COSINE | Graph pruning parameter controlling edge diversity |
| COMPRESSION | enum | NONE | See compression table | Storage backend for vector data |
| LEANVEC_DIMS | int | -- | >0 and <DIM | Target dimensionality after LeanVec projection. Required for LEANVEC variants. |
| LEANVEC_TRAINING_THRESHOLD | int | 10000 | >=1 | Number of vectors to buffer before training the LeanVec projection |
| RAW_VECTOR_STORAGE | enum | KEEP | KEEP, DROP | Whether SVS retains the original FP32 vectors alongside its compressed representation. **KEEP**: exact FP32 bytes preserved inside the SVS index; required for full-precision RDB round-trips (Phase 2) after VectorRegistry bytes are no longer available post-restart; prerequisite for a future user-visible vector reconstruction API. **DROP**: only the compressed representation is stored; lower memory footprint; reconstruction uses SVS's approximate decompressed value (lossy for FP16/SQ8/LVQ/LeanVec). Note: the VectorRegistry (PR #1316) holds exact raw bytes for live Valkey keys, but those are not persisted across server restarts. |

#### Compression Types

| Compression | Category | Description |
|-------------|----------|-------------|
| NONE | Baseline | Full precision FP32 storage (no compression) |
| FP16 | Baseline | IEEE 754 half-precision float storage |
| SQ8 | Scalar quantization | Scalar 8-bit quantization |
| LVQ4 | LVQ | 4-bit Locally-adaptive Vector Quantization |
| LVQ8 | LVQ | 8-bit Locally-adaptive Vector Quantization |
| LVQ4X4 | LVQ | Two-level LVQ: 4-bit primary + 4-bit residual |
| LVQ4X8 | LVQ | Two-level LVQ: 4-bit primary + 8-bit residual |
| LEANVEC4X4 | LeanVec | LeanVec dimensionality reduction + 4x4 LVQ |
| LEANVEC4X8 | LeanVec | LeanVec dimensionality reduction + 4x8 LVQ |
| LEANVEC8X8 | LeanVec | LeanVec dimensionality reduction + 8x8 LVQ |

In the open-source submodule build (this RFC), baseline, FP16, and SQ8 types are available. LVQ and LeanVec types require proprietary Intel backends; see Future Considerations for distribution options.

#### Example

```text
FT.CREATE my_index SCHEMA vec VECTOR SVS_VAMANA 16
    TYPE FLOAT32
    DIM 768
    DISTANCE_METRIC COSINE
    GRAPH_MAX_DEGREE 64
    CONSTRUCTION_WINDOW_SIZE 200
    SEARCH_WINDOW_SIZE 20
    ALPHA 0.95
    COMPRESSION SQ8
```

This creates an index with SQ8 scalar quantization. The index is ready immediately with no training phase required. LVQ and LeanVec compression types are available via a future proprietary build; see Future Considerations.

### FT.INFO Response

For SVS indexes, `FT.INFO` returns in the vector field's algorithm section:

- `algorithm`: `SVS_VAMANA`
- `graph_max_degree`: integer
- `construction_window_size`: integer
- `search_window_size`: integer
- `alpha`: float
- `compression`: string (NONE, FP16, SQ8, LVQ4, LVQ8, LVQ4X4, LVQ4X8, LEANVEC4X4, LEANVEC4X8, LEANVEC8X8)
- `state`: `ready` or `training`
- `raw_vector_storage`: `KEEP` or `DROP`

Additional fields for LeanVec compression types:
- `leanvec_dims`: integer
- `leanvec_training_threshold`: integer
- `training_progress`: string `"<buffered>/<threshold>"` (e.g., `"7500/10000"`)

### FT.SEARCH Behavior

No new `FT.SEARCH` parameters are introduced. The existing KNN query syntax applies:

```text
FT.SEARCH my_index "*=>[KNN 10 @vec $query_vec]" PARAMS 2 query_vec <blob>
```

The recall/latency trade-off is controlled by `SEARCH_WINDOW_SIZE` set at index creation time.

**Hybrid (filtered) queries** -- combining KNN with scalar predicate filters (tag, numeric, text) -- are supported via `svs_index_search_topk` (SVS PR #352, interface updated for production in PR #363). The SVS filter interface (`svs_id_filter_t`) passes an `is_member(id)` callback invoked post-traversal on each batch of candidates (adaptive batching pattern). A `filter_rate()` function pointer provides the hit-rate hint that drives adaptive batch sizing; valkey-search populates this from its existing qualified-entry-count estimate in `search.cc`, which already governs pre-filter vs inline-filter mode selection for HNSW. True inline-filter (predicate gates which graph edges are explored during traversal) is not yet available in SVS; upstream contribution is planned.

### RDB

> **Status: Planned (Phase 2).** RDB persistence for SVS is not yet implemented. `SaveIndexImpl` currently returns `UnimplementedError`. The design below describes the target implementation.

The SVS C API provides `save()` / `load()` APIs that serialize the complete DynamicVamana index (graph, vector data, metadata) to a stream.

1. **Save**: An `RDBOstreamAdapter` wraps RDB chunk I/O as a `std::streambuf`, buffering at 4MB boundaries.
2. **Load**: An `RDBIstreamAdapter` provides the input stream for `DynamicVamanaIndex::load()`. The index is reconstructed with all graph edges, vector data, and compression state intact.
3. **Deferred compression state**: For LeanVec indexes below their training threshold, the pending buffer and training data are serialized alongside the index metadata.

### Configuration

| Configuration | Scope | Default | Description |
|---------------|-------|---------|-------------|
| SVS submodule | Build-time | Always ON (x86_64 Linux) | SVS is compiled unconditionally on x86_64 Linux; no separate flag |

### Module API

#### SVS Submodule Integration

valkey-search integrates with SVS by compiling the SVS source tree (Apache-2.0) as a git submodule under `third_party/svs/`, linked as a static/object library into `libsearch.so`. This follows the same pattern as hnswlib and highwayhash.

The SVS C API (`svs/c/svs_c.h`, production path post-PR-#363) provides the primary interface for all index operations:
- `DynamicVamanaIndex` -- graph-based ANN index with dynamic insert/remove
- All open-source storage backends (FP32, FP16, SQ8)
- Thread-safe concurrent operations via the custom threadpool interface
- `save()` / `load()` for persistence
- `get_distance()` for pairwise distance computation
- `get_memory_usage()` for per-index byte attribution (see Memory Accounting)

Note: `reconstruct_at()` (retrieve a stored vector from the index) is not exposed as user-visible surface area in this RFC. The `VectorRegistry` (PR #1316) owns the canonical raw vector bytes for the duration of each key's lifetime, making index-level vector retrieval unnecessary for current use cases. Exposing approximate vector reconstruction is deferred to a future RFC that will require changes to the Valkey core and JSON module and will apply uniformly to all vector index types.

#### C API Operations

**C API status:** The SVS C API is implemented on the `dev/c-api` branch. PR #363 ("Refactor C API to be ready for the main branch", approved 2026-08-17, open) is the production-readiness refactor that lands on `dev/c-api` before that branch merges to `main`. The submodule pin should target a commit post-PR-#363 merge.

**Forward-compatibility contract (PR #363):** All vtable structs (`svs_threadpool_interface_ops`, `svs_id_filter_interface_ops`, `svs_memory_breakdown`) include `uint32_t version` and `size_t struct_size` as leading fields. The library only writes fields up to the caller-provided `struct_size`, enabling forward compatibility with older compiled binaries. The `svs_get_version()` / `svs_get_version_string()` functions allow valkey-search to verify the linked SVS version at startup. Error codes and `svs_data_type` values carry explicit stability contracts ("existing values are never changed; new values only appended").

The production C API (`include/svs/c/svs_c.h` -- note: path changed from `c_api/` to `c/` in PR #363) provides:
- Stable ABI (C linkage, opaque handles, versioned structs) for source-buildable integration
- Source-buildable from the Apache-2.0 repository (FP32, FP16, SQ8 baseline)
- `svs_index_search_topk()` (lowercase `k`) for KNN search -- returns `bool`; out-results via caller-initialized `svs_search_results_t` (`SVS_INIT_SEARCH_RESULTS()` macro); optional filter via `svs_id_filter_t`
- `svs_index_dynamic_add_points()` / `svs_index_dynamic_delete_points()` -- return `bool`; actual added/deleted count via out-parameter
- `svs_index_get_memory_usage()` for per-index byte attribution
- Clone with recompression: `svs_index_clone_dynamic()` for deferred compression transitions

valkey-search calls `svs_index_build_dynamic`, `svs_index_search_topk`, `svs_index_dynamic_add_points`, etc. directly via statically linked symbols. All graph construction, search, add/remove, and persistence operations go through this interface.

**VectorRecord integration (PR #1316):** `AddRecordImpl` and `ModifyRecordImpl` receive `std::shared_ptr<const VectorRecord>&&` -- an immutable record holding raw vector bytes and a precomputed `reciprocal_magnitude_`. The `VectorRegistry` owns the canonical raw bytes for each `(db_num, key, attribute)` tuple for as long as the key exists in Valkey; the SVS index does not need to maintain its own copy.

#### Thread Ownership

SVS's concurrency model integrates directly with valkey-search's existing reader/writer thread pools via the C API threadpool interface (SVS PR #305).

**Current state (PoC):** SVS internally manages its own OpenMP thread pool. The `--svs-omp-threads` module option sets the OMP thread count per-search-thread via `omp_set_num_threads`. valkey-search's reader/writer pools do not coordinate with OMP -- this creates thread over-subscription risk on high-core machines. This is a PoC artifact.

**Target state (post-submodule integration):** SVS is compiled without OpenMP. The `svs_threadpool_interface` C struct is registered at index construction via `svs_index_builder_set_threadpool_custom()`.

**Alignment with Valkey's concurrency model:** Valkey's threading contract requires that neither query nor mutation operations spawn sub-threads -- only the thread that invoked the operation executes it. This applies to SVS exactly as it does to HNSW: a reader thread executes a search to completion on that thread alone; a writer thread executes a graph mutation to completion on that thread alone. SVS's internal graph parallelism (parallel candidate evaluation, parallel distance computation) is therefore not available in this integration. The `svs_threadpool_interface` adapter implements this constraint directly:

```c
// Ops vtable -- versioned for forward compatibility (PR #363)
struct svs_threadpool_interface_ops {
    uint32_t version;
    size_t struct_size;
    size_t (*size)(void* self);
    bool (*parallel_for)(void* self,
                         void (*func)(void* svs_param, size_t i),
                         void* svs_param,
                         size_t n,
                         svs_error_h out_err);
};

struct svs_threadpool_interface { struct svs_threadpool_interface_ops* ops; void* self; };
typedef struct svs_threadpool_interface svs_threadpool_t;
```

valkey-search implements the adapter as follows, consistent with the no-sub-threading contract:
- `size()` -> returns `1`; SVS treats the index as single-threaded
- `parallel_for()` -> executes all N work items sequentially on the **calling thread**; no sub-threads spawned

```c
size_t svs_tp_size(void* self) { return 1; }

bool svs_tp_parallel_for(void* self,
                          void (*func)(void* svs_param, size_t i),
                          void* svs_param, size_t n,
                          svs_error_h out_err) {
    for (size_t i = 0; i < n; i++) func(svs_param, i);
    return true;
}
```

**Thread model by operation:**

| Operation | Executing thread | Notes |
|-----------|-----------------|-------|
| KNN search / graph traversal | The reader thread that picked up the query | No sub-threading; single-threaded per Valkey model |
| Add / graph mutation (flush) | The writer thread that picked up the mutation | No sub-threading; serialized above index level for same key |
| Save / Load (RDB) | Caller thread | Single-threaded stream I/O |
| Deferred compression transition | Writer thread | Atomic swap is single-threaded |

**Concurrency between operations:** The reader/writer phase gate enforced by valkey-search's index lock ensures the no-concurrent-reads-and-writes guarantee independently of SVS -- SVS does not need to enforce this itself. Multiple reader threads may execute independent queries concurrently (on separate index reads); multiple writer threads may execute independent mutations concurrently, with same-key mutations serialized at the level above the index as per Valkey's mutation contract.

**Trade-off:** Disabling SVS's internal graph parallelism means per-query latency does not benefit from multi-core parallelism within a single search. Throughput scales horizontally through Valkey's reader pool (multiple concurrent queries on separate threads), consistent with how HNSW behaves today. See Known Gaps.

#### Memory Accounting

For the open-source submodule build (this RFC), all SVS C and C++ allocations are intercepted automatically via `vmsdk::__wrap_malloc` and the global `operator new` overloads at link time. SVS memory is immediately visible in `used_memory` and `FT.INFO` with no additional code -- the same mechanism used for hnswlib.

A prerequisite for this to work: the SVS CMake target must be set up with `target_compile_definitions(... VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES)`, identical to how hnswlib is configured. Without this definition, SVS allocations silently bypass the tracked allocator -- `maxmemory` enforcement and key eviction triggers would not account for vector index memory. This requirement interacts with the ongoing build system modernization in [PR #1225](https://github.com/valkey-io/valkey-search/pull/1225); see the Future Considerations section for details.

`svs_index_get_memory_usage()` supplements automatic tracking by providing per-index byte counts for `FT.INFO` detail reporting.

Note: the prior runtime model (`libsvs_runtime.so.0.4.0`) had genuinely opaque memory accounting because it predates the `get_memory_usage()` API and has its own PLT entries. The submodule approach does not share this limitation.

**Pending: Valkey PR #4128 -- `ValkeyModule_AllocateExternalMemory`:** This in-progress Valkey core PR introduces a pair of module APIs for accounting memory that bypasses `zmalloc`:

```c
int ValkeyModule_AllocateExternalMemory(size_t bytes);  // report allocation
int ValkeyModule_FreeExternalMemory(size_t bytes);      // report deallocation
```

The counter flows into `zmalloc_used_memory()` and therefore into `used_memory`, `maxmemory` enforcement, and OOM detection. This is directly relevant to SVS in two cases that `__wrap_malloc` cannot cover:

- **`mmap`-backed storage:** If SVS uses `mmap` for huge-page-aligned index regions (which can reduce vector search latency by ~30% per community benchmarks by enabling alignment control and lazy population that `ValkeyModule_Alloc` cannot provide), those allocations bypass `__wrap_malloc` entirely. `ValkeyModule_AllocateExternalMemory` provides the accounting path.
- **Proprietary pre-built objects:** For builds where SVS allocations cross a PLT boundary, `ValkeyModule_AllocateExternalMemory(delta)` / `ValkeyModule_FreeExternalMemory(delta)` called after each mutating operation replaces the `UpdateReportedMemory()` polling workaround with an officially-supported Valkey API.

Note: the final API name is still under discussion in the PR (candidates include `AllocateExternalMemory` and `IncrExternalMemory`). An open concern about the external counter being included in `used_memory_dataset` calculations has not yet been resolved. The PR has one MEMBER approval and is awaiting TSC vote before merge.

#### Proprietary Compression (LVQ, LeanVec)

LVQ and LeanVec backends are out of scope for this RFC. Three distribution approaches -- a valkey-bundle build flag (`SVS_PRO`), a separate Intel-optimized image, and a standalone user build -- are evaluated in the Future Considerations section, including the community concerns and engineering trade-offs for each.

### Dependencies

| Dependency | Version | License | Purpose | Owner |
|------------|---------|---------|---------|-------|
| SVS git submodule (`third_party/svs/`) | TBD (pinned to release tag post-PR-#363) | Apache-2.0 | Static submodule: DynamicVamana graph, FP32/FP16/SQ8 backends, C API (`svs/c/svs_c.h`) | Intel |

The submodule is compiled from source as part of valkey-search's CMake build. AVX-512 is recommended; AVX2 is the minimum for x86_64. LVQ/LeanVec proprietary backend distribution is a future discussion; see Future Considerations.

### Testing

- **Functional tests**: FT.CREATE with ALGORITHM SVS_VAMANA -> insert vectors -> FT.SEARCH verifies recall >= 0.95
- **Platform tests**: Verify SVS functions correctly on x86_64 Linux
- **Compression backend tests**: All compression types produce functional indexes with expected recall
- **RDB round-trip tests**: BGSAVE -> restart -> FT.SEARCH verifies index integrity and recall
- **Deferred compression tests**: Threshold triggers training/compression transition; search works throughout
- **Parameter validation tests**: Invalid combinations produce appropriate errors
- **Performance tests**: Search latency and recall benchmarks across compression types and dataset sizes
- **Hybrid query tests**: Filtered KNN with tag/numeric predicates; verify results match pre-filter and adaptive-batching paths
- **Deferred compression non-blocking test**: Verify P99 search latency stays <10ms during compression transition under concurrent query load
- **Thread pool integration tests**: Verify SVS search parallelism uses reader pool; mutations use writer pool; no OMP threads spawned post-migration

### Observability

`SVS_VAMANA` indexes report the following metrics:

- **Index metrics**: vector count, graph degree statistics (mean/max), memory usage (bytes), compression state
- **Search metrics**: query latency histogram (p50/p95/p99), queries per second
- **Memory accounting**: automatic via `vmsdk::__wrap_malloc`/`operator new` interception (requires `VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES` on the SVS CMake target); per-index byte counts via `svs_index_get_memory_usage()` for `FT.INFO` detail.

## Implementation Status

### Completed

| Feature | Description |
|---------|-------------|
| Runtime v0.4.0 integration | save/load, get_distance, thread-safe add |
| VectorRegistry integration | PR #1316 merged: VectorExternalizer replaced by VectorRegistry; `AddRecordImpl`/`ModifyRecordImpl` now receive `shared_ptr<const VectorRecord>&&`; raw vector bytes owned by registry for key lifetime; `IsVectorMatch` removed from codebase; `RAW_VECTOR_STORAGE` parameter removed |
| Memory accounting | Per-index delta reporting via `DynamicVamanaIndex::get_memory_usage()` (SVS C++ runtime API); deltas reported to Valkey through `vmsdk::ReportAllocMemorySize` / `vmsdk::ReportFreeMemorySize` after each mutation (`UpdateReportedMemory()`, svs-memory-reporting branch) |
| Metrics suite | Full SVS-specific metrics in metrics framework |
| Basic index operations | Create, add, search, remove functional |

### Remaining (Feature Parity with HNSW)

> All SVS integration work below is being contributed to the valkey-search project by Intel. "Owner: Intel (contributor)" indicates Intel as the implementing team submitting changes to the valkey-search codebase -- it does not imply any obligation from the broader valkey open-source community.

| Phase | Feature | Priority | Owner | Description |
|-------|---------|----------|-------|-------------|
| 1 | SVS always-on (x86_64 Linux) | High | Intel (contributor) | SVS submodule compiled unconditionally on x86_64 Linux; no compile flag |
| 2 | RDB persistence | Critical | Intel (contributor) | Save/load SVS indexes across server restarts |
| 3 | Dispatch latency sampling | Medium | Intel (contributor) | Per-query latency metrics at dispatch layer |
| 4 | Partial results on timeout | Medium | Intel (contributor) | Return best results found so far when search times out |
| 5 | SVS submodule integration | Blocked (on Intel SVS C API) | Intel (contributor) | Replace pre-built runtime with static submodule under `third_party/svs/`; remove OpenMP; integrate C API threadpool |
| 6 | Proprietary compression path | Low (follow-on) | Intel + community decision | LVQ/LeanVec distribution model (build flag, dynamic .so, or standalone build); see Future Considerations |
| 7 | Deferred compression | Medium | Intel (contributor) | Copy-semantic transition orchestration with non-blocking swap; once the compressed clone is nearly caught up, route incoming mutations to the new index before the final pointer swap to minimize divergence at cutover |

### Known Gaps

| Gap | Impact | Status | Mitigation |
|-----|--------|--------|------------|
| Inline graph-traversal filter | True inline-filter (predicate gates which edges are explored during traversal) is not yet available in SVS | Planned upstream contribution | Hybrid queries supported via `svs_index_search_topk` adaptive batching (SVS PRs #352/#363): `svs_id_filter_t` passes `is_member()` callback and `filter_rate()` function pointer; filter applied post-traversal per batch |
| OMP thread over-subscription (PoC) | OpenMP threads conflict with valkey-search's reader/writer pools on high-core machines | Resolved by Phase 5 (submodule + C API threadpool) | Set `--svs-omp-threads 1` to minimize interference; Phase 5 removes OMP entirely |
| LVQ / LeanVec unavailable | Proprietary compression backends not included in the open-source build | Follow-on (Phase 6) -- see Future Considerations | FP32/FP16/SQ8 cover the majority of use cases; `FT.CREATE` with LVQ/LeanVec returns a clear error |

## Future Considerations: Proprietary Compression Backends (LVQ, LeanVec)

LVQ and LeanVec are proprietary compression backends and are out of scope for this RFC.

### Why Deferred

- **Integration model under discussion.** The 3rd-party binary integration model for delivering LVQ/LeanVec alongside the community build requires additional consideration still under discussion with the Intel.
- **Open-source initial release.** A fully open-source initial release lowers the barrier for the valkey community to evaluate SVS_VAMANA. Proprietary compression can be introduced as an opt-in once the baseline is proven.

### Memory Accounting and Allocation Considerations

Any path that adds LVQ/LeanVec -- whether via a valkey-bundle build flag, a standalone user build, or any other mechanism -- must address the same memory tracking requirements as the open-source submodule.

**How tracking works for the open-source build:** valkey-search intercepts all C and C++ allocations from statically linked third-party code via the `VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES` compile definition. When this definition is present on a CMake target, `vmsdk/src/memory_allocation_overrides.h` redefines `malloc`/`free`/`calloc`/`realloc` and overrides `operator new`/`delete` to route through Valkey's tracked allocator. This causes all vector index memory to be counted in `used_memory`, which is what enables `maxmemory` enforcement and key eviction triggers to function correctly against the index.

**The fragility risk -- PR #1225:** The open-source build system is being modernized ([PR #1225](https://github.com/valkey-io/valkey-search/pull/1225)), which converts intermediate static libraries to CMake OBJECT libraries and removes significant custom CMake scaffolding. During review, a reviewer caught that the `target_compile_definitions(... VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES)` on the hnswlib target was at risk of being silently lost during the target rename (`hnswlib_vmsdk` INTERFACE -> `hnswlib` OBJECT). OBJECT libraries propagate INTERFACE properties differently than INTERFACE libraries in complex link graphs, and the definition must explicitly reach every translation unit that includes hnswlib headers. If it is dropped, hnswlib silently falls back to system `malloc` -- bypassing the tracked allocator entirely.

The SVS submodule will face the same requirement: its CMake target must be set up with `VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES` in the same way hnswlib is. This must be verified when the submodule is integrated under the modernized build system from PR #1225.

**Challenges if this is not correctly propagated:**

| Failure | Impact |
|---------|--------|
| SVS allocations bypass `__wrap_malloc` | `used_memory` undercounts vector index memory |
| `used_memory` undercounts | Valkey does not trigger `maxmemory` evictions when the index grows |
| No eviction trigger | Server can OOM-kill under memory pressure with no warning |
| `FT.INFO` memory fields | Per-index byte counts come from `get_memory_usage()` API, not from allocator tracking -- this remains accurate as a supplemental figure, but does not feed into Valkey's global memory enforcement |

**Sanitizer build gap:** `memory_allocation_overrides.h` explicitly disables all overrides under `SAN_BUILD` (the `#ifdef SAN_BUILD` guard). This means ASAN/TSAN test runs do not exercise the tracked-allocator path -- allocator bypass bugs that would only manifest in production builds are not caught by sanitizer CI runs. Any integration test that validates memory accounting must run in a non-sanitizer Release build.

**For proprietary compression builds (all alternatives):** If LVQ/LeanVec backends are compiled from source (e.g., via the tarball download pattern), the same `VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES` mechanism applies and must be explicitly set on the SVS PRO CMake target. If any component is provided as a pre-built binary object, the PLT boundary problem from the prior pre-built runtime model re-emerges for that component -- allocations from pre-built objects cannot be intercepted at link time.

Once [Valkey PR #4128](https://github.com/valkey-io/valkey/pull/4128) merges, the preferred accounting path for pre-built objects is `ValkeyModule_AllocateExternalMemory(delta)` / `ValkeyModule_FreeExternalMemory(delta)` called after each mutating operation -- replacing the `UpdateReportedMemory()` / `get_memory_usage()` polling pattern with an officially-supported Valkey module API. Until PR #4128 merges, `get_memory_usage()` polling remains the only available mechanism.

### Alternative A: valkey-bundle Build Flag (Preferred Starting Point)

The `valkey-bundle` Dockerfile builds `libsearch.so` in a dedicated build stage and copies it into the final image at `/usr/lib/valkey/libsearch.so`. The entrypoint script (`bundle-docker-entrypoint.sh`) auto-discovers all `.so` files in `/usr/lib/valkey/` and loads them at startup -- no explicit `MODULE LOAD` required.

Adding SVS proprietary compression support would require three changes:

**1. `Dockerfile.template`** -- add a build ARG and pass it through to `build.sh`:

```dockerfile
ARG SVS_PRO=0

# Build Search module
WORKDIR /opt/valkey-search
RUN set -eux; \
    sed -i 's/-DCMAKE_CXX_STANDARD=20"/-DCMAKE_CXX_STANDARD=20 -DCMAKE_POLICY_VERSION_MINIMUM=3.5"/' submodules/CMakeLists.txt; \
    SVS_PRO=${SVS_PRO} ./build.sh
```

**2. `valkey-search/build.sh`** -- forward the flag to CMake:

```sh
case "${SVS_PRO}" in
  1|ON) SVS_PRO_FLAG="-DSVS_PRO=ON" ;;
  *)    SVS_PRO_FLAG="" ;;
esac
cmake ... ${SVS_PRO_FLAG} ...
```

**3. `valkey-search/CMakeLists.txt`** -- when `SVS_PRO=ON`, fetch the SVS release tarball from GitHub using the established SVS FetchContent pattern (see [SVS C++ quickstart](https://intel.github.io/ScalableVectorSearch/start_cpp.html)):

```cmake
if(SVS_PRO)
    set(SVS_PRO_URL
        "https://github.com/intel/ScalableVectorSearch/releases/download/<tag>/svs-shared-library-<version>.tar.gz"
        CACHE STRING "URL to download SVS proprietary tarball")
    include(FetchContent)
    FetchContent_Declare(svs_pro URL "${SVS_PRO_URL}")
    FetchContent_MakeAvailable(svs_pro)
    list(APPEND CMAKE_PREFIX_PATH "${svs_pro_SOURCE_DIR}")
    find_package(svs_pro REQUIRED)
    # Note: VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES cannot be applied retroactively
    # to pre-compiled objects in the tarball. Memory accounting for SVS PRO uses
    # get_memory_usage() polling via UpdateReportedMemory() after each mutation.
    # See Future Considerations -- Memory Accounting.
endif()
```

Intel-optimized image build:
```sh
docker build --build-arg SVS_PRO=1 \
             -t valkey/valkey-bundle:<tag>-svs-pro .
```

**Pros:** Single Dockerfile; standard valkey-bundle distribution path; no separate image CI/CD pipeline; uses the established SVS tarball download pattern; source-compiled proprietary backends allow `VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES` to be enforced.

**Cons:** Community approval required for Dockerfile.template changes. Image tag naming and ownership must be agreed with the valkey community. Intel must publish a compatible tarball to GitHub releases for each valkey-search release.

**C++ ABI and LTO risks specific to this alternative:** Unlike the open-source submodule (where all objects are compiled together from source in the same toolchain), a naive pre-compiled proprietary tarball contains C++ objects built outside the valkey-search build environment. This introduces:

- **Compiler/STL version mismatch:** SVS C++ template instantiations carry symbol names mangled by the compiler used to build them. If that compiler differs from valkey-search's gcc-13/Ubuntu 24.04 environment, ODR violations or undefined behavior can occur at link time or runtime.
- **LTO degradation:** valkey-search links `libsearch.so` with `-flto`. Pre-built objects not compiled with `-ffat-lto-objects` cannot participate in LTO, silently disabling cross-module optimization for those translation units.
- **Build environment coupling:** Raw proprietary C++ objects require Intel to match valkey-search's exact toolchain for every release.

These risks do not apply to the open-source submodule baseline, where SVS is compiled from source in the same pass.

**Mitigation -- hardened static library with symbol localization:**

Rather than shipping raw pre-compiled C++ objects, Intel can build `libsvs_c_api.a` directly from private C++ sources and apply a symbol localization step before distribution:

```bash
# Combine all SVS C++ objects into a single relocatable object
ld -r -o svs_combined.o svs_private/*.o

# Localize all hidden C++ internals, then apply explicit allowlist
# keeping only svs_* symbols (all SVS public C API exports) as globally visible.
# Note: SVS C API symbols are prefixed svs_index_*, svs_error_*, svs_get_*, etc.
objcopy --localize-hidden svs_combined.o svs_localized.o

# Generate allowlist from localized object (all remaining svs_* globals) and apply
nm -g --defined-only svs_localized.o \
    | awk '$3 ~ /^svs_/ {print $3}' > svs_allowlist.txt
objcopy --keep-global-symbols=svs_allowlist.txt \
    --strip-unneeded svs_localized.o svs_c_api.o

# Package as a static library
ar rcs libsvs_c_api.a svs_c_api.o

# Verify required public API symbols are present
for sym in svs_index_search_topk svs_index_get_memory_usage \
           svs_index_build_dynamic svs_index_dynamic_add_points \
           svs_index_dynamic_delete_points svs_get_version; do
    nm -g --defined-only svs_c_api.o | grep -q " $sym$" || {
        echo "ERROR: required symbol $sym not found in libsvs_c_api.a" >&2; exit 1
    }
done

# Verify: fail the build if any non-allowlisted global symbol remains
LEAKED=$(nm -g --defined-only svs_c_api.o | awk '$3 !~ /^svs_/' | grep -v ' U ')
if [ -n "$LEAKED" ]; then
    echo "ERROR: non-allowlisted global symbols in libsvs_c_api.a:" >&2
    echo "$LEAKED" >&2
    exit 1
fi
```

This produces a static library where all C++ mangled symbols (template instantiations, STL types, internal methods) are localized and stripped from the visible symbol table. The `svs_*` allowlist covers all SVS public C API exports (`svs_index_*`, `svs_error_*`, `svs_get_*`, `svs_search_results_*`, etc.); the symbol probe and `nm` verification steps fail the build if required symbols are missing or if non-allowlisted symbols leak through, making the guarantees enforceable in CI.

**On LTO:** If `libsvs_c_api.a` is built directly from C++ sources (rather than from a pre-compiled shared-library tarball package), the compiler performs source-level optimization across the full `index -> dataset -> distance` call chain at SVS build time. LTO at the `libsearch.so` link step is then not needed for SVS code paths -- the performance-critical inlining is already baked into the static library. The hardened `libsvs_c_api.a` can then be linked into `libsearch.so` as an opaque pre-compiled artifact without LTO participation.

This refined approach -- source-compiled, symbol-localized `libsvs_c_api.a` -- materially narrows the gap between Alternative A and Alternative D: Intel retains build independence, C++ symbol conflicts are resolved, and LTO is not required. The remaining difference from Alternative D is operational (single-binary vs. dynamic `.so` deployment) rather than correctness or performance.

### Alternative B: Separate Intel-Optimized Image

A separate image (e.g., `valkey/valkey-bundle-intel`) would always be built with `SVS_PRO=1` and published alongside the standard valkey-bundle.

**The valkey community has previously expressed concern about maintaining separate image variants.** A separate image requires additional CI/CD, a parallel tag lifecycle, and clear ownership. This option is listed for completeness and is expected to face community resistance. It should only be pursued if Alternative A is blocked by constraints on the tarball access model.

### Alternative C: Standalone User Build (Roll-Your-Own)

Users who want LVQ/LeanVec build `libsearch.so` locally and replace the binary in their deployment -- no changes to valkey-bundle required.

```sh
# 1. Clone valkey-search at the desired release tag
git clone --depth 1 --branch <tag> \
    https://github.com/valkey-io/valkey-search.git
cd valkey-search
git submodule update --init --recursive

# 2. Build with SVS_PRO enabled (fetches tarball from GitHub releases)
SVS_PRO=ON ./build.sh

# 3a. Replace in a Docker-based deployment (bundle entrypoint auto-loads)
docker cp .build-release/libsearch.so \
    <container>:/usr/lib/valkey/libsearch.so
docker restart <container>

# 3b. Replace in a running server (live reload)
cp .build-release/libsearch.so /usr/lib/valkey/libsearch.so
valkey-cli MODULE UNLOAD search
valkey-cli MODULE LOADEX /usr/lib/valkey/libsearch.so
```

**Pros:** No valkey-bundle changes; user controls their build; useful as a developer preview path.

**Cons:** Highest user burden; no official distribution channel; users must track releases and rebuild manually.

### Alternative D: Dynamic libsvs_c_api.so Swap

Rather than statically linking proprietary backends into `libsearch.so`, `libsearch.so` dynamically links a separate `libsvs_c_api.so` whose implementation can be swapped without recompiling `libsearch.so`:

```text
valkey-server
  \-- MODULE LOAD libsearch.so
         +-- statically links: gRPC, Abseil, Protobuf, hnswlib, ICU, snowball, ...
         \-- dynamically links: libsvs_c_api.so (versioned, e.g. libsvs_c_api.so.0.4.0)
               (public variant)  FP32/FP16/SQ8 implemented; LVQ/LV calls return SVS_ERROR_NOT_IMPLEMENTED
               (Intel variant)   All backends implemented; optimized per hardware target
```

The two variants ship the same `svs/c/svs_c.h` exports and differ only in implementation. Operators swap the `.so` file and reload the module (or restart) to gain LVQ/LeanVec -- no recompilation of `libsearch.so` required.

**How Intel builds the Intel variant:** Because the boundary is a pure C ABI (no mangled C++ symbols, no STL types in the interface), Intel can compile `libsvs_c_api.so` with any compiler, any optimization level, and any hardware-specific flags independently from the valkey-search build environment. Multiple `.so` variants targeting different hardware (AVX-512, AVX2) can be distributed as separate files.

**Memory accounting for this model:** Since `libsvs_c_api.so` has its own PLT entries, `__wrap_malloc` interposition does not cross the `.so` boundary. Memory tracking uses `svs_index_get_memory_usage()` polling with `UpdateReportedMemory()` after each mutating operation -- the same approach already implemented in the svs-memory-reporting branch and documented in the Memory Accounting section above.

**Pros:**
- No C++ ABI risks: pure C boundary eliminates compiler version, STL, and LTO concerns
- Intel builds independently: no toolchain matching required; proprietary optimizations are self-contained
- Hot-swap without recompilation: `libsearch.so` does not need to be rebuilt to gain LVQ/LeanVec
- Hardware-specific variants: Intel can ship separate `.so` files for AVX-512, AVX2, etc.
- Fits the established SVS C API model: the C API (PR #363) is specifically designed for this kind of stable-ABI dynamic linking

**Cons:**
- Deployment complexity: `LD_LIBRARY_PATH` or `/etc/ld.so.conf.d/` must point to `libsvs_c_api.so`; breaks the single-binary model
- Memory accounting via polling: not automatic like `__wrap_malloc`; delta reporting after mutations is close-to-realtime but not instantaneous
- Version compatibility matrix: `libsearch.so` and `libsvs_c_api.so` must be compatible versions; this must be enforced and documented

**Relationship to the main spec:** The open-source submodule integration (this RFC) targets the C API as the integration layer (`svs/c/svs_c.h`). Once that is in place, the C API interface is already designed to work with either static or dynamic linking. Switching from static submodule to dynamic `libsvs_c_api.so` would require `libsearch.so` to call `dlopen`/`dlsym` or link against the `.so` at build time, but the function signatures remain the same. This alternative is therefore a plausible follow-on once the C API is stable on main.

### Recommendation

Publish **Alternative C** build instructions as a developer preview once the open-source integration is stable. For the official LVQ/LeanVec distribution path, **Alternative D** (dynamic `libsvs_c_api.so`) and **Alternative A** (build flag with static tarball) represent two distinct architectural positions under active discussion:

- **Alternative A** (static) is simpler to distribute (single binary) but locks Intel to matching the valkey-search build environment and introduces C++ ABI risks for pre-built proprietary objects.
- **Alternative D** (dynamic) preserves Intel's build independence, avoids all C++ ABI risks, and enables hardware-specific variants, but reintroduces deployment complexity and requires polling-based memory accounting.

The community discussion should resolve this trade-off before committing to a distribution model. Alternative B (separate image) should be avoided unless both A and D are blocked.

A fourth possibility -- Intel building the entire `libsearch.so` (valkey-search + proprietary SVS backends) as a drop-in replacement -- maximizes optimization potential but requires Intel to maintain the full valkey-search build pipeline for every release (toolchain, dependencies, CI). This carries the highest ongoing engineering burden for Intel and its community acceptance is uncertain; it is not recommended without explicit community endorsement.

---

## Appendix

### References

- [Intel Scalable Vector Search -- GitHub](https://github.com/intel/ScalableVectorSearch)
- [Intel SVS Documentation](https://intel.github.io/ScalableVectorSearch/)
- [valkey-search PR #1316 -- VectorRegistry for memory sharing](https://github.com/valkey-io/valkey-search/pull/1316)
- [Valkey PR #4128 -- VM_AllocateExternalMemory (pending merge)](https://github.com/valkey-io/valkey/pull/4128)
- [SVS PR #326 -- Deferred Compression](https://github.com/intel/ScalableVectorSearch/pull/326)
- [SVS PR #352 -- C API Filtered TopK Search](https://github.com/intel/ScalableVectorSearch/pull/352)
- [SVS PR #305 -- C API Threadpool Getter/Setter](https://github.com/intel/ScalableVectorSearch/pull/305)
- [SVS PR #363 -- C API Production-Ready Refactor (targeting main)](https://github.com/intel/ScalableVectorSearch/pull/363)
- [SVS PR #68 -- C++ ThreadPool Concept](https://github.com/intel/ScalableVectorSearch/pull/68)
- [ABHT23] Aguerrebere, C.; Bhati, I.; Hildebrand, M.; Tepper, M.; Willke, T.: Similarity search in the blink of an eye with compressed indices. VLDB Endowment, 16(11), 3433-3446. (2023)
- [TBAH24] Tepper, M.; Bhati, I.; Aguerrebere, C.; Hildebrand, M.; Willke, T.: LeanVec: Searching vectors faster by making them fit. TMLR, ISSN 2835-8856. (2024)
