---
RFC: (PR number)
Status: Proposed
---

# Scalable Vector Search (SVS) Integration for valkey-search

## Abstract

This RFC proposes integrating [Intel Scalable Vector Search (SVS)](https://github.com/intel/ScalableVectorSearch) into valkey-search as a new `ALGORITHM SVS_VAMANA` option alongside the existing HNSW and FLAT algorithms. SVS_VAMANA uses the DynamicVamana graph-based index for high-performance approximate nearest neighbor (ANN) search optimized for x86_64 platforms, with multiple compression backends (FP16, SQ8, LVQ 4/8-bit, LeanVec dimensionality reduction).

The RFC also specifies the linking architecture for separating proprietary compression backends (LVQ, LeanVec) from the open-source core, recommending a C API shared library swap model that keeps valkey-search's default dependency chain fully open-source while allowing opt-in to proprietary compression at deploy time.

## Motivation

The valkey-search module currently provides two vector indexing algorithms: FLAT (brute-force exact search) and HNSW (Hierarchical Navigable Small World graph). While HNSW is effective for many workloads, there are scenarios where alternative graph-based algorithms offer better trade-offs:

1. **Memory efficiency at scale.** Large-scale vector datasets (millions to billions of vectors) benefit from advanced compression techniques. Intel SVS provides Locally-adaptive Vector Quantization (LVQ) and LeanVec dimensionality reduction, which can reduce memory footprint by 4-16x while maintaining high recall, outperforming scalar quantization approaches available in HNSW implementations.

2. **x86_64 hardware optimization.** SVS is purpose-built for Intel platforms, leveraging AVX-512 and AVX2 instruction sets for vectorized distance computations and graph traversal. Deployments running on Intel hardware can achieve higher throughput compared to platform-agnostic implementations.

3. **Cold-start problem.** Compressed indexes traditionally require a minimum dataset size for training (e.g., learning quantization codebooks or projection matrices). SVS's deferred compression model starts the index uncompressed and searchable immediately, then transparently transitions to the compressed backend once sufficient vectors are present. This eliminates the need for a separate "training" phase where the index is unavailable for queries.

4. **Algorithm diversity.** DynamicVamana uses a single-level graph with alpha-pruning and greedy search, which produces different recall/throughput/memory trade-offs compared to HNSW's multi-layer probabilistic navigation. Providing both gives operators the flexibility to select the best fit for their workload characteristics and hardware.

## Design Considerations

### Comparison with Existing Algorithms

| Property | FLAT | HNSW | SVS_VAMANA |
|----------|------|------|------------|
| Search complexity | O(n) exact | O(log n) approximate | O(log n) approximate |
| Graph structure | None | Multi-layer skip-list graph | Single-level Vamana graph with alpha-pruning |
| Compression | None | None | FP16, SQ8, LVQ (4/8-bit), LeanVec |
| Platform | Any | Any | x86_64 Linux (pre-built runtime); ARM64 macOS (source build, future) |
| Dynamic updates | N/A | Supported | Supported (thread-safe add/remove) |
| Memory overhead | Vectors only | Vectors + multi-layer graph | Vectors + single-layer graph |

### Deployment Architecture and Linking Model

#### Current Model: Pre-Built Runtime `.so`

```
valkey-server
  └── MODULE LOAD libsearch.so (73 MB, built by valkey-search)
         ├── statically links: gRPC, Abseil, Protobuf, hnswlib, ICU, snowball, ...
         └── dynamically links: libsvs_runtime.so.0.4.0 (43 MB, pre-built by Intel)
```

The SVS nightly tarball ships a pre-compiled runtime with all algorithms (FP32, FP16, SQ8, LVQ, LeanVec) baked into a single shared library, exposing 55 C++ mangled symbols via vtable interface.

**Limitations of the current model:**
- All-or-nothing: cannot ship open-source-only `libsearch.so` that later gains LVQ
- C++ vtable ABI is fragile across compiler versions
- Memory accounting is opaque — `malloc` interposition cannot intercept SVS allocations because the runtime has its own PLT entries

#### Target Model: C API + SharedAPI Hybrid (Recommended)

```
valkey-server
  └── MODULE LOAD libsearch.so
         ├── links: libsvs_c_api.so (open-source, built from Apache-2.0 source)
         │     └── stable C ABI: graph ops, search, add, save/load
         │     └── supports: FP32, FP16, SQ8
         │
         └── discovers at runtime via ValkeyModule_GetSharedAPI:
               └── MODULE LOAD libsearch_svs_pro.so (optional, loaded at any time)
                     └── exports: "SVS_CompressStorage", "SVS_GetSupportedTypes", ...
                     └── enables: LVQ4/8, LeanVec
```

This model combines the C API's ABI stability for core operations with Valkey's existing SharedAPI mechanism for runtime-extensible compression backends. The precedent is the JSON module integration in `src/attribute_data_type.cc`, where `ValkeyModule_GetSharedAPI(ctx, "JSON_GetValue")` discovers the JSON module's function pointer at runtime without a restart.

**How it works:**

1. `libsearch.so` links against `libsvs_c_api.so` (open-source, Apache-2.0) for all core index operations: graph construction, search, add/remove, save/load, memory accounting. This provides FP32, FP16, and SQ8 out of the box.

2. For proprietary compression (LVQ, LeanVec), `libsearch.so` discovers an optional `libsearch_svs_pro.so` module via SharedAPI at runtime — identical to how it discovers JSON support:

```cpp
static SvsCompressStorageFn svs_compress = nullptr;
static SvsGetSupportedTypesFn svs_get_supported = nullptr;
static std::optional<bool> is_svs_pro_loaded;

bool IsSvsProSupported(ValkeyModuleCtx *ctx) {
    if (is_svs_pro_loaded.has_value() && is_svs_pro_loaded.value()) {
        return true;
    }
    is_svs_pro_loaded = vmsdk::IsModuleLoaded(ctx, "search_svs_pro");
    if (!is_svs_pro_loaded.value()) return false;

    svs_compress = (SvsCompressStorageFn)ValkeyModule_GetSharedAPI(
        ctx, "SVS_CompressStorage");
    svs_get_supported = (SvsGetSupportedTypesFn)ValkeyModule_GetSharedAPI(
        ctx, "SVS_GetSupportedTypes");
    return svs_compress != nullptr;
}
```

3. Deferred compression integrates naturally: when the training threshold is crossed, valkey-search checks `IsSvsProSupported()`. If the pro module is present, it compresses to the target type (LVQ/LeanVec). If absent, the index either stays uncompressed or falls back to SQ8 via the base C API.

**Runtime behavior:**

| Scenario | Behavior |
|----------|----------|
| Only `libsearch.so` loaded | FP32/FP16/SQ8 via C API. LVQ/LeanVec requests → error at FT.CREATE |
| `libsearch_svs_pro.so` loaded later | Hot-registers LVQ/LeanVec. New FT.CREATE calls can use them immediately |
| Pro module unloaded | Existing compressed indexes continue serving (data is in-memory). New FT.CREATE with LVQ → error |
| Deferred compression triggers | Pro present → compress to target. Pro absent → stay uncompressed or fall back to SQ8, log warning |

**Module unload safety:** `MODULE UNLOAD libsearch_svs_pro` is refused if any active index uses a compression type that requires the pro module's SIMD distance kernels. This is enforced via a reference count incremented at index creation and decremented at index drop.

**Migration path:**
1. valkey-search migrates from `svs::svs_runtime` C++ vtable to `libsvs_c_api.so` C functions for core operations
2. `vector_svs.cc` calls `svs_index_build_dynamic`, `svs_index_search`, etc. via the C API
3. Compression backends beyond SQ8 are registered by `libsearch_svs_pro.so` via `ValkeyModule_ExportSharedAPI`
4. Memory accounting uses `svs_index_get_memory_usage()` from the C API for base operations; pro module reports additional compressed storage via its own SharedAPI function

**Responsibilities:**
- Intel SVS team: implements and maintains the C API (`libsvs_c_api.so`)
- valkey-search (this team): implements the SharedAPI integration, pro module interface contract, and deferred compression orchestration

#### Alternative Approaches Considered

| Dimension | Current (Runtime .so) | C API .so Swap | dlopen Plugin | **C API + SharedAPI (recommended)** |
|-----------|----------------------|----------------|---------------|--------------------------------------|
| Deployment files | 2 | 2 | 2 + plugins | 2–3 |
| Operator complexity | Low | Low | Medium | Low |
| Hot-pluggable | No | No (restart) | Partial (load only) | Yes (MODULE LOAD/UNLOAD) |
| ABI stability | C++ vtable (fragile) | C (stable) | C function ptrs (stable) | C (stable) |
| Memory accounting | Needs external contract | Via C API | Unified (same process) | Via C API + SharedAPI |
| Licensing boundary | Build artifact | .so file boundary | .so file boundary | Module boundary |
| Existing precedent | Current model | dev/c-api branch | Common pattern | JSON integration in valkey-search |
| Multi-vendor support | No | No (single .so) | Yes (multiple plugins) | Yes (multiple modules) |
| Graceful degradation | No | No (binary choice) | Partial | Yes (features degrade per-type) |

**Alternative 1: Pure C API .so Swap** — `libsearch.so` links a single `libsvs_c_api.so` that is either the open-source build (FP32/FP16/SQ8) or the proprietary build (adds LVQ/LeanVec). Simple deployment, but requires a server restart to switch variants and cannot gracefully degrade per-compression-type.

**Alternative 2: dlopen Plugin** — `libsearch.so` scans a `--svs-plugin-dir` at module load time, loading `.so` plugins that register compression backends via a versioned C function-pointer contract. This avoids depending on Valkey's module management but loses hot-pluggability (plugins load only at module init).

**Alternative 3: Current model (deferred)** — Continue using Intel's pre-built runtime `.so` with C++ vtable interface. Not viable long-term due to ABI fragility, all-or-nothing licensing, and opaque memory accounting.

### Deferred Compression

Traditional compressed vector indexes require a training phase. SVS implements deferred compression:

1. The index starts with FP32 or FP16 storage regardless of target compression type.
2. Queries are served immediately using the uncompressed representation.
3. When live vector count reaches `LEANVEC_TRAINING_THRESHOLD`, valkey-search initiates the compression transition.
4. The graph structure, ID translator, and entry point are preserved — only the data storage layer changes.

This is exposed in `FT.INFO` as a `state` field: `"training"` while below threshold, `"ready"` after compression has been applied.

#### Transition Mechanics

The compression transition uses **copy semantics** to avoid blocking concurrent searches:

1. Threshold crossed — valkey-search detects the live vector count exceeds `LEANVEC_TRAINING_THRESHOLD`.
2. Capability check — valkey-search calls `IsSvsProSupported(ctx)` to verify the target compression is available:
   - FP16/SQ8: always available via `libsvs_c_api.so`
   - LVQ/LeanVec: requires `libsearch_svs_pro.so` to be loaded
3. Clone with new storage — a new index is built with compressed storage, copying the graph structure from the existing index. Searches continue against the original uncompressed index during this phase.
4. Atomic swap — once the compressed index is ready, valkey-search atomically swaps the index pointer. The old uncompressed storage is freed.
5. Memory accounting update — the freed memory is reflected in `FT.INFO` and per-index byte counters.

**Hard constraint:** Searches must never block for more than ~10ms during the transition. The copy-then-swap approach ensures this — 2× peak memory during the overlap window is acceptable.

**Fallback behavior:** If the target compression is unavailable at transition time (pro module not loaded), the index remains uncompressed and logs a warning. If the target is unavailable at `FT.CREATE` time, the command returns an error immediately.

### Platform Requirements

- **x86_64 Linux** (current): pre-built runtime binary. Optimal with AVX-512; functional with AVX2 at reduced throughput. All compression backends available.
- **ARM64 / macOS** (future): SVS compiles from source on ARM64 macOS and passes upstream CI. Once the C API migration enables source builds, ARM64 support becomes viable.

The `ENABLE_SVS` CMake flag (currently defaults to OFF) controls whether SVS is compiled into valkey-search. Phase 1 will default it to ON on x86_64 Linux.

### Comparison with Vector Search in Other Systems

| System | Vamana/DiskANN Support | Compression | Platform-Specific Optimizations |
|--------|----------------------|-------------|-------------------------------|
| RediSearch (>=2.8.10) | Yes (SVS_VAMANA) | LVQ + LeanVec | x86_64 (Intel optimized) |
| Milvus | DiskANN (Vamana family) | Scalar/Product quantization | Limited |
| Qdrant | No (HNSW only) | Scalar/Product quantization | No |
| Weaviate | No (HNSW only) | Product quantization | No |
| **valkey-search + SVS** | **Yes (SVS_VAMANA)** | **LVQ + LeanVec** | **x86_64 AVX-512/AVX2** |

## Specification

### FT.CREATE with ALGORITHM SVS

The `SVS_VAMANA` algorithm is selected via the `ALGORITHM` parameter in the `VECTOR` field specification of `FT.CREATE`:

```
FT.CREATE <index> ... SCHEMA <field> VECTOR SVS <num_params>
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
| TYPE | enum | — | FLOAT32 | Vector element type (currently only FLOAT32 supported) |
| DIM | int | — | Required | Vector dimensionality |
| DISTANCE_METRIC | enum | — | L2, IP, COSINE | Distance function for similarity computation |
| INITIAL_CAP | int | 10240 | — | Initial capacity hint for memory pre-allocation |
| GRAPH_MAX_DEGREE | int | 64 | >=2 | Maximum out-degree of each node in the Vamana graph |
| CONSTRUCTION_WINDOW_SIZE | int | 128 | >=1 | Candidate window size during graph construction |
| SEARCH_WINDOW_SIZE | int | 10 | >=1 | Beam width during greedy graph search |
| ALPHA | float | 1.2 | >0.0; <=1.0 for IP/COSINE | Graph pruning parameter controlling edge diversity |
| COMPRESSION | enum | NONE | See compression table | Storage backend for vector data |
| LEANVEC_DIMS | int | — | >0 and <DIM | Target dimensionality after LeanVec projection. Required for LEANVEC variants. |
| LEANVEC_TRAINING_THRESHOLD | int | 10000 | >=1 | Number of vectors to buffer before training the LeanVec projection |
| RAW_VECTOR_STORAGE | enum | KEEP | KEEP, DROP | Whether to retain original uncompressed vectors alongside the index |

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

In the target architecture (post C API migration), baseline and SQ8 types will be available in the open-source variant; LVQ and LeanVec types will require the Intel binary release.

#### Example

```
FT.CREATE my_index SCHEMA vec VECTOR SVS 18
    TYPE FLOAT32
    DIM 768
    DISTANCE_METRIC COSINE
    GRAPH_MAX_DEGREE 64
    CONSTRUCTION_WINDOW_SIZE 200
    SEARCH_WINDOW_SIZE 20
    ALPHA 0.95
    COMPRESSION LEANVEC4X8
    LEANVEC_DIMS 128
    LEANVEC_TRAINING_THRESHOLD 50000
```

This creates an index that immediately accepts vectors and serves queries using FP32 storage, then after 50,000 vectors transparently trains a LeanVec projection from 768 to 128 dimensions with LVQ4x8 compression.

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

```
FT.SEARCH my_index "*=>[KNN 10 @vec $query_vec]" PARAMS 2 query_vec <blob>
```

The recall/latency trade-off is controlled by `SEARCH_WINDOW_SIZE` set at index creation time.

### RDB

The SVS runtime v0.4.0 provides `save()` / `load()` APIs that serialize the complete DynamicVamana index (graph, vector data, metadata) to a stream.

1. **Save**: An `RDBOstreamAdapter` wraps RDB chunk I/O as a `std::streambuf`, buffering at 4MB boundaries.
2. **Load**: An `RDBIstreamAdapter` provides the input stream for `DynamicVamanaIndex::load()`. The index is reconstructed with all graph edges, vector data, and compression state intact.
3. **Deferred compression state**: For LeanVec indexes below their training threshold, the pending buffer and training data are serialized alongside the index metadata. Once deferred compression lands upstream, this staging state is eliminated.

### Configuration

| Configuration | Scope | Default | Description |
|---------------|-------|---------|-------------|
| `ENABLE_SVS` | Build-time (CMake) | OFF (will default to ON on x86_64 Linux) | Whether to compile SVS support into valkey-search |

### Module API

#### SVS Runtime Integration

valkey-search integrates with SVS via Intel's runtime library, which provides:
- `DynamicVamanaIndex` — graph-based ANN index with dynamic insert/remove
- All storage backends (FP32, FP16, SQ8, LVQ, LeanVec)
- Thread-safe concurrent `add()` operations
- `save()` / `load()` for persistence
- `reconstruct_at()` for exact vector retrieval
- `get_distance()` for pairwise distance computation

#### C API (Core Operations)

The stable C API (`svs_c.h`, implemented by Intel SVS team) provides the foundation for all index operations:
- Stable ABI (C linkage, opaque handles) replacing C++ vtable interface
- Custom threadpool callback interface for integration with valkey-search's reader thread pools
- Source-buildable from the Apache-2.0 repository (FP32, FP16, SQ8)
- Memory accounting: `svs_index_get_memory_usage()` for per-index byte attribution
- Clone with recompression: `svs_index_clone_dynamic()` for deferred compression transitions

valkey-search links `libsvs_c_api.so` at build time. All graph construction, search, add/remove, and persistence operations go through this interface regardless of whether proprietary backends are loaded.

#### SharedAPI Extension (Proprietary Compression)

Proprietary compression backends (LVQ, LeanVec) are provided by a separate Valkey module (`libsearch_svs_pro.so`) that registers its capabilities via `ValkeyModule_ExportSharedAPI`. This follows the same pattern as JSON support in `src/attribute_data_type.cc`.

The pro module exports:
- `SVS_CompressStorage` — performs the actual vector recompression (FP32 → LVQ/LeanVec)
- `SVS_GetSupportedTypes` — returns the list of compression types available
- `SVS_GetStorageMemory` — reports memory usage of compressed storage for accounting

valkey-search discovers these at runtime:
```cpp
svs_compress = (SvsCompressStorageFn)ValkeyModule_GetSharedAPI(
    ctx, "SVS_CompressStorage");
```

This separation means:
- `libsearch.so` ships as fully open-source (BSD-3-Clause) with no proprietary dependencies
- Operators opt in to LVQ/LeanVec by loading the pro module (`MODULE LOAD libsearch_svs_pro.so`) at any time — no restart required
- The pro module can be upgraded independently of valkey-search

### Dependencies

| Dependency | Version | License | Purpose | Owner |
|------------|---------|---------|---------|-------|
| Intel SVS Runtime (`libsvs_runtime.so`) | 0.4.0 | Proprietary (binary-only, free license) | Current: DynamicVamana graph, all compression backends | Intel SVS team |
| Intel SVS C API (`libsvs_c_api.so`) | TBD | Apache-2.0 | Target: stable C ABI for core operations (FP32/FP16/SQ8) | Intel SVS team |
| `libsearch_svs_pro.so` | TBD | Proprietary | Optional: LVQ/LeanVec compression via SharedAPI | valkey-search team (wraps Intel kernels) |

The C API library is built from the Apache-2.0 source. AVX-512 is recommended; AVX2 is the minimum. The pro module links Intel's proprietary compression kernels and is distributed as a pre-built binary.

### Testing

- **Functional tests**: FT.CREATE with ALGORITHM SVS_VAMANA -> insert vectors -> FT.SEARCH verifies recall >= 0.95
- **Platform tests**: Verify SVS functions on x86_64 Linux; graceful fallback when ENABLE_SVS=OFF
- **Compression backend tests**: All compression types produce functional indexes with expected recall
- **RDB round-trip tests**: BGSAVE -> restart -> FT.SEARCH verifies index integrity and recall
- **Deferred compression tests**: Threshold triggers training/compression transition; search works throughout
- **Parameter validation tests**: Invalid combinations produce appropriate errors
- **Performance tests**: Search latency and recall benchmarks across compression types and dataset sizes
- **SharedAPI integration tests**: Pro module load/unload, capability discovery, refuse unload with active LVQ indexes
- **Deferred compression non-blocking test**: Verify P99 search latency stays <10ms during compression transition under concurrent query load

### Observability

`SVS_VAMANA` indexes report the following metrics:

- **Index metrics**: vector count, graph degree statistics (mean/max), memory usage (bytes), compression state
- **Search metrics**: query latency histogram (p50/p95/p99), queries per second
- **Memory accounting**: VmRSS-based tracking with per-index byte attribution via `ShardedAtomic` counters; future C API-based reporting via `svs_index_get_memory_usage()`

## Implementation Status

### Completed

| Feature | Description |
|---------|-------------|
| Runtime v0.4.0 integration | save/load, reconstruct_at, get_distance, thread-safe add |
| Memory accounting | VmRSS-based tracking, ShardedAtomic counters |
| Metrics suite | Full SVS-specific metrics in metrics framework |
| Basic index operations | Create, add, search, remove functional |

### Remaining (Feature Parity with HNSW)

| Phase | Feature | Priority | Owner | Description |
|-------|---------|----------|-------|-------------|
| 1 | ENABLE_SVS=ON default | High | valkey-search | CMake flag defaults to ON on x86_64 Linux |
| 2 | RDB persistence | Critical | valkey-search | Save/load SVS indexes across server restarts |
| 3 | Dispatch latency sampling | Medium | valkey-search | Per-query latency metrics at dispatch layer |
| 4 | Partial results on timeout | Medium | valkey-search | Return best results found so far when search times out |
| 5 | C API migration | Blocked (on Intel) | valkey-search | Migrate core ops to `libsvs_c_api.so`; Intel delivers the C API |
| 6 | SharedAPI pro module | Medium | valkey-search | Implement `libsearch_svs_pro.so` with LVQ/LeanVec via SharedAPI |
| 7 | Deferred compression | Medium | valkey-search | Copy-semantic transition orchestration with non-blocking swap |

### Known Gaps

| Gap | Impact | Mitigation |
|-----|--------|------------|
| Filtered search (C API) | Hybrid queries (vector + tag/numeric predicates) cannot use native SVS filtering | Over-fetch with inflated K + local post-filter in valkey-search; pursuing upstream contribution |

## Appendix

### References

- [Intel Scalable Vector Search — GitHub](https://github.com/intel/ScalableVectorSearch)
- [Intel SVS Documentation](https://intel.github.io/ScalableVectorSearch/)
- [SVS PR #326 — Deferred Compression](https://github.com/intel/ScalableVectorSearch/pull/326)
- [SVS C API branch](https://github.com/intel/ScalableVectorSearch/tree/dev/c-api/bindings/c)
- [ABHT23] Aguerrebere, C.; Bhati, I.; Hildebrand, M.; Tepper, M.; Willke, T.: Similarity search in the blink of an eye with compressed indices. VLDB Endowment, 16(11), 3433-3446. (2023)
- [TBAH24] Tepper, M.; Bhati, I.; Aguerrebere, C.; Hildebrand, M.; Willke, T.: LeanVec: Searching vectors faster by making them fit. TMLR, ISSN 2835-8856. (2024)
