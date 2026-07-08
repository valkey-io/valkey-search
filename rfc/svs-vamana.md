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

#### Target Model: C API `.so` Swap (Recommended)

```
valkey-server
  └── MODULE LOAD libsearch.so
         └── DT_NEEDED: libsvs_c_api.so (one of two variants)

Variant A (open-source, default):
  libsvs_c_api.so — built without SVS_RUNTIME_ENABLE_LVQ_LEANVEC
  Supports: FP32, FP16, SQ8
  svs_storage_create_lvq() → SVS_ERROR_NOT_IMPLEMENTED

Variant B (proprietary, opt-in):
  libsvs_c_api.so — built with SVS_RUNTIME_ENABLE_LVQ_LEANVEC + private headers
  Supports: FP32, FP16, SQ8, LVQ4/8, LeanVec
  svs_storage_create_lvq() → success
```

The C API (`svs_c.h`, Apache-2.0) is under development on the [dev/c-api branch](https://github.com/intel/ScalableVectorSearch/tree/dev/c-api/bindings/c). It provides:
- Stable C ABI across compiler versions and platforms
- `extern "C"` symbols only (via `-fvisibility=hidden` + explicit `SVS_API` marking)
- Proprietary algorithms controlled by compile-time defines (`SVS_RUNTIME_ENABLE_LVQ_LEANVEC`)
- Both variants share identical SONAME (`libsvs_c_api.so.0`) and symbol table — they differ only in runtime behavior

**Migration path:**
1. valkey-search switches from `svs::svs_runtime` to `libsvs_c_api.so`
2. `vector_svs.cc` calls C functions (`svs_index_build_dynamic`, `svs_index_search`, etc.) instead of C++ vtable methods
3. Memory accounting calls the C API: `svs_index_get_memory_usage()`
4. Intel publishes two variants of the tarball: open-source (default) and proprietary

#### Alternative Approaches Considered

| Dimension | Current (Runtime .so) | SharedAPI (Module-to-Module) | dlopen Plugin | C API .so Swap |
|-----------|----------------------|------------------------------|---------------|----------------|
| Deployment files | 2 | 3+ | 2 + plugins | 2 |
| Operator complexity | Low | Medium | Medium | Low |
| Hot-pluggable | No | Yes | Partial (load only) | No (restart) |
| ABI stability | C++ vtable (fragile) | void* (manual version) | C function ptrs (stable) | C (stable) |
| Memory accounting | Needs external contract | Unified (same process) | Unified (same process) | Via C API |
| Licensing boundary | Build artifact | Module boundary | .so file boundary | .so file boundary |
| Existing precedent | Current model | JSON integration in valkey-search | Common pattern | dev/c-api branch |
| Multi-vendor support | No | Yes (multiple modules) | Yes (multiple plugins) | No (single .so) |

**Approach 2 (SharedAPI)** uses `ValkeyModule_GetSharedAPI` / `ValkeyModule_ExportSharedAPI` (precedent: JSON integration in `src/attribute_data_type.cc`) to let a separate `libsearch_svs_pro.so` module register proprietary algorithms at load time. This is viable if hot-pluggability or multi-tenant licensing is needed.

**Approach 3 (dlopen Plugin)** scans a `--svs-plugin-dir` at module load time, loading `.so` plugins that register algorithms via a versioned C function-pointer contract. This gives the module full control over plugin lifecycle without depending on Valkey's module management.

Both alternative approaches are deferred — the C API swap (Approach 4) has the lowest integration risk and the upstream infrastructure already exists.

### Deferred Compression

Traditional compressed vector indexes require a training phase. SVS implements deferred compression:

1. The index starts with FP32 or FP16 storage regardless of target compression type.
2. Queries are served immediately using the uncompressed representation.
3. When live vector count reaches `LEANVEC_TRAINING_THRESHOLD`, the SVS runtime trains the compression model and swaps the data backend.
4. The graph structure, ID translator, and entry point are preserved — only the data storage layer changes.

This is exposed in `FT.INFO` as a `state` field: `"training"` while below threshold, `"ready"` after compression has been applied.

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

#### C API Migration (Target Architecture)

The stable C API (`svs_c.h`) will provide:
- Stable ABI (C linkage, opaque handles) replacing C++ vtable interface
- Custom threadpool callback interface for integration with valkey-search's reader thread pools
- Source-buildable library enabling the swappable deployment model
- Runtime capability detection: `svs_storage_is_supported(kind)` for graceful handling of missing backends
- Memory accounting: `svs_index_get_memory_usage()` for per-index byte attribution

#### Proprietary Algorithm Separation

The C API build system controls algorithm availability:

```cmake
if(SVS_RUNTIME_ENABLE_LVQ_LEANVEC)
    target_compile_definitions(svs_c_api PRIVATE SVS_RUNTIME_ENABLE_LVQ_LEANVEC)
    target_compile_definitions(svs_c_api PRIVATE SVS_LVQ_HEADER="${SVS_LVQ_HEADER}")
    target_compile_definitions(svs_c_api PRIVATE SVS_LEANVEC_HEADER="${SVS_LEANVEC_HEADER}")
else()
    # Open-source only — LVQ/LeanVec calls return NOT_IMPLEMENTED
endif()
```

When `SVS_RUNTIME_ENABLE_LVQ_LEANVEC` is OFF, calling `svs_storage_create_lvq()` returns `SVS_ERROR_NOT_IMPLEMENTED`. valkey-search translates this to a user-facing error: `"LVQ compression requires the Intel SVS proprietary runtime"`.

### Dependencies

| Dependency | Version | License | Purpose |
|------------|---------|---------|---------|
| Intel SVS Runtime (`libsvs_runtime.so`) | 0.4.0 | Proprietary (binary-only, free license) | Current: DynamicVamana graph, all compression backends |
| Intel SVS C API (`libsvs_c_api.so`) | TBD | Apache-2.0 (open-source variant) or Proprietary (full variant) | Target: stable C ABI, swappable algorithm set |

The runtime is fetched as a pre-built x86_64 Linux binary at build time. AVX-512 is recommended; AVX2 is the minimum.

### Testing

- **Functional tests**: FT.CREATE with ALGORITHM SVS_VAMANA -> insert vectors -> FT.SEARCH verifies recall >= 0.95
- **Platform tests**: Verify SVS functions on x86_64 Linux; graceful fallback when ENABLE_SVS=OFF
- **Compression backend tests**: All compression types produce functional indexes with expected recall
- **RDB round-trip tests**: BGSAVE -> restart -> FT.SEARCH verifies index integrity and recall
- **Deferred compression tests**: Threshold triggers training/compression transition; search works throughout
- **Parameter validation tests**: Invalid combinations produce appropriate errors
- **Performance tests**: Search latency and recall benchmarks across compression types and dataset sizes
- **C API variant tests**: Open-source variant returns NOT_IMPLEMENTED for LVQ/LeanVec; proprietary variant succeeds

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

| Phase | Feature | Priority | Description |
|-------|---------|----------|-------------|
| 1 | ENABLE_SVS=ON default | High | CMake flag defaults to ON on x86_64 Linux |
| 2 | RDB persistence | Critical | Save/load SVS indexes across server restarts |
| 3 | Dispatch latency sampling | Medium | Per-query latency metrics at dispatch layer |
| 4 | Partial results on timeout | Medium | Return best results found so far when search times out |
| 5 | C API migration | Blocked | Migrate to stable C API; enables open-source variant and swappable library model |
| 6 | Deferred compression | Medium | Transparent FP32 -> compressed transition |

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
