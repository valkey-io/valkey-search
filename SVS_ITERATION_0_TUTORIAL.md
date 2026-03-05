# Adding SVS (Vamana) as a Vector Backend in Valkey-Search

A step-by-step guide to how we integrated Intel's SVS graph-based ANN index alongside HNSW and FLAT in valkey-search.

## Introduction

**What is SVS?** Scalable Vector Search — Intel's implementation of the Vamana graph-based approximate nearest neighbor algorithm. Key differentiators vs HNSW:
- **LVQ compression**: Locally-adaptive vector quantization reduces memory per vector while maintaining recall
- **Pruning rule**: `alpha * dist(selected, candidate) < dist(node, candidate)` — the alpha parameter controls graph diversity
- **Physical deletion**: SVS removes vectors from the graph (HNSW only marks deleted)

**What we built**: A third vector backend (`SVS`) that plugs into valkey-search's existing architecture. Users create SVS indexes with `FT.CREATE ... VECTOR SVS ...` and query them with standard `FT.SEARCH` KNN syntax.

---

## Prerequisites and Setup

### System Requirements
- Linux x86_64 (tested on Ubuntu 24.04)
- cmake >= 3.22, ninja-build
- C++20 compiler (gcc 12+ or clang 15+)
- Python 3.10+ with numpy and redis-py (`pip install redis numpy`)

### Step 1: Clone Repositories

```bash
mkdir -p ~/projects/cee-valkey-svs && cd ~/projects/cee-valkey-svs

# Fork and clone valkey-search
gh repo fork valkey-io/valkey-search --clone=true -- valkey-search-svs
cd valkey-search-svs
git checkout -b svs-iteration-0 1.2.0-rc2
cd ..

# Clone SVS runtime
git clone https://github.com/intel/ScalableVectorSearch.git

# Clone Valkey server
git clone https://github.com/valkey-io/valkey.git
```

### Step 2: Build SVS Runtime Library

```bash
cd ScalableVectorSearch/bindings/cpp
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
# Produces: libsvs_runtime.so
cd ~/projects/cee-valkey-svs
```

### Step 3: Build Valkey Server

```bash
cd valkey && make -j$(nproc)
# Produces: valkey/src/valkey-server and valkey/src/valkey-cli
cd ..
```

### Step 4: Build valkey-search with SVS

```bash
# Install protoc if not available
# Download protoc v29.0: https://github.com/protocolbuffers/protobuf/releases

cmake -S valkey-search-svs -B valkey-search-svs/.build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_SVS=ON \
  -G Ninja

ninja -C valkey-search-svs/.build-release libsearch.so
# Produces: valkey-search-svs/.build-release/libsearch.so (~140MB)
```

If cmake can't find SVS runtime automatically (FetchContent fails), specify paths manually:
```bash
cmake -S valkey-search-svs -B valkey-search-svs/.build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_SVS=ON \
  -DSVS_RUNTIME_INCLUDE_DIR=$(pwd)/ScalableVectorSearch/bindings/cpp/include \
  -DSVS_RUNTIME_LIB=$(pwd)/ScalableVectorSearch/bindings/cpp/build/libsvs_runtime.so \
  -G Ninja
```

### Step 5: Start the Server

```bash
LD_LIBRARY_PATH=$(pwd)/ScalableVectorSearch/bindings/cpp/build:$LD_LIBRARY_PATH \
  valkey/src/valkey-server \
  --loadmodule valkey-search-svs/.build-release/libsearch.so \
  --port 6399 \
  --save ""

# In another terminal:
valkey/src/valkey-cli -p 6399 PING
# → PONG
```

---

## Validation: End-to-End Test

### Test 1: Create an SVS Index

```bash
valkey/src/valkey-cli -p 6399 FT.CREATE idx ON HASH PREFIX 1 "doc:" \
  SCHEMA vec VECTOR SVS 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2
# → OK
```

### Test 2: Insert Vectors (via Python)

```python
import redis, struct, numpy as np

r = redis.Redis(host='127.0.0.1', port=6399)

# Insert doc:1 with vector [1, 2, 3, 4]
vec1 = np.array([1, 2, 3, 4], dtype=np.float32).tobytes()
r.hset("doc:1", mapping={"vec": vec1})

# Insert doc:2 with vector [5, 6, 7, 8]
vec2 = np.array([5, 6, 7, 8], dtype=np.float32).tobytes()
r.hset("doc:2", mapping={"vec": vec2})
```

### Test 3: KNN Search

```python
from redis.commands.search.query import Query

query_vec = np.array([1, 2, 3, 4], dtype=np.float32).tobytes()

q = Query("*=>[KNN 2 @vec $blob]").return_fields("__vec_score").paging(0, 2).dialect(2)
result = r.ft("idx").search(q, query_params={"blob": query_vec})

for doc in result.docs:
    print(f"  {doc.id}: distance={doc.__vec_score}")
# → doc:1: distance=0         (exact match)
# → doc:2: distance=64        (L2: 4*(4²) = 64)
```

### Test 4: SEARCH_WINDOW_SIZE Override

```python
q = Query("*=>[KNN 2 @vec $blob SEARCH_WINDOW_SIZE 50]") \
    .return_fields("__vec_score").paging(0, 2).dialect(2)
result = r.ft("idx").search(q, query_params={"blob": query_vec})
# Same results, but searched with window_size=50 instead of default 10
```

### Test 5: COSINE Metric

```bash
valkey/src/valkey-cli -p 6399 FT.CREATE idx_cos ON HASH PREFIX 1 "cos:" \
  SCHEMA vec VECTOR SVS 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC COSINE
```

### Test 6: LVQ4X8 Compression

```bash
valkey/src/valkey-cli -p 6399 FT.CREATE idx_lvq ON HASH PREFIX 1 "lvq:" \
  SCHEMA vec VECTOR SVS 8 TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2 COMPRESSION LVQ4X8
```

### Test 7: FT.INFO

```bash
valkey/src/valkey-cli -p 6399 FT.INFO idx
# Shows: algorithm=SVS, graph_max_degree=64, compression=NONE, etc.
```

---

## Running the Benchmark

### Setup

```bash
pip install redis numpy  # if not already installed
```

### Run

```bash
# From the project root:
python3 -u valkey-search-svs/benchmark_svs.py \
  --num-vectors 50 --num-queries 20 --dim 4 --k 5

# For larger tests (warning: SVS insert is very slow at 128-dim):
python3 -u valkey-search-svs/benchmark_svs.py \
  --num-vectors 50 --num-queries 20 --dim 128 --k 10
```

The benchmark runs HNSW, SVS FP32, and SVS LVQ4X8 sequentially, printing a comparison table at the end.

### VectorDBBench Client

We also created a VectorDBBench-compatible Valkey client at:
```
VectorDBBench/vectordb_bench/backend/clients/valkey_search/
├── __init__.py
├── config.py          # ValkeySearchHNSWConfig, ValkeySearchSVSConfig
└── valkey_search.py   # ValkeySearch client (supports both algorithms)
```

This client can be used with the VectorDBBench framework for standardized benchmarks once the SVS bulk loading issue is resolved.

---

## Phase 1: Making FT.CREATE Work

### 1.1 Protobuf Schema

Added SVS-specific types to `index_schema.proto`:

```protobuf
enum SVSCompressionType {
  SVS_COMPRESSION_NONE = 0;
  SVS_COMPRESSION_FP16 = 1;
  SVS_COMPRESSION_LVQ4 = 2;
  SVS_COMPRESSION_LVQ8 = 3;
  SVS_COMPRESSION_LVQ4X4 = 4;
  SVS_COMPRESSION_LVQ4X8 = 5;
}

message SVSVamanaAlgorithm {
  uint32 graph_max_degree = 1;
  uint32 construction_window_size = 2;
  float alpha = 3;
  uint32 search_window_size = 4;
  SVSCompressionType compression = 5;
}
```

**HNSW comparison**: HNSW has 3 parameters (m, ef_construction, ef_runtime). SVS has 5 + compression selection — more knobs to tune.

### 1.2 Index Type Registration

Added `kSVS` to the `IndexerType` enum and created `IsVectorIndexType()` helper to replace scattered `kHNSW || kFlat` checks:

```cpp
enum class IndexerType { kVector, kText, kNumeric, kTag, kHNSW, kFlat, kSVS };

inline bool IsVectorIndexType(IndexerType type) {
  return type == IndexerType::kVector || type == IndexerType::kHNSW ||
         type == IndexerType::kFlat || type == IndexerType::kSVS;
}
```

### 1.3 FT.CREATE Parser

The SVS parser follows the same pattern as HNSW. User command:
```
FT.CREATE idx ON HASH SCHEMA vec VECTOR SVS 14
  TYPE FLOAT32 DIM 128 DISTANCE_METRIC L2
  GRAPH_MAX_DEGREE 64 CONSTRUCTION_WINDOW_SIZE 128
  SEARCH_WINDOW_SIZE 10 ALPHA 1.2
```

| SVS Parameter | Default | HNSW Equivalent |
|--------------|---------|-----------------|
| GRAPH_MAX_DEGREE | 64 | M (default 16) |
| CONSTRUCTION_WINDOW_SIZE | 128 | EF_CONSTRUCTION (default 200) |
| SEARCH_WINDOW_SIZE | 10 | EF_RUNTIME (default 10) |
| ALPHA | 1.2 | — (no equivalent) |
| COMPRESSION | NONE | — (no equivalent) |

**Alpha** controls pruning aggressiveness: `prune if alpha * dist(selected, candidate) < dist(node, candidate)`. Higher alpha keeps more edges. Must be <= 1.0 for IP/COSINE metrics.

### 1.4 VectorSVS Skeleton

`VectorSVS<T>` inherits from `VectorBase` (same as `VectorHNSW<T>`) and implements all pure virtual methods. The `Create()` factory calls `DynamicVamanaIndex::build()` to construct an empty SVS index.

### 1.5 Build System

SVS is conditionally compiled via `ENABLE_SVS` cmake flag. When disabled, no SVS code is compiled and `FT.CREATE ... SVS` returns an error.

```cmake
if(ENABLE_SVS)
  target_compile_definitions(vector_base PUBLIC ENABLE_SVS)
  # ... vector_svs target + SVS runtime linking
endif()
```

### 1.6 Index Factory

Added SVS case in `IndexFactory()`, guarded by `#ifdef ENABLE_SVS`:

```cpp
#ifdef ENABLE_SVS
case data_model::VectorIndex::kSvsVamanaAlgorithm:
  return VectorSVS<float>::Create(vector_index, identifier, data_type);
#endif
```

**Validation**: `FT.CREATE idx ON HASH SCHEMA vec VECTOR SVS 6 TYPE FLOAT32 DIM 4 DISTANCE_METRIC L2` → OK

---

## Phase 2: Making Vector Insertion Work

### 2.1 AddRecordImpl

HNSW calls `algo_->addPoint()` which auto-resizes on overflow. SVS calls `svs_index_->add(1, &label, data)` — no resize needed (dynamic index grows automatically), but we maintain a `raw_vectors_` map for data retrieval.

```cpp
absl::Status VectorSVS<T>::AddRecordImpl(uint64_t internal_id,
                                          absl::string_view record) {
  try {
    absl::MutexLock lock(&index_mutex_);
    raw_vectors_[internal_id] = std::vector<char>(record.begin(), record.end());
    size_t label = static_cast<size_t>(internal_id);
    auto status = svs_index_->add(1, &label,
        reinterpret_cast<const float*>(record.data()));
    if (!status.ok()) {
      raw_vectors_.erase(internal_id);
      return absl::InternalError(...);
    }
    ++num_elements_;
    return absl::OkStatus();
  } catch (const std::exception& e) {
    raw_vectors_.erase(internal_id);
    return absl::InternalError(...);
  }
}
```

### 2.2 RemoveRecordImpl

HNSW uses `algo_->markDelete()` (soft delete — vector stays in graph). SVS uses `svs_index_->remove()` (physical removal from graph).

### 2.3 ModifyRecordImpl

HNSW does `markDelete + addPoint` under a reader lock (hnswlib handles atomicity internally). SVS needs an exclusive lock because `remove() + add()` are separate API calls. Includes full rollback logic if `add()` fails after `remove()`.

### 2.4 Vector Tracking & Distance Computation

Since SVS doesn't expose `reconstruct()` or `compute_distance()`, we maintain:
- `raw_vectors_` — FP32 copy of every vector for `GetValueImpl()` and distance computation
- `tracked_vectors_` — interned string pointers for `IsVectorMatch()`
- hnswlib `space_` — distance function for `ComputeDistanceFromRecordImpl()`

These are **temporary for Iteration 0** and will be replaced when SVS exposes the needed APIs.

---

## Phase 3: Making Search Work

### 3.1 Search Method

SVS search fills flat arrays (unlike HNSW which returns a priority queue directly):

```cpp
std::vector<float> distances(k);
std::vector<size_t> labels(k);
svs_index_->search(1, query_data, k, distances.data(), labels.data(),
                   params_ptr, svs_filter.get());
```

Key differences from HNSW search:
- **Filter adapter**: `SVSIDFilterAdapter` bridges hnswlib's `BaseFilterFunctor` to SVS's `IDFilter`
- **Result conversion**: Flat arrays → priority queue for `CreateReply()`
- **Sentinel filtering**: Skip `SIZE_MAX` labels and infinity distances for filtered searches
- **No cancellation**: SVS search is non-interruptible (post-search token check only)
- **COSINE normalization**: Same pattern as HNSW — normalize query, search with IP metric

### 3.2 SEARCH_WINDOW_SIZE

Per-query runtime parameter following the `EF_RUNTIME` pattern:

```
FT.SEARCH idx "*=>[KNN 10 @vec $blob SEARCH_WINDOW_SIZE 50]"
  PARAMS 2 blob <binary> DIALECT 2
```

| Stage | EF_RUNTIME (HNSW) | SEARCH_WINDOW_SIZE (SVS) |
|-------|-------------------|--------------------------|
| Parse | `ParseKnnInner()` | `ParseKnnInner()` |
| Substitute | `SubstituteParam()` | `SubstituteParam()` |
| Validate | 1 to configurable max | 1 to 10000 |
| Pass to search | `VectorHNSW::Search(ef)` | `VectorSVS::Search(sws)` |

---

## Phase 4: Benchmark

### Results (50 vectors, 4-dim, L2, K=5)

| Metric | HNSW | SVS FP32 | SVS LVQ4X8 |
|--------|------|----------|------------|
| Index create | 4.0ms | 2.6ms | 29.7ms |
| Insert rate | 25 vec/s | 22 vec/s | 22 vec/s |
| Search p50 | 0.83ms | **0.21ms** | 0.49ms |
| Search mean | 0.82ms | **0.38ms** | 0.74ms |
| QPS (serial) | ~1,220 | **~2,630** | ~1,350 |
| Recall@5 | 1.0000 | 1.0000 | 1.0000 |

**Caveat**: 50 vectors is too small for meaningful ANN comparison. Perfect recall is expected at this scale. Larger benchmarks blocked by SVS insert performance (see Learnings document).

---

## Summary and Next Steps

### What Works
- Full FT.CREATE → HSET → FT.SEARCH pipeline for SVS
- L2, IP, COSINE distance metrics
- LVQ4X8 compression
- Per-query SEARCH_WINDOW_SIZE tuning
- Pre-filter path with SVS distance computation

### What's Blocked
- **Meaningful benchmarks** — SVS per-vector insert is ~0.1 vec/s at 128-dim
- **RDB persistence** — not implemented
- **Memory efficiency** — raw_vectors_ doubles memory footprint

### Iteration 1 Priorities
1. **Bulk vector loading** — buffer + build instead of per-vector add (critical path)
2. **Replace raw_vectors_** with SVS reconstruct/compute_distance
3. **RDB persistence**
4. **Run VectorDBBench at scale** — SIFT-128 10K, OpenAI-1536 50K
