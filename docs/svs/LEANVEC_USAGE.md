# LeanVec — Quick Usage Guide

LeanVec is an SVS compression mode that learns a low-dimensional
projection (PCA-style) from a sample of your vectors and uses it as a
**primary** low-dim graph traversal structure. A secondary full-precision
representation reranks candidates. Result: smaller memory footprint and
often higher recall for high-dim embeddings (≥ 768d) than per-vector
quantizers like LVQ.

This guide covers (1) creating a LeanVec index, (2) verifying it
trained correctly, and (3) recipes for benchmarking it. It is
self-contained — no external scripts or files are required to follow
the examples.

---

## 1. Prereqs

You need a `valkey-search` build that includes the LeanVec changes
(this branch). Standard build flow:

```bash
# From the valkey-search repo root, with submodules / FetchContent populated:
cmake -S . -B .build-release -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_SVS=ON -G Ninja
ninja -C .build-release libsearch.so
```

Then start a `valkey-server` with `libsearch.so` loaded as a module
and `libsvs_runtime.so` on the library path. The exact invocation
depends on your environment; the only LeanVec-specific concern is
that you're running a `libsearch.so` built from this branch.

Verify the binary contains the expected symbol:

```bash
nm -DC .build-release/libsearch.so | grep -i TrainAndBuildLeanVec
# expect:
# ... W valkey_search::indexes::VectorSVS<float>::TrainAndBuildLeanVecIndex()
```

---

## 2. Creating a LeanVec index

LeanVec adds three new compression types to the existing SVS
`COMPRESSION` enum:

| Compression | Primary bits | Secondary (rerank) bits |
|---|---|---|
| `LEANVEC4X4` | 4 | 4 |
| `LEANVEC4X8` | 4 | 8 |
| `LEANVEC8X8` | 8 | 8 |

Two new keywords are required when compression is one of `LEANVEC*`:

| Keyword | Required? | Default | Meaning |
|---|---|---|---|
| `LEANVEC_DIMS <N>` | **yes** | — | Target reduced dimensionality. Must be < `DIM`. Typical: `DIM / 4` (e.g., 192 for 768-d). |
| `LEANVEC_TRAINING_THRESHOLD <N>` | optional | 10000 | Number of vectors to buffer before training matrices and constructing the index. |

### Example: 768-d index with LEANVEC4X8

```
FT.CREATE myidx ON HASH PREFIX 1 doc: SCHEMA
  vec VECTOR SVS 12
    TYPE FLOAT32
    DIM 768
    DISTANCE_METRIC COSINE
    COMPRESSION LEANVEC4X8
    LEANVEC_DIMS 192
    LEANVEC_TRAINING_THRESHOLD 10000
```

The `12` after `SVS` is the count of args that follow (6 key-value
pairs × 2 = 12). If you change the keyword set, update this count.

### Common errors at creation time

| Error message | Why | Fix |
|---|---|---|
| `LEANVEC_DIMS is required (>0) when COMPRESSION is LEANVEC*.` | You used a LEANVEC compression but didn't pass `LEANVEC_DIMS`. | Add `LEANVEC_DIMS N`. |
| `LEANVEC_DIMS only valid with LEANVEC* COMPRESSION.` | You passed `LEANVEC_DIMS` with a non-LeanVec (or no) compression. | Drop `LEANVEC_DIMS` or change compression. |
| `LEANVEC_DIMS must be less than DIM.` | The reduced dim is ≥ the original. | Pick something smaller (e.g., DIM/4). |

---

## 3. The training lifecycle

Unlike LVQ (which compresses each vector independently), LeanVec needs
to **see a training set first** to compute its projection matrices.
That means right after `FT.CREATE`, the index is in a **staging** state
and **search returns an error** until enough vectors arrive.

### Lifecycle

```
state=training (FT.CREATE)
        │
        │  HSETs accumulate in pending_buffer_
        ▼
state=training (N/THRESHOLD)
        │
        │  buffer reaches LEANVEC_TRAINING_THRESHOLD
        │  → train matrices  → build LeanVec index → add buffered vectors
        ▼
state=ready
        │
        │  subsequent HSETs go through normal SVS flush path
        ▼
... ready for FT.SEARCH
```

### Verifying via FT.INFO

Look at the algorithm block of `FT.INFO`. For LeanVec indexes you'll see:

```
algorithm
  name              SVS
  graph_max_degree  64
  ...
  compression       LEANVEC4X8
  state             training        ← or "ready" once threshold is crossed
  leanvec_dims      192
  leanvec_training_threshold  10000
  training_progress 5234/10000      ← buffered vectors / threshold
```

### Search before training is ready

```
> FT.SEARCH myidx "*=>[KNN 5 @vec $q]" PARAMS 2 q <bytes> DIALECT 2
(error) Index is training (5234/10000 vectors); search is unavailable until ready.
```

This is expected. Insert more vectors until the threshold is crossed.

`Remove` and `Modify` return the same `FailedPreconditionError` while
the index is in `state=training`. Wait until `state=ready` to mutate.

### Server log signposts

Tailing the server log shows the full transition:

```
Created SVS Vamana index in STAGING state (LeanVec): dim=768 ...
Training LeanVec on 10000 buffered vectors (leanvec_dims=192)
LeanVec index ready. Trained on 10000 vectors, ingested 10000 vectors. State=ready.
```

---

## 4. Benchmarking LeanVec

You will use your own loader, tuner, and search-perf framework
(VectorDBBench, ann-benchmarks, an internal harness, etc.). The
notes below are LeanVec-specific gotchas to plug into whatever you
already have.

### 4.1 Loading the dataset

Two things to be aware of when loading vectors into a LeanVec index:

1. **Avoid Redis pipelining for the first 10K vectors.** SVS's
   per-buffer flush is synchronous on the Redis main thread; pipelined
   HSETs can deadlock when a flush takes seconds while the client has
   N pipelined commands awaiting responses. Insert one HSET at a time
   (each `r.hset(...)` waits for its response) at least until the
   threshold is crossed. After `state=ready`, larger batches behave
   normally because the per-flush latency is bounded.

2. **Don't try to search before `state=ready`.** Your loader can keep
   ingesting; just gate the benchmark phase on
   `FT.INFO myidx | <find state field> == "ready"` (or simpler:
   poll until `FT.SEARCH` stops returning the `Index is training`
   error).

A minimal loader pattern (Python, `redis-py`):

```python
import redis, numpy as np

r = redis.Redis(host="localhost", port=6379, decode_responses=False)

# Create index (LEANVEC4X8, dims=192)
r.execute_command(
    "FT.CREATE", "myidx", "ON", "HASH", "PREFIX", "1", "doc:",
    "SCHEMA", "vec", "VECTOR", "SVS", "12",
    "TYPE", "FLOAT32",
    "DIM", "768",
    "DISTANCE_METRIC", "COSINE",
    "COMPRESSION", "LEANVEC4X8",
    "LEANVEC_DIMS", "192",
    "LEANVEC_TRAINING_THRESHOLD", "10000",
)

# Insert non-pipelined
for i, v in enumerate(your_vectors):  # v: np.ndarray[float32, 768]
    r.hset(f"doc:{i}", mapping={"vec": v.astype(np.float32).tobytes()})
```

### 4.2 Tuning `SEARCH_WINDOW_SIZE`

LeanVec uses a low-dimensional projected primary representation for
graph traversal and full-precision rerank for the final top-K. The
lossier primary search means **the search window must be wider than
LVQ for comparable recall**.

In practice, on a 768-d → 192-d LeanVec4X8 index hitting ~95% recall
on a 1M-vector benchmark, `SEARCH_WINDOW_SIZE` typically lands
**substantially higher than for LVQ4X8**. If your tuner sweeps
`[50, 100, 150, 200, 250, 300, 400, 500]` and recall is still climbing
at 500, extend the sweep upward (600, 800, 1000). Don't conclude
LeanVec hit a recall ceiling without first widening the window.

You can override `SEARCH_WINDOW_SIZE` per-query at search time
(no rebuild needed), e.g.:

```
FT.SEARCH myidx "*=>[KNN 100 @vec $q SEARCH_WINDOW_SIZE 500]" \
  PARAMS 2 q <bytes> DIALECT 2
```

### 4.3 Measuring memory

`VmRSS` from `/proc/<pid>/status` is the right number to track. SVS
allocates much of its working set via `mmap` (not the Valkey
allocator), so `INFO memory used_memory` undercounts SVS by a large
margin. Sample twice — once after load completes (and any auto-flush
finishes), and once after the search workload — to capture both the
steady-state index footprint and any search-time scratch growth.

```bash
PID=$(pgrep -f 'valkey-server.*:6379' | head -1)
awk '/^VmRSS:/{print $2" kB"}' /proc/$PID/status
```

### 4.4 What to compare against

Useful baselines on the same hardware and dataset:

- **HNSW** at the same target recall — represents the algorithmic
  alternative.
- **SVS with no compression** (drop the `COMPRESSION` keyword) — shows
  the per-vector cost of the FP32 SVS baseline before compression.
- **SVS LVQ4X8** — the other compressed SVS variant. Typically faster
  per query than LeanVec but with a different recall/memory profile.

Tuning each algorithm to the **same target recall** (e.g., 95%) before
comparing QPS / latency / RSS is the apples-to-apples way to read the
results.

---

## 5. FAQ

**Q: How do I pick `LEANVEC_DIMS`?**
Common rule of thumb: `DIM / 4`. For 768-d embeddings that's 192.
Smaller values save more memory but lose recall. Larger values approach
LVQ-like memory and recall behavior. For your own data, sweep
`[DIM/8, DIM/4, DIM/2]` at fixed `SEARCH_WINDOW_SIZE` to find the
recall plateau.

**Q: When can I start querying?**
Once `FT.INFO` shows `state=ready` (after the
`LEANVEC_TRAINING_THRESHOLD`-th HSET). Before that, `FT.SEARCH` errors
with the training-state message. There's no async ready-notification
API — just retry on the error or poll `FT.INFO`.

**Q: Can I change `LEANVEC_DIMS` after creation?**
No. The projection matrices are computed once at the threshold and
frozen with the index. To change it, drop and recreate.

**Q: What if I have fewer than 10,000 vectors total?**
Lower `LEANVEC_TRAINING_THRESHOLD` to your dataset size. There is a
quality floor — training on very few vectors produces poor projection
matrices — but for development and testing values as low as 100-1000
are fine.

**Q: Does it persist across server restarts?**
RDB persistence is **not yet implemented** for any SVS index (the save
path returns `UnimplementedError`). The index must be rebuilt on
restart, including retraining LeanVec matrices.

**Q: How do I delete vectors during staging?**
You can't. `Remove` and `Modify` return `FailedPreconditionError` while
the index is in `state=training`. Insert until threshold, then use as
normal.

**Q: Where's the source for the train-then-build flow?**
`src/indexes/vector_svs.cc` — function `TrainAndBuildLeanVecIndex()`.
The train → build → add sequence matches what the SVS runtime's own
C++ test does (in the SVS source tree:
`bindings/cpp/tests/runtime_test.cpp`, around the
`DynamicVamanaIndexLeanVec::build` call site).
