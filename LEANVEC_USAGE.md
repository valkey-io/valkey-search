# LeanVec — Quick Usage Guide

LeanVec is an SVS compression mode that learns a low-dimensional
projection (PCA-style) from a sample of your vectors and uses it as a
**primary** low-dim graph traversal structure. A secondary full-precision
representation reranks candidates. Result: smaller memory footprint and
often higher recall for high-dim embeddings (≥ 768d) than per-vector
quantizers like LVQ.

This guide covers (1) creating a LeanVec index, (2) verifying it
trained correctly, and (3) benchmarking it head-to-head against HNSW
and LVQ4X8 on the Cohere 1M dataset.

---

## 1. Prereqs

- valkey-search-svs built on the `leanvec-support` branch (or any
  branch that includes commit `48f6390`).
- valkey-server running with the freshly-built `libsearch.so`.
- Python deps: `redis numpy pyarrow polars`.

Quick build + start:

```bash
cd /home/ubuntu/projects/cee-valkey-svs
ninja -C valkey-search-svs/.build-release libsearch.so
LD_LIBRARY_PATH=$(pwd)/ScalableVectorSearch/bindings/cpp/build:$LD_LIBRARY_PATH \
  valkey/src/valkey-server \
  --loadmodule valkey-search-svs/.build-release/libsearch.so \
  --port 6399 --daemonize yes --logfile /tmp/valkey.log
```

---

## 2. Creating a LeanVec index

LeanVec adds three new compression types to the existing SVS
`COMPRESSION` enum: `LEANVEC4X4`, `LEANVEC4X8`, `LEANVEC8X8`.
The trailing digits mean (primary bits)X(secondary bits) — `LEANVEC4X8`
uses 4-bit primary and 8-bit secondary representations.

Two new keywords are required when compression is one of `LEANVEC*`:

| Keyword | Required? | Default | Meaning |
|---|---|---|---|
| `LEANVEC_DIMS <N>` | **yes** | — | Target reduced dimensionality. Must be < DIM. Typical: DIM/4 (e.g., 192 for 768-d). |
| `LEANVEC_TRAINING_THRESHOLD <N>` | optional | 10000 | Number of vectors to buffer before training matrices and constructing the index. |

### Example: Cohere-style 768-d index with LEANVEC4X8

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
pairs × 2 = 12). If you change the keyword set, update this number.

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

### Server log signposts

Tailing the server log shows the full transition:

```
Created SVS Vamana index in STAGING state (LeanVec): dim=768 ...
Training LeanVec on 10000 buffered vectors (leanvec_dims=192)
LeanVec index ready. Trained on 10000 vectors, ingested 10000 vectors. State=ready.
```

---

## 4. Benchmarking LeanVec

The benchmark scripts in this repo support all four algorithms head-to-head
on the Cohere 1M dataset. To benchmark **only** LeanVec:

### Quick single-run benchmark (15 min)

```bash
cd /home/ubuntu/projects/cee-valkey-svs

# 1. (Once per VM) acquire dataset — see SKILLS.md Step 2
ls /tmp/vectordb_bench/dataset/cohere/cohere_medium_1m/shuffle_train.parquet \
  || (echo "Run SKILLS.md Step 2 first" && exit 1)

# 2. Start a clean server
pkill -9 valkey-server 2>/dev/null; sleep 2
LD_LIBRARY_PATH=$(pwd)/ScalableVectorSearch/bindings/cpp/build:$LD_LIBRARY_PATH \
  valkey/src/valkey-server \
  --loadmodule valkey-search-svs/.build-release/libsearch.so \
  --port 6399 --daemonize yes --logfile /tmp/valkey.log
sleep 2

# 3. Load Cohere 1M into a LeanVec index
python3 load_cohere_simple.py \
  --algorithm svs \
  --compression LEANVEC4X8 \
  --leanvec-dims 192 \
  --flush-db

# 4. Tune SEARCH_WINDOW_SIZE to ~95% recall
python3 tune_recall.py --algorithm svs --num-queries 200

# 5. Pick the SWS from the table that's closest to 0.95 recall, then:
python3 -m vectordb_bench.cli.vectordbbench valkeysearchsvs \
  --host localhost --port 6399 \
  --case-type Performance768D1M \
  --graph-max-degree 64 --construction-window-size 128 \
  --search-window-size <SWS> --alpha 1.0 \
  --skip-load
```

**Heads-up**: in our 2026-05-19 runs, LeanVec needed `SWS=500`
(the sweep maximum) to reach ~95% recall, while LVQ4X8 hit it at
`SWS=150`. If `tune_recall.py` shows recall still climbing at SWS=500,
edit the sweep range in the script (line ~83) to add larger values
([600, 800, 1000]) before picking.

### Full 4-way comparison (~80 min)

For the apples-to-apples HNSW vs SVS-NONE vs LVQ4X8 vs LEANVEC4X8 run:

```bash
bash VectorDBBench/run_all_benchmarks.sh 2>&1 | tee /tmp/run_4way.log
python3 VectorDBBench/extract_4way_report.py
```

The report writer produces a markdown summary table with recall, NDCG,
peak QPS, P99 latency, load time, and **VmRSS at end of load + after
benchmark** for each of the four algorithms.

The runner script (`run_all_benchmarks.sh`), report generator
(`extract_4way_report.py`), and complete runbook (`SKILLS.md`,
`VECTORDBBENCH_SVS_BENCHMARK_RESULTS.md`) live one directory up at
`/home/ubuntu/projects/cee-valkey-svs/` — outside this repo.

---

## 5. Reference: existing 4-way result (Cohere 1M, 768d, COSINE)

Run completed 2026-05-19, m7i.2xlarge:

| Algorithm | Recall@100 | Peak QPS | P99 Latency | VmRSS (after load) | Tuned SWS/EF |
|---|---|---|---|---|---|
| HNSW M=32 | 94.24% | 303 | 4.70 ms | 7.36 GiB | EF=150 |
| SVS NONE (FP32) | 94.29% | 346 | 3.90 ms | 13.49 GiB | SWS=150 |
| SVS LVQ4X8 | 93.99% | 480 | 2.90 ms | 11.72 GiB | SWS=150 |
| **SVS LEANVEC4X8 (dims=192)** | **94.68%** | 358 | 3.40 ms | **11.48 GiB** | SWS=500 |

Full discussion (per-algorithm sections, methodology, observations) is
in `/home/ubuntu/projects/cee-valkey-svs/VECTORDBBENCH_SVS_BENCHMARK_RESULTS.md`
under "4-Way Comparison: HNSW vs SVS Variants (2026-05-19)".

---

## 6. FAQ

**Q: How do I pick `LEANVEC_DIMS`?**
Common rule of thumb: `DIM / 4`. For 768-d Cohere we used 192. Smaller
values save more memory but lose recall. Larger values approach LVQ in
both memory and recall. For your data, sweep [DIM/8, DIM/4, DIM/2] at
fixed SWS to find the recall plateau.

**Q: When can I start querying?**
Once `FT.INFO` shows `state=ready` (after the 10,000th HSET by default).
Before that, `FT.SEARCH` errors with the training-state message above.
There's no async polling API — just retry on the error.

**Q: Can I change `LEANVEC_DIMS` after creation?**
No. The matrices are computed once and frozen with the index. To change
it, drop and recreate.

**Q: What if I have fewer than 10,000 vectors total?**
Lower `LEANVEC_TRAINING_THRESHOLD` to your dataset size. There's a
correctness floor — training on too few vectors produces poor
projection matrices — but for development/testing values as low as
100-1000 are fine.

**Q: Does it persist across server restarts?**
RDB persistence is **not yet implemented** for any SVS index (returns
`UnimplementedError` on save/load). The index must be rebuilt on
restart — this includes retraining LeanVec matrices.

**Q: How do I delete vectors during staging?**
You can't. `Remove`/`Modify` return `FailedPreconditionError` while the
index is in `state=training`. Insert until threshold; then use as normal.

**Q: Where's the source for the train-then-build flow?**
`src/indexes/vector_svs.cc` — function `TrainAndBuildLeanVecIndex()`.
The train→build→add sequence matches SVS's own runtime test
(SVS's `bindings/cpp/tests/runtime_test.cpp:342-365`).
