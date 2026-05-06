# valkey-search Memory Footprint: Measured Breakdown

**Purpose:** Replace back-of-envelope estimates with measured numbers for how
much memory valkey-search uses across HNSW, SVS FP32, and SVS LVQ4X8, decomposed
into the three logical layers (Valkey core, valkey-search module, index graph).

**Scope:** This document is about the current `svs-iteration-0` prototype. It
does **not** include the "drop `raw_vectors_`" fix proposed in
`SVS_INTEGRATION_SPEC.md §3.5` — those numbers are measured after the fix
separately below.

**Reproducibility:** Experiment scripts live at the repo root as
[`measure_memory.sh`](./measure_memory.sh) and
[`measure_smaps.sh`](./measure_smaps.sh). Snapshots are in
`/tmp/memory_experiment/` after running.

---

## 1. Experimental setup

| Parameter | Value |
|---|---|
| Host | m7i.2xlarge (8 vCPU, 32 GB RAM) |
| Valkey | `valkey-255.255.255-dev` (local build of `valkey-io/valkey`) |
| Module | `libsearch.so` built from `izaakk/valkey-search:svs-integration-spec` with `-DENABLE_SVS=ON` |
| SVS runtime | `libsvs_runtime.so.0.2.0` (FetchContent) |
| Vectors | 100,000 × 768-dimension FP32 random |
| Raw vector data size | 100,000 × 768 × 4 = **293.0 MiB** |
| Index params | HNSW: `M=32, EF_CONSTRUCTION=128`; SVS: `GRAPH_MAX_DEGREE=32, CONSTRUCTION_WINDOW_SIZE=128` |
| Distance metric | L2 |
| Valkey config | `--save "" --appendonly no` (no persistence fork noise) |

The experiment runs three configurations in sequence, each starting from a fresh
server, then snapshots memory at three stages:

- **Stage 0 — Empty server:** valkey-server + loaded module, no data, no index.
  Establishes the baseline of "module code + runtime with nothing in it."
- **Stage 1 — Hashes loaded, no index:** 100 K `HSET doc:N vec <bytes>`
  operations executed. No `FT.CREATE` yet. Establishes the Valkey-core cost of
  just storing the vectors in hashes.
- **Stage 2 — Indexed:** `FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR <algo> ...`
  run, and backfill completed. Adds the valkey-search module cost on top of
  stage 1.

All measurements use `VmRSS` from `/proc/<pid>/status` as ground truth (actual
resident memory). `INFO memory` is also captured and reported for comparison,
with the caveat that `used_memory` only tracks Valkey-allocator allocations —
SVS uses `mmap` directly and its memory does not show up there. `/proc/<pid>/smaps`
decomposition is used to attribute RSS to heap vs. mmap regions.

---

## 2. Top-line results

### 2.1 RSS at each stage

| Config | Stage 0 (empty) | Stage 1 (hashes) | Stage 2 (indexed) |
|---|---|---|---|
| HNSW | 20.5 MiB | 341.4 MiB | **1,020.2 MiB** |
| SVS FP32 | 20.5 MiB | 341.4 MiB | **2,190.3 MiB** |
| SVS LVQ4X8 | 20.5 MiB | 341.4 MiB | **2,022.7 MiB** |

### 2.2 Deltas

| Delta | HNSW | SVS FP32 | SVS LVQ4X8 |
|---|---|---|---|
| **Stage 0 → Stage 1** (Valkey core cost of HSET'ing 100 K hashes) | 320.8 MiB | 320.8 MiB | 320.8 MiB |
| **Stage 1 → Stage 2** (module cost of adding an index over the same data) | 678.8 MiB | 1,848.9 MiB | 1,681.4 MiB |

### 2.3 Sanity checks

- The 320.8 MiB Valkey-core cost for 100 K × 768 × 4 = 293.0 MiB of raw bytes
  implies ~9.5% overhead for Valkey hash-object bookkeeping (listpack/hashtable
  structure, key strings, object headers). Matches expectations.
- `FT.INFO idx.user_indexed_memory` reports exactly 307,200,000 bytes = 293.0 MiB
  in all three configs. That's the size of the valkey-search intern store for
  the vector attribute (one FP32 copy of each vector). This is the same across
  configs because interning is algorithm-agnostic.

---

## 3. Where the memory actually lives

`INFO memory` / `used_memory` only counts allocations going through Valkey's
allocator wrappers. SVS uses `mmap` extensively and is invisible to that counter.
For a real decomposition we go to `/proc/<pid>/smaps` and bucket virtual memory
regions by kind.

At stage 2:

### 3.1 HNSW

```
bucket                                 virt_MB     rss_MB
[heap] (glibc main arena)               628.7      628.2
(anon, mmap)                            970.2      371.0
libsearch.so (code)                      19.9        9.7
other .so (code)                         12.3        6.6
valkey-server (code)                      3.2        2.7
libsvs_runtime.so (code)                 16.5        1.0
───────────────────────────────────────────────────────
total RSS                                         1019.3
```

### 3.2 SVS FP32

```
bucket                                 virt_MB     rss_MB
(anon, mmap)                           4597.2     1549.0
[heap] (glibc main arena)               650.3      649.8
libsearch.so (code)                      19.9        9.4
other .so (code)                         12.3        6.4
valkey-server (code)                      3.2        2.7
libsvs_runtime.so (code)                 16.5        2.7
───────────────────────────────────────────────────────
total RSS                                         2220.1
```

### 3.3 SVS LVQ4X8

```
bucket                                 virt_MB     rss_MB
(anon, mmap)                           4943.1     1347.8
[heap] (glibc main arena)               649.6      649.1
libsearch.so (code)                      19.9        9.4
other .so (code)                         12.3        6.4
libsvs_runtime.so (code)                 16.5        2.9
valkey-server (code)                      3.2        2.7
───────────────────────────────────────────────────────
total RSS                                         2018.5
```

### 3.4 Observations

- **Heap is almost identical across configs** (~628-650 MiB). The heap holds
  Valkey-core hash data (~330 MiB) plus valkey-search's intern store (~290 MiB
  for the FP32 copies of vectors) plus bookkeeping (~10 MiB of maps and
  pointers). HNSW's graph lives on the heap too; SVS's does not.
- **Anonymous mmap regions vary dramatically.** 371 MiB for HNSW, 1,549 MiB for
  SVS FP32, 1,348 MiB for SVS LVQ4X8. These are allocations that happen outside
  glibc's main arena — large chunks requested via `mmap`. The 371 MiB for HNSW
  is largely glibc secondary arenas and per-thread arenas (normal for a
  multi-threaded process); the SVS numbers include SVS's own graph/data
  structures, which are substantial.
- **SVS LVQ4X8 is only ~200 MiB smaller than SVS FP32** (1,348 vs 1,549 MiB in
  the mmap region). Naively LVQ4X8 should save ~3× of 293 MiB ≈ 220 MiB on the
  vector data alone — which is what we see. So LVQ4X8 really is compressing the
  graph storage, but the compression savings are exactly offset by other
  SVS-internal bookkeeping at this scale (construction workspaces, LRU caches,
  graph edges at GMD=32).

---

## 4. Layer-by-layer accounting

Combining §2 and §3, here's the decomposition each layer costs. All numbers in
MiB, 100 K × 768-d FP32.

| Layer | What it is | HNSW | SVS FP32 | SVS LVQ4X8 |
|---|---|---|---|---|
| **Valkey-core hashes** | `HSET doc:N vec <bytes>` stored in Valkey keyspace. Paid regardless of whether an index exists. | 320.8 | 320.8 | 320.8 |
| **valkey-search intern store** (on heap) | One FP32 copy of each vector, held in a `FixedSizeAllocator` slab via `InternedStringPtr`. Kept alive by `tracked_vectors_`. Same for every index kind. | ~293 | ~293 | ~293 |
| **HNSW graph payload** (heap + mmap) | hnswlib's per-node link lists + graph structure. Vector bytes are stored as *pointers* into the intern store (no copy in the graph itself). | ~386 | — | — |
| **SVS graph payload** (mmap) | SVS's own FP32 or compressed vector storage + Vamana graph + internal workspaces. | — | ~1,549 | ~1,348 |
| **`raw_vectors_`** (heap) | Prototype-only redundant FP32 copy. Same size as the intern store. | — | ~293 | ~293 |
| **Other module overhead** (maps, pointers, thread pool, code, glibc per-thread arenas, etc.) | | ~20 | ~20 | ~20 |
| **Total RSS** | | **~1,020** | **~2,190** | **~2,023** |

### 4.1 Reading the numbers

Three things to notice:

1. **Valkey-core is 320 MiB whether an index exists or not.** It's a fixed
   "cost of storing your vectors in Valkey," not something the index adds.
   Any memory-efficiency comparison between algorithms should focus on the
   incremental module cost on top of this.

2. **HNSW adds ~700 MiB of module cost** for 100 K vectors. Most of that
   (~293 MiB) is the intern store — a second FP32 copy of every vector, needed
   so hnswlib's graph can point at stable addresses. The rest (~386 MiB) is
   graph edges, glibc arena overhead, and miscellaneous module structures.

3. **SVS adds ~1,680–1,850 MiB of module cost** for the same workload — **2.4×
   to 2.7× the HNSW module cost.** Of that:
   - ~293 MiB intern store (same as HNSW)
   - ~293 MiB `raw_vectors_` redundant copy (prototype-only)
   - ~1,000–1,250 MiB SVS-runtime-internal allocations (its own graph + vector
     storage + workspaces)

### 4.2 Why `used_memory` was misleading

`INFO memory → used_memory` reports:

| Config | `used_memory` reported |
|---|---|
| HNSW stage 2 | 696.9 MiB |
| SVS FP32 stage 2 | 955.4 MiB |
| SVS LVQ4X8 stage 2 | 955.5 MiB |

SVS FP32 and SVS LVQ4X8 report identical `used_memory` even though the real RSS
differs by 168 MiB. That's because SVS's allocations go through `mmap` (not the
Valkey allocator wrapper), so `used_memory` only sees the valkey-search-owned
part of the SVS footprint — the intern store + `raw_vectors_` + metadata,
which is the same for FP32 and compressed. For any SVS memory analysis,
**use VmRSS**, not `used_memory`.

---

## 5. Projected effect of the "drop `raw_vectors_`" fix

Section §3.5 of the SVS Integration Spec proposes dropping the redundant
`raw_vectors_` copy (pure valkey-search work, no SVS-runtime change required).
Based on the measurements:

| Config | Today | After dropping `raw_vectors_` | Reduction |
|---|---|---|---|
| HNSW | 1,020 MiB | 1,020 MiB | 0 (HNSW never had the shadow) |
| SVS FP32 | 2,190 MiB | ~1,900 MiB | ~13% |
| SVS LVQ4X8 | 2,023 MiB | ~1,730 MiB | ~14% |

The fix moves SVS LVQ4X8 from "2× HNSW total RSS" to "1.7× HNSW total RSS." The
intern store still exists and still costs ~293 MiB; that's the next (larger)
optimization discussed in §3.5's roadmap.

To get SVS LVQ4X8 actually below HNSW on memory would require dropping the
intern store too — see the next section.

---

## 6. Why HNSW keeps the structural advantage

After the easy-win fix, SVS LVQ4X8 is still ~1,730 MiB versus HNSW's 1,020 MiB —
the intern store costs both the same, but SVS adds an additional ~700 MiB of
SVS-runtime-internal structures (its own graph storage, even compressed). HNSW
has no equivalent, because hnswlib stores graph edges plus 8-byte *pointers*
into the intern store rather than keeping its own copy of the vectors.

The only way to eliminate this gap is to drop the intern store for SVS indexes.
That would require:

- SVS to expose `reconstruct(label, float* out)` so valkey-search can serve
  `FT.SEARCH ... RETURN vec` without the intern store.
- SVS to expose `compute_distance(label, query, float* out)` so the pre-filter
  path can also run without the intern store.
- A user-facing policy decision about whether `FT.SEARCH ... RETURN vec` may
  return reconstructed bytes (potentially differing from `HGET ... vec`).

If all three happen, projected SVS LVQ4X8 total RSS drops from ~1,730 MiB to
~1,440 MiB — below HNSW.  If only the first two happen and valkey-search
defaults to KEEP-intern-store, memory stays at ~1,730 MiB.

Details and trade-offs: see `SVS_INTEGRATION_SPEC.md §3.5`.

---

## 7. Measured at 1M scale

Re-ran the same experiment at N=1,000,000. Raw vector data scales to
2,929.7 MiB. Same dim (768), same index parameters.

### 7.1 Stage-by-stage RSS (MiB)

| Config | Stage 0 (empty) | Stage 1 (hashes) | Stage 2 (indexed) |
|---|---|---|---|
| HNSW | 20.6 | 3,228.4 | **10,299.3** |
| SVS FP32 | 20.6 | 3,228.7 | **16,228.4** |
| SVS LVQ4X8 | 20.6 | 3,228.5 | **14,456.3** |

### 7.2 Deltas

| Delta | HNSW | SVS FP32 | SVS LVQ4X8 |
|---|---|---|---|
| Stage 0 → 1 (Valkey hashes) | 3,207.8 MiB | 3,208.1 MiB | 3,207.9 MiB |
| Stage 1 → 2 (module overhead) | 7,070.9 MiB | 12,999.7 MiB | 11,227.8 MiB |

### 7.3 smaps decomposition at stage 2

```
HNSW:                                  virt_MB     rss_MB
[heap] (glibc main arena)              6422.1     6421.2
(anon, mmap)                           4469.1     3857.8
(code + other)                          55             20
                                                   ──────
                                                   10299.3

SVS FP32:                              virt_MB     rss_MB
(anon, mmap)                          13202.0    10039.5
[heap] (glibc main arena)              6187.2     6186.2
(code + other)                          55             20
                                                   ──────
                                                   16245.7

SVS LVQ4X8:                            virt_MB     rss_MB
(anon, mmap)                          11683.9     8214.3
[heap] (glibc main arena)              6266.7     6265.8
(code + other)                          55             20
                                                   ──────
                                                   14500.1
```

### 7.4 Layer accounting at 1M

| Layer | HNSW | SVS FP32 | SVS LVQ4X8 |
|---|---|---|---|
| Valkey-core hash values (on heap) | 3,158 MiB | 3,158 MiB | 3,158 MiB |
| valkey-search intern store (on heap) | 2,930 MiB | 2,930 MiB | 2,930 MiB |
| `raw_vectors_` redundant shadow (on heap, prototype only) | — | implicit in heap+mmap mix | implicit |
| HNSW graph (heap + mmap) | ~4,191 MiB | — | — |
| SVS graph + internal workspaces (mmap) | — | 10,040 MiB | 8,214 MiB |
| Other (code, maps, arenas) | ~20 MiB | ~20 MiB | ~20 MiB |
| **Total RSS** | **~10,299 MiB** | **~16,245 MiB** | **~14,500 MiB** |

### 7.5 LVQ compression savings are more visible at 1M

| Scale | SVS FP32 total | SVS LVQ4X8 total | Saved | As % of FP32 |
|---|---|---|---|---|
| 100K | 2,190 MiB | 2,023 MiB | 168 MiB | 7.6% |
| 1M | 16,228 MiB | 14,456 MiB | **1,772 MiB** | **10.9%** |

The per-vector savings are nearly identical (1.75 KB → 1.82 KB per vector),
but the fractional saving grows because SVS's fixed-overhead portion (construction
workspaces, caches) doesn't scale with N. At even larger scales the fraction would
continue to grow.

### 7.6 Surprising finding: SVS memory amplification

At 1M scale, SVS's mmap footprint is substantially larger than the raw vector
data would predict:

| Metric | SVS FP32 | SVS LVQ4X8 |
|---|---|---|
| Raw vector data (FP32 or compressed) | 2,930 MiB FP32 | ~400 MiB LVQ4X8 |
| Plus graph edges (GMD=32, 8B per edge × N) | ~245 MiB | ~245 MiB |
| **Expected index-internal memory** | ~3,175 MiB | ~645 MiB |
| **Actual mmap bucket** | **10,040 MiB** | **8,214 MiB** |
| Amplification factor | **3.2×** | **12.7×** |

SVS is holding 3-13× more memory internally than the stored graph contents
justify. Possible causes (we have not confirmed which):

- Construction scratch space not released after backfill.
- Multiple internal copies for search-path cache locality.
- LRU/prefetch buffers.
- Per-node metadata beyond just the compressed bytes (re-quantization info,
  LVQ rescale factors per block, etc.).
- Default `blocksize_exp=30` (1 GB blocks) in `DynamicVamanaIndex::DynamicIndexParams`
  leading to rounded-up allocations.

Worth flagging to the SVS team: regardless of compression, SVS's per-index
memory amplification is the single biggest lever for reducing operator RSS.
Even if LVQ compresses the vectors 7×, the fact that SVS holds 12× that
compressed size in other internal state means the operator-visible
compression ratio is closer to 2×.

### 7.7 Projected effect of the easy-win fix at 1M

| Config | Today | After dropping `raw_vectors_` | Reduction |
|---|---|---|---|
| HNSW | 10,299 MiB | 10,299 MiB | 0 (no shadow to drop) |
| SVS FP32 | 16,228 MiB | ~13,298 MiB | ~18% |
| SVS LVQ4X8 | 14,456 MiB | ~11,526 MiB | ~20% |

SVS LVQ4X8 drops from 14.5 GiB to ~11.5 GiB — **1.1× HNSW** instead of 1.4×.
Still worse than HNSW, but much closer.

---

## 8. Reproducing these measurements

### 8.1 Prerequisites

```
# Clone and build as usual:
git clone https://github.com/izaakk/valkey-search.git
cd valkey-search && git checkout svs-integration-spec
cmake -S . -B .build-release -DCMAKE_BUILD_TYPE=Release -DENABLE_SVS=ON -G Ninja
ninja -C .build-release libsearch.so

# Need valkey binary alongside:
# (already assumed present at ../valkey/src/valkey-server in this repo layout)
```

### 8.2 Run

```
# Primary experiment: 9 snapshots (3 configs × 3 stages)
./measure_memory.sh
# ~3-5 minutes wall-clock

# Follow-up: smaps decomposition per config
./measure_smaps.sh
# ~3-5 minutes wall-clock

# Snapshots land in /tmp/memory_experiment/
ls /tmp/memory_experiment/
```

### 8.3 Parse

Snapshots are plain text. Key fields to grep:

- `VmRSS:` in each stage file = authoritative total RSS
- `used_memory:` = Valkey-allocator-tracked heap only (undercounts SVS)
- `search_used_memory_bytes:` = valkey-search module's own tracked allocations
- `user_indexed_memory:` in the `FT.INFO idx` block = intern store + vector
  attribute size (always 293 MB at 100 K × 768d FP32 in our runs)

The smaps files give the heap-vs-mmap split needed to attribute SVS-runtime
internal memory.

### 8.4 Varying the scale

```
N=1000000 DIM=768 ./measure_memory.sh      # 1M vectors
N=100000 DIM=1536 ./measure_memory.sh      # 100K × 1536d
```

---

## 9. Workflow order: backfill vs. VectorDBBench-style live ingest

All numbers in §2-§7 come from `measure_memory.sh`, which HSETs all vectors
first and *then* calls `FT.CREATE`, so the module backfills against an
already-populated keyspace. VectorDBBench does the opposite: it creates the
index on an empty keyspace first, then sends HSETs — every HSET fires a
keyspace notification that the module consumes and indexes live.

We re-ran the same 100K × 768-d experiment with the VDBBench ordering
([`measure_memory_vdbbench_order.sh`](../measure_memory_vdbbench_order.sh))
to see whether the workflow affects steady-state memory. Snapshots land in
`/tmp/memory_experiment_vdb/` so the two runs don't overwrite each other.

### 9.1 Side-by-side RSS (stage 2, 100K × 768-d)

| Config | Backfill-order `VmRSS` | VDB-order `VmRSS` | Δ |
|---|---|---|---|
| HNSW | 1,020.2 MiB | 709.4 MiB | −310.8 MiB (−30.5%) |
| SVS FP32 | 2,190.3 MiB | 1,879.7 MiB | −310.6 MiB (−14.2%) |
| SVS LVQ4X8 | 2,022.7 MiB | 1,697.7 MiB | −325.0 MiB (−16.0%) |

Live ingest ends up **~310 MiB cheaper** across every algorithm. The savings
are a constant offset, not proportional — so they're not a property of the
index but of the path the bytes took on the way in.

### 9.2 Logical memory is identical; the delta is all fragmentation

`INFO memory` at stage 2 tells a much tamer story:

| | HNSW | SVS FP32 | SVS LVQ4X8 |
|---|---|---|---|
| `used_memory` (backfill) | 696.9 MiB | 955.4 MiB | 955.5 MiB |
| `used_memory` (VDB) | 682.0 MiB | 941.1 MiB | 941.1 MiB |
| Δ `used_memory` | −14.9 MiB | −14.3 MiB | −14.4 MiB |

`used_memory` — the bytes Valkey's allocator thinks are live — is within 15 MiB
between the two orders. The data structures the module built are the same.

The ~300 MiB delta shows up only in the `RSS` − `used_memory` gap:

| | HNSW | SVS FP32 | SVS LVQ4X8 |
|---|---|---|---|
| `mem_fragmentation_bytes` (backfill) | 338 MiB | 1,294 MiB | 1,118 MiB |
| `mem_fragmentation_bytes` (VDB) | 28 MiB | 983 MiB | 793 MiB |
| `mem_fragmentation_ratio` (backfill) | 1.46 | 2.29 | 2.12 |
| `mem_fragmentation_ratio` (VDB) | 1.04 | 2.00 | 1.80 |

Backfill-order fragmentation is **10-12× higher on HNSW** and **~300 MiB
higher on SVS**. That's exactly the RSS delta in §9.1.

### 9.3 Why the backfill order fragments more

Backfill-order has two distinct allocation phases:

1. `HSET × N` populates the keyspace — Valkey core grows the heap with lots
   of 3 KiB (one per 768-d FP32 vector) hash value allocations.
2. `FT.CREATE` triggers backfill — the module reads those hashes in order,
   allocates intern-store FP32 copies, builds graph nodes.

Between phases the heap is packed with hash-value allocations. In phase 2
the module requests new large allocations (intern-store slabs, HNSW
node blocks, glibc arenas for worker threads) which don't fit in the
existing freelist holes, so glibc grows the heap further. The pages
freed by the allocator never get returned to the OS.

In the VDBBench order, every HSET is immediately followed by the module
creating its corresponding intern-store entry and graph node — the two
allocations are co-temporal, interleaved rather than stratified, and live
in the same arena pages. Less internal waste, lower RSS.

### 9.4 Implications

- **Steady-state memory is a workflow-dependent measurement.** The 1,020 MiB
  vs. 709 MiB for HNSW isn't a bug in either run; they're both true "RSS at
  steady state" numbers for the same 100K indexed vectors. The difference is
  what the allocator was asked to do on the way there.
- **`used_memory` is a more stable signal** than RSS across workflows.
  `used_memory` tracks the byte-level footprint of the data; RSS tracks the
  OS-visible cost of how fragmented the allocator got along the way.
- **For customer-facing sizing estimates**, the VDB-order numbers are the
  more realistic upper bound since customer ingestion is almost always live
  (stream-to-index), not offline-bulk-then-reindex.
- **For the `raw_vectors_` fix impact calculation** (§5), both orders give
  the same ~14% reduction because the fix removes a fixed allocation
  (the 293 MiB shadow copy) that exists in either workflow.
- **`MEMORY PURGE` probably reclaims most of the fragmentation delta.**
  We didn't run it here because we wanted the as-observed operator RSS, but
  an operator who explicitly calls `MEMORY PURGE` after a large offline
  rebuild should see both orders converge closer to the VDB-order numbers.

Raw VDB-order snapshots live in `/tmp/memory_experiment_vdb/`.

---

## 10. Snapshot files (2026-05-06, initial run)

All raw data for this document is preserved in `/tmp/memory_experiment/`
(backfill order) and `/tmp/memory_experiment_vdb/` (VDB order):

- `{hnsw,svs_fp32,svs_lvq4x8}_stage{0,1,2}_*.txt` — per-stage `INFO` / `FT.INFO` / `VmRSS` snapshots
- `{hnsw,svs_fp32,svs_lvq4x8}_smaps.txt` — smaps-based heap-vs-mmap decomposition at stage 2 (backfill run only)
- `server.log` — valkey-server stderr across the run

These are on a host, not in the repo. Re-run `measure_memory.sh` +
`measure_smaps.sh` + `measure_memory_vdbbench_order.sh` to regenerate.
