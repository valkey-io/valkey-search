# Pure-library memory comparison: hnswlib vs. libsvs

Two standalone C++ programs that index N random FP32 vectors and report
VmRSS + smaps breakdown — **with no Valkey, no valkey-search module, no
intern store, no `raw_vectors_`**. Just the underlying libraries.

The question this answers: **is SVS's internal memory amplification
really inside the library, or is it an artifact of how valkey-search
wraps it?** Answer, documented in [`FINDINGS.md`](./FINDINGS.md): it's
inside libsvs, specifically a `prevpow2()` block-sizing rule that over-
allocates the graph by ~20× at 100K vectors.

## Files

| File | Purpose |
|---|---|
| `bench_hnsw.cc` | Indexes N vectors via upstream hnswlib, snapshots per stage |
| `bench_svs.cc` | Indexes N vectors via `DynamicVamanaIndex`, snapshots per stage |
| `common.h` | In-process snapshot helper (reads /proc/self/smaps natively) |
| `mmap_trace.c` | `LD_PRELOAD` tracer that logs `mmap`/`munmap` with stack traces |
| `build.sh` | Compiles both binaries + the tracer |
| `run.sh` | Runs the default matrix and drops snapshots under `/tmp/pure_lib_bench/` |
| `compare.py` | Parses snapshots and prints per-bucket heap-vs-mmap tables |
| `FINDINGS.md` | Root-cause analysis and ask to the SVS team |
| `traces/` | Captured mmap traces cited in `FINDINGS.md` |

## Dependencies

- **libsvs**: a completed valkey-search build populates
  `.build-release/_deps/svs-src/{include,lib}/` — same runtime
  valkey-search links against.
- **hnswlib**: the benchmark reuses the vendored upstream hnswlib under
  `svs_integration_tests/hnsw/third_party/hnswlib/`. No separate clone
  needed.
- **g++ with C++20** (SVS runtime headers use `std::span`).

## One-time setup

```bash
# From repo root:
cmake -S . -B .build-release -DCMAKE_BUILD_TYPE=Release -DENABLE_SVS=ON -G Ninja
ninja -C .build-release libsearch.so
```

## Running

```bash
cd pure_lib_bench
./build.sh
./run.sh                       # defaults N=100000 DIM=768
N=1000000 ./run.sh             # 1M variant
```

Snapshots land under `/tmp/pure_lib_bench/<N>/<config>/<stage>/`:

- `status.txt` — /proc/self/status (authoritative VmRSS, VmHWM, VmData)
- `smaps.txt`  — /proc/self/smaps (heap-vs-mmap attribution)

Then:

```bash
./compare.py 100000
```

## What each benchmark proves

Both benchmarks run in stages:

0. Process startup (module + runtime loaded).
1. Construct an empty index.
2. Generate N random FP32 vectors into a `std::vector<float>`.
3. `add()` them all.
4. Free the input `std::vector<float>` and re-snapshot — showing the
   true library-only resident cost.

HNSW's stage-4 RSS converges to `~(graph + vectors)` — hnswlib memcpy'd
the vectors into its internal slab during `add`, so freeing the caller's
buffer doesn't change RSS.

SVS's stage-4 RSS converges to an mmap region **much larger than the
stored graph contents justify**. At 100K × 768-d the measured excess is
~528 MiB of anonymous mmap — fully explained by the finding in
`FINDINGS.md`.

## Reproducing the root-cause trace

To regenerate the stack traces cited in `FINDINGS.md`:

```bash
MMAP_MIN_MB=8 MMAP_LOG=/tmp/tr.log \
  LD_PRELOAD=./mmap_trace.so ./bench_svs 10000 768 fp32
```

Every anonymous `mmap` of >= 8 MiB is logged with a 10-frame backtrace.
Resolve libsvs addresses with:

```bash
addr2line -e ../.build-release/_deps/svs-src/lib/libsvs_runtime.so.0.2.0 \
  -f -C 0x1bd976 0x1d88a0 0x1f007a 0x1f2d49
```
