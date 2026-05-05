# SVS Integration Tests

Standalone C++ programs that exercise the call patterns valkey-search uses
against a vector index.

**Two parallel sets:**

- `hnsw/` — each test builds against vendored upstream hnswlib (v0.8.0,
  header-only, shipped in-tree under `hnsw/third_party/hnswlib/`). These
  establish the **baseline behavior** valkey-search relies on today via
  `VectorHNSW`. No external dependencies.

- `svs/` — each test builds against the SVS runtime `libsvs_runtime.so`
  fetched by the main valkey-search build. Same scenario as the HNSW
  twin, but exercising the SVS runtime directly (without the
  valkey-search wrapper's workarounds like `pending_buffer_` or
  `raw_vectors_`).

Each `svs/test_0X_*.cc` is a direct twin of `hnsw/test_0X_*.cc`. Read
them side by side to see how the current SVS runtime differs from the
behavior valkey-search gets from hnswlib.

These tests are companions to [`../SVS_INTEGRATION_SPEC.md`](../SVS_INTEGRATION_SPEC.md).

## Dependencies

- A completed `libsearch.so` build of valkey-search, which populates
  `.build-release/_deps/svs-src/` with the SVS runtime headers and
  shared library.
- `g++` with C++20 support (required by the SVS runtime headers, which
  use `std::span`).
- `libgomp` (OpenMP).

The HNSW tests have no external deps beyond a C++20 compiler and
`libpthread`.

## One-time setup

```bash
git clone https://github.com/izaakk/valkey-search.git
cd valkey-search
git checkout svs-integration-spec

# Build valkey-search once; this fetches libsvs_runtime.so + headers.
cmake -S . -B .build-release -DCMAKE_BUILD_TYPE=Release \
      -DENABLE_SVS=ON -G Ninja
ninja -C .build-release libsearch.so
```

## Building and running

```bash
cd svs_integration_tests

# Build one test:
./build_test.sh hnsw/test_02_concurrent_search
./build_test.sh svs/test_02_concurrent_search

# Build everything:
./build_test.sh all

# Run one:
./hnsw/test_02_concurrent_search
./svs/test_02_concurrent_search
```

Read `build_test.sh` — it's ~70 lines of `g++` invocation, no CMake,
no hidden magic.

## Test index

| # | Scenario | Spec section |
|---|---|---|
| 01 | Basic call pattern: add → search → modify → remove → search | §3.1-§3.4 |
| 02 | Concurrent search scaling (1, 2, 4, 8 threads) | §3.1 |
| 03 | Concurrent add with disjoint labels | §3.2 |
| 04 | Search latency while a writer is adding | §3.1 + §3.2 |
| 05 | Per-vector add latency as N grows | §3.2, §4.1 |
| 06 | Save/load round-trip | §3.9 |
| 07 | Reconstruct stored vector by label | §3.5 |
| 08 | Compute distance to stored vector by label | §3.6 |

## Observed results on `libsvs_runtime.so.0.2.0`

Measured on m7i.2xlarge (8 vCPU), 2026-05-05. These are **current**
baseline numbers; they are what we want the next SVS runtime to
improve on.

| Test | HNSW baseline | SVS 0.2.0 | Notes |
|---|---|---|---|
| 01 call pattern | PASS (0.022 ms/vec add) | PASS (1.163 ms/vec add) | ~53× slower per-vector add |
| 02 concurrent search | PASS (5.1× scaling at n=8) | PASS at OMP=1 (4.9×); degrades at OMP=4 (baseline QPS drops 5.5×) | OMP oversubscription |
| 03 concurrent add | PASS (2.5× scaling at n=8) | **SEGFAULT** | `add()` is not thread-safe |
| 04 search during add | PASS (p99 0.12 ms, no stall) | PASS but p99 baseline is 1 ms | Writer doesn't tank p99 |
| 05 incremental add latency | PASS (~330 µs avg at N=20k) | PASS (~260 µs avg at N=5k, high variance) | Variance suggests internal batching |
| 06 save/load | PASS (10/10 match) | **FAIL (2/10 match)** | Reload returns different top-K |
| 07 reconstruct | PASS | **LINK ERROR** (no `reconstruct()` API) | Ask: §3.5 |
| 08 compute distance | PASS | **LINK ERROR** (no `compute_distance()` API) | Ask: §3.6 |

The three bolded failures/link-errors are **deliberate** — they each
correspond directly to an ask in the spec. A passing run on the new
SVS runtime means that ask has been delivered.

## Interpreting output

Tests print measurements followed by a `[PASS]` / `[FAIL]` line. The
pass/fail threshold is a **suggested** target in the source; where a
hard comparison exists (e.g., test_06's top-K match count), it's
explicit. Read the numbers — they are what will ultimately matter.
