// svs/test_02_concurrent_search.cc
//
// Purpose:
//   Twin of hnsw/test_02_concurrent_search.cc. Measures how concurrent
//   SVS searches scale. Tries OMP=1 (our production setting) and OMP=4
//   to show the oversubscription effect the spec describes.
//
// valkey-search code path this mimics:
//   VectorSVS::Search  src/indexes/vector_svs.cc:459-610 — reader-pool
//   thread holds index_mutex_ reader lock and calls
//   svs_index_->search(...). We re-apply omp_set_num_threads() per
//   search at src/indexes/vector_svs.cc:540-546.
//
// Spec sections: §3.1 Search, §3.10 index build / OMP pin.
//
// Reproduction:
//   ./build_test.sh svs/test_02_concurrent_search
//   ./svs/test_02_concurrent_search
//
// Expected output on current SVS runtime 0.2.0:
//   OMP=1: near-linear scaling N=1 → 8.
//   OMP=4: scaling breaks down at N=4+ due to oversubscription (each
//   client thread forks 4 OMP helpers; 4 clients × 4 helpers = 16
//   threads competing for 8 cores). See SVS_OMP_PERF_ANALYSIS.md.
//
// Expected output on the new SVS runtime:
//   Scaling stays near-linear regardless of internal thread setting,
//   or the oversubscription knob no longer needs to exist.

#include <atomic>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <omp.h>
#include <thread>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim     = 128;
constexpr size_t kN       = 20000;
constexpr double kRunSecs = 3.0;

static void run_at_omp(DVamana* idx, int omp_threads) {
  std::printf("\n[OMP_NUM_THREADS = %d]\n", omp_threads);
  double baseline_qps = 0.0;
  for (size_t n_threads : {1u, 2u, 4u, 8u}) {
    std::atomic<uint64_t> total{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    auto t0 = clock_t_::now();
    for (size_t t = 0; t < n_threads; ++t) {
      threads.emplace_back([&, t, omp_threads]() {
        // Per-thread OMP pin (valkey-search does this per search too:
        // src/indexes/vector_svs.cc:540-546).
        omp_set_num_threads(omp_threads);
        std::mt19937 r(100 + t);
        std::uniform_real_distribution<float> d(-1.0f, 1.0f);
        std::vector<float> q(kDim);
        std::vector<float> dists(10);
        std::vector<size_t> labels(10);
        uint64_t local = 0;
        while (!stop.load(std::memory_order_relaxed)) {
          for (auto& x : q) x = d(r);
          idx->search(1, q.data(), 10, dists.data(), labels.data(),
                      nullptr, nullptr);
          ++local;
        }
        total.fetch_add(local, std::memory_order_relaxed);
      });
    }
    std::this_thread::sleep_for(
        std::chrono::duration<double>(kRunSecs));
    stop.store(true);
    for (auto& th : threads) th.join();
    double elapsed = ms_since(t0) / 1000.0;
    double qps = total.load() / elapsed;
    if (n_threads == 1) baseline_qps = qps;
    double scale = qps / baseline_qps;
    std::printf("  n=%zu: %.0f qps  (scale %.2fx vs n=1)\n",
                n_threads, qps, scale);
  }
}

int main() {
  std::printf("svs/test_02_concurrent_search: dim=%zu N=%zu run=%.0fs\n",
              kDim, kN, kRunSecs);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, kDim);
  if (!st.ok()) { fail("build", st.message()); return 1; }

  // Build the index.
  rng(1);
  auto data = random_vecs(kN, kDim);
  std::vector<size_t> labels(kN);
  for (size_t i = 0; i < kN; ++i) labels[i] = i;
  st = idx->add(kN, labels.data(), data.data());
  if (!st.ok()) { fail("add", st.message()); DVamana::destroy(idx); return 1; }

  section("concurrent search");
  run_at_omp(idx, 1);
  run_at_omp(idx, 4);

  DVamana::destroy(idx);
  pass("svs/test_02_concurrent_search");
  return 0;
}
