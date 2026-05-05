// hnsw/test_02_concurrent_search.cc
//
// Purpose:
//   Measure how well concurrent searches scale on hnswlib. Builds an
//   index once, then launches 1, 2, 4, 8 threads that each call
//   searchKnn() in a tight loop for a fixed duration. Prints per-thread
//   QPS and scaling efficiency.
//
// valkey-search code path this mimics:
//   VectorHNSW::Search  src/indexes/vector_hnsw.cc:320-361 — reader-pool
//   thread holds a shared resize_mutex_ reader lock and calls
//   algo_->searchKnn(...). hnswlib's searchKnn is safe for concurrent
//   callers on the same index.
//
// Spec section: §3.1 Search — concurrent-safe search.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh hnsw/test_02_concurrent_search
//   ./hnsw/test_02_concurrent_search
//
// Expected output:
//   QPS roughly doubles from N=1 → N=2, scales near-linearly until core
//   count is saturated.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim     = 128;
constexpr size_t kN       = 20000;
constexpr double kRunSecs = 3.0;

int main() {
  std::printf("hnsw/test_02_concurrent_search: dim=%zu N=%zu run=%.0fs\n",
              kDim, kN, kRunSecs);

  auto idx = hnsw::make_hnsw(kDim, /*initial_cap*/ kN + 1000);

  // Build the index.
  rng(1);
  auto data = random_vecs(kN, kDim);
  for (size_t i = 0; i < kN; ++i)
    idx.algo->addPoint(data.data() + i * kDim, i);

  section("concurrent search");
  double baseline_qps = 0.0;
  for (size_t n_threads : {1u, 2u, 4u, 8u}) {
    std::atomic<uint64_t> total{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    auto t0 = clock_t_::now();
    for (size_t t = 0; t < n_threads; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 r(100 + t);
        std::uniform_real_distribution<float> d(-1.0f, 1.0f);
        std::vector<float> q(kDim);
        uint64_t local = 0;
        while (!stop.load(std::memory_order_relaxed)) {
          for (auto& x : q) x = d(r);
          auto res = idx.algo->searchKnn(q.data(), 10);
          (void)res;
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

  pass("hnsw/test_02_concurrent_search");
  return 0;
}
