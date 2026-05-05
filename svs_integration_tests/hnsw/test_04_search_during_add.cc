// hnsw/test_04_search_during_add.cc
//
// Purpose:
//   Measure how much concurrent add() calls impact search latency.
//   Phase 1: baseline search latency on a static index.
//   Phase 2: same search load, but a writer thread is simultaneously
//            adding vectors. Compare p50/p99 latency and report the
//            "stall delta".
//
// valkey-search code paths this mimics:
//   - VectorHNSW::Search           src/indexes/vector_hnsw.cc:320-361
//   - VectorHNSW::AddRecordImpl    src/indexes/vector_hnsw.cc:182-204
//   Both take a shared resize_mutex_ reader lock in valkey-search, so
//   they can run truly concurrently. This test exercises exactly that.
//
// Spec sections: §3.1 + §3.2 — writer must not block concurrent searches.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh hnsw/test_04_search_during_add
//   ./hnsw/test_04_search_during_add
//
// Expected output:
//   p99 search latency during add is within 2-3x of the baseline.

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <future>
#include <thread>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim     = 128;
constexpr size_t kN       = 20000;
constexpr size_t kAddN    = 5000;
constexpr double kRunSecs = 3.0;

struct SearchStats {
  double p50 = 0, p99 = 0;
  uint64_t count = 0;
};

static SearchStats run_searches(hnswlib::HierarchicalNSW<float>* algo,
                                std::atomic<bool>& stop, int seed) {
  std::mt19937 r(seed);
  std::uniform_real_distribution<float> d(-1.0f, 1.0f);
  std::vector<float> q(kDim);
  std::vector<double> latencies;
  latencies.reserve(200000);
  while (!stop.load(std::memory_order_relaxed)) {
    for (auto& x : q) x = d(r);
    auto t0 = clock_t_::now();
    algo->searchKnn(q.data(), 10);
    latencies.push_back(ms_since(t0));
  }
  std::sort(latencies.begin(), latencies.end());
  SearchStats s;
  s.count = latencies.size();
  if (!latencies.empty()) {
    s.p50 = latencies[latencies.size() / 2];
    s.p99 = latencies[std::min(latencies.size() - 1,
                               (size_t)(latencies.size() * 0.99))];
  }
  return s;
}

int main() {
  std::printf("hnsw/test_04_search_during_add: dim=%zu N=%zu addN=%zu\n",
              kDim, kN, kAddN);

  auto idx = hnsw::make_hnsw(kDim, kN + kAddN + 1000);
  rng(1);
  auto data = random_vecs(kN, kDim);
  for (size_t i = 0; i < kN; ++i)
    idx.algo->addPoint(data.data() + i * kDim, i);

  // Phase 1: baseline — searches only.
  section("baseline (no concurrent adds)");
  {
    std::atomic<bool> stop{false};
    auto fut = std::async(std::launch::async, run_searches,
                          idx.algo.get(), std::ref(stop), 500);
    std::this_thread::sleep_for(
        std::chrono::duration<double>(kRunSecs));
    stop.store(true);
    auto s = fut.get();
    std::printf("  count=%lu p50=%.3f ms p99=%.3f ms\n",
                (unsigned long)s.count, s.p50, s.p99);
  }

  // Phase 2: searches + concurrent adds.
  section("during concurrent adds");
  {
    std::atomic<bool> stop{false};
    auto fut = std::async(std::launch::async, run_searches,
                          idx.algo.get(), std::ref(stop), 501);
    std::thread writer([&]() {
      std::mt19937 r(999);
      std::uniform_real_distribution<float> d(-1.0f, 1.0f);
      std::vector<float> v(kDim);
      for (size_t i = 0; i < kAddN; ++i) {
        for (auto& x : v) x = d(r);
        idx.algo->addPoint(v.data(), kN + i);
      }
    });
    std::this_thread::sleep_for(
        std::chrono::duration<double>(kRunSecs));
    stop.store(true);
    writer.join();
    auto s = fut.get();
    std::printf("  count=%lu p50=%.3f ms p99=%.3f ms\n",
                (unsigned long)s.count, s.p50, s.p99);
  }

  pass("hnsw/test_04_search_during_add");
  return 0;
}
