// svs/test_04_search_during_add.cc
//
// Purpose:
//   Twin of hnsw/test_04_search_during_add.cc. Measure how much
//   concurrent add() stalls concurrent search() on the SVS runtime.
//
// valkey-search code paths this mimics:
//   - VectorSVS::Search            src/indexes/vector_svs.cc:459-610
//   - VectorSVS::AddRecordImpl     src/indexes/vector_svs.cc:230-274
//   - VectorSVS::FlushBuffer       src/indexes/vector_svs.cc:277-338
//   The wrapper takes an *exclusive* lock on every add and inline-flushes
//   on a full buffer, so in production searches are blocked for the full
//   flush duration. This test calls idx->add() directly to see what the
//   raw SVS runtime does.
//
// Spec sections: §3.1 + §3.2 — adds must not block concurrent searches.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh svs/test_04_search_during_add
//   ./svs/test_04_search_during_add
//
// Expected output on current SVS runtime 0.2.0:
//   Phase 2 p99 >> Phase 1 p99 — adds cause visible search stalls.
//
// Expected output on the new SVS runtime:
//   Phase 2 p99 within 2-3x of Phase 1.

#include <algorithm>
#include <atomic>
#include <climits>
#include <cstdio>
#include <future>
#include <thread>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim     = 128;
constexpr size_t kN       = 20000;
constexpr size_t kAddN    = 5000;
constexpr double kRunSecs = 3.0;

struct SearchStats { double p50 = 0, p99 = 0; uint64_t count = 0; };

static SearchStats run_searches(DVamana* idx, std::atomic<bool>& stop,
                                int seed) {
  std::mt19937 r(seed);
  std::uniform_real_distribution<float> d(-1.0f, 1.0f);
  std::vector<float> q(kDim);
  std::vector<float> dists(10);
  std::vector<size_t> labels(10);
  std::vector<double> latencies;
  latencies.reserve(200000);
  while (!stop.load(std::memory_order_relaxed)) {
    for (auto& x : q) x = d(r);
    auto t0 = clock_t_::now();
    idx->search(1, q.data(), 10, dists.data(), labels.data(),
                nullptr, nullptr);
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
  std::printf("svs/test_04_search_during_add: dim=%zu N=%zu addN=%zu\n",
              kDim, kN, kAddN);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, kDim);
  if (!st.ok()) { fail("build", st.message()); return 1; }

  rng(1);
  auto data = random_vecs(kN, kDim);
  std::vector<size_t> labels(kN);
  for (size_t i = 0; i < kN; ++i) labels[i] = i;
  st = idx->add(kN, labels.data(), data.data());
  if (!st.ok()) { fail("initial add", st.message()); DVamana::destroy(idx); return 1; }

  section("baseline (no concurrent adds)");
  {
    std::atomic<bool> stop{false};
    auto fut = std::async(std::launch::async, run_searches,
                          idx, std::ref(stop), 500);
    std::this_thread::sleep_for(
        std::chrono::duration<double>(kRunSecs));
    stop.store(true);
    auto s = fut.get();
    std::printf("  count=%lu p50=%.3f ms p99=%.3f ms\n",
                (unsigned long)s.count, s.p50, s.p99);
  }

  section("during concurrent adds");
  {
    std::atomic<bool> stop{false};
    auto fut = std::async(std::launch::async, run_searches,
                          idx, std::ref(stop), 501);
    std::thread writer([&]() {
      std::mt19937 r(999);
      std::uniform_real_distribution<float> d(-1.0f, 1.0f);
      std::vector<float> v(kDim);
      for (size_t i = 0; i < kAddN; ++i) {
        for (auto& x : v) x = d(r);
        size_t label = kN + i;
        idx->add(1, &label, v.data());
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

  DVamana::destroy(idx);
  pass("svs/test_04_search_during_add");
  return 0;
}
