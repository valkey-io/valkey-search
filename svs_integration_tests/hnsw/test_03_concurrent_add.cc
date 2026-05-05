// hnsw/test_03_concurrent_add.cc
//
// Purpose:
//   Measure how well concurrent add operations scale on hnswlib.
//   N threads each insert disjoint label ranges; we report total QPS
//   and scaling relative to N=1.
//
// valkey-search code path this mimics:
//   VectorHNSW::AddRecordImpl  src/indexes/vector_hnsw.cc:182-204 —
//   writer-pool thread holds a *shared* resize_mutex_ reader lock and
//   calls algo_->addPoint(...). Multiple writers can add concurrently
//   because hnswlib uses per-label locks internally.
//
// Spec section: §3.2 AddRecordImpl — concurrent add under shared lock.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh hnsw/test_03_concurrent_add
//   ./hnsw/test_03_concurrent_add
//
// Expected output:
//   Per-thread add rate stays roughly constant as N grows; aggregate
//   add rate scales sub-linearly (contention on hot labels/links).

#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim           = 128;
constexpr size_t kVectorsPerThd = 5000;

int main() {
  std::printf("hnsw/test_03_concurrent_add: dim=%zu per-thread=%zu\n",
              kDim, kVectorsPerThd);

  section("concurrent add");
  double baseline_rate = 0.0;
  for (size_t n_threads : {1u, 2u, 4u, 8u}) {
    // Fresh index per run so the baseline is clean.
    auto idx = hnsw::make_hnsw(
        kDim, /*initial_cap*/ n_threads * kVectorsPerThd + 1000);

    std::vector<std::thread> threads;
    auto t0 = clock_t_::now();
    for (size_t t = 0; t < n_threads; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 r(200 + t);
        std::uniform_real_distribution<float> d(-1.0f, 1.0f);
        std::vector<float> v(kDim);
        size_t label_base = t * kVectorsPerThd;
        for (size_t i = 0; i < kVectorsPerThd; ++i) {
          for (auto& x : v) x = d(r);
          idx.algo->addPoint(v.data(), label_base + i);
        }
      });
    }
    for (auto& th : threads) th.join();
    double elapsed = ms_since(t0) / 1000.0;
    double rate = (n_threads * kVectorsPerThd) / elapsed;
    if (n_threads == 1) baseline_rate = rate;
    std::printf("  n=%zu: %.0f vec/s  (scale %.2fx vs n=1)\n",
                n_threads, rate, rate / baseline_rate);
  }

  pass("hnsw/test_03_concurrent_add");
  return 0;
}
