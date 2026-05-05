// svs/test_03_concurrent_add.cc
//
// Purpose:
//   Twin of hnsw/test_03_concurrent_add.cc. Measures concurrent add()
//   scaling on the SVS runtime when multiple threads insert disjoint
//   label ranges.
//
// valkey-search code path this mimics:
//   The valkey-search wrapper at src/indexes/vector_svs.cc:230-274
//   currently takes an *exclusive* index_mutex_ around every add,
//   forcing concurrent HSETs to serialize. This test bypasses that
//   wrapper and calls idx->add() directly from multiple threads so the
//   SVS team can see whether the underlying runtime itself can handle
//   concurrent adds.
//
// Spec section: §3.2 AddRecordImpl — concurrent add.
//
// Reproduction:
//   (Assumes valkey-search is already built so libsvs_runtime.so and
//   headers exist under .build-release/_deps/svs-src/. If not, see
//   svs_integration_tests/README.md 'One-time setup'.)
//
//   ./build_test.sh svs/test_03_concurrent_add
//   ./svs/test_03_concurrent_add
//
// Expected output on current SVS runtime 0.2.0:
//   **SEGFAULT.** DynamicVamanaIndex::add() is not safe to call from
//   multiple threads concurrently, even with disjoint labels. This is
//   why valkey-search serializes all writes with an *exclusive*
//   index_mutex_ around add() in VectorSVS::AddRecordImpl
//   (src/indexes/vector_svs.cc:230-274).
//
// Expected output on the new SVS runtime:
//   No crashes; aggregate add rate scales roughly with thread count,
//   at least up to core count.

#include <atomic>
#include <climits>
#include <cstdio>
#include <thread>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim           = 128;
constexpr size_t kVectorsPerThd = 500;

int main() {
  std::printf("svs/test_03_concurrent_add: dim=%zu per-thread=%zu\n",
              kDim, kVectorsPerThd);

  section("concurrent add");
  double baseline_rate = 0.0;
  for (size_t n_threads : {1u, 2u, 4u, 8u}) {
    DVamana* idx = nullptr;
    auto st = svs_::build_svs(&idx, kDim);
    if (!st.ok()) { fail("build", st.message()); return 1; }

    std::atomic<bool> any_err{false};
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
          size_t label = label_base + i;
          auto s = idx->add(1, &label, v.data());
          if (!s.ok()) {
            any_err.store(true);
            std::fprintf(stderr, "thread=%zu i=%zu: %s\n", t, i, s.message());
            return;
          }
        }
      });
    }
    for (auto& th : threads) th.join();
    double elapsed = ms_since(t0) / 1000.0;
    double rate = (n_threads * kVectorsPerThd) / elapsed;
    if (n_threads == 1) baseline_rate = rate;
    const char* err_tag = any_err.load() ? "  [ERRORS]" : "";
    std::printf("  n=%zu: %.0f vec/s  (scale %.2fx vs n=1)%s\n",
                n_threads, rate, rate / baseline_rate, err_tag);
    DVamana::destroy(idx);
  }

  pass("svs/test_03_concurrent_add");
  return 0;
}
