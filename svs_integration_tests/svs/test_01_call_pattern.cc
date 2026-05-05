// svs/test_01_call_pattern.cc
//
// Purpose:
//   Twin of hnsw/test_01_call_pattern.cc. Replays the same valkey-search
//   call sequence (add → search → modify → remove → search) against the
//   SVS runtime so the SVS team can compare behavior and latency side
//   by side with the HNSW baseline.
//
// valkey-search code paths this mimics:
//   VectorSVS::AddRecordImpl            src/indexes/vector_svs.cc:230-274
//   VectorSVS::ModifyRecordImpl         src/indexes/vector_svs.cc:370-441
//   VectorSVS::RemoveRecordImpl         src/indexes/vector_svs.cc:341-367
//   VectorSVS::Search                   src/indexes/vector_svs.cc:459-610
//
// Spec sections: §3.1, §3.2, §3.3, §3.4.
//
// Reproduction:
//   1. git clone https://github.com/izaakk/valkey-search.git
//      cd valkey-search && git checkout svs-integration-spec
//   2. cmake -S . -B .build-release -DCMAKE_BUILD_TYPE=Release
//            -DENABLE_SVS=ON -G Ninja
//      ninja -C .build-release libsearch.so
//   3. cd svs_integration_tests
//      ./build_test.sh svs/test_01_call_pattern
//   4. ./svs/test_01_call_pattern
//
// Expected output on current SVS runtime 0.2.0:
//   All operations PASS. Per-vector add latency is substantially higher
//   than the HNSW baseline (tens to hundreds of ms per vector), which
//   is why valkey-search buffers 10 000 inserts internally.
//
// Expected output on the new SVS runtime:
//   Per-vector add latency within single-digit ms; search latency
//   comparable to HNSW.

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <string>

#include "svs_common.h"
#include "test_common.h"

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim = 64;
constexpr size_t kN   = 200;

int main() {
  std::printf("svs/test_01_call_pattern: dim=%zu N=%zu\n", kDim, kN);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, kDim);
  if (!st.ok()) { fail("build", st.message()); return 1; }

  // --- add ---
  section("add");
  rng(1);
  auto data = random_vecs(kN, kDim);
  auto t0 = clock_t_::now();
  for (size_t i = 0; i < kN; ++i) {
    size_t label = i;
    st = idx->add(1, &label, data.data() + i * kDim);
    if (!st.ok()) {
      fail("add", "i=" + std::to_string(i) + ": " + st.message());
      DVamana::destroy(idx); return 1;
    }
  }
  double add_ms = ms_since(t0);
  std::printf("  add: %.2f ms total, %.3f ms/vec\n", add_ms, add_ms / kN);

  // --- search ---
  section("search");
  auto q = random_vec(kDim);
  std::vector<float> dists(10);
  std::vector<size_t> labels(10, SIZE_MAX);
  auto t1 = clock_t_::now();
  st = idx->search(1, q.data(), 10, dists.data(), labels.data(),
                   nullptr, nullptr);
  if (!st.ok()) { fail("search", st.message()); DVamana::destroy(idx); return 1; }
  size_t found = 0;
  for (auto l : labels) if (l != SIZE_MAX) ++found;
  std::printf("  search: %.3f ms, %zu neighbors\n", ms_since(t1), found);
  if (found != 10) { fail("search", "expected 10"); DVamana::destroy(idx); return 1; }

  // --- modify (remove+add) ---
  section("modify");
  size_t label0 = 0;
  auto newv = random_vec(kDim);
  auto t2 = clock_t_::now();
  st = idx->remove(1, &label0);
  if (!st.ok()) { fail("modify/remove", st.message()); DVamana::destroy(idx); return 1; }
  st = idx->add(1, &label0, newv.data());
  if (!st.ok()) { fail("modify/add", st.message()); DVamana::destroy(idx); return 1; }
  std::printf("  modify: %.3f ms\n", ms_since(t2));

  // --- remove ---
  section("remove");
  size_t label1 = 1;
  auto t3 = clock_t_::now();
  st = idx->remove(1, &label1);
  if (!st.ok()) { fail("remove", st.message()); DVamana::destroy(idx); return 1; }
  std::printf("  remove: %.3f ms\n", ms_since(t3));

  // --- search-after-modify ---
  section("search-after-modify");
  st = idx->search(1, q.data(), 10, dists.data(), labels.data(),
                   nullptr, nullptr);
  if (!st.ok()) { fail("search-after-modify", st.message()); DVamana::destroy(idx); return 1; }
  found = 0;
  for (auto l : labels) if (l != SIZE_MAX) ++found;
  std::printf("  %zu neighbors\n", found);

  DVamana::destroy(idx);
  pass("svs/test_01_call_pattern");
  return 0;
}
