// hnsw/test_01_call_pattern.cc
//
// Purpose:
//   Exercise the exact valkey-search call pattern against upstream
//   hnswlib: add → search → modify(remove+add) → remove → search.
//   This is the HNSW baseline. The twin under svs/test_01_call_pattern.cc
//   replays the same pattern against the SVS runtime.
//
// valkey-search code paths this mimics:
//   VectorHNSW::AddRecordImpl    src/indexes/vector_hnsw.cc:182-204
//   VectorHNSW::ModifyRecordImpl src/indexes/vector_hnsw.cc:278-294
//   VectorHNSW::RemoveRecordImpl src/indexes/vector_hnsw.cc:296-307
//   VectorHNSW::Search           src/indexes/vector_hnsw.cc:320-361
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
//      ./build_test.sh hnsw/test_01_call_pattern
//   4. ./hnsw/test_01_call_pattern
//
// Expected output: all operations PASS; per-vector add latency is
// sub-millisecond at N=200.

#include <cstdio>
#include <cstdlib>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim = 64;
constexpr size_t kN   = 200;

int main() {
  std::printf("hnsw/test_01_call_pattern: dim=%zu N=%zu\n", kDim, kN);

  auto idx = hnsw::make_hnsw(kDim, /*initial_cap*/ kN * 2);

  // --- add ---
  section("add");
  rng(1);
  auto data = random_vecs(kN, kDim);
  auto t0 = clock_t_::now();
  for (size_t i = 0; i < kN; ++i) {
    idx.algo->addPoint(data.data() + i * kDim, /*label*/ i);
  }
  double add_ms = ms_since(t0);
  std::printf("  add: %.2f ms total, %.3f ms/vec\n", add_ms, add_ms / kN);

  // --- search ---
  section("search");
  auto q = random_vec(kDim);
  auto t1 = clock_t_::now();
  auto res = idx.algo->searchKnn(q.data(), 10);
  std::printf("  search: %.3f ms, %zu neighbors\n", ms_since(t1), res.size());
  if (res.size() != 10) { fail("search", "expected 10"); return 1; }

  // --- modify (markDelete + addPoint) ---
  section("modify");
  auto newv = random_vec(kDim);
  auto t2 = clock_t_::now();
  idx.algo->markDelete(/*label*/ 0);
  idx.algo->addPoint(newv.data(), /*label*/ 0);
  std::printf("  modify: %.3f ms\n", ms_since(t2));

  // --- remove ---
  section("remove");
  auto t3 = clock_t_::now();
  idx.algo->markDelete(/*label*/ 1);
  std::printf("  remove: %.3f ms\n", ms_since(t3));

  // --- search-after-modify ---
  section("search-after-modify");
  res = idx.algo->searchKnn(q.data(), 10);
  std::printf("  %zu neighbors\n", res.size());

  pass("hnsw/test_01_call_pattern");
  return 0;
}
