// hnsw/test_07_reconstruct.cc
//
// Purpose:
//   Retrieve the stored vector for a given label and compare byte-wise
//   with the originally inserted data. hnswlib stores raw vectors
//   inline in its graph nodes, so reconstruction is free.
//
// valkey-search code path this mimics:
//   VectorHNSW::GetValueImpl  src/indexes/vector_hnsw.h:92-95 —
//   returns algo_->getPoint(internal_id) directly. valkey-search uses
//   this for pre-filter evaluation, DUMP, and a few other paths.
//
// Spec section: §3.5 — GetValueImpl / reconstruct.
//
// Reproduction:
//   ./build_test.sh hnsw/test_07_reconstruct
//   ./hnsw/test_07_reconstruct
//
// Expected output:
//   Byte-exact round-trip: read-back matches input for every label.

#include <cstdio>
#include <cstring>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim = 64;
constexpr size_t kN   = 500;

int main() {
  std::printf("hnsw/test_07_reconstruct: dim=%zu N=%zu\n", kDim, kN);

  auto idx = hnsw::make_hnsw(kDim, kN + 100);
  rng(1);
  auto data = random_vecs(kN, kDim);
  for (size_t i = 0; i < kN; ++i)
    idx.algo->addPoint(data.data() + i * kDim, i);

  section("reconstruct");
  size_t mismatches = 0;
  for (size_t i = 0; i < kN; ++i) {
    char* stored = idx.algo->getDataByInternalId(
        idx.algo->label_lookup_[i]);
    if (std::memcmp(stored, data.data() + i * kDim,
                    kDim * sizeof(float)) != 0)
      ++mismatches;
  }
  std::printf("  mismatches: %zu / %zu\n", mismatches, kN);
  if (mismatches == 0) { pass("hnsw/test_07_reconstruct"); return 0; }
  fail("hnsw/test_07_reconstruct",
       "byte-exact round-trip failed");
  return 1;
}
