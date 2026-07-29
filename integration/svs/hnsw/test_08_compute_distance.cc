// hnsw/test_08_compute_distance.cc
//
// Purpose:
//   For a given query, compute distance to every stored vector by two
//   methods and verify they match:
//     (a) direct: dist = fstdistfunc_(query, stored_ptr)
//     (b) indirect: run searchKnn(query, N) and read back the distances
//
// valkey-search code path this mimics:
//   VectorHNSW::ComputeDistanceFromRecordImpl
//       src/indexes/vector_hnsw.cc:383-397 — calls
//       algo_->fstdistfunc_(query, algo_->getDataByInternalId(id),
//                           algo_->dist_func_param_)
//   valkey-search uses this for pre-filter scoring.
//
// Spec section: §3.6 — expose compute_distance(label, query, float* out).
//
// Reproduction:
//   ./build_test.sh hnsw/test_08_compute_distance
//   ./hnsw/test_08_compute_distance
//
// Expected output:
//   All per-label distances match searchKnn() distances exactly.

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <vector>

#include "hnsw_common.h"
#include "test_common.h"

using namespace svstest;

constexpr size_t kDim = 64;
constexpr size_t kN   = 500;

int main() {
  std::printf("hnsw/test_08_compute_distance: dim=%zu N=%zu\n", kDim, kN);

  auto idx = hnsw::make_hnsw(kDim, kN + 100);
  rng(1);
  auto data = random_vecs(kN, kDim);
  for (size_t i = 0; i < kN; ++i)
    idx.algo->addPoint(data.data() + i * kDim, i);

  auto q = random_vec(kDim);

  // Indirect: searchKnn returns distances for top-K. Ask for everyone.
  auto res = idx.algo->searchKnn(q.data(), kN);
  std::vector<float> dist_from_search(kN, 0.0f);
  while (!res.empty()) {
    dist_from_search[res.top().second] = res.top().first;
    res.pop();
  }

  section("compute_distance");
  size_t mismatches = 0;
  double max_abs_err = 0.0;
  for (size_t i = 0; i < kN; ++i) {
    char* stored = idx.algo->getDataByInternalId(idx.algo->label_lookup_[i]);
    float d_direct = idx.algo->fstdistfunc_(q.data(), stored,
                                            idx.algo->dist_func_param_);
    float d_search = dist_from_search[i];
    double diff = std::fabs(d_direct - d_search);
    if (diff > 1e-4) ++mismatches;
    if (diff > max_abs_err) max_abs_err = diff;
  }
  std::printf("  max |direct - search| = %g (mismatches=%zu)\n",
              max_abs_err, mismatches);

  if (mismatches == 0) { pass("hnsw/test_08_compute_distance"); return 0; }
  fail("hnsw/test_08_compute_distance",
       "mismatches: " + std::to_string(mismatches));
  return 1;
}
