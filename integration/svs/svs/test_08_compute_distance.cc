// svs/test_08_compute_distance.cc
//
// Purpose:
//   Twin of hnsw/test_08_compute_distance.cc. For a given query,
//   compute distance to every stored vector via a hypothetical
//   compute_distance() API on DynamicVamanaIndex, and cross-check
//   against the distance returned by a full-N search().
//
// Status on current SVS runtime 0.2.0:
//   **This test is expected to fail at link time.** DynamicVamanaIndex
//   has no compute_distance() method. The link error IS the ask
//   (spec §3.6).
//
// valkey-search code path this mimics:
//   VectorSVS::ComputeDistanceFromRecordImpl
//       src/indexes/vector_svs.cc:640-658 — currently uses raw_vectors_
//       + an hnswlib SpaceInterface to avoid calling SVS. We want to
//       replace that with a native SVS call.
//
// Spec section: §3.6 — expose compute_distance(label, query, float*).
//
// Reproduction:
//   ./build_test.sh svs/test_08_compute_distance
//   # On current SVS 0.2.0:
//   #   undefined reference to svs::runtime::v0::DynamicVamanaIndex::compute_distance
//   # On the new SVS runtime:
//   ./svs/test_08_compute_distance
//
// Expected output (new runtime): |direct - search| below 1e-4 for FP32;
// for compressed storage, the exact error bound depends on the
// quantizer (typically < 1% of distance magnitude).

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

// Forward declaration of the desired API. Absent from 0.2.0; will link
// once the SVS team adds it.
namespace svs { namespace runtime { namespace v0 {
Status dynamic_vamana_compute_distance(const DynamicVamanaIndex* idx,
                                       size_t label,
                                       const float* query,
                                       float* out) noexcept;
}}}

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim = 64;
constexpr size_t kN   = 200;

int main() {
  std::printf("svs/test_08_compute_distance: dim=%zu N=%zu\n", kDim, kN);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, kDim);
  if (!st.ok()) { fail("build", st.message()); return 1; }

  rng(1);
  auto data = random_vecs(kN, kDim);
  std::vector<size_t> labels(kN);
  for (size_t i = 0; i < kN; ++i) labels[i] = i;
  st = idx->add(kN, labels.data(), data.data());
  if (!st.ok()) { fail("add", st.message()); DVamana::destroy(idx); return 1; }

  auto q = random_vec(kDim);

  // Indirect: full-N search to get distance for every label.
  std::vector<float> sdists(kN);
  std::vector<size_t> slabels(kN);
  st = idx->search(1, q.data(), kN, sdists.data(), slabels.data(),
                   nullptr, nullptr);
  if (!st.ok()) { fail("search", st.message()); DVamana::destroy(idx); return 1; }
  std::vector<float> dist_from_search(kN, 0.0f);
  for (size_t i = 0; i < kN; ++i)
    if (slabels[i] != SIZE_MAX) dist_from_search[slabels[i]] = sdists[i];

  section("compute_distance");
  size_t mismatches = 0;
  double max_abs_err = 0.0;
  for (size_t i = 0; i < kN; ++i) {
    float d_direct = 0.0f;
    auto s = ::svs::runtime::v0::dynamic_vamana_compute_distance(
        idx, /*label*/ i, q.data(), &d_direct);
    if (!s.ok()) {
      fail("compute_distance",
           "i=" + std::to_string(i) + ": " + s.message());
      DVamana::destroy(idx);
      return 1;
    }
    double diff = std::fabs(d_direct - dist_from_search[i]);
    if (diff > 1e-4) ++mismatches;
    if (diff > max_abs_err) max_abs_err = diff;
  }
  std::printf("  max |direct - search| = %g (mismatches=%zu)\n",
              max_abs_err, mismatches);

  DVamana::destroy(idx);
  if (mismatches == 0) { pass("svs/test_08_compute_distance"); return 0; }
  fail("svs/test_08_compute_distance",
       "mismatches: " + std::to_string(mismatches));
  return 1;
}
