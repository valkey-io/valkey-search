// svs/test_07_reconstruct.cc
//
// Purpose:
//   Add a known vector to the index, then read it back via a
//   *hypothetical* reconstruct() API on DynamicVamanaIndex, and verify
//   the round-trip matches within quantization tolerance.
//
// Status on current SVS runtime 0.2.0:
//   **This test is expected to fail at link time.** DynamicVamanaIndex
//   has no reconstruct() method. The link error is the ask (spec §3.5).
//   Once the SVS team adds reconstruct(size_t label, float* out), this
//   test compiles and runs.
//
// valkey-search code path this mimics:
//   VectorSVS::GetValueImpl  src/indexes/vector_svs.cc:661-673 —
//   currently returns from the raw_vectors_ FP32 shadow copy we
//   maintain externally. We want to delete that shadow and use
//   SVS's own reconstruct() instead.
//
// Spec section: §3.5 — expose reconstruct(label, float* out).
//
// Reproduction:
//   ./build_test.sh svs/test_07_reconstruct
//   # On current SVS 0.2.0, expect linker error:
//   #   undefined reference to svs::runtime::v0::DynamicVamanaIndex::reconstruct
//   # That error IS the ask.
//   # On the new SVS runtime:
//   ./svs/test_07_reconstruct
//
// Expected output (new runtime): low L2 distance between stored vector
// and reconstruct() output (exact for StorageKind::FP32, small for
// lossy compression like LVQ4x8).

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "svs_common.h"
#include "test_common.h"

// Forward declaration of the desired API. This is intentional — if the
// SVS team adds this virtual method to DynamicVamanaIndex, we won't
// need this forward decl anymore. Until then, linking this test
// against libsvs_runtime 0.2.0 produces an "undefined reference"
// error, which IS the ask.
//
// The forward decl must match the proposed signature:
//     virtual Status reconstruct(size_t label, float* out) const noexcept;
// We access it via an extern "C"-free local wrapper.
namespace svs { namespace runtime { namespace v0 {
// Defined by the new SVS runtime; not present in 0.2.0.
Status dynamic_vamana_reconstruct(const DynamicVamanaIndex* idx,
                                  size_t label, float* out) noexcept;
}}}

using namespace svstest;
using svstest::svs_::DVamana;

constexpr size_t kDim = 64;
constexpr size_t kN   = 50;

int main() {
  std::printf("svs/test_07_reconstruct: dim=%zu N=%zu\n", kDim, kN);

  DVamana* idx = nullptr;
  auto st = svs_::build_svs(&idx, kDim);
  if (!st.ok()) { fail("build", st.message()); return 1; }

  rng(1);
  auto data = random_vecs(kN, kDim);
  std::vector<size_t> labels(kN);
  for (size_t i = 0; i < kN; ++i) labels[i] = i;
  st = idx->add(kN, labels.data(), data.data());
  if (!st.ok()) { fail("add", st.message()); DVamana::destroy(idx); return 1; }

  section("reconstruct");
  std::vector<float> out(kDim);
  double max_err_l2 = 0.0;
  for (size_t i = 0; i < kN; ++i) {
    auto s = ::svs::runtime::v0::dynamic_vamana_reconstruct(
        idx, /*label*/ i, out.data());
    if (!s.ok()) {
      fail("reconstruct",
           "i=" + std::to_string(i) + ": " + s.message());
      DVamana::destroy(idx);
      return 1;
    }
    double err = 0;
    for (size_t d = 0; d < kDim; ++d) {
      double dd = out[d] - data[i * kDim + d];
      err += dd * dd;
    }
    err = std::sqrt(err);
    if (err > max_err_l2) max_err_l2 = err;
  }
  std::printf("  max L2 reconstruction error: %.6f (target: <1e-4 for FP32)\n",
              max_err_l2);

  DVamana::destroy(idx);
  pass("svs/test_07_reconstruct");
  return 0;
}
