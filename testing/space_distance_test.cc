/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Direct unit tests for the hnswlib distance/space implementations used by the
// vector index. The rest of the test suite covers these functions only
// indirectly (through KNN / VectorBase::Search). Here we call
// space.get_dist_func()(v1, v2, space.get_dist_func_param()) with hand-picked
// vectors and assert the scalar result.
//
// Multi-dim coverage (dims 3, 4, 16, 17, 20) exercises the path-selection
// logic inside the FP32 spaces:
//   * dim=3  : pure scalar tail (no SIMD blocks, not 4-aligned)
//   * dim=4  : SIMD4Ext path
//   * dim=16 : full SIMD16Ext block
//   * dim=20 : SIMD16Ext block + 4-tail
//   * dim=17 : SIMD16Ext block + 1-element residual handler
//
// USE_SIMSIMD is now defined unconditionally in third_party/hnswlib/hnswlib.h,
// so the FP16/BF16 spaces take the simsimd kernels here (simsimd does its own
// runtime CPU dispatch). The dim sweep therefore exercises simsimd's own
// blocked/tail handling as well.
//
// All test inputs use values exactly representable in FP16 (small integers,
// zero) so FP32 and FP16 expectations agree without precision drift.

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "absl/log/check.h"
#include "gtest/gtest.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
// hnswlib.h must precede space_*.h: stop_condition.h (pulled in by hnswlib.h)
// references symbols defined later in space_ip.h / space_l2.h, so the umbrella
// header has to be parsed end-to-end first to satisfy unqualified lookup.
#include "third_party/hnswlib/hnswlib.h"
#include "third_party/hnswlib/space_ip.h"
#include "third_party/hnswlib/space_ip_bfloat16.h"
#include "third_party/hnswlib/space_ip_fp16.h"
#include "third_party/hnswlib/space_l2.h"
#include "third_party/hnswlib/space_l2_bfloat16.h"
#include "third_party/hnswlib/space_l2_fp16.h"

namespace valkey_search {
namespace {

constexpr float kFp16Tolerance = 1e-3f;
// BF16 has the same exponent range as FP32 but only 7 mantissa bits (vs FP16's
// 10), so integer-valued sums up to ~128 are exact but products can introduce
// ~2^-7 relative error per term. 1e-2 absolute slack covers our test inputs.
constexpr float kBf16Tolerance = 1e-2f;

const std::vector<size_t>& AllDims() {
  static const std::vector<size_t> dims{3, 4, 16, 17, 20};
  return dims;
}

// Pads a prefix of integer values out to `dim` with zeros. The L2 distance
// between two such vectors equals the squared difference of their non-zero
// prefixes, regardless of dim — so the same expectation holds at every dim.
std::vector<float> PadFp32(std::initializer_list<float> prefix, size_t dim) {
  std::vector<float> v(dim, 0.0f);
  size_t i = 0;
  for (float x : prefix) {
    if (i >= dim) {
      break;
    }
    v[i++] = x;
  }
  return v;
}

std::vector<float16> PadFp16(std::initializer_list<float> prefix, size_t dim) {
  std::vector<float16> v(dim, static_cast<float16>(0.0f));
  size_t i = 0;
  for (float x : prefix) {
    if (i >= dim) {
      break;
    }
    v[i++] = static_cast<float16>(x);
  }
  return v;
}

std::vector<bfloat16> PadBf16(std::initializer_list<float> prefix, size_t dim) {
  std::vector<bfloat16> v(dim, bfloat16{0.0f});
  size_t i = 0;
  for (float x : prefix) {
    if (i >= dim) {
      break;
    }
    v[i++] = bfloat16{x};
  }
  return v;
}

// The trailing argument is the product of the two vectors' reciprocal
// magnitudes. L2 kernels ignore it; IP kernels multiply the dot product by it
// before subtracting from 1. These tests pass un-normalized vectors, so 1.0f
// is the identity and the expectations below are the raw distances.
float CallDist(hnswlib::SpaceInterface<float>& space, const void* a,
               const void* b, float magnitude = 1.0f) {
  return space.get_dist_func()(a, b, space.get_dist_func_param(), magnitude);
}

// ---------------------------------------------------------------------------
// L2Space (FP32)
// ---------------------------------------------------------------------------

TEST(SpaceDistanceL2Fp32, HandComputed) {
  // |{3,4,0,...} - {0,0,0,...}|^2 = 9 + 16 = 25.
  for (size_t dim : AllDims()) {
    hnswlib::L2Space space(dim);
    auto v1 = PadFp32({3.0f, 4.0f}, dim);
    auto v2 = PadFp32({}, dim);
    EXPECT_FLOAT_EQ(CallDist(space, v1.data(), v2.data()), 25.0f)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Fp32, SelfDistanceZero) {
  for (size_t dim : AllDims()) {
    hnswlib::L2Space space(dim);
    auto v = PadFp32({1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, dim);
    EXPECT_FLOAT_EQ(CallDist(space, v.data(), v.data()), 0.0f) << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Fp32, Symmetric) {
  for (size_t dim : AllDims()) {
    hnswlib::L2Space space(dim);
    auto a = PadFp32({1.0f, -2.0f, 3.0f}, dim);
    auto b = PadFp32({4.0f, 5.0f, -6.0f}, dim);
    float a_b = CallDist(space, a.data(), b.data());
    float b_a = CallDist(space, b.data(), a.data());
    EXPECT_FLOAT_EQ(a_b, b_a) << "dim=" << dim;
  }
}

// ---------------------------------------------------------------------------
// InnerProductSpace (FP32) — returns 1.0 - dot(a, b)
// ---------------------------------------------------------------------------

TEST(SpaceDistanceIpFp32, HandComputed) {
  // dot({1,2,3,0,...}, {4,5,6,0,...}) = 4+10+18 = 32, distance = 1 - 32 = -31.
  for (size_t dim : AllDims()) {
    hnswlib::InnerProductSpace space(dim);
    auto v1 = PadFp32({1.0f, 2.0f, 3.0f}, dim);
    auto v2 = PadFp32({4.0f, 5.0f, 6.0f}, dim);
    EXPECT_FLOAT_EQ(CallDist(space, v1.data(), v2.data()), -31.0f)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceIpFp32, OrthogonalBases) {
  // Standard basis vectors e0 and e1 — dot=0, distance=1.0. Skip dim<2.
  for (size_t dim : AllDims()) {
    if (dim < 2) {
      continue;
    }
    hnswlib::InnerProductSpace space(dim);
    auto e0 = PadFp32({1.0f}, dim);
    std::vector<float> e1(dim, 0.0f);
    e1[1] = 1.0f;
    EXPECT_FLOAT_EQ(CallDist(space, e0.data(), e1.data()), 1.0f)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceIpFp32, Symmetric) {
  for (size_t dim : AllDims()) {
    hnswlib::InnerProductSpace space(dim);
    auto a = PadFp32({1.0f, -2.0f, 3.0f}, dim);
    auto b = PadFp32({4.0f, 5.0f, -6.0f}, dim);
    EXPECT_FLOAT_EQ(CallDist(space, a.data(), b.data()),
                    CallDist(space, b.data(), a.data()))
        << "dim=" << dim;
  }
}

// ---------------------------------------------------------------------------
// L2SpaceFP16
// ---------------------------------------------------------------------------

TEST(SpaceDistanceL2Fp16, HandComputed) {
  for (size_t dim : AllDims()) {
    hnswlib::L2SpaceFP16 space(dim);
    auto v1 = PadFp16({3.0f, 4.0f}, dim);
    auto v2 = PadFp16({}, dim);
    EXPECT_NEAR(CallDist(space, v1.data(), v2.data()), 25.0f, kFp16Tolerance)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Fp16, SelfDistanceZero) {
  for (size_t dim : AllDims()) {
    hnswlib::L2SpaceFP16 space(dim);
    auto v = PadFp16({1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, dim);
    EXPECT_NEAR(CallDist(space, v.data(), v.data()), 0.0f, kFp16Tolerance)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Fp16, Symmetric) {
  for (size_t dim : AllDims()) {
    hnswlib::L2SpaceFP16 space(dim);
    auto a = PadFp16({1.0f, -2.0f, 3.0f}, dim);
    auto b = PadFp16({4.0f, 5.0f, -6.0f}, dim);
    EXPECT_NEAR(CallDist(space, a.data(), b.data()),
                CallDist(space, b.data(), a.data()), kFp16Tolerance)
        << "dim=" << dim;
  }
}

// ---------------------------------------------------------------------------
// InnerProductSpaceFP16 — returns 1.0 - dot(a, b)
// ---------------------------------------------------------------------------

TEST(SpaceDistanceIpFp16, HandComputed) {
  for (size_t dim : AllDims()) {
    hnswlib::InnerProductSpaceFP16 space(dim);
    auto v1 = PadFp16({1.0f, 2.0f, 3.0f}, dim);
    auto v2 = PadFp16({4.0f, 5.0f, 6.0f}, dim);
    EXPECT_NEAR(CallDist(space, v1.data(), v2.data()), -31.0f, kFp16Tolerance)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceIpFp16, OrthogonalBases) {
  for (size_t dim : AllDims()) {
    if (dim < 2) {
      continue;
    }
    hnswlib::InnerProductSpaceFP16 space(dim);
    auto e0 = PadFp16({1.0f}, dim);
    std::vector<float16> e1(dim, static_cast<float16>(0.0f));
    e1[1] = static_cast<float16>(1.0f);
    EXPECT_NEAR(CallDist(space, e0.data(), e1.data()), 1.0f, kFp16Tolerance)
        << "dim=" << dim;
  }
}

// ---------------------------------------------------------------------------
// L2SpaceBF16
// ---------------------------------------------------------------------------

TEST(SpaceDistanceL2Bf16, HandComputed) {
  for (size_t dim : AllDims()) {
    hnswlib::L2SpaceBF16 space(dim);
    auto v1 = PadBf16({3.0f, 4.0f}, dim);
    auto v2 = PadBf16({}, dim);
    EXPECT_NEAR(CallDist(space, v1.data(), v2.data()), 25.0f, kBf16Tolerance)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Bf16, SelfDistanceZero) {
  for (size_t dim : AllDims()) {
    hnswlib::L2SpaceBF16 space(dim);
    auto v = PadBf16({1.0f, 2.0f, 3.0f, 4.0f, 5.0f}, dim);
    EXPECT_NEAR(CallDist(space, v.data(), v.data()), 0.0f, kBf16Tolerance)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Bf16, Symmetric) {
  for (size_t dim : AllDims()) {
    hnswlib::L2SpaceBF16 space(dim);
    auto a = PadBf16({1.0f, -2.0f, 3.0f}, dim);
    auto b = PadBf16({4.0f, 5.0f, -6.0f}, dim);
    EXPECT_NEAR(CallDist(space, a.data(), b.data()),
                CallDist(space, b.data(), a.data()), kBf16Tolerance)
        << "dim=" << dim;
  }
}

// ---------------------------------------------------------------------------
// InnerProductSpaceBF16 — returns 1.0 - dot(a, b)
// ---------------------------------------------------------------------------

TEST(SpaceDistanceIpBf16, HandComputed) {
  for (size_t dim : AllDims()) {
    hnswlib::InnerProductSpaceBF16 space(dim);
    auto v1 = PadBf16({1.0f, 2.0f, 3.0f}, dim);
    auto v2 = PadBf16({4.0f, 5.0f, 6.0f}, dim);
    EXPECT_NEAR(CallDist(space, v1.data(), v2.data()), -31.0f, kBf16Tolerance)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceIpBf16, OrthogonalBases) {
  for (size_t dim : AllDims()) {
    if (dim < 2) {
      continue;
    }
    hnswlib::InnerProductSpaceBF16 space(dim);
    auto e0 = PadBf16({1.0f}, dim);
    std::vector<bfloat16> e1(dim, bfloat16{0.0f});
    e1[1] = bfloat16{1.0f};
    EXPECT_NEAR(CallDist(space, e0.data(), e1.data()), 1.0f, kBf16Tolerance)
        << "dim=" << dim;
  }
}

// ---------------------------------------------------------------------------
// SimsimdCpuConfig — reports which BF16 kernel simsimd selected here.
//
// This used to assert that the CPU had a SIMD BF16 kernel, on the grounds that
// simsimd's serial fallback misdecodes bfloat16 when SIMSIMD_NATIVE_BF16 is 1.
// That fallback is now correct (see SIMSIMD_UNCOMPRESS_BF16 in
// third_party/simsimd/include/simsimd/types.h), so the assertion would now
// fail spuriously on hardware that is perfectly able to serve BFLOAT16 --
// notably Arm cores without FEAT_BF16, such as AWS Graviton 2, for which
// simsimd ships no shift-based kernel and the serial path is the only option.
//
// The serial kernels are covered directly by the SpaceDistanceSerialKernel
// cases above, on every machine, so no canary is needed to tell us whether
// they happened to run here. This records the dispatch instead, so a failure
// elsewhere in this file can be read against the path that actually executed.
// ---------------------------------------------------------------------------

extern "C" {
int simsimd_uses_haswell(void);
int simsimd_uses_genoa(void);
int simsimd_uses_sapphire(void);
int simsimd_uses_neon_bf16(void);
int simsimd_uses_sve_bf16(void);
}

// ---------------------------------------------------------------------------
// Dense-vector validation against a double-precision reference
//
// The hand-computed cases above pad with zeros, so only the first two lanes
// carry a value. A kernel that drops lanes, mis-strides, or skips its tail
// loop still returns the right answer there, because everything it skipped
// contributed nothing. These cases fill every lane with a distinct
// non-representable value and compare against the exact computation over the
// *quantized* inputs, so the only permitted error is float accumulation.
// ---------------------------------------------------------------------------

// Deterministic values in [-1, 1) that are not exactly representable in any of
// the three storage types, so a mis-decode cannot coincidentally agree.
std::vector<float> DenseValues(size_t dim, uint32_t seed) {
  std::vector<float> v;
  v.reserve(dim);
  uint32_t state = seed * 2654435761u + 1u;
  for (size_t i = 0; i < dim; ++i) {
    state = state * 1664525u + 1013904223u;
    // Map to [-1, 1) with a long mantissa.
    v.push_back(static_cast<float>(static_cast<double>(state) / 2147483648.0) -
                1.0f);
  }
  return v;
}

template <typename T>
std::vector<T> Quantize(const std::vector<float>& values) {
  std::vector<T> out;
  out.reserve(values.size());
  for (float x : values) {
    out.push_back(static_cast<T>(x));
  }
  return out;
}

template <>
std::vector<bfloat16> Quantize<bfloat16>(const std::vector<float>& values) {
  std::vector<bfloat16> out;
  out.reserve(values.size());
  for (float x : values) {
    out.push_back(bfloat16{x});
  }
  return out;
}

// Exact L2 and dot over the quantized values, accumulated in double so the
// reference itself contributes no meaningful error.
template <typename T>
double ReferenceL2Sqr(const std::vector<T>& a, const std::vector<T>& b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    const double d = static_cast<double>(static_cast<float>(a[i])) -
                     static_cast<double>(static_cast<float>(b[i]));
    sum += d * d;
  }
  return sum;
}

template <typename T>
double ReferenceDot(const std::vector<T>& a, const std::vector<T>& b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    sum += static_cast<double>(static_cast<float>(a[i])) *
           static_cast<double>(static_cast<float>(b[i]));
  }
  return sum;
}

// Both the kernel and the reference consume the same quantized inputs, so the
// gap is float accumulation over `dim` terms, not the storage type's own
// precision. This bound is far below the O(1) error a decode or lane bug
// produces.
double AccumulationTolerance(size_t dim, double reference) {
  return std::max(1e-4, 1e-4 * std::abs(reference)) * static_cast<double>(dim) /
         4.0;
}

template <typename T, typename SpaceT>
void ExpectDenseL2Matches(size_t dim) {
  const std::vector<float> raw_a = DenseValues(dim, 11);
  const std::vector<float> raw_b = DenseValues(dim, 29);
  const std::vector<T> a = Quantize<T>(raw_a);
  const std::vector<T> b = Quantize<T>(raw_b);

  SpaceT space(dim);
  const double reference = ReferenceL2Sqr(a, b);
  EXPECT_NEAR(CallDist(space, a.data(), b.data()), reference,
              AccumulationTolerance(dim, reference))
      << "dim=" << dim;
}

template <typename T, typename SpaceT>
void ExpectDenseIpMatches(size_t dim) {
  const std::vector<float> raw_a = DenseValues(dim, 37);
  const std::vector<float> raw_b = DenseValues(dim, 53);
  const std::vector<T> a = Quantize<T>(raw_a);
  const std::vector<T> b = Quantize<T>(raw_b);

  SpaceT space(dim);
  // A magnitude other than 1 exercises the scaling the cosine metric relies
  // on; the hand-computed cases above all leave it at the identity.
  constexpr float kMagnitude = 0.375f;
  const double reference = 1.0 - ReferenceDot(a, b) * kMagnitude;
  EXPECT_NEAR(CallDist(space, a.data(), b.data(), kMagnitude), reference,
              AccumulationTolerance(dim, reference))
      << "dim=" << dim;
}

TEST(SpaceDistanceDense, L2Fp32) {
  for (size_t dim : AllDims()) {
    ExpectDenseL2Matches<float, hnswlib::L2Space>(dim);
  }
}
TEST(SpaceDistanceDense, L2Fp16) {
  for (size_t dim : AllDims()) {
    ExpectDenseL2Matches<float16, hnswlib::L2SpaceFP16>(dim);
  }
}
TEST(SpaceDistanceDense, L2Bf16) {
  for (size_t dim : AllDims()) {
    ExpectDenseL2Matches<bfloat16, hnswlib::L2SpaceBF16>(dim);
  }
}
TEST(SpaceDistanceDense, IpFp32) {
  for (size_t dim : AllDims()) {
    ExpectDenseIpMatches<float, hnswlib::InnerProductSpace>(dim);
  }
}
TEST(SpaceDistanceDense, IpFp16) {
  for (size_t dim : AllDims()) {
    ExpectDenseIpMatches<float16, hnswlib::InnerProductSpaceFP16>(dim);
  }
}
TEST(SpaceDistanceDense, IpBf16) {
  for (size_t dim : AllDims()) {
    ExpectDenseIpMatches<bfloat16, hnswlib::InnerProductSpaceBF16>(dim);
  }
}

// Larger dimensions, past the point where every kernel has taken its blocked
// path at least twice and left a non-trivial tail.
TEST(SpaceDistanceDense, LargeDimensions) {
  for (size_t dim : {33u, 64u, 127u, 128u, 200u}) {
    ExpectDenseL2Matches<float16, hnswlib::L2SpaceFP16>(dim);
    ExpectDenseL2Matches<bfloat16, hnswlib::L2SpaceBF16>(dim);
    ExpectDenseIpMatches<float16, hnswlib::InnerProductSpaceFP16>(dim);
    ExpectDenseIpMatches<bfloat16, hnswlib::InnerProductSpaceBF16>(dim);
  }
}

// ---------------------------------------------------------------------------
// The serial kernels, called directly
//
// simsimd dispatches on CPU features, so on any machine only one kernel per
// metric actually runs and the rest are never executed by the tests above. The
// serial kernels are the fallback for hardware with no SIMD implementation --
// notably Arm cores without FEAT_BF16, for which simsimd ships no shift-based
// alternative -- and they are where a decode bug hides longest, because the
// machines that run them are the least likely to be the ones running CI.
//
// These call them directly so the fallback is validated everywhere.
// ---------------------------------------------------------------------------

TEST(SpaceDistanceSerialKernel, Bf16DecodesCorrectly) {
  // The bf16 encoding of 1.0 is 0x3F80, the high half of the float32 encoding.
  // Read as IEEE half it is 1.875, which is what the serial path returned
  // before SIMSIMD_UNCOMPRESS_BF16 was corrected.
  const std::vector<bfloat16> one = Quantize<bfloat16>({1.0f, 1.0f, 1.0f});
  const std::vector<bfloat16> zero = Quantize<bfloat16>({0.0f, 0.0f, 0.0f});

  simsimd_distance_t l2 = 0.0;
  simsimd_l2sq_bf16_serial(reinterpret_cast<const simsimd_bf16_t*>(one.data()),
                           reinterpret_cast<const simsimd_bf16_t*>(zero.data()),
                           one.size(), &l2);
  EXPECT_NEAR(static_cast<double>(l2), 3.0, 1e-6)
      << "serial BF16 L2 decoded the stored bits as the wrong type";

  simsimd_distance_t dot = 0.0;
  simsimd_dot_bf16_serial(reinterpret_cast<const simsimd_bf16_t*>(one.data()),
                          reinterpret_cast<const simsimd_bf16_t*>(one.data()),
                          one.size(), &dot);
  EXPECT_NEAR(static_cast<double>(dot), 3.0, 1e-6)
      << "serial BF16 dot decoded the stored bits as the wrong type";
}

TEST(SpaceDistanceSerialKernel, Bf16MatchesReferenceOnDenseVectors) {
  for (size_t dim : AllDims()) {
    const std::vector<bfloat16> a = Quantize<bfloat16>(DenseValues(dim, 71));
    const std::vector<bfloat16> b = Quantize<bfloat16>(DenseValues(dim, 89));

    simsimd_distance_t l2 = 0.0;
    simsimd_l2sq_bf16_serial(reinterpret_cast<const simsimd_bf16_t*>(a.data()),
                             reinterpret_cast<const simsimd_bf16_t*>(b.data()),
                             dim, &l2);
    const double l2_reference = ReferenceL2Sqr(a, b);
    EXPECT_NEAR(static_cast<double>(l2), l2_reference,
                AccumulationTolerance(dim, l2_reference))
        << "dim=" << dim;

    simsimd_distance_t dot = 0.0;
    simsimd_dot_bf16_serial(reinterpret_cast<const simsimd_bf16_t*>(a.data()),
                            reinterpret_cast<const simsimd_bf16_t*>(b.data()),
                            dim, &dot);
    const double dot_reference = ReferenceDot(a, b);
    EXPECT_NEAR(static_cast<double>(dot), dot_reference,
                AccumulationTolerance(dim, dot_reference))
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceSerialKernel, Fp16MatchesReferenceOnDenseVectors) {
  for (size_t dim : AllDims()) {
    const std::vector<float16> a = Quantize<float16>(DenseValues(dim, 101));
    const std::vector<float16> b = Quantize<float16>(DenseValues(dim, 113));

    simsimd_distance_t l2 = 0.0;
    simsimd_l2sq_f16_serial(reinterpret_cast<const simsimd_f16_t*>(a.data()),
                            reinterpret_cast<const simsimd_f16_t*>(b.data()),
                            dim, &l2);
    const double l2_reference = ReferenceL2Sqr(a, b);
    EXPECT_NEAR(static_cast<double>(l2), l2_reference,
                AccumulationTolerance(dim, l2_reference))
        << "dim=" << dim;

    simsimd_distance_t dot = 0.0;
    simsimd_dot_f16_serial(reinterpret_cast<const simsimd_f16_t*>(a.data()),
                           reinterpret_cast<const simsimd_f16_t*>(b.data()),
                           dim, &dot);
    const double dot_reference = ReferenceDot(a, b);
    EXPECT_NEAR(static_cast<double>(dot), dot_reference,
                AccumulationTolerance(dim, dot_reference))
        << "dim=" << dim;
  }
}

// Records which kernel simsimd selected here, so a failure elsewhere in this
// file can be read against the path that actually ran.
TEST(SimsimdCpuConfig, ReportsDispatchedBf16Path) {
  const bool native_x86 = simsimd_uses_genoa() || simsimd_uses_sapphire();
  const bool native_arm = simsimd_uses_neon_bf16() || simsimd_uses_sve_bf16();
  const bool shift_based_x86 = simsimd_uses_haswell();

  RecordProperty("bf16_native_x86", native_x86);
  RecordProperty("bf16_native_arm", native_arm);
  RecordProperty("bf16_shift_based_x86", shift_based_x86);
  RecordProperty("bf16_serial_fallback",
                 !native_x86 && !native_arm && !shift_based_x86);
  SUCCEED() << "BF16 dispatch on this CPU -- native x86: " << native_x86
            << ", native ARM: " << native_arm
            << ", shift-based AVX2: " << shift_based_x86
            << ". The serial kernels are covered directly by "
               "SpaceDistanceSerialKernel regardless of which ran here.";
}

}  // namespace
}  // namespace valkey_search
