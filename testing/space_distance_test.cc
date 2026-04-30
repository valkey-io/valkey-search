/*
 * Copyright (c) 2025, valkey-search contributors
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
// FP16 spaces use the scalar fallback in this build (USE_SIMSIMD is not
// defined in third_party/hnswlib/CMakeLists.txt), so the dim sweep mainly
// guards against future regressions there.
//
// All test inputs use values exactly representable in FP16 (small integers,
// zero) so FP32 and FP16 expectations agree without precision drift.

#include <cstddef>
#include <vector>

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

float CallDist(hnswlib::SpaceInterface<float>& space, const void* a,
               const void* b) {
  return space.get_dist_func()(a, b, space.get_dist_func_param());
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
    EXPECT_FLOAT_EQ(CallDist(space, v.data(), v.data()), 0.0f)
        << "dim=" << dim;
  }
}

TEST(SpaceDistanceL2Fp32, Symmetric) {
  for (size_t dim : AllDims()) {
    hnswlib::L2Space space(dim);
    auto a = PadFp32({1.0f, -2.0f, 3.0f}, dim);
    auto b = PadFp32({4.0f, 5.0f, -6.0f}, dim);
    float ab = CallDist(space, a.data(), b.data());
    float ba = CallDist(space, b.data(), a.data());
    EXPECT_FLOAT_EQ(ab, ba) << "dim=" << dim;
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
// SimsimdCpuConfig — guards the contract between third_party/simsimd/c/lib.c's
// build flags and the CPU running the binary.
//
// lib.c sets SIMSIMD_NATIVE_BF16=1, which on x86 makes simsimd_bf16_t a
// `_Float16` typedef instead of `unsigned short`. The serial fallback path
// (`simsimd_l2sq_bf16_serial`) then assumes implicit type-to-float conversion
// and produces wrong distances on BF16 input — `_Float16` interprets the
// stored bits as IEEE half-precision rather than brain-float. The
// SIMD-targeted variants (`*_haswell`, `*_genoa`, `*_sapphire` on x86;
// neon_bf16 / sve_bf16 on ARM) are immune because they cast to `__m128i*`
// and convert via SIMD shifts — bit-correct regardless of the typedef.
//
// So as long as the runtime CPU exposes at least one of those caps,
// simsimd's dispatcher picks a safe path and the BF16 spaces are correct.
// If we ever land on a host with neither, we'd silently corrupt BF16
// distances; this test is the canary.
// ---------------------------------------------------------------------------

extern "C" {
int simsimd_uses_haswell(void);
int simsimd_uses_genoa(void);
int simsimd_uses_sapphire(void);
int simsimd_uses_neon_bf16(void);
int simsimd_uses_sve_bf16(void);
}

TEST(SimsimdCpuConfig, BF16HasSimdSafePath) {
  const bool has_x86_simd_bf16_path = simsimd_uses_haswell() ||
                                      simsimd_uses_genoa() ||
                                      simsimd_uses_sapphire();
  const bool has_arm_simd_bf16_path =
      simsimd_uses_neon_bf16() || simsimd_uses_sve_bf16();

  EXPECT_TRUE(has_x86_simd_bf16_path || has_arm_simd_bf16_path)
      << "simsimd's dispatcher will fall back to simsimd_l2sq_bf16_serial / "
         "simsimd_dot_bf16_serial on this CPU. With SIMSIMD_NATIVE_BF16=1 "
         "(set in third_party/simsimd/c/lib.c) the serial path mis-interprets "
         "BF16 bits as IEEE FP16. Either run on a Haswell+ x86 (or BF16-"
         "capable ARM) CPU, or set SIMSIMD_NATIVE_BF16 back to 0 in lib.c.";
}

}  // namespace
}  // namespace valkey_search
