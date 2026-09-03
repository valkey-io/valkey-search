/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Validation for the type-aware normalization helpers introduced alongside
// FLOAT16 / BFLOAT16 storage: CalcReciprocalMagnitude<T> and NormalizeVector<T>.
//
// The invariant being validated is NOT "fp16 equals fp32". It is:
//
//   for storage type T, the engine's result must equal the exact (double)
//   computation performed on the *quantized* inputs, to within one rounding
//   of T -- i.e. quantization is the only source of error, and no extra error
//   is introduced by accumulating, scaling, or rounding in the wrong domain.
//
// This matters because the magnitude accumulator deliberately runs in float
// (extended precision relative to a 2-byte T): squaring a half-precision value
// overflows fp16's own exponent range long before it overflows float, so
// accumulating in T would produce inf/0 for perfectly ordinary vectors. These
// tests pin that choice down, and pin down that the scale is applied in float
// and rounded back into T exactly once (double-rounding would show up as error
// above the single-rounding bound).

#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/vector_base.h"

namespace valkey_search::indexes {
namespace {

// Relative spacing between representable values (2^-(mantissa bits + 1)).
template <typename T>
constexpr double UnitRoundoff();
template <>
constexpr double UnitRoundoff<float>() {
  return 1.0 / (1 << 24);  // 23 explicit mantissa bits
}
template <>
constexpr double UnitRoundoff<float16>() {
  return 1.0 / (1 << 11);  // 10 explicit mantissa bits
}
template <>
constexpr double UnitRoundoff<bfloat16>() {
  return 1.0 / (1 << 8);  // 7 explicit mantissa bits
}

template <typename T>
T Quantize(double v) {
  return static_cast<T>(static_cast<float>(v));
}

template <typename T>
std::vector<T> QuantizeAll(const std::vector<double>& v) {
  std::vector<T> out;
  out.reserve(v.size());
  for (double x : v) out.push_back(Quantize<T>(x));
  return out;
}

template <typename T>
absl::string_view AsBytes(const std::vector<T>& v) {
  return absl::string_view(reinterpret_cast<const char*>(v.data()),
                           v.size() * sizeof(T));
}

// Exact magnitude of the quantized vector, computed in double.
template <typename T>
double ExactMagnitude(const std::vector<T>& q) {
  double sum = 0.0;
  for (const T& x : q) {
    double d = static_cast<double>(static_cast<float>(x));
    sum += d * d;
  }
  return std::sqrt(sum);
}

// A spread of values that are deliberately NOT exactly representable in fp16
// or bf16, so any missing/extra rounding step shows up.
const std::vector<double>& Sample() {
  static const std::vector<double>* v = new std::vector<double>{
      0.1, -0.7333, 1.37, -2.19, 3.0001, -0.0031, 12.75, -7.125};
  return *v;
}

template <typename T>
void ExpectMagnitudeMatchesReference(const std::vector<double>& raw) {
  auto q = QuantizeAll<T>(raw);
  const double expected_recip = 1.0 / ExactMagnitude<T>(q);
  const float got = CalcReciprocalMagnitude(q.data(), q.size());

  // The accumulator runs in float, so the achievable bound is float's unit
  // roundoff scaled by the term count -- NOT T's. If the accumulation were
  // (incorrectly) done in T, the error would land near UnitRoundoff<T>(),
  // which for bf16 is ~2500x looser than this bound.
  const double tol = expected_recip * UnitRoundoff<float>() * q.size() * 4;
  EXPECT_NEAR(static_cast<double>(got), expected_recip, tol);
}

TEST(VectorNormalizeValidation, ReciprocalMagnitudeFloat32) {
  ExpectMagnitudeMatchesReference<float>(Sample());
}
TEST(VectorNormalizeValidation, ReciprocalMagnitudeFloat16) {
  ExpectMagnitudeMatchesReference<float16>(Sample());
}
TEST(VectorNormalizeValidation, ReciprocalMagnitudeBFloat16) {
  ExpectMagnitudeMatchesReference<bfloat16>(Sample());
}

// The magnitude accumulator must not inherit T's exponent range. Squaring
// 300.0 gives 90000, which overflows fp16 (max ~65504); accumulating in fp16
// would yield inf and a reciprocal of 0.
TEST(VectorNormalizeValidation, Float16MagnitudeDoesNotOverflowInAccumulator) {
  std::vector<float16> q = QuantizeAll<float16>({300.0, 300.0, 300.0, 300.0});
  const float got = CalcReciprocalMagnitude(q.data(), q.size());
  ASSERT_TRUE(std::isfinite(got)) << "accumulator overflowed to inf";
  EXPECT_GT(got, 0.0f) << "accumulator overflowed, reciprocal collapsed to 0";
  EXPECT_NEAR(static_cast<double>(got), 1.0 / 600.0, 1.0 / 600.0 * 1e-5);
}

// Every element must be scaled in float and rounded back into T exactly once.
// A double-rounding (e.g. scaling in T) shows up as error above 1 ulp of T.
template <typename T>
void ExpectNormalizeRoundsOnce(const std::vector<double>& raw) {
  auto q = QuantizeAll<T>(raw);
  const float recip = CalcReciprocalMagnitude(q.data(), q.size());

  std::vector<char> out = NormalizeVector<T>(AsBytes(q), recip);
  ASSERT_EQ(out.size(), q.size() * sizeof(T));

  std::vector<T> got(q.size());
  std::memcpy(got.data(), out.data(), out.size());

  for (size_t i = 0; i < q.size(); ++i) {
    // Reference: scale in float, then a single rounding into T.
    const T want = static_cast<T>(recip * static_cast<float>(q[i]));
    EXPECT_EQ(std::memcmp(&got[i], &want, sizeof(T)), 0)
        << "element " << i << " was not a single-rounding of the float scale";
  }
}

TEST(VectorNormalizeValidation, NormalizeRoundsOnceFloat32) {
  ExpectNormalizeRoundsOnce<float>(Sample());
}
TEST(VectorNormalizeValidation, NormalizeRoundsOnceFloat16) {
  ExpectNormalizeRoundsOnce<float16>(Sample());
}
TEST(VectorNormalizeValidation, NormalizeRoundsOnceBFloat16) {
  ExpectNormalizeRoundsOnce<bfloat16>(Sample());
}

// After normalizing, the vector's magnitude must be 1 to within the
// quantization of T. This is the property the COSINE save/restore path
// depends on: SaveIndexImpl writes normalized vectors, and the loader
// recomputes the magnitude from them.
template <typename T>
void ExpectUnitMagnitudeAfterNormalize(const std::vector<double>& raw) {
  auto q = QuantizeAll<T>(raw);
  const float recip = CalcReciprocalMagnitude(q.data(), q.size());
  std::vector<char> out = NormalizeVector<T>(AsBytes(q), recip);

  std::vector<T> norm(q.size());
  std::memcpy(norm.data(), out.data(), out.size());

  // Each element absorbs at most one rounding of T, so the norm of the
  // rounded vector is within ~sqrt(n) roundings of 1.
  const double tol = UnitRoundoff<T>() * std::sqrt(double(q.size())) * 4;
  EXPECT_NEAR(ExactMagnitude<T>(norm), 1.0, tol);
}

TEST(VectorNormalizeValidation, UnitMagnitudeAfterNormalizeFloat32) {
  ExpectUnitMagnitudeAfterNormalize<float>(Sample());
}
TEST(VectorNormalizeValidation, UnitMagnitudeAfterNormalizeFloat16) {
  ExpectUnitMagnitudeAfterNormalize<float16>(Sample());
}
TEST(VectorNormalizeValidation, UnitMagnitudeAfterNormalizeBFloat16) {
  ExpectUnitMagnitudeAfterNormalize<bfloat16>(Sample());
}

// NormalizeVector must interpret the payload with T's element width. Reading
// a 2-byte vector as 4-byte floats would both halve the element count and
// produce garbage; assert the output length tracks sizeof(T).
template <typename T>
void ExpectElementWidthRespected() {
  auto q = QuantizeAll<T>(Sample());
  float magnitude = 0.0f;
  std::vector<char> out = NormalizeVector<T>(AsBytes(q), &magnitude);
  EXPECT_EQ(out.size(), Sample().size() * sizeof(T));
  EXPECT_NEAR(static_cast<double>(magnitude), ExactMagnitude<T>(q),
              ExactMagnitude<T>(q) * 1e-4);
}

TEST(VectorNormalizeValidation, ElementWidthFloat32) {
  ExpectElementWidthRespected<float>();
}
TEST(VectorNormalizeValidation, ElementWidthFloat16) {
  ExpectElementWidthRespected<float16>();
}
TEST(VectorNormalizeValidation, ElementWidthBFloat16) {
  ExpectElementWidthRespected<bfloat16>();
}

// Zero vectors must not produce inf/NaN: the reciprocal is defined as 1.0.
template <typename T>
void ExpectZeroVectorSafe() {
  std::vector<T> q(8, Quantize<T>(0.0));
  const float recip = CalcReciprocalMagnitude(q.data(), q.size());
  EXPECT_EQ(recip, 1.0f);
  std::vector<char> out = NormalizeVector<T>(AsBytes(q), recip);
  std::vector<T> norm(q.size());
  std::memcpy(norm.data(), out.data(), out.size());
  for (const T& x : norm) {
    EXPECT_TRUE(std::isfinite(static_cast<float>(x)));
  }
}

TEST(VectorNormalizeValidation, ZeroVectorFloat32) {
  ExpectZeroVectorSafe<float>();
}
TEST(VectorNormalizeValidation, ZeroVectorFloat16) {
  ExpectZeroVectorSafe<float16>();
}
TEST(VectorNormalizeValidation, ZeroVectorBFloat16) {
  ExpectZeroVectorSafe<bfloat16>();
}

}  // namespace
}  // namespace valkey_search::indexes
