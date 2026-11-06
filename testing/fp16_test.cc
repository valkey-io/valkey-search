/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/fp16.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

#include "gtest/gtest.h"

namespace {

float Decode(uint16_t bits) {
  float16 value;
  value.bits = bits;
  return float16_to_float(value);
}

uint32_t FloatBits(float value) {
  uint32_t raw;
  std::memcpy(&raw, &value, sizeof(raw));
  return raw;
}

float FloatFromBits(uint32_t raw) {
  float value;
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

TEST(Float16Test, DecodesEveryEncodingAndRoundTripsFiniteValues) {
  for (uint32_t raw = 0; raw <= 0xffff; ++raw) {
    const auto bits = static_cast<uint16_t>(raw);
    const int exponent = (bits >> 10) & 0x1f;
    const int fraction = bits & 0x03ff;
    const uint32_t sign = static_cast<uint32_t>(bits & 0x8000) << 16;
    const float actual = Decode(bits);
    if (exponent == 31) {
      const uint32_t expected = sign | 0x7f800000 |
                                (static_cast<uint32_t>(fraction) << 13) |
                                (fraction == 0 ? 0 : 0x00400000);
      ASSERT_EQ(FloatBits(actual), expected) << raw;
    } else {
      const float magnitude =
          exponent == 0
              ? std::ldexp(static_cast<float>(fraction), -24)
              : std::ldexp(static_cast<float>(1024 + fraction), exponent - 25);
      const uint32_t expected = FloatBits(magnitude) | sign;
      ASSERT_EQ(FloatBits(actual), expected) << raw;
      ASSERT_EQ(float_to_float16(actual).bits, bits) << raw;
    }
  }
}

TEST(Float16Test, RoundsAtEveryFiniteBoundaryToNearestEven) {
  for (uint32_t raw = 0; raw < 0x7bff; ++raw) {
    const float lower = Decode(static_cast<uint16_t>(raw));
    const float upper = Decode(static_cast<uint16_t>(raw + 1));
    const float halfway = (lower + upper) * 0.5f;
    const uint16_t even = static_cast<uint16_t>(raw + (raw & 1));
    const float below = std::nextafter(halfway, lower);
    const float above = std::nextafter(halfway, upper);
    ASSERT_EQ(float_to_float16(below).bits, raw) << raw;
    ASSERT_EQ(float_to_float16(halfway).bits, even) << raw;
    ASSERT_EQ(float_to_float16(above).bits, raw + 1) << raw;
    ASSERT_EQ(float_to_float16(-below).bits, raw | 0x8000) << raw;
    ASSERT_EQ(float_to_float16(-halfway).bits, even | 0x8000) << raw;
    ASSERT_EQ(float_to_float16(-above).bits, (raw + 1) | 0x8000) << raw;
  }
}

TEST(Float16Test, OverflowAndSpecialValues) {
  EXPECT_EQ(float_to_float16(65504.0f).bits, 0x7bff);
  EXPECT_EQ(float_to_float16(std::nextafter(65520.0f, 0.0f)).bits, 0x7bff);
  EXPECT_EQ(float_to_float16(65520.0f).bits, 0x7c00);
  EXPECT_EQ(float_to_float16(-65520.0f).bits, 0xfc00);
  EXPECT_EQ(float_to_float16(std::numeric_limits<float>::max()).bits, 0x7c00);
  EXPECT_EQ(float_to_float16(FloatFromBits(0x7f800000)).bits, 0x7c00);
  EXPECT_EQ(float_to_float16(FloatFromBits(0xff800000)).bits, 0xfc00);
  EXPECT_EQ(float_to_float16(FloatFromBits(0x80000000)).bits, 0x8000);
  EXPECT_EQ(float_to_float16(FloatFromBits(0x00000001)).bits, 0);
  EXPECT_EQ(float_to_float16(FloatFromBits(0x80000001)).bits, 0x8000);
  EXPECT_EQ(float_to_float16(FloatFromBits(0x7f800001)).bits, 0x7e00);
  EXPECT_EQ(float_to_float16(FloatFromBits(0xffc12000)).bits, 0xfe09);
}

}  // namespace
