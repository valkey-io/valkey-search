/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_FP16_H_
#define VALKEYSEARCH_SRC_INDEXES_FP16_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

// float16: 1 sign + 5 exponent + 10 mantissa, IEEE 754 binary16.
// Like bfloat16, use a distinct two-byte storage type with explicit FLOAT32
// conversions. Their bit layouts differ and must remain distinct for template
// dispatch. Keep storage independent of compiler half-float support and
// libgcc's half-float conversion helpers. Conversions round to nearest, ties
// to even, including subnormals.
struct float16 {
  uint16_t bits;

  float16() = default;

  explicit float16(float value) {
#if defined(__aarch64__)
    // GCC 12 supports __fp16 on AArch64 even though it rejects _Float16 in
    // C++. Keep hardware conversion without exposing a native storage type.
    const __fp16 native = static_cast<__fp16>(value);
    std::memcpy(&bits, &native, sizeof(bits));
#else
    uint32_t raw;
    std::memcpy(&raw, &value, sizeof(raw));
    const uint16_t sign = static_cast<uint16_t>((raw >> 16) & 0x8000);
    const uint32_t exponent = (raw >> 23) & 0xff;
    const uint32_t fraction = raw & 0x7fffff;
    if (exponent == 0xff) {
      // Preserve infinity and the high NaN payload bits; quiet NaNs so a
      // payload that disappears on truncation never becomes infinity.
      bits = sign | 0x7c00;
      if (fraction != 0) {
        bits |= static_cast<uint16_t>((fraction >> 13) | 0x0200);
      }
      return;
    }
    const int unbiased_exponent = static_cast<int>(exponent) - 127;
    if (unbiased_exponent > 15) {
      bits = sign | 0x7c00;
      return;
    }
    if (unbiased_exponent < -25) {
      bits = sign;
      return;
    }
    if (unbiased_exponent < -14) {
      const uint32_t significand = fraction | 0x800000;
      const int shift = -unbiased_exponent - 1;
      uint32_t rounded = significand >> shift;
      const uint32_t remainder = significand & ((uint32_t{1} << shift) - 1);
      const uint32_t halfway = uint32_t{1} << (shift - 1);
      rounded += remainder > halfway || (remainder == halfway && (rounded & 1));
      bits = sign | static_cast<uint16_t>(rounded);
      return;
    }
    uint32_t rounded = (static_cast<uint32_t>(unbiased_exponent + 15) << 10) |
                       (fraction >> 13);
    const uint32_t remainder = fraction & 0x1fff;
    rounded += remainder > 0x1000 || (remainder == 0x1000 && (rounded & 1));
    // Carry naturally crosses exponent boundaries, including overflow to Inf.
    bits = sign | static_cast<uint16_t>(rounded);
#endif
  }

  explicit operator float() const {
#if defined(__aarch64__)
    __fp16 native;
    std::memcpy(&native, &bits, sizeof(bits));
    return static_cast<float>(native);
#else
    const uint32_t sign = static_cast<uint32_t>(bits & 0x8000) << 16;
    uint32_t exponent = (bits >> 10) & 0x1f;
    uint32_t fraction = bits & 0x03ff;
    uint32_t raw;
    if (exponent == 0) {
      raw = sign;
      if (fraction != 0) {
        int unbiased_exponent = -14;
        while ((fraction & 0x0400) == 0) {
          fraction <<= 1;
          --unbiased_exponent;
        }
        raw |= static_cast<uint32_t>(unbiased_exponent + 127) << 23;
        raw |= (fraction & 0x03ff) << 13;
      }
    } else if (exponent == 0x1f) {
      raw = sign | 0x7f800000 | (fraction << 13);
      if (fraction != 0) {
        raw |= 0x00400000;
      }
    } else {
      raw = sign | ((exponent + 112) << 23) | (fraction << 13);
    }
    float value;
    std::memcpy(&value, &raw, sizeof(value));
    return value;
#endif
  }
};

static_assert(sizeof(float16) == 2, "float16 must be 2 bytes");
static_assert(alignof(float16) == alignof(uint16_t));
static_assert(std::is_trivially_copyable_v<float16>);
static_assert(std::is_standard_layout_v<float16>);

inline float float16_to_float(float16 v) { return static_cast<float>(v); }
inline float16 float_to_float16(float v) { return float16{v}; }

#endif  // VALKEYSEARCH_SRC_INDEXES_FP16_H_
