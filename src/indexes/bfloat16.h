/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_BFLOAT16_H_
#define VALKEYSEARCH_SRC_INDEXES_BFLOAT16_H_

#include <cstddef>
#include <cstdint>
#include <cstring>

// bfloat16: 1 sign + 8 exponent + 7 mantissa, equivalent to "the top half of an
// IEEE 754 binary32." Same byte width as IEEE half-precision (`_Float16`) but
// completely different layout — they MUST be distinct C++ types so that
// std::is_same_v dispatches and template specializations cannot conflate them.
//
// Conversion rule is round-to-nearest, ties-to-even (IEEE 754 default).
// Matches RediSearch's BF16 conversion used during cosine normalization, so
// stored normalized vectors agree bit-for-bit. The integration test encoder
// in integration/indexes.py applies the identical bias in pure Python.
//
// NaN/Inf caveat: the rounding bias can promote a finite-with-saturated-
// exponent value into a NaN; the engine never feeds those through here
// (vectors are validated upstream), so we don't special-case them.
struct bfloat16 {
  uint16_t bits;

  bfloat16() = default;

  explicit bfloat16(float v) {
    uint32_t u;
    std::memcpy(&u, &v, sizeof(u));
    uint32_t rounding_bias = 0x7FFFu + ((u >> 16) & 1u);
    u += rounding_bias;
    bits = static_cast<uint16_t>(u >> 16);
  }

  explicit operator float() const {
    uint32_t u = static_cast<uint32_t>(bits) << 16;
    float f;
    std::memcpy(&f, &u, sizeof(f));
    return f;
  }
};

static_assert(sizeof(bfloat16) == 2, "bfloat16 must be 2 bytes");

inline float bfloat16_to_float(bfloat16 v) { return static_cast<float>(v); }
inline bfloat16 float_to_bfloat16(float v) { return bfloat16{v}; }

#endif  // VALKEYSEARCH_SRC_INDEXES_BFLOAT16_H_
