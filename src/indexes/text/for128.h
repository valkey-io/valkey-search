/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_FOR128_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_FOR128_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace valkey_search::indexes::text {

constexpr size_t kFORBlockSize = 128;

// Frame of Reference (FOR128) Bit-Packing Codec
class FOR128Codec {
 public:
  // Calculate minimum bits needed to store a block of deltas
  static uint8_t BitsRequired(const uint32_t *deltas, size_t count) {
    uint32_t max_val = 0;
    for (size_t i = 0; i < count; ++i) {
      if (deltas[i] > max_val) max_val = deltas[i];
    }
    if (max_val == 0) return 0;
    return 32 - __builtin_clz(max_val);
  }

  // Pack a block of up to 128 deltas into dst buffer.
  // Format: [count: uint8_t][bits: uint8_t][bit-packed payload...]
  // Returns total bytes written to dst.
  static size_t Pack(const uint32_t *deltas, size_t count, uint8_t bits,
                     uint8_t *dst) {
    dst[0] = static_cast<uint8_t>(count);
    dst[1] = bits;
    size_t out_byte_idx = 2;

    if (bits == 0) return out_byte_idx;

    uint64_t bit_buf = 0;
    int bit_cnt = 0;

    for (size_t i = 0; i < count; ++i) {
      bit_buf |= (static_cast<uint64_t>(deltas[i]) << bit_cnt);
      bit_cnt += bits;
      while (bit_cnt >= 8) {
        dst[out_byte_idx++] = static_cast<uint8_t>(bit_buf & 0xFF);
        bit_buf >>= 8;
        bit_cnt -= 8;
      }
    }
    if (bit_cnt > 0) {
      dst[out_byte_idx++] = static_cast<uint8_t>(bit_buf & 0xFF);
    }

    return out_byte_idx;
  }

  // Unpack a block from src buffer into dst_deltas array.
  // Returns total bytes read from src.
  static size_t Unpack(const uint8_t *src, uint32_t *dst_deltas,
                       size_t &out_count) {
    uint8_t count = src[0];
    uint8_t bits = src[1];
    out_count = count;
    size_t in_byte_idx = 2;

    if (bits == 0) {
      std::fill(dst_deltas, dst_deltas + count, 0);
      return in_byte_idx;
    }

    uint64_t bit_buf = 0;
    int bit_cnt = 0;
    uint32_t mask = (1ULL << bits) - 1;

    for (size_t i = 0; i < count; ++i) {
      while (bit_cnt < bits) {
        bit_buf |= (static_cast<uint64_t>(src[in_byte_idx++]) << bit_cnt);
        bit_cnt += 8;
      }
      dst_deltas[i] = static_cast<uint32_t>(bit_buf & mask);
      bit_buf >>= bits;
      bit_cnt -= bits;
    }

    return in_byte_idx;
  }
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_FOR128_H_
