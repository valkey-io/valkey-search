/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/text/for128.h"

#include "gtest/gtest.h"

namespace valkey_search::indexes::text {

TEST(FOR128Test, PackUnpack) {
  uint32_t original[128];
  for (size_t i = 0; i < 128; ++i) {
    original[i] = (i * 3 + 7) % 15; // Requires 4 bits max
  }

  uint8_t bits = FOR128Codec::BitsRequired(original, 128);
  EXPECT_EQ(bits, 4);

  uint8_t buffer[256];
  size_t bytes_written = FOR128Codec::Pack(original, 128, bits, buffer);
  EXPECT_EQ(bytes_written, 2 + 64); // 2 bytes header + 64 bytes payload

  uint32_t decoded[128];
  size_t count = 0;
  size_t bytes_read = FOR128Codec::Unpack(buffer, decoded, count);
  EXPECT_EQ(bytes_read, bytes_written);
  EXPECT_EQ(count, 128);

  for (size_t i = 0; i < 128; ++i) {
    EXPECT_EQ(decoded[i], original[i]);
  }
}

}  // namespace valkey_search::indexes::text
