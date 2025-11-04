/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

#include "src/indexes/text/posting.h"

namespace valkey_search::indexes::text {

// FlatPositionMap is a compact byte array representation
// Layout: [Header: 4 bytes][Optional Partition Map][Position/Field Data]
using FlatPositionMap = char*;

// Encoding schemes
enum class EncodingScheme : uint8_t {
  SIMPLE = 0,         // 1 byte per position, no field mask (single field)
  EXPANDABLE = 1,     // Variable bytes with encoding bit
  BINARY_SEARCH = 2,  // With partition map for skip operations
  RESERVED = 3
};

// Header layout (4 bytes / 32 bits):
// Bit 0:     Header selection (0=standard, 1=special)
// Bits 1-2:  Encoding scheme (2 bits)
// Bits 3-31: Number of positions (29 bits)

// Serialize PositionMap to FlatPositionMap
FlatPositionMap SerializePositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields);

// Free FlatPositionMap
void FreeFlatPositionMap(FlatPositionMap flat_map);

// Iterator for FlatPositionMap - minimal state, just pointer
class FlatPositionMapIterator {
 public:
  FlatPositionMapIterator(FlatPositionMap flat_map);

  bool IsValid(size_t num_text_fields) const;
  void Next(size_t num_text_fields);
  bool SkipForward(Position target, size_t num_text_fields);
  Position GetPosition(size_t num_text_fields) const;
  uint64_t GetFieldMask(size_t num_text_fields) const;

 private:
  FlatPositionMap flat_map_;
  const char* current_ptr_;
  Position cumulative_position_;
  uint32_t positions_read_;
  uint32_t total_positions_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_
