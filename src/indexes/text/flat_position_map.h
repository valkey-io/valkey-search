/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_

/*

FlatPositionMap is a space-optimized serialized representation of position data,
replacing std::map<Position, FieldMask> which consumes 80+ bytes per position
with a byte array achieving 1-8 bytes per position. This is critical for memory
efficiency as millions of these structures exist across the full-text corpus.

During document ingestion, positions accumulate in std::map for efficient
random insertion. Upon completion, the map is serialized into FlatPositionMap
and the map is destroyed. The FlatPositionMap is read-only thereafter and used
by search queries.

Structure Layout:
  [4-byte header] [optional partition map] [position/field data]

Header layout (4 bytes / 32 bits):
Bit 0:     Header selection (0=standard, 1=special)
Bits 1-2:  Encoding scheme (2 bits)
Bits 3-31: Number of positions (29 bits).

Three encoding schemes are auto-selected:

- SIMPLE: Single-field docs with positions < 256. Uses 1 byte per position
  delta, no field masks. Most compact.

- EXPANDABLE: General case. Variable-length encoding with bit 7 as type flag
  (0=position byte, 1=field mask byte). Position deltas expand across multiple
  bytes when needed (LSB first). Field masks use 7 bits per byte, supporting
  up to 64 fields.

- BINARY_SEARCH: Large position lists (>128). Adds partition map before data
  for O(log n) skip-forward. Each partition stores [4-byte offset, 4-byte
  cumulative delta] enabling binary search to target positions.

Delta encoding stores position differences not absolutes.

A FlatPositionMapIterator provides sequential iteration and skip-forward with
minimal state overhead, maintaining cumulative position for delta decoding.

*/

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

namespace valkey_search::indexes::text {

// Forward declarations to avoid circular dependency
using Position = uint32_t;
class FieldMask;

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

// Serialize PositionMap to FlatPositionMap
FlatPositionMap SerializePositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields);

// Free FlatPositionMap
void FreeFlatPositionMap(FlatPositionMap flat_map);

// Get position count from FlatPositionMap
uint32_t CountPositions(FlatPositionMap flat_map);

// Get total term frequency from FlatPositionMap
size_t CountTermFrequency(FlatPositionMap flat_map);

// Iterator for FlatPositionMap
class FlatPositionMapIterator {
 public:
  FlatPositionMapIterator(FlatPositionMap flat_map);

  bool IsValid() const;
  void NextPosition();
  bool SkipForwardPosition(Position target);
  Position GetPosition() const;
  uint64_t GetFieldMask() const;

 private:
  FlatPositionMap flat_map_;
  const char* current_ptr_;
  Position cumulative_position_;
  uint32_t positions_read_;
  uint32_t total_positions_;
  uint8_t field_bytes_;  // Inferred from flat_map data
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_
