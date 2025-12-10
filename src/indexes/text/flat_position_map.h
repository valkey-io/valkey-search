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
  [Variable header] [optional partition map] [position/field data]

Header layout (variable length):
First byte (8 bits):
  Bit 0:     Header selection (0=standard, 1=special)
  Bits 1-2:  Encoding scheme (2 bits) - reserved for future use
  Bits 3-4:  Number of bytes to store position count (0-3 = 1-4 bytes)
  Bits 5-6:  Number of bytes to store partition count (0-3 = 1-4 bytes)
  Bit 7:     Reserved

After first byte:
  - N bytes for number of positions (N determined by bits 3-4)
  - M bytes for number of partitions (M determined by bits 5-6, can be 0)

Encoding scheme:
- Single general case with byte-based partitions
- Partitions created every 128 bytes (PARTITION_SIZE) of serialized data
- Each partition stores only the cumulative sum of deltas (offset implicit from
byte count)
- Position bytes have 2-bit prefix: bit 0=1 (position), bit 1=1 (start), bit 1=0
(continuation)
- Field mask bytes have 2-bit prefix: bit 0=0 (field mask)
- Field masks optimized: if num_fields=1, no field mask bytes stored
- Field masks only stored when they change or at partition start (when
num_fields > 1)

Delta encoding stores position differences not absolutes.

A FlatPositionMapIterator provides sequential iteration and skip-forward with
minimal state overhead, maintaining cumulative position for delta decoding.

*/

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>

namespace valkey_search::indexes::text {

// FlatPositionMap format constants
constexpr size_t kPartitionSize = 128;  // Partition every 128 bytes

// Encoding bit flags for position/field mask bytes
constexpr uint8_t kBitPosition = 0x01;  // Bit 0: 1=position, 0=field mask
constexpr uint8_t kBitStartPosition =
    0x02;  // Bit 1: 1=start of position, 0=continuation
constexpr uint8_t kValueMask = 0xFC;  // Bits 2-7 for actual value (6 bits)
constexpr uint8_t kValueShift = 2;    // Shift amount for value bits

// Field mask encoding (when bit 0 = 0)
constexpr uint8_t kFieldMaskValueMask =
    0xFC;  // Bits 2-7 for field mask (6 bits)
constexpr uint8_t kFieldMaskBitsPerByte = 6;  // 6 bits per byte for field mask

// Forward declarations to avoid circular dependency
using Position = uint32_t;
class FieldMask;

// FlatPositionMap is a compact byte array representation
// Layout: [Variable Header][Optional Partition Map][Position/Field Data]
class FlatPositionMap {
 public:
  // Default constructor: empty map
  FlatPositionMap() : data_(nullptr) {}

  // Constructor from raw pointer: takes ownership
  explicit FlatPositionMap(char* d) : data_(d) {}

  // Destructor: frees the allocated memory
  ~FlatPositionMap();

  // Move constructor: transfers ownership, nullifies source
  FlatPositionMap(FlatPositionMap&& other) noexcept;

  // Move assignment: frees current, transfers ownership, nullifies source
  FlatPositionMap& operator=(FlatPositionMap&& other) noexcept;

  // Serialize PositionMap to FlatPositionMap
  static FlatPositionMap SerializePositionMap(
      const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
      size_t num_text_fields);

  // Get position count
  uint32_t CountPositions() const;

  // Get total term frequency
  size_t CountTermFrequency() const;

  // Access to raw data pointer
  const char* data() const { return data_; }

 private:
  char* data_;
};

// Iterator for FlatPositionMap
class FlatPositionMapIterator {
 public:
  FlatPositionMapIterator(const FlatPositionMap& flat_map);

  bool IsValid() const;
  void NextPosition();
  bool SkipForwardPosition(Position target);
  Position GetPosition() const;
  uint64_t GetFieldMask() const;

 private:
  const char* flat_map_;
  const char* current_start_ptr_;
  const char* current_end_ptr_;
  const char* data_start_;
  Position cumulative_position_;
  Position current_position_;
  uint32_t total_positions_;
  uint32_t num_partitions_;
  uint8_t header_size_;
  uint64_t current_field_mask_;
  uint8_t field_mask_bytes_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_
