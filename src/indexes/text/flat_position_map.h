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
- Varint encoding: bit 7=1 (continuation, more bytes follow), bit 7=0 (end/last byte)
- Position/field mask distinction: LSB of decoded composite value (1=position, 0=field mask)
- Field masks optimized: if num_fields=1, no field mask bytes stored
- Field masks only stored when they change or at partition start (when
num_fields > 1)

Delta encoding stores position differences not absolutes.

A PositionIterator provides sequential iteration and skip-forward with
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
// Layout: [Bitfield Header][Optional Partition Map][Position/Field Data]
class FlatPositionMap {
 public:
  // Factory: allocates single block [FlatPositionMap | data...]
  static FlatPositionMap* Create(
      const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
      size_t num_text_fields);

  // Destructor: frees the allocated memory
  static void Destroy(FlatPositionMap* map);

  // Get position count
  uint32_t CountPositions() const;

  // Get total term frequency
  size_t CountTermFrequency() const;

  // Access to raw data pointer (stored immediately after this object)
  inline char* data() { return reinterpret_cast<char*>(this + 1); }

  inline const char* data() const {
    return reinterpret_cast<const char*>(this + 1);
  }

 private:
  friend class PositionIterator;  // Allow iterator to access bitfield members

  // Helper methods (implemented in .cc)
  static uint8_t BytesNeeded(uint32_t value);
  std::pair<uint32_t, uint32_t> ReadCounts(const char*& p) const;
  void WriteCounts(char*& p, uint32_t num_positions,
                   uint32_t num_partitions) const;

  // Bitfield members for automatic packing/unpacking (no manual operations)
  uint8_t header_scheme_ : 1;    // Bit 0: Header selection (0 or 1)
  uint8_t encoding_scheme_ : 2;  // Bits 1-2: Encoding scheme (0 to 3)
  uint8_t pos_bytes_ : 2;        // Bits 3-4: Position bytes count (0 to 3)
  uint8_t part_bytes_ : 2;       // Bits 5-6: Partition bytes count (0 to 3)
  uint8_t unused_ : 1;           // Bit 7: Reserved

  // Data is stored immediately after this object in memory
  // Layout: [num_positions][num_partitions][Partition Map][Position/Field Data]

  // Private constructor for factory
  FlatPositionMap() = default;
};

// Iterator for FlatPositionMap
class PositionIterator {
 public:
  PositionIterator(const FlatPositionMap& flat_map);

  bool IsValid() const;
  void NextPosition();
  bool SkipForwardPosition(Position target);
  Position GetPosition() const;
  uint64_t GetFieldMask() const;

 private:
  const char* flat_map_;           // Pointer to start of serialized data
  const char* current_start_ptr_;  // Start of current position entry
  const char* current_end_ptr_;    // End of current position entry
  const char*
      data_start_;  // Start of position/field data (after header+partition map)
  Position cumulative_position_;  // Absolute position (sum of all deltas)
  uint32_t num_partitions_;       // Number of partition boundaries
  size_t header_size_;            // Size of variable-length header
  uint64_t current_field_mask_;   // Bit mask of fields at current position

  // Private static helper functions for decoding and navigation
  static uint32_t ReadVarUint(const char* ptr, uint8_t num_bytes);
  static uint32_t FindPartitionForTarget(const char* partition_map,
                                         uint32_t num_partitions,
                                         Position target);
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_FLAT_POSITION_MAP_H_
