/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/flat_position_map.h"

#include <cstdlib>
#include <cstring>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "src/indexes/text/posting.h"
#include "vmsdk/src/memory_allocation_overrides.h"

namespace valkey_search::indexes::text {

// Partition and encoding constants
constexpr size_t kPartitionSize = 128;
constexpr uint8_t kContinueBit = 0x80;     // Bit 7: 0=start/last, 1=continue
constexpr uint8_t kSevenBitMask = 0x7F;    // Bits 0-6: data
constexpr uint8_t kBitsPerByte = 7;        // 7 bits of data per byte
constexpr uint8_t kTerminatorByte = 0x00;  // Terminator byte
constexpr uint8_t kPartitionDeltaBytes = 4;

// Type conversion helpers
inline uint8_t U8(char c) { return static_cast<uint8_t>(c); }
inline uint32_t U32(uint8_t v) { return static_cast<uint32_t>(v); }
template <typename T>
inline uint64_t U64(T v) {
  return static_cast<uint64_t>(v);
}
template <typename T>
inline __uint128_t U128(T v) {
  return static_cast<__uint128_t>(v);
}
inline char C(uint8_t v) { return static_cast<char>(v); }

//=============================================================================
// Encoding and Decoding Functions
//=============================================================================

// Encode: composite = (value << 1) | type_bit, big-endian, 7 bits/byte
template <typename T>
static inline void EncodeValue(
    absl::InlinedVector<char, kPartitionSize>& buffer, T value,
    bool is_position) {
  __uint128_t composite = (U128(value) << 1) | __uint128_t(is_position);

  auto emit = [&](auto&& self, __uint128_t v) -> void {
    if (v >> kBitsPerByte) {
      self(self, v >> kBitsPerByte);
      buffer.push_back(C((v & kSevenBitMask) | kContinueBit));
    } else {
      buffer.push_back(C(v & kSevenBitMask));  // first byte, bit 7=0
    }
  };

  emit(emit, composite);
}

// Decode: big-endian, first byte bit 7=0, continuation bytes bit 7=1
template <typename T>
static inline T DecodeValue(const char*& ptr, bool& out_is_position) {
  // Read first byte (should not be terminator)
  CHECK(U8(*ptr) != kTerminatorByte) << "Attempted to decode terminator byte";
  T composite = U8(*ptr++) & kSevenBitMask;

  // Read continuation bytes and assemble big-endian
  while (U8(*ptr) & kContinueBit) {
    composite = (composite << kBitsPerByte) | (U8(*ptr++) & kSevenBitMask);
  }

  // Extract type from LSB and value from remaining bits
  out_is_position = composite & 1;
  return composite >> 1;
}

//=============================================================================
// Serialization
//=============================================================================

// Static factory and destroyer
FlatPositionMap* FlatPositionMap::Create(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  CHECK(!position_map.empty())
      << "Cannot create FlatPositionMap from empty position_map";

  // First pass: compute data size (same logic as old constructor)
  uint32_t num_positions = position_map.size();

  absl::InlinedVector<char, kPartitionSize> position_data;
  std::vector<uint32_t> partition_byte_offsets;
  std::vector<Position> partition_deltas;

  Position prev_pos = 0;
  Position cumulative_delta = 0;
  uint64_t prev_field_mask = 0;
  bool is_partition_start = true;

  // Encode: [position][field_mask (if changed)]...
  for (const auto& [pos, field_mask] : position_map) {
    // Check for new partition boundary
    if (position_data.size() >=
            (partition_byte_offsets.size() + 1) * kPartitionSize &&
        !is_partition_start) {
      partition_byte_offsets.push_back(position_data.size());
      partition_deltas.push_back(cumulative_delta);
      is_partition_start = true;
      prev_field_mask = 0;
    }

    Position delta = pos - prev_pos;
    cumulative_delta += delta;

    // Encode position delta
    EncodeValue(position_data, delta, true);  // true = position

    // Encode field mask if multi-field and changed
    if (num_text_fields > 1) {
      uint64_t current_mask = field_mask->AsUint64();
      if (is_partition_start || current_mask != prev_field_mask) {
        EncodeValue(position_data, current_mask, false);  // false = field_mask
        prev_field_mask = current_mask;
      }
    }

    prev_pos = pos;
    is_partition_start = false;
  }

  uint32_t num_partitions = partition_byte_offsets.size();

  // Terminator: Field mask with value 0: composite = 0, encoded as 0x00
  position_data.push_back(C(kTerminatorByte));

  // Calculate sizes
  uint8_t pos_bytes = BytesNeeded(num_positions) - 1;
  uint8_t part_bytes = BytesNeeded(num_partitions) - 1;
  size_t partition_map_size = num_partitions * kPartitionDeltaBytes * 2;
  size_t counts_size = (pos_bytes + 1) + (part_bytes + 1);
  size_t data_size = counts_size + partition_map_size + position_data.size();
  size_t total_size = sizeof(FlatPositionMap) + data_size;

  // Allocate single block: [FlatPositionMap | data...]
  void* mem = operator new(total_size);
  auto* map = new (mem) FlatPositionMap();

  // Initialize header fields
  map->header_scheme_ = 0;
  map->encoding_scheme_ = 0;
  map->pos_bytes_ = pos_bytes;
  map->part_bytes_ = part_bytes;
  map->unused_ = 0;

  // Write data immediately after struct
  char* p = map->data();

  // Write counts
  map->WriteCounts(p, num_positions, num_partitions);

  // Write partition map
  for (size_t i = 0; i < num_partitions; ++i) {
    std::memcpy(p, &partition_byte_offsets[i], kPartitionDeltaBytes);
    p += kPartitionDeltaBytes;
    std::memcpy(p, &partition_deltas[i], kPartitionDeltaBytes);
    p += kPartitionDeltaBytes;
  }

  // Write position data
  std::memcpy(p, position_data.data(), position_data.size());

  return map;
}

void FlatPositionMap::Destroy(FlatPositionMap* map) {
  if (!map) return;
  map->~FlatPositionMap();
  operator delete(map);
}

//=============================================================================
// Helper Functions
//=============================================================================

uint8_t FlatPositionMap::BytesNeeded(uint32_t value) {
  if (value <= 0xFF) return 1;
  if (value <= 0xFFFF) return 2;
  if (value <= 0xFFFFFF) return 3;
  return 4;
}

std::pair<uint32_t, uint32_t> FlatPositionMap::ReadCounts(
    const char*& p) const {
  uint32_t num_positions = 0, num_partitions = 0;
  std::memcpy(&num_positions, p, pos_bytes_ + 1);
  p += pos_bytes_ + 1;
  std::memcpy(&num_partitions, p, part_bytes_ + 1);
  p += part_bytes_ + 1;
  return {num_positions, num_partitions};
}

void FlatPositionMap::WriteCounts(char*& p, uint32_t num_positions,
                                  uint32_t num_partitions) const {
  std::memcpy(p, &num_positions, pos_bytes_ + 1);
  p += pos_bytes_ + 1;
  std::memcpy(p, &num_partitions, part_bytes_ + 1);
  p += part_bytes_ + 1;
}

// Read fixed-length unsigned int from partition map (little-endian)
uint32_t PositionIterator::ReadVarUint(const char* ptr, uint8_t num_bytes) {
  uint32_t value = 0;
  for (uint8_t i = 0; i < num_bytes; ++i) {
    value |= (U32(U8(ptr[i])) << (i * 8));
  }
  return value;
}

//=============================================================================
// Iterator Implementation
//=============================================================================

PositionIterator::PositionIterator(const FlatPositionMap& flat_map)
    : flat_map_(flat_map.data()),
      current_start_ptr_(nullptr),
      current_end_ptr_(nullptr),
      data_start_(nullptr),
      cumulative_position_(0),
      num_partitions_(0),
      header_size_(0),
      current_field_mask_(1) {
  CHECK(flat_map_)
      << "Cannot create PositionIterator from null FlatPositionMap";

  const char* p = flat_map_;
  auto [num_positions, num_partitions] = flat_map.ReadCounts(p);
  CHECK(num_positions > 0)
      << "Cannot create PositionIterator from FlatPositionMap with 0 positions";

  num_partitions_ = num_partitions;
  header_size_ = p - flat_map_;

  size_t partition_map_size = num_partitions_ * kPartitionDeltaBytes * 2;
  data_start_ = flat_map_ + header_size_ + partition_map_size;
  current_start_ptr_ = current_end_ptr_ = data_start_;

  NextPosition();
}

bool PositionIterator::IsValid() const { return current_start_ptr_ != nullptr; }

// Advance to next position, updating current_position_ and current_field_mask_
void PositionIterator::NextPosition() {
  if (!IsValid()) return;

  // Check end conditions
  if (U8(*current_end_ptr_) == kTerminatorByte) {
    current_start_ptr_ = current_end_ptr_ = nullptr;
    return;
  }

  current_start_ptr_ = current_end_ptr_;
  const char* ptr = current_start_ptr_;

  // Decode position (always comes first)
  bool is_position;
  Position delta = DecodeValue<Position>(ptr, is_position);
  CHECK(is_position) << "Expected position, got field mask";

  cumulative_position_ += delta;

  // Check if next value is a field mask (not terminator, not continuation byte)
  if (U8(*ptr) != kTerminatorByte && (U8(*ptr) & kContinueBit) == 0) {
    // Decode next value to check its type (use __uint128_t for large field
    // masks)
    const char* peek_ptr = ptr;
    bool next_is_position;
    __uint128_t next_value =
        DecodeValue<__uint128_t>(peek_ptr, next_is_position);

    if (!next_is_position) {
      // It's a field mask, consume it
      current_field_mask_ = U64(next_value);
      ptr = peek_ptr;
    }
  }

  current_end_ptr_ = ptr;
}

// Binary search to find partition index before target position
uint32_t PositionIterator::FindPartitionForTarget(const char* partition_map,
                                                  uint32_t num_partitions,
                                                  Position target) {
  uint32_t left = 0, right = num_partitions;

  while (left < right) {
    uint32_t mid = left + (right - left) / 2;
    uint32_t partition_delta = ReadVarUint(
        partition_map + (mid * kPartitionDeltaBytes * 2) + kPartitionDeltaBytes,
        kPartitionDeltaBytes);
    if (partition_delta < target)
      left = mid + 1;
    else
      right = mid;
  }

  return left ? left - 1 : 0;
}

// Skip forward to target position using partition map for optimization
// Returns true if exact match found, false otherwise (iter positioned at next
// >= target)
bool PositionIterator::SkipForwardPosition(Position target) {
  CHECK(target >= cumulative_position_)
      << "SkipForwardPosition called with target < current position";

  // Linear search for one partition
  const char* partition_start = current_start_ptr_;

  while (IsValid() && (current_start_ptr_ - partition_start) < kPartitionSize) {
    if (cumulative_position_ >= target) return cumulative_position_ == target;
    NextPosition();
  }

  // Jump to partition if beneficial
  if (IsValid() && num_partitions_) {
    const char* partition_map = flat_map_ + header_size_;
    uint32_t partition_idx =
        FindPartitionForTarget(partition_map, num_partitions_, target);

    uint32_t byte_offset =
        ReadVarUint(partition_map + (partition_idx * kPartitionDeltaBytes * 2),
                    kPartitionDeltaBytes);
    uint32_t partition_delta =
        ReadVarUint(partition_map + (partition_idx * kPartitionDeltaBytes * 2) +
                        kPartitionDeltaBytes,
                    kPartitionDeltaBytes);

    if (partition_delta < target && partition_delta > cumulative_position_) {
      // Jump to partition
      cumulative_position_ = partition_delta;
      current_start_ptr_ = current_end_ptr_ = data_start_ + byte_offset;
      current_field_mask_ = 1;
      NextPosition();
    }
  }

  // Continue linear search
  while (IsValid()) {
    if (cumulative_position_ >= target) return cumulative_position_ == target;
    NextPosition();
  }
  return false;
}

Position PositionIterator::GetPosition() const { return cumulative_position_; }

uint64_t PositionIterator::GetFieldMask() const { return current_field_mask_; }

//=============================================================================
// Public Query Methods
//=============================================================================

uint32_t FlatPositionMap::CountPositions() const {
  const char* p = data();
  auto [num_positions, _] = ReadCounts(p);
  return num_positions;
}

size_t FlatPositionMap::CountTermFrequency() const {
  size_t total_frequency = 0;
  for (PositionIterator iter(*this); iter.IsValid(); iter.NextPosition()) {
    total_frequency += __builtin_popcountll(iter.GetFieldMask());
  }
  return total_frequency;
}

}  // namespace valkey_search::indexes::text
