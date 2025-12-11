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

constexpr size_t kPartitionSize = 128;        // Partition every 128 bytes
constexpr uint8_t kIsPositionBit = 0x01;      // Bit 0: 1=position, 0=field mask
constexpr uint8_t kStartPositionBit = 0x02;   // Bit 1: 1=start, 0=continuation
constexpr uint8_t kPositionValueMask = 0xFC;  // Bits 2-7 for position value
constexpr uint8_t kValueShift = 2;            // Shift to extract 6-bit value
constexpr uint8_t kFieldMaskValueMask = 0xFC;   // Bits 2-7 for field mask value
constexpr uint8_t kFieldMaskPrefix = 0x02;      // Field mask byte prefix (01)
constexpr uint8_t kPositionStartPrefix = 0x03;  // Position start prefix (11)
constexpr uint8_t kTwoBitMask = 0x03;           // Mask for lower 2 bits
constexpr uint8_t kSixBitMask = 0x3F;           // Mask for 6-bit values
constexpr uint8_t kTerminatorByte = 0x00;       // Terminator byte
constexpr uint8_t kBitsPerValue = 6;            // 6 bits per encoded value
constexpr uint8_t kByteMask = 0xFF;             // Single byte mask
constexpr uint8_t kPartitionDeltaBytes = 4;  // Bytes per partition delta entry

// Type conversion helpers
inline uint8_t U8(char c) { return static_cast<uint8_t>(c); }
inline uint32_t U32(uint8_t v) { return static_cast<uint32_t>(v); }
inline uint64_t U64(uint8_t v) { return static_cast<uint64_t>(v); }
inline char C(uint8_t v) { return static_cast<char>(v); }

FlatPositionMap::~FlatPositionMap() {
  if (data_ != nullptr) {
    free(data_);
  }
}

FlatPositionMap::FlatPositionMap(FlatPositionMap&& other) noexcept
    : data_(other.data_) {
  other.data_ = nullptr;
}

FlatPositionMap& FlatPositionMap::operator=(FlatPositionMap&& other) noexcept {
  if (this != &other) {
    if (data_ != nullptr) {
      free(data_);
    }
    data_ = other.data_;
    other.data_ = nullptr;
  }
  return *this;
}

//=============================================================================
// Header Parsing & Detection
//=============================================================================

// Calculate minimum bytes needed to store a value
static uint8_t BytesNeeded(uint32_t value) {
  if (value == 0) return 1;
  if (value <= 0xFF) return 1;
  if (value <= 0xFFFF) return 2;
  if (value <= 0xFFFFFF) return 3;
  return 4;
}

// Read variable-length unsigned int (little-endian)
static uint32_t ReadVarUint(const char* ptr, uint8_t num_bytes) {
  uint32_t value = 0;
  for (uint8_t i = 0; i < num_bytes; ++i) {
    value |= (U32(U8(ptr[i])) << (i * 8));
  }
  return value;
}

// Parse header to extract position count, partition count, and header size
static uint8_t ParseHeader(const char* flat_map, uint32_t& num_positions,
                           uint32_t& num_partitions) {
  if (!flat_map) {
    num_positions = num_partitions = 0;
    return 0;
  }

  uint8_t first_byte = U8(flat_map[0]);
  uint8_t pos_bytes = ((first_byte >> 3) & kTwoBitMask) + 1;
  uint8_t part_bytes = ((first_byte >> 5) & kTwoBitMask) + 1;

  num_positions = ReadVarUint(flat_map + 1, pos_bytes);
  num_partitions = ReadVarUint(flat_map + 1 + pos_bytes, part_bytes);

  return 1 + pos_bytes + part_bytes;
}

// Detect how many bytes per field mask by reading first position's field mask
static uint8_t DetectFieldMaskBytes(const char* data_ptr) {
  const char* ptr = data_ptr;

  // Skip to end of first position bytes
  while ((U8(*ptr) & kIsPositionBit)) {
    ptr++;
    if ((U8(*(ptr - 1)) & kStartPositionBit) &&
        (!(U8(*ptr) & kIsPositionBit) || (U8(*ptr) & kStartPositionBit))) {
      break;
    }
  }

  // Count field mask bytes following first position
  uint8_t byte_count = 0;
  while ((U8(*ptr++) & kTwoBitMask) == kFieldMaskPrefix) byte_count++;

  return byte_count;
}

//=============================================================================
// Encoding Functions
//=============================================================================

// Encode position delta with 6 bits per byte, first byte marked with start bit
static void EncodePositionDelta(
    absl::InlinedVector<char, kPartitionSize>& buffer, uint32_t delta,
    bool is_start) {
  bool first = true;
  do {
    uint8_t byte_val = (delta & kSixBitMask) << kValueShift;
    byte_val |= kIsPositionBit;
    if (first && is_start) {
      byte_val |= kStartPositionBit;
      first = false;
    }
    buffer.push_back(C(byte_val));
    delta >>= kBitsPerValue;
  } while (delta > 0);
}

// Encode field mask with 6 bits per byte
static void EncodeFieldMask(absl::InlinedVector<char, kPartitionSize>& buffer,
                            uint64_t field_mask, size_t num_text_fields) {
  size_t num_bytes = (num_text_fields + kBitsPerValue - 1) / kBitsPerValue;

  for (size_t i = 0; i < num_bytes; ++i) {
    uint8_t byte_val = (field_mask & kSixBitMask) << kValueShift;
    byte_val |= kFieldMaskPrefix;
    buffer.push_back(C(byte_val));
    field_mask >>= kBitsPerValue;
  }
}

static void EncodeTerminator(
    absl::InlinedVector<char, kPartitionSize>& buffer) {
  buffer.push_back(C(kTerminatorByte));
}

//=============================================================================
// Decoding Functions
//=============================================================================

// Decode position delta from variable-length encoding, advances ptr past
// position
static uint32_t DecodePositionDelta(const char*& ptr) {
  uint32_t delta = 0, shift = 0;

  while ((U8(*ptr) & kIsPositionBit)) {
    uint8_t byte_val = U8(*ptr);
    uint8_t value = (byte_val & kPositionValueMask) >> kValueShift;
    delta |= (U32(value) << shift);
    shift += kBitsPerValue;
    ptr++;

    // Stop if: multi-byte position complete, or next byte not a position
    // continuation
    if (((byte_val & kStartPositionBit) && shift > kBitsPerValue) ||
        !(U8(*ptr) & kIsPositionBit) || (U8(*ptr) & kStartPositionBit)) {
      break;
    }
  }
  return delta;
}

// Decode field mask from variable-length encoding, advances ptr past field mask
static uint64_t DecodeFieldMask(const char*& ptr) {
  uint64_t mask = 0;
  for (uint32_t shift = 0; (U8(*ptr) & kTwoBitMask) == kFieldMaskPrefix;
       shift += kBitsPerValue) {
    mask |= (U64((U8(*ptr++) & kFieldMaskValueMask) >> kValueShift) << shift);
  }
  return mask;
}

//=============================================================================
// Serialization
//=============================================================================

// Serialize position map to compact byte array format
// Layout: [Header][Partition Map][Position/Field Mask Data][Terminator]
FlatPositionMap FlatPositionMap::SerializePositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  if (position_map.empty()) {
    char* flat_map = static_cast<char*>(malloc(3));
    CHECK(flat_map != nullptr);
    flat_map[0] = flat_map[1] = flat_map[2] = kTerminatorByte;
    return FlatPositionMap(flat_map);
  }

  uint32_t num_positions = position_map.size();
  bool single_field = (num_text_fields == 1);

  absl::InlinedVector<char, kPartitionSize> position_data;
  std::vector<uint32_t>
      partition_deltas;  // Cumulative deltas at partition boundaries

  Position prev_pos = 0;
  Position cumulative_delta = 0;
  uint64_t prev_field_mask = 0;
  bool first_position = true;
  bool is_first_in_partition = true;
  size_t partition_start_offset = 0;

  // Encode each position with delta compression
  for (const auto& [pos, field_mask] : position_map) {
    uint32_t delta = pos - prev_pos;
    cumulative_delta += delta;

    // Create partition boundary every kPartitionSize bytes
    if (position_data.size() - partition_start_offset >= kPartitionSize &&
        !first_position) {
      partition_deltas.push_back(cumulative_delta - delta);
      partition_start_offset += kPartitionSize;
      is_first_in_partition = true;
      if (!single_field) prev_field_mask = 0;  // Reset for partition start
    }

    EncodePositionDelta(position_data, delta, true);

    // Encode field mask only if it changes, or at first/partition-start
    // position
    if (!single_field) {
      uint64_t current_mask = field_mask->AsUint64();
      if (first_position || current_mask != prev_field_mask ||
          is_first_in_partition) {
        EncodeFieldMask(position_data, current_mask, num_text_fields);
        prev_field_mask = current_mask;
      }
    }

    prev_pos = pos;
    first_position = false;
    is_first_in_partition = false;
  }

  EncodeTerminator(position_data);

  // Build final byte array: [Header][Partition Map][Position Data]
  uint32_t num_partitions = partition_deltas.size();
  uint8_t pos_bytes = BytesNeeded(num_positions);
  uint8_t part_bytes = BytesNeeded(num_partitions);

  uint8_t header_size = 1 + pos_bytes + part_bytes;
  size_t partition_map_size =
      num_partitions ? (num_partitions + 1) * kPartitionDeltaBytes : 0;
  size_t total_size = header_size + partition_map_size + position_data.size();

  char* flat_map = static_cast<char*>(malloc(total_size));
  CHECK(flat_map != nullptr);

  // Write header byte encoding position/partition byte counts
  uint8_t first_byte = ((pos_bytes - 1) << 3) | ((part_bytes - 1) << 5);
  size_t offset = 0;
  flat_map[offset++] = C(first_byte);

  // Write position count (little-endian)
  for (uint8_t i = 0; i < pos_bytes; ++i) {
    flat_map[offset++] = C((num_positions >> (i * 8)) & kByteMask);
  }

  // Write partition count (little-endian)
  for (uint8_t i = 0; i < part_bytes; ++i) {
    flat_map[offset++] = C((num_partitions >> (i * 8)) & kByteMask);
  }

  // Write partition map: cumulative deltas at each partition boundary
  if (num_partitions) {
    for (uint32_t delta : partition_deltas) {
      for (uint8_t i = 0; i < kPartitionDeltaBytes; ++i) {
        flat_map[offset++] = C((delta >> (i * 8)) & kByteMask);
      }
    }
    // Final entry is total cumulative delta
    for (uint8_t i = 0; i < kPartitionDeltaBytes; ++i) {
      flat_map[offset++] = C((cumulative_delta >> (i * 8)) & kByteMask);
    }
  }

  std::memcpy(flat_map + offset, position_data.data(), position_data.size());

  return FlatPositionMap(flat_map);
}

//=============================================================================
// Iterator Implementation
//=============================================================================

FlatPositionMapIterator::FlatPositionMapIterator(
    const FlatPositionMap& flat_map)
    : flat_map_(flat_map.data()),
      current_start_ptr_(nullptr),
      current_end_ptr_(nullptr),
      data_start_(nullptr),
      cumulative_position_(0),
      current_position_(0),
      total_positions_(0),
      num_partitions_(0),
      header_size_(0),
      current_field_mask_(1),
      field_mask_bytes_(0) {
  if (!flat_map_) return;

  header_size_ = ParseHeader(flat_map_, total_positions_, num_partitions_);
  if (!total_positions_) return;

  size_t partition_map_size =
      num_partitions_ ? (num_partitions_ + 1) * kPartitionDeltaBytes : 0;
  data_start_ = flat_map_ + header_size_ + partition_map_size;
  current_start_ptr_ = current_end_ptr_ = data_start_;
  field_mask_bytes_ = DetectFieldMaskBytes(data_start_);
  NextPosition();
}

bool FlatPositionMapIterator::IsValid() const {
  return current_start_ptr_ != nullptr;
}

// Advance to next position, updating current_position_ and current_field_mask_
void FlatPositionMapIterator::NextPosition() {
  if (!IsValid()) return;

  current_start_ptr_ = current_end_ptr_;

  if (U8(*current_start_ptr_) == kTerminatorByte) {
    current_start_ptr_ = current_end_ptr_ = nullptr;
    return;
  }

  const char* ptr = current_start_ptr_;
  cumulative_position_ += DecodePositionDelta(ptr);
  current_position_ = cumulative_position_;

  // Update field mask if present at this position
  if (field_mask_bytes_ && (U8(*ptr) & kTwoBitMask) == kFieldMaskPrefix) {
    current_field_mask_ = DecodeFieldMask(ptr);
  }

  current_end_ptr_ = ptr;
}

// Binary search to find partition index before target position
static uint32_t FindPartitionForTarget(const char* partition_map,
                                       uint32_t num_partitions,
                                       Position target) {
  uint32_t left = 0, right = num_partitions;

  while (left < right) {
    uint32_t mid = left + (right - left) / 2;
    uint32_t partition_delta = ReadVarUint(
        partition_map + (mid * kPartitionDeltaBytes), kPartitionDeltaBytes);
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
bool FlatPositionMapIterator::SkipForwardPosition(Position target) {
  // Always restart from beginning
  current_start_ptr_ = current_end_ptr_ = data_start_;
  cumulative_position_ = 0;

  // Use partition map to jump close to target
  if (num_partitions_) {
    const char* partition_map = flat_map_ + header_size_;
    uint32_t partition_idx =
        FindPartitionForTarget(partition_map, num_partitions_, target);

    if (partition_idx) {
      cumulative_position_ =
          ReadVarUint(partition_map + (partition_idx * kPartitionDeltaBytes),
                      kPartitionDeltaBytes);
      const char* partition_ptr =
          data_start_ + (partition_idx * kPartitionSize);

      // Find first position start (11 prefix) in partition
      while (U8(*partition_ptr) != kTerminatorByte &&
             (U8(*partition_ptr) & kTwoBitMask) != kPositionStartPrefix)
        partition_ptr++;

      if (U8(*partition_ptr) == kTerminatorByte) {
        current_start_ptr_ = current_end_ptr_ = nullptr;
        return false;
      }

      current_start_ptr_ = current_end_ptr_ = partition_ptr;
    }
  }

  NextPosition();

  // Linear search from partition start (or beginning) to target
  while (IsValid()) {
    if (current_position_ >= target) return current_position_ == target;
    NextPosition();
  }
  return false;
}

Position FlatPositionMapIterator::GetPosition() const {
  return current_position_;
}

uint64_t FlatPositionMapIterator::GetFieldMask() const {
  return field_mask_bytes_ ? current_field_mask_ : 1;
}

//=============================================================================
// Public Query Methods
//=============================================================================

uint32_t FlatPositionMap::CountPositions() const {
  if (!data_) return 0;
  uint32_t num_positions, num_partitions;
  ParseHeader(data_, num_positions, num_partitions);
  return num_positions;
}

size_t FlatPositionMap::CountTermFrequency() const {
  if (!data_) return 0;
  size_t total_frequency = 0;
  for (FlatPositionMapIterator iter(*this); iter.IsValid();
       iter.NextPosition()) {
    total_frequency += __builtin_popcountll(iter.GetFieldMask());
  }
  return total_frequency;
}

}  // namespace valkey_search::indexes::text
