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

constexpr size_t kPartitionSize = 128;        // Partition size constant
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
constexpr uint8_t kPartitionDeltaBytes = 4;  // Bytes per partition delta entry

// Type conversion helpers
inline uint8_t U8(char c) { return static_cast<uint8_t>(c); }
inline uint32_t U32(uint8_t v) { return static_cast<uint32_t>(v); }
inline uint64_t U64(uint8_t v) { return static_cast<uint64_t>(v); }
inline char C(uint8_t v) { return static_cast<char>(v); }

FlatPositionMap::~FlatPositionMap() {
  if (data_ != nullptr) {
    delete[] data_;
  }
}

FlatPositionMap::FlatPositionMap(FlatPositionMap&& other) noexcept
    : data_(other.data_) {
  other.data_ = nullptr;
}

FlatPositionMap& FlatPositionMap::operator=(FlatPositionMap&& other) noexcept {
  if (this != &other) {
    if (data_ != nullptr) {
      delete[] data_;
    }
    data_ = other.data_;
    other.data_ = nullptr;
  }
  return *this;
}

//=============================================================================
// Header Structure & Helpers
//=============================================================================

// Header: bit-packed byte + variable-length num_positions + variable-length
// num_partitions
// Bit field layout: [0]=header_scheme (1 bit), [1-2]=encoding_scheme (2 bits),
// [3-4]=pos_bytes (2 bits), [5-6]=part_bytes (2 bits), [7]=unused (1 bit)
struct Header {
  unsigned int header_scheme : 1;    // Bit 0: Header selection (0 or 1)
  unsigned int encoding_scheme : 2;  // Bits 1-2: Encoding scheme (0 to 3)
  unsigned int pos_bytes : 2;        // Bits 3-4: Position bytes count (0 to 3)
  unsigned int part_bytes : 2;       // Bits 5-6: Partition bytes count (0 to 3)
  unsigned int unused : 1;           // Bit 7: Reserved
  uint32_t num_positions = 0;
  uint32_t num_partitions = 0;

  static uint8_t BytesNeeded(uint32_t value) {
    return value <= 0xFF ? 1 : value <= 0xFFFF ? 2 : value <= 0xFFFFFF ? 3 : 4;
  }

  // Constructor for serialization
  Header(uint32_t num_pos, uint32_t num_part)
      : header_scheme(0),
        encoding_scheme(0),
        pos_bytes(BytesNeeded(num_pos) - 1),
        part_bytes(BytesNeeded(num_part) - 1),
        unused(0),
        num_positions(num_pos),
        num_partitions(num_part) {}

  // Default constructor for deserialization
  Header()
      : header_scheme(0),
        encoding_scheme(0),
        pos_bytes(0),
        part_bytes(0),
        unused(0) {}

  size_t pack(char* p) const {
    char* start = p;
    // Pack bit fields into single byte
    *p++ = C((header_scheme & 1) | ((encoding_scheme & 3) << 1) |
             ((pos_bytes & 3) << 3) | ((part_bytes & 3) << 5));
    // Write num_positions and num_partitions (little-endian)
    std::memcpy(p, &num_positions, pos_bytes + 1);
    p += pos_bytes + 1;
    std::memcpy(p, &num_partitions, part_bytes + 1);
    p += part_bytes + 1;
    return p - start;
  }

  static Header unpack(const char* p, size_t& header_size) {
    if (!p) {
      header_size = 0;
      return {};
    }
    const char* start = p;
    uint8_t b = U8(*p++);

    Header h;
    // Unpack bit fields from single byte
    h.header_scheme = b & 1;
    h.encoding_scheme = (b >> 1) & 3;
    h.pos_bytes = (b >> 3) & 3;
    h.part_bytes = (b >> 5) & 3;
    // Read num_positions and num_partitions (little-endian)
    std::memcpy(&h.num_positions, p, h.pos_bytes + 1);
    p += h.pos_bytes + 1;
    std::memcpy(&h.num_partitions, p, h.part_bytes + 1);
    p += h.part_bytes + 1;
    header_size = p - start;
    return h;
  }
};

// Read fixed-length unsigned int from partition map (little-endian)
uint32_t PositionIterator::ReadVarUint(const char* ptr, uint8_t num_bytes) {
  uint32_t value = 0;
  for (uint8_t i = 0; i < num_bytes; ++i) {
    value |= (U32(U8(ptr[i])) << (i * 8));
  }
  return value;
}

//=============================================================================
// Encoding and Decoding Functions
//=============================================================================

// Common variable-length encoder for integers (6 bits per byte)
// Encodes value using 6 bits per byte with given prefix
template <typename T>
static inline void EncodeVarInt(
    absl::InlinedVector<char, kPartitionSize>& buffer, T value, uint8_t prefix,
    bool set_start_bit = false) {
  CHECK(value > 0 || prefix == kIsPositionBit);  // Field masks must be > 0
  do {
    buffer.push_back(C(((value & kSixBitMask) << kValueShift) | prefix |
                       (set_start_bit ? kStartPositionBit : 0)));
    value >>= kBitsPerValue;
    set_start_bit = false;
  } while (value > 0);
}

// Common variable-length decoder for integers (6 bits per byte)
template <typename T>
static inline T DecodeVarInt(const char*& ptr, uint8_t mask, uint8_t expected,
                             uint8_t value_mask, uint8_t stop_bit = 0) {
  T result = 0;
  for (uint32_t shift = 0; (U8(*ptr) & mask) == expected;
       shift += kBitsPerValue) {
    uint8_t byte_val = U8(*ptr++);
    result |= static_cast<T>((byte_val & value_mask) >> kValueShift) << shift;
    if (stop_bit && (((byte_val & stop_bit) && shift > 0) ||
                     !(U8(*ptr) & mask) || (U8(*ptr) & stop_bit)))
      break;
  }
  return result;
}

//=============================================================================
// Serialization
//=============================================================================

// Constructor: Serialize position map to compact byte array format
// Layout: [Header][Partition Map][Position/Field Mask Data][Terminator]
FlatPositionMap::FlatPositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  CHECK(!position_map.empty())
      << "Cannot create FlatPositionMap from empty position_map";

  uint32_t num_positions = position_map.size();

  absl::InlinedVector<char, kPartitionSize> position_data;
  std::vector<uint32_t>
      partition_deltas;  // Cumulative deltas at partition boundaries

  Position prev_pos = 0;
  Position cumulative_delta = 0;
  uint64_t prev_field_mask = 0;
  bool is_first_in_partition = true;

  // Encode each position with delta compression
  for (const auto& [pos, field_mask] : position_map) {
    uint32_t delta = pos - prev_pos;
    cumulative_delta += delta;

    // Create partition boundary every kPartitionSize bytes
    if (position_data.size() >=
            (partition_deltas.size() + 1) * kPartitionSize &&
        !is_first_in_partition) {
      partition_deltas.push_back(cumulative_delta - delta);
      is_first_in_partition = true;
      if (num_text_fields > 1)
        prev_field_mask = 0;  // Reset for partition start
    }

    // Encode position delta
    EncodeVarInt(position_data, delta, kIsPositionBit, true);

    // Encode field mask if multi-field and (changed or at boundary)
    if (num_text_fields > 1) {
      uint64_t current_mask = field_mask->AsUint64();
      if (is_first_in_partition || current_mask != prev_field_mask) {
        EncodeVarInt(position_data, current_mask, kFieldMaskPrefix);
        prev_field_mask = current_mask;
      }
    }

    prev_pos = pos;
    is_first_in_partition = false;
  }

  // Encode Terminator Byte
  position_data.push_back(C(kTerminatorByte));

  // Build final byte array: [Header][Partition Map][Position Data]
  uint32_t num_partitions = partition_deltas.size();
  Header header(num_positions, num_partitions);

  uint8_t header_size = 1 + header.pos_bytes + 1 + header.part_bytes + 1;
  size_t partition_map_size =
      num_partitions ? (num_partitions + 1) * kPartitionDeltaBytes : 0;
  size_t total_size = header_size + partition_map_size + position_data.size();

  data_ = new char[total_size];

  size_t offset = header.pack(data_);

  // Write partition map: cumulative deltas at each partition boundary
  if (num_partitions) {
    uint32_t* header = reinterpret_cast<uint32_t*>(data_ + offset);
    for (auto delta : partition_deltas) *header++ = delta;
    *header++ = cumulative_delta;
    offset += sizeof(uint32_t) * (partition_deltas.size() + 1);
  }

  std::memcpy(data_ + offset, position_data.data(), position_data.size());
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

  Header h = Header::unpack(flat_map_, header_size_);
  CHECK(h.num_positions > 0)
      << "Cannot create PositionIterator from FlatPositionMap with 0 positions";
  num_partitions_ = h.num_partitions;

  size_t partition_map_size =
      num_partitions_ ? (num_partitions_ + 1) * kPartitionDeltaBytes : 0;
  data_start_ = flat_map_ + header_size_ + partition_map_size;
  current_start_ptr_ = current_end_ptr_ = data_start_;
  NextPosition();
}

bool PositionIterator::IsValid() const { return current_start_ptr_ != nullptr; }

// Advance to next position, updating current_position_ and current_field_mask_
void PositionIterator::NextPosition() {
  if (!IsValid()) return;

  current_start_ptr_ = current_end_ptr_;

  if (U8(*current_start_ptr_) == kTerminatorByte) {
    current_start_ptr_ = current_end_ptr_ = nullptr;
    return;
  }

  const char* ptr = current_start_ptr_;
  cumulative_position_ +=
      DecodeVarInt<uint32_t>(ptr, kIsPositionBit, kIsPositionBit,
                             kPositionValueMask, kStartPositionBit);

  // Decode field mask if present (keeps previous value if not encoded)
  if ((U8(*ptr) & kTwoBitMask) == kFieldMaskPrefix)
    current_field_mask_ = DecodeVarInt<uint64_t>(
        ptr, kTwoBitMask, kFieldMaskPrefix, kFieldMaskValueMask);

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
bool PositionIterator::SkipForwardPosition(Position target) {
  CHECK(target >= cumulative_position_)
      << "SkipForwardPosition called with target < current position";

  // Try linear search in current partition first (good cache locality)
  size_t current_offset = current_start_ptr_ - data_start_;
  size_t partition_end =
      ((current_offset / kPartitionSize) + 1) * kPartitionSize;

  while (IsValid() && (current_start_ptr_ - data_start_) < partition_end) {
    if (cumulative_position_ >= target) return cumulative_position_ == target;
    NextPosition();
  }

  // If not found in current partition and have partitions, use partition map
  if (IsValid() && num_partitions_) {
    const char* partition_map = flat_map_ + header_size_;
    uint32_t partition_idx =
        FindPartitionForTarget(partition_map, num_partitions_, target);
    uint32_t partition_pos =
        ReadVarUint(partition_map + (partition_idx * kPartitionDeltaBytes),
                    kPartitionDeltaBytes);

    // Jump to partition only if beneficial
    if (partition_pos < target && partition_pos > cumulative_position_) {
      cumulative_position_ = partition_pos;
      const char* partition_ptr =
          data_start_ + (partition_idx * kPartitionSize);

      // Find first position start in partition
      while (U8(*partition_ptr) != kTerminatorByte &&
             (U8(*partition_ptr) & kTwoBitMask) != kPositionStartPrefix)
        partition_ptr++;

      if (U8(*partition_ptr) == kTerminatorByte) {
        current_start_ptr_ = current_end_ptr_ = nullptr;
        return false;
      }

      current_start_ptr_ = current_end_ptr_ = partition_ptr;
      current_field_mask_ = 1;
      NextPosition();
    }
  }

  // Continue linear search to target
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
  CHECK(data_);
  size_t header_size;
  return Header::unpack(data_, header_size).num_positions;
}

size_t FlatPositionMap::CountTermFrequency() const {
  CHECK(data_);
  size_t total_frequency = 0;
  for (PositionIterator iter(*this); iter.IsValid(); iter.NextPosition()) {
    total_frequency += __builtin_popcountll(iter.GetFieldMask());
  }
  return total_frequency;
}

}  // namespace valkey_search::indexes::text
