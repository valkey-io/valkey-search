/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/flat_position_map.h"

#include <cstdlib>
#include <cstring>
#include <sstream>
#include <vector>

#include "absl/log/check.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_index.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/memory_allocation_overrides.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes::text {

// FlatPositionMap member function implementations

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
// Helper functions
// Helper to read uint32_t from byte array
static uint32_t ReadUint32(const char* ptr) {
  uint32_t value;
  std::memcpy(&value, ptr, sizeof(uint32_t));
  return value;
}

// Helper to write uint32_t to byte array
static void WriteUint32(char* ptr, uint32_t value) {
  std::memcpy(ptr, &value, sizeof(uint32_t));
}

// Helper to encode a position delta using variable-length encoding
// Returns pointer to next write position
static char* EncodePositionDelta(char* data_ptr, uint32_t delta) {
  while (delta >= 128) {
    *data_ptr++ = static_cast<char>(0x00 | (delta & kEncodingValueMask));
    delta >>= 7;
  }
  *data_ptr++ = static_cast<char>(0x00 | delta);
  return data_ptr;
}

// Helper to encode a field mask
// Returns pointer to next write position
static char* EncodeFieldMask(char* data_ptr,
                             const std::unique_ptr<FieldMask>& field_mask,
                             uint8_t field_bytes) {
  if (field_bytes > 0) {
    // Multiple fields: encode the mask
    uint64_t mask = field_mask->AsUint64();
    for (uint8_t i = 0; i < field_bytes; ++i) {
      *data_ptr++ = static_cast<char>(
          kEncodingBitFieldMask |
          ((mask >> (i * kFieldMaskBitsPerByte)) & kEncodingValueMask));
    }
  } else {
    // Single field: write terminator byte (bit 7=1, value=1 for field 0)
    *data_ptr++ = static_cast<char>(kEncodingBitFieldMask | 0x01);
  }
  return data_ptr;
}

// Helper to decode a position delta from variable-length encoding
// Returns the decoded delta value
static uint32_t DecodePositionDelta(const char* ptr) {
  uint32_t delta = 0;
  uint32_t shift = 0;
  while ((*ptr & 0x80) == 0) {
    delta |= ((*ptr & 0x7F) << shift);
    shift += 7;
    ptr++;
  }
  return delta;
}

// Helper to create and write a header for serialization
static void WriteSerializationHeader(char* flat_map, EncodingScheme scheme,
                                     uint32_t num_positions) {
  uint32_t header = (0 << 0) |  // Standard header
                    (static_cast<uint32_t>(scheme) << 1) | (num_positions << 3);
  WriteUint32(flat_map, header);
}

// Helper to reallocate flat_map to actual size
static char* ReallocateToActualSize(char* flat_map, char* data_ptr) {
  size_t actual_size = data_ptr - flat_map;
  char* resized = static_cast<char*>(realloc(flat_map, actual_size));
  return (resized != nullptr) ? resized : flat_map;
}

// Helper to skip position bytes in encoded data (returns pointer after position
// bytes)
static const char* SkipPositionBytes(const char* ptr) {
  while ((*ptr & 0x80) == 0) {
    ptr++;
  }
  return ptr;
}

// Helper to decode field mask from encoded data
static uint64_t DecodeFieldMask(const char* ptr, uint8_t field_bytes) {
  if (field_bytes == 0) {
    // Single field: read terminator byte (should be 0x81 = bit7=1, value=1)
    return (*ptr & 0x7F);
  }

  // Multiple fields: decode full mask
  uint64_t mask = 0;
  for (uint8_t i = 0; i < field_bytes; ++i) {
    mask |= (static_cast<uint64_t>(*ptr & 0x7F) << (i * 7));
    ptr++;
  }
  return mask;
}

// Calculate number of partitions for binary search. Have limited the number of
// partitions to avoid bloating the position map
static uint32_t CalculateNumPartitions(uint32_t num_positions) {
  if (num_positions <= 128) return 0;
  if (num_positions <= 512) return 4;
  if (num_positions <= 2048) return 16;
  if (num_positions <= 8192) return 64;
  return 256;
}

// Calculate field mask bytes for num_text_fields (7 bits/byte, bit 7 = flag)
// Returns 0 for single field; EXPANDABLE/BINARY_SEARCH add 1 terminator byte
static uint8_t GetFieldMaskBytes(size_t num_text_fields) {
  if (num_text_fields <= 1) return 0;
  // Each byte can store 7 bits of field mask (bit 7 is the flag bit)
  return (num_text_fields + kFieldMaskBitsPerByte - 1) / kFieldMaskBitsPerByte;
}

// Infer field_bytes from the flat_map itself by examining the first position
// Returns 0 for SIMPLE encoding, else counts field mask bytes in first entry
static uint8_t GetFieldMaskBytes(const char* flat_map) {
  if (flat_map == nullptr) return 0;

  uint32_t header = ReadUint32(flat_map);
  uint32_t num_positions = (header >> 3);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  // SIMPLE encoding has no field mask bytes, or empty map
  if (scheme == EncodingScheme::SIMPLE || num_positions == 0) {
    return 0;
  }

  // Skip header
  const char* ptr = flat_map + kFlatPositionMapHeaderSize;

  // Skip partition map if binary search encoding
  if (scheme == EncodingScheme::BINARY_SEARCH) {
    uint32_t num_partitions = CalculateNumPartitions(num_positions);
    ptr += num_partitions * kPartitionMapEntrySize;
  }

  // Skip position bytes (first bit = 0) to reach field bytes
  while ((*ptr & kEncodingBitFieldMask) == 0) {
    ptr++;
  }

  // Count field mask bytes (first bit = 1)
  uint8_t count = 0;
  while ((*ptr & kEncodingBitFieldMask) != 0) {
    count++;
    ptr++;
  }

  return count;
}

// Determine encoding scheme based on position map characteristics
static EncodingScheme DetermineEncodingScheme(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  if (position_map.empty()) {
    return EncodingScheme::SIMPLE;
  }

  uint32_t num_positions = position_map.size();

  // For large position lists, use binary search encoding
  if (num_positions > kBinarySearchThreshold) {
    return EncodingScheme::BINARY_SEARCH;
  }

  // Check if simple encoding is sufficient
  // Simple encoding: single field, all positions fit in 1 byte
  if (num_text_fields == 1) {
    Position max_pos = position_map.rbegin()->first;
    if (max_pos < kSimpleEncodingMaxPosition) {
      return EncodingScheme::SIMPLE;
    }
  }

  // Default to expandable encoding
  return EncodingScheme::EXPANDABLE;
}

// Serialize using SIMPLE encoding
static char* SerializeSimple(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map) {
  uint32_t num_positions = position_map.size();

  // Header (4 bytes) + position data (1 byte per position)
  size_t total_size = kFlatPositionMapHeaderSize + num_positions;
  char* flat_map = static_cast<char*>(malloc(total_size));
  CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";

  // Write header
  uint32_t header = (0 << 0) |  // Standard header
                    (static_cast<uint32_t>(EncodingScheme::SIMPLE) << 1) |
                    (num_positions << 3);
  WriteUint32(flat_map, header);

  // Write positions (delta encoded)
  char* data_ptr = flat_map + kFlatPositionMapHeaderSize;
  Position prev_pos = 0;
  for (const auto& [pos, field_mask] : position_map) {
    uint32_t delta = pos - prev_pos;
    CHECK(delta < 256) << "Delta too large for simple encoding";
    *data_ptr++ = static_cast<char>(delta);
    prev_pos = pos;
  }

  return flat_map;
}

// Serialize using EXPANDABLE encoding
static char* SerializeExpandable(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  uint32_t num_positions = position_map.size();
  uint8_t field_bytes = GetFieldMaskBytes(num_text_fields);

  // Estimate size (conservative upper bound)
  // Each entry: up to 4 bytes for position + at least 1 byte for field
  // mask/terminator
  // Add 1 extra byte for terminator when num_positions == 1
  size_t estimated_size =
      kFlatPositionMapHeaderSize +
      (num_positions * (4 + (field_bytes > 0 ? field_bytes : 1))) +
      (num_positions == 1 ? 1 : 0);
  char* flat_map = static_cast<char*>(malloc(estimated_size));
  CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";

  WriteSerializationHeader(flat_map, EncodingScheme::EXPANDABLE, num_positions);

  // Write position and field data
  char* data_ptr = flat_map + kFlatPositionMapHeaderSize;
  Position prev_pos = 0;

  for (const auto& [pos, field_mask] : position_map) {
    uint32_t delta = pos - prev_pos;
    data_ptr = EncodePositionDelta(data_ptr, delta);
    data_ptr = EncodeFieldMask(data_ptr, field_mask, field_bytes);
    prev_pos = pos;
  }

  // Add terminator byte for single position case
  if (num_positions == 1) {
    *data_ptr++ = 0x00;  // Terminator byte (bit 7 = 0)
  }

  return ReallocateToActualSize(flat_map, data_ptr);
}

// Serialize using BINARY_SEARCH encoding
static char* SerializeBinarySearch(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  uint32_t num_positions = position_map.size();
  uint32_t num_partitions = CalculateNumPartitions(num_positions);
  uint8_t field_bytes = GetFieldMaskBytes(num_text_fields);

  // Estimate size
  // Data: up to 4 bytes per position + at least 1 byte for field
  // mask/terminator
  size_t estimated_size =
      kFlatPositionMapHeaderSize +                 // Header
      (num_partitions * kPartitionMapEntrySize) +  // Partition map
      (num_positions * (4 + (field_bytes > 0 ? field_bytes : 1)));  // Data
  char* flat_map = static_cast<char*>(malloc(estimated_size));
  CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";

  WriteSerializationHeader(flat_map, EncodingScheme::BINARY_SEARCH,
                           num_positions);

  char* partition_ptr = flat_map + kFlatPositionMapHeaderSize;
  char* data_ptr = partition_ptr + (num_partitions * kPartitionMapEntrySize);
  char* data_start = data_ptr;

  // Build partition map and write data
  std::vector<uint32_t> partition_offsets;
  std::vector<uint32_t> partition_deltas;

  Position prev_pos = 0;
  Position cumulative_delta = 0;
  uint32_t positions_per_partition = num_positions / num_partitions;
  uint32_t current_partition_count = 0;

  for (const auto& [pos, field_mask] : position_map) {
    // Record partition entry
    if (current_partition_count % positions_per_partition == 0 &&
        partition_offsets.size() < num_partitions) {
      partition_offsets.push_back(data_ptr - data_start);
      partition_deltas.push_back(cumulative_delta);
    }

    uint32_t delta = pos - prev_pos;
    cumulative_delta += delta;

    data_ptr = EncodePositionDelta(data_ptr, delta);
    data_ptr = EncodeFieldMask(data_ptr, field_mask, field_bytes);

    prev_pos = pos;
    current_partition_count++;
  }

  // Write partition map
  for (size_t i = 0; i < partition_offsets.size(); ++i) {
    WriteUint32(partition_ptr, partition_offsets[i]);
    partition_ptr += 4;
    WriteUint32(partition_ptr, partition_deltas[i]);
    partition_ptr += 4;
  }

  return ReallocateToActualSize(flat_map, data_ptr);
}

FlatPositionMap FlatPositionMap::SerializePositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  if (position_map.empty()) {
    // Empty map: just header with 0 positions
    char* flat_map = static_cast<char*>(malloc(kFlatPositionMapHeaderSize));
    CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";
    WriteUint32(flat_map, 0);
    return FlatPositionMap(flat_map);
  }

  EncodingScheme scheme =
      DetermineEncodingScheme(position_map, num_text_fields);

  char* data = nullptr;
  switch (scheme) {
    case EncodingScheme::SIMPLE:
      data = SerializeSimple(position_map);
      break;
    case EncodingScheme::EXPANDABLE:
      data = SerializeExpandable(position_map, num_text_fields);
      break;
    case EncodingScheme::BINARY_SEARCH:
      data = SerializeBinarySearch(position_map, num_text_fields);
      break;
    default:
      CHECK(false) << "Unsupported encoding scheme";
  }

  return FlatPositionMap(data);
}

// Iterator implementation
FlatPositionMapIterator::FlatPositionMapIterator(
    const FlatPositionMap& flat_map)
    : flat_map_(flat_map.data()),
      current_ptr_(nullptr),
      cumulative_position_(0),
      positions_read_(0),
      total_positions_(0),
      field_bytes_(0) {
  if (flat_map_ != nullptr) {
    uint32_t header = ReadUint32(flat_map_);
    total_positions_ = (header >> 3);
    EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

    // Infer field_bytes from the flat_map data itself
    field_bytes_ = GetFieldMaskBytes(flat_map_);

    if (total_positions_ > 0) {
      // Skip header
      current_ptr_ = flat_map_ + kFlatPositionMapHeaderSize;

      // Skip partition map if binary search encoding
      if (scheme == EncodingScheme::BINARY_SEARCH) {
        uint32_t num_partitions = CalculateNumPartitions(total_positions_);
        current_ptr_ += num_partitions * kPartitionMapEntrySize;
      }
    }
  }
}

bool FlatPositionMapIterator::IsValid() const {
  return current_ptr_ != nullptr && positions_read_ < total_positions_;
}

void FlatPositionMapIterator::NextPosition() {
  if (!IsValid()) return;

  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  // Decode and add current delta to cumulative position before moving
  if (scheme == EncodingScheme::SIMPLE) {
    cumulative_position_ += static_cast<uint8_t>(*current_ptr_);
    current_ptr_++;
  } else {
    cumulative_position_ += DecodePositionDelta(current_ptr_);

    // Skip position bytes (first bit = 0)
    while ((*current_ptr_ & 0x80) == 0) {
      current_ptr_++;
    }
    // Skip field bytes (first bit = 1)
    // For single-field: 1 terminator byte, otherwise field_bytes_
    uint8_t bytes_to_skip = (field_bytes_ > 0) ? field_bytes_ : 1;
    for (uint8_t i = 0; i < bytes_to_skip; ++i) {
      current_ptr_++;
    }
  }

  // Increment positions read counter
  positions_read_++;

  // If we've read all positions, invalidate the iterator
  if (positions_read_ >= total_positions_) {
    current_ptr_ = nullptr;
  }
}

bool FlatPositionMapIterator::SkipForwardPosition(Position target) {
  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  // For BINARY_SEARCH encoding, use partition map to skip ahead
  if (scheme == EncodingScheme::BINARY_SEARCH && total_positions_ > 0) {
    uint32_t num_partitions = CalculateNumPartitions(total_positions_);
    const char* partition_map_ptr = flat_map_ + kFlatPositionMapHeaderSize;

    // Find partition where target position falls
    size_t target_partition_idx = 0;
    for (uint32_t i = 0; i < num_partitions; ++i) {
      uint32_t partition_delta = ReadUint32(partition_map_ptr + i * 8 + 4);

      if (partition_delta >= target) {
        target_partition_idx = i;
        break;
      }
      target_partition_idx = i + 1;
    }

    // Skip to the target partition
    if (target_partition_idx > 0 && target_partition_idx < num_partitions) {
      uint32_t partition_offset =
          ReadUint32(partition_map_ptr + target_partition_idx * 8);
      uint32_t partition_delta =
          ReadUint32(partition_map_ptr + target_partition_idx * 8 + 4);

      const char* data_start = flat_map_ + kFlatPositionMapHeaderSize +
                               (num_partitions * kPartitionMapEntrySize);
      current_ptr_ = data_start + partition_offset;
      cumulative_position_ = partition_delta;

      uint32_t positions_per_partition = total_positions_ / num_partitions;
      positions_read_ = target_partition_idx * positions_per_partition;
    }
  }

  // Linear search from current position to reach exact target
  while (IsValid()) {
    Position current_pos = GetPosition();
    if (current_pos >= target) {
      return current_pos == target;
    }
    NextPosition();
  }

  return false;
}

Position FlatPositionMapIterator::GetPosition() const {
  if (current_ptr_ == nullptr) return 0;

  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  // Decode current delta and add to cumulative position
  if (scheme == EncodingScheme::SIMPLE) {
    return cumulative_position_ + static_cast<uint8_t>(*current_ptr_);
  } else {
    return cumulative_position_ + DecodePositionDelta(current_ptr_);
  }
}

uint64_t FlatPositionMapIterator::GetFieldMask() const {
  if (current_ptr_ == nullptr) return 0;

  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  if (scheme == EncodingScheme::SIMPLE) {
    return 1;  // Single field
  }

  const char* ptr = SkipPositionBytes(current_ptr_);
  return DecodeFieldMask(ptr, field_bytes_);
}

// Get position count from FlatPositionMap (reads from header)
uint32_t FlatPositionMap::CountPositions() const {
  if (data_ == nullptr) {
    return 0;
  }

  uint32_t header = ReadUint32(data_);
  return (header >> 3);
}

// Get total term frequency from FlatPositionMap (iterates and counts set
// fields)
size_t FlatPositionMap::CountTermFrequency() const {
  if (data_ == nullptr) {
    return 0;
  }

  size_t total_frequency = 0;
  FlatPositionMapIterator iter(*this);

  while (iter.IsValid()) {
    uint64_t field_mask = iter.GetFieldMask();
    total_frequency += __builtin_popcountll(field_mask);
    iter.NextPosition();
  }

  return total_frequency;
}

// Helper function to print FlatPositionMap at bit level
void PrintFlatPositionMapBits(const FlatPositionMap& flat_map) {
  if (flat_map.data() == nullptr) {
    return;
  }

  // Read the header to determine encoding and size
  uint32_t header;
  std::memcpy(&header, flat_map.data(), sizeof(uint32_t));
  uint32_t num_positions = (header >> 3);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  // Calculate actual size based on encoding scheme
  size_t bytes_to_print;
  if (scheme == EncodingScheme::SIMPLE) {
    // SIMPLE: header + 1 byte per position
    bytes_to_print = kFlatPositionMapHeaderSize + num_positions;
  } else {
    // For other encodings, estimate (this is conservative)
    bytes_to_print =
        kFlatPositionMapHeaderSize + std::min(num_positions * 4, 256u);
  }

  std::ostringstream oss;
  oss << "FlatPositionMap bits (" << bytes_to_print << " bytes, "
      << "scheme=" << static_cast<int>(scheme)
      << ", positions=" << num_positions << "):\n";

  const unsigned char* raw_data =
      reinterpret_cast<const unsigned char*>(flat_map.data());
  for (size_t i = 0; i < bytes_to_print; ++i) {
    unsigned char byte = raw_data[i];

    // Print each bit from MSB to LSB
    for (int bit = 7; bit >= 0; --bit) {
      oss << ((byte >> bit) & 1);
    }

    // Add space after every 8 bits (every byte)
    oss << " ";

    // Add newline every 8 bytes for readability
    if ((i + 1) % 8 == 0) {
      oss << "\n";
    }
  }

  VMSDK_LOG(WARNING, nullptr) << oss.str();
}

}  // namespace valkey_search::indexes::text
