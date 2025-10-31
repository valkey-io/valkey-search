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

#include "absl/log/check.h"

namespace valkey_search::indexes::text {

namespace {

// Helper to read uint32_t from byte array
uint32_t ReadUint32(const char* ptr) {
  uint32_t value;
  std::memcpy(&value, ptr, sizeof(uint32_t));
  return value;
}

// Helper to write uint32_t to byte array
void WriteUint32(char* ptr, uint32_t value) {
  std::memcpy(ptr, &value, sizeof(uint32_t));
}

// Calculate bytes needed for field mask based on num_text_fields
uint8_t GetFieldMaskBytes(size_t num_text_fields) {
  if (num_text_fields <= 1) return 0;
  if (num_text_fields <= 8) return 1;
  if (num_text_fields <= 16) return 2;
  if (num_text_fields <= 24) return 3;
  if (num_text_fields <= 32) return 4;
  if (num_text_fields <= 40) return 5;
  if (num_text_fields <= 48) return 6;
  if (num_text_fields <= 56) return 7;
  return 8;  // Up to 64 fields
}

// Determine encoding scheme based on position map characteristics
EncodingScheme DetermineEncodingScheme(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  if (position_map.empty()) {
    return EncodingScheme::SIMPLE;
  }

  uint32_t num_positions = position_map.size();
  
  // For large position lists, use binary search encoding
  if (num_positions > 128) {
    return EncodingScheme::BINARY_SEARCH;
  }

  // Check if simple encoding is sufficient
  // Simple encoding: single field, all positions fit in 1 byte
  if (num_text_fields == 1) {
    Position max_pos = position_map.rbegin()->first;
    if (max_pos < 256) {
      return EncodingScheme::SIMPLE;
    }
  }

  // Default to expandable encoding
  return EncodingScheme::EXPANDABLE;
}

// Calculate number of partitions for binary search
uint32_t CalculateNumPartitions(uint32_t num_positions) {
  if (num_positions <= 128) return 0;
  if (num_positions <= 512) return 4;
  if (num_positions <= 2048) return 16;
  if (num_positions <= 8192) return 64;
  return 256;
}

}  // namespace

// Serialize using SIMPLE encoding
FlatPositionMap SerializeSimple(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map) {
  uint32_t num_positions = position_map.size();
  
  // Header (4 bytes) + position data (1 byte per position)
  size_t total_size = 4 + num_positions;
  char* flat_map = static_cast<char*>(std::malloc(total_size));
  CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";

  // Write header
  uint32_t header = (0 << 0) |  // Standard header
                    (static_cast<uint32_t>(EncodingScheme::SIMPLE) << 1) |
                    (num_positions << 3);
  WriteUint32(flat_map, header);

  // Write positions (delta encoded)
  char* data_ptr = flat_map + 4;
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
FlatPositionMap SerializeExpandable(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  uint32_t num_positions = position_map.size();
  uint8_t field_bytes = GetFieldMaskBytes(num_text_fields);

  // Estimate size (conservative upper bound)
  // Each entry: up to 4 bytes for position + field_bytes for field mask
  size_t estimated_size = 4 + (num_positions * (4 + field_bytes));
  char* flat_map = static_cast<char*>(std::malloc(estimated_size));
  CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";

  // Write header (will update later with actual size)
  uint32_t header = (0 << 0) |  // Standard header
                    (static_cast<uint32_t>(EncodingScheme::EXPANDABLE) << 1) |
                    (num_positions << 3);
  WriteUint32(flat_map, header);

  // Write position and field data
  char* data_ptr = flat_map + 4;
  Position prev_pos = 0;

  for (const auto& [pos, field_mask] : position_map) {
    uint32_t delta = pos - prev_pos;

    // Encode position delta with variable bytes
    // First bit: 0 = position byte, 1 = field byte
    // Remaining 7 bits: value
    while (delta >= 128) {
      *data_ptr++ = static_cast<char>(0x00 | (delta & 0x7F));
      delta >>= 7;
    }
    *data_ptr++ = static_cast<char>(0x00 | delta);

    // Encode field mask
    if (field_bytes > 0) {
      uint64_t mask = field_mask->AsUint64();
      for (uint8_t i = 0; i < field_bytes; ++i) {
        *data_ptr++ = static_cast<char>(0x80 | ((mask >> (i * 7)) & 0x7F));
      }
    }

    prev_pos = pos;
  }

  // Reallocate to actual size
  size_t actual_size = data_ptr - flat_map;
  char* resized = static_cast<char*>(std::realloc(flat_map, actual_size));
  if (resized != nullptr) {
    flat_map = resized;
  }

  return flat_map;
}

// Serialize using BINARY_SEARCH encoding
FlatPositionMap SerializeBinarySearch(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  uint32_t num_positions = position_map.size();
  uint32_t num_partitions = CalculateNumPartitions(num_positions);
  uint8_t field_bytes = GetFieldMaskBytes(num_text_fields);

  // Estimate size
  size_t estimated_size = 4 +                              // Header
                          (num_partitions * 8) +           // Partition map
                          (num_positions * (4 + field_bytes));  // Data
  char* flat_map = static_cast<char*>(std::malloc(estimated_size));
  CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";

  // Write header
  uint32_t header = (0 << 0) |  // Standard header
                    (static_cast<uint32_t>(EncodingScheme::BINARY_SEARCH) << 1) |
                    (num_positions << 3);
  WriteUint32(flat_map, header);

  char* partition_ptr = flat_map + 4;
  char* data_ptr = partition_ptr + (num_partitions * 8);
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

    // Encode position delta
    while (delta >= 128) {
      *data_ptr++ = static_cast<char>(0x00 | (delta & 0x7F));
      delta >>= 7;
    }
    *data_ptr++ = static_cast<char>(0x00 | delta);

    // Encode field mask
    if (field_bytes > 0) {
      uint64_t mask = field_mask->AsUint64();
      for (uint8_t i = 0; i < field_bytes; ++i) {
        *data_ptr++ = static_cast<char>(0x80 | ((mask >> (i * 7)) & 0x7F));
      }
    }

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

  // Reallocate to actual size
  size_t actual_size = data_ptr - flat_map;
  char* resized = static_cast<char*>(std::realloc(flat_map, actual_size));
  if (resized != nullptr) {
    flat_map = resized;
  }

  return flat_map;
}

FlatPositionMap SerializePositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {
  if (position_map.empty()) {
    // Empty map: just header with 0 positions
    char* flat_map = static_cast<char*>(std::malloc(4));
    CHECK(flat_map != nullptr) << "Failed to allocate FlatPositionMap";
    WriteUint32(flat_map, 0);
    return flat_map;
  }

  EncodingScheme scheme = DetermineEncodingScheme(position_map, num_text_fields);

  switch (scheme) {
    case EncodingScheme::SIMPLE:
      return SerializeSimple(position_map);
    case EncodingScheme::EXPANDABLE:
      return SerializeExpandable(position_map, num_text_fields);
    case EncodingScheme::BINARY_SEARCH:
      return SerializeBinarySearch(position_map, num_text_fields);
    default:
      CHECK(false) << "Unsupported encoding scheme";
      return nullptr;
  }
}

void FreeFlatPositionMap(FlatPositionMap flat_map) {
  if (flat_map != nullptr) {
    std::free(flat_map);
  }
}

// Iterator implementation
FlatPositionMapIterator::FlatPositionMapIterator(FlatPositionMap flat_map)
    : flat_map_(flat_map), current_ptr_(nullptr), cumulative_position_(0) {
  if (flat_map_ != nullptr) {
    uint32_t header = ReadUint32(flat_map_);
    uint32_t num_positions = (header >> 3);
    EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

    if (num_positions > 0) {
      // Skip header
      current_ptr_ = flat_map_ + 4;

      // Skip partition map if binary search encoding
      if (scheme == EncodingScheme::BINARY_SEARCH) {
        uint32_t num_partitions = CalculateNumPartitions(num_positions);
        current_ptr_ += num_partitions * 8;
      }
    }
  }
}

bool FlatPositionMapIterator::IsValid(size_t num_text_fields) const {
  return current_ptr_ != nullptr;
}

void FlatPositionMapIterator::Next(size_t num_text_fields) {
  if (current_ptr_ == nullptr) return;

  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);
  uint8_t field_bytes = GetFieldMaskBytes(num_text_fields);

  // Decode and add current delta to cumulative position before moving
  if (scheme == EncodingScheme::SIMPLE) {
    cumulative_position_ += static_cast<uint8_t>(*current_ptr_);
    current_ptr_++;
  } else {
    // Decode position delta
    uint32_t delta = 0;
    uint32_t shift = 0;
    const char* temp_ptr = current_ptr_;
    while ((*temp_ptr & 0x80) == 0) {
      delta |= ((*temp_ptr & 0x7F) << shift);
      shift += 7;
      temp_ptr++;
    }
    cumulative_position_ += delta;

    // Skip position bytes (first bit = 0)
    while ((*current_ptr_ & 0x80) == 0) {
      current_ptr_++;
    }
    // Skip field bytes (first bit = 1)
    for (uint8_t i = 0; i < field_bytes; ++i) {
      current_ptr_++;
    }
  }
}

bool FlatPositionMapIterator::SkipForward(Position target,
                                          size_t num_text_fields) {
  // For now, use linear scan
  // TODO: Implement binary search for BINARY_SEARCH encoding
  Position current_pos = 0;
  Position prev_pos = 0;

  while (IsValid(num_text_fields)) {
    current_pos = GetPosition(num_text_fields);
    if (current_pos >= target) {
      return current_pos == target;
    }
    Next(num_text_fields);
  }

  return false;
}

Position FlatPositionMapIterator::GetPosition(size_t num_text_fields) const {
  if (current_ptr_ == nullptr) return 0;

  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);

  // Decode current delta and add to cumulative position
  if (scheme == EncodingScheme::SIMPLE) {
    return cumulative_position_ + static_cast<uint8_t>(*current_ptr_);
  } else {
    uint32_t delta = 0;
    uint32_t shift = 0;
    const char* temp_ptr = current_ptr_;
    while ((*temp_ptr & 0x80) == 0) {
      delta |= ((*temp_ptr & 0x7F) << shift);
      shift += 7;
      temp_ptr++;
    }
    return cumulative_position_ + delta;
  }
}

uint64_t FlatPositionMapIterator::GetFieldMask(size_t num_text_fields) const {
  if (current_ptr_ == nullptr) return 0;

  uint32_t header = ReadUint32(flat_map_);
  EncodingScheme scheme = static_cast<EncodingScheme>((header >> 1) & 0x3);
  uint8_t field_bytes = GetFieldMaskBytes(num_text_fields);

  if (scheme == EncodingScheme::SIMPLE || field_bytes == 0) {
    return 1;  // Single field
  }

  // Skip position bytes to get to field bytes
  const char* ptr = current_ptr_;
  while ((*ptr & 0x80) == 0) {
    ptr++;
  }

  // Decode field mask
  uint64_t mask = 0;
  for (uint8_t i = 0; i < field_bytes; ++i) {
    mask |= (static_cast<uint64_t>(*ptr & 0x7F) << (i * 7));
    ptr++;
  }

  return mask;
}

}  // namespace valkey_search::indexes::text
