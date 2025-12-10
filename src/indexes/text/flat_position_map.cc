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

#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_index.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/memory_allocation_overrides.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes::text {

//=============================================================================
// FlatPositionMap Member Functions
//=============================================================================

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

static uint8_t BytesNeeded(uint32_t value) {
  if (value == 0) return 1;
  if (value <= 0xFF) return 1;
  if (value <= 0xFFFF) return 2;
  if (value <= 0xFFFFFF) return 3;
  return 4;
}

static uint32_t ReadVarUint(const char* ptr, uint8_t num_bytes) {
  uint32_t value = 0;
  for (uint8_t i = 0; i < num_bytes; ++i) {
    value |= (static_cast<uint32_t>(static_cast<uint8_t>(ptr[i])) << (i * 8));
  }
  return value;
}

static uint8_t ParseHeader(const char* flat_map, uint32_t& num_positions,
                           uint32_t& num_partitions) {
  if (flat_map == nullptr) {
    num_positions = num_partitions = 0;
    return 0;
  }

  uint8_t first_byte = static_cast<uint8_t>(flat_map[0]);
  uint8_t pos_bytes = ((first_byte >> 3) & 0x3) + 1;
  uint8_t part_bytes = ((first_byte >> 5) & 0x3) + 1;
  
  num_positions = ReadVarUint(flat_map + 1, pos_bytes);
  num_partitions = ReadVarUint(flat_map + 1 + pos_bytes, part_bytes);
  
  return 1 + pos_bytes + part_bytes;
}

static uint8_t DetectFieldMaskBytes(const char* data_ptr) {
  const char* ptr = data_ptr;
  
  while ((static_cast<uint8_t>(*ptr) & kBitPosition) != 0) {
    ptr++;
    if ((static_cast<uint8_t>(*(ptr - 1)) & kBitStartPosition) != 0) {
      if ((static_cast<uint8_t>(*ptr) & kBitPosition) == 0 ||
          (static_cast<uint8_t>(*ptr) & kBitStartPosition) != 0) {
        break;
      }
    }
  }
  
  uint8_t byte_count = 0;
  
  while ((static_cast<uint8_t>(*ptr) & 0x03) == 0x02) {
    byte_count++;
    ptr++;
  }
  
  return byte_count;
}

static void EncodePositionDelta(absl::InlinedVector<char, 128>& buffer,
                                 uint32_t delta, bool is_start) {
  bool first = true;
  do {
    uint8_t byte_val = (delta & 0x3F) << kValueShift;
    byte_val |= kBitPosition;
    if (first && is_start) {
      byte_val |= kBitStartPosition;
      first = false;
    }
    buffer.push_back(static_cast<char>(byte_val));
    delta >>= 6;
  } while (delta > 0);
}

static void EncodeFieldMask(absl::InlinedVector<char, 128>& buffer,
                             uint64_t field_mask, size_t num_text_fields) {
  size_t num_bytes = (num_text_fields + 5) / 6;
  
  for (size_t i = 0; i < num_bytes; ++i) {
    uint8_t byte_val = (field_mask & 0x3F) << kValueShift;
    byte_val |= 0x02;
    buffer.push_back(static_cast<char>(byte_val));
    field_mask >>= 6;
  }
}

static void EncodeTerminator(absl::InlinedVector<char, 128>& buffer) {
  buffer.push_back(static_cast<char>(0x00));
}

static uint32_t DecodePositionDelta(const char*& ptr) {
  uint32_t delta = 0;
  uint32_t shift = 0;

  while ((static_cast<uint8_t>(*ptr) & kBitPosition) != 0) {
    uint8_t byte_val = static_cast<uint8_t>(*ptr);
    uint8_t value = (byte_val & kValueMask) >> kValueShift;
    delta |= (static_cast<uint32_t>(value) << shift);
    shift += 6;
    ptr++;

    // If we're at the start of a position and have read more than one byte, break
    if ((byte_val & kBitStartPosition) != 0 && shift > 6) {
      break;
    }
    
    // Break if next byte is not a position byte or starts a new position
    if ((static_cast<uint8_t>(*ptr) & kBitPosition) == 0 ||
        (static_cast<uint8_t>(*ptr) & kBitStartPosition) != 0) {
      break;
    }
  }
  return delta;
}

static uint64_t DecodeFieldMask(const char*& ptr) {
  uint64_t mask = 0;
  uint32_t shift = 0;

  while ((static_cast<uint8_t>(*ptr) & 0x03) == 0x02) {
    uint8_t byte_val = static_cast<uint8_t>(*ptr);
    uint8_t value = (byte_val & kFieldMaskValueMask) >> kValueShift;
    mask |= (static_cast<uint64_t>(value) << shift);
    shift += 6;
    ptr++;
  }
  return mask;
}


FlatPositionMap FlatPositionMap::SerializePositionMap(
    const std::map<Position, std::unique_ptr<FieldMask>>& position_map,
    size_t num_text_fields) {

  if (position_map.empty()) {
    char* flat_map = static_cast<char*>(malloc(3));
    CHECK(flat_map != nullptr);
    flat_map[0] = 0x00;
    flat_map[1] = 0x00;
    flat_map[2] = 0x00;
    return FlatPositionMap(flat_map);
  }

  uint32_t num_positions = position_map.size();
  bool single_field = (num_text_fields == 1);

  absl::InlinedVector<char, 128> position_data;
  std::vector<uint32_t> partition_deltas;
  
  Position prev_pos = 0;
  Position cumulative_delta = 0;
  uint64_t prev_field_mask = 0;
  bool first_position = true;
  bool is_first_in_partition = true;
  size_t partition_start_offset = 0;

  for (const auto& [pos, field_mask] : position_map) {
    uint32_t delta = pos - prev_pos;
    cumulative_delta += delta;

    if (position_data.size() - partition_start_offset >= kPartitionSize && !first_position) {
      partition_deltas.push_back(cumulative_delta - delta);
      partition_start_offset += kPartitionSize;
      is_first_in_partition = true;
      if (!single_field) prev_field_mask = 0;
    }

    EncodePositionDelta(position_data, delta, true);

    if (!single_field) {
      uint64_t current_mask = field_mask->AsUint64();
      if (first_position || current_mask != prev_field_mask || is_first_in_partition) {
        EncodeFieldMask(position_data, current_mask, num_text_fields);
        prev_field_mask = current_mask;
      }
    }

    prev_pos = pos;
    first_position = false;
    is_first_in_partition = false;
  }

  EncodeTerminator(position_data);

  uint32_t num_partitions = partition_deltas.size();
  uint8_t pos_bytes = BytesNeeded(num_positions);
  uint8_t part_bytes = BytesNeeded(num_partitions);
  
  uint8_t header_size = 1 + pos_bytes + part_bytes;
  size_t partition_map_size = (num_partitions > 0) ? (num_partitions + 1) * 4 : 0;
  size_t total_size = header_size + partition_map_size + position_data.size();
  
  char* flat_map = static_cast<char*>(malloc(total_size));
  CHECK(flat_map != nullptr);
  
  uint8_t first_byte = ((pos_bytes - 1) << 3) | ((part_bytes - 1) << 5);
  
  size_t offset = 0;
  flat_map[offset++] = static_cast<char>(first_byte);
  
  for (uint8_t i = 0; i < pos_bytes; ++i) {
    flat_map[offset++] = static_cast<char>((num_positions >> (i * 8)) & 0xFF);
  }
  
  for (uint8_t i = 0; i < part_bytes; ++i) {
    flat_map[offset++] = static_cast<char>((num_partitions >> (i * 8)) & 0xFF);
  }
  
  if (num_partitions > 0) {
    for (uint32_t delta : partition_deltas) {
      for (uint8_t i = 0; i < 4; ++i) {
        flat_map[offset++] = static_cast<char>((delta >> (i * 8)) & 0xFF);
      }
    }
    for (uint8_t i = 0; i < 4; ++i) {
      flat_map[offset++] = static_cast<char>((cumulative_delta >> (i * 8)) & 0xFF);
    }
  }
  
  std::memcpy(flat_map + offset, position_data.data(), position_data.size());
  
  return FlatPositionMap(flat_map);
}

//=============================================================================
// Iterator Implementation
//=============================================================================

FlatPositionMapIterator::FlatPositionMapIterator(const FlatPositionMap& flat_map)
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

  if (flat_map_ == nullptr) return;

  header_size_ = ParseHeader(flat_map_, total_positions_, num_partitions_);

  if (total_positions_ == 0) return;

  size_t partition_map_size = (num_partitions_ > 0) ? (num_partitions_ + 1) * 4 : 0;
  data_start_ = flat_map_ + header_size_ + partition_map_size;
  current_start_ptr_ = data_start_;
  current_end_ptr_ = data_start_;

  field_mask_bytes_ = DetectFieldMaskBytes(data_start_);
  
  NextPosition();
}

bool FlatPositionMapIterator::IsValid() const {
  return current_start_ptr_ != nullptr;
}

void FlatPositionMapIterator::NextPosition() {
  if (!IsValid()) return;

  current_start_ptr_ = current_end_ptr_;
  
  if (static_cast<uint8_t>(*current_start_ptr_) == 0x00) {
    current_start_ptr_ = nullptr;
    current_end_ptr_ = nullptr;
    return;
  }

  const char* ptr = current_start_ptr_;
  uint32_t delta = DecodePositionDelta(ptr);
  cumulative_position_ += delta;
  current_position_ = cumulative_position_;

  if (field_mask_bytes_ > 0 && (static_cast<uint8_t>(*ptr) & 0x03) == 0x02) {
    current_field_mask_ = DecodeFieldMask(ptr);
  }

  current_end_ptr_ = ptr;
}

// Binary search to find the partition containing the target position
static uint32_t FindPartitionForTarget(const char* partition_map, 
                                        uint32_t num_partitions, 
                                        Position target) {
  uint32_t left = 0;
  uint32_t right = num_partitions;
  
  while (left < right) {
    uint32_t mid = left + (right - left) / 2;
    uint32_t partition_delta = ReadVarUint(partition_map + (mid * 4), 4);
    
    if (partition_delta < target) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  
  // Return the partition index before the target (or 0 if target is in first partition)
  return (left > 0) ? left - 1 : 0;
}

bool FlatPositionMapIterator::SkipForwardPosition(Position target) {
  current_start_ptr_ = data_start_;
  current_end_ptr_ = data_start_;
  cumulative_position_ = 0;

  if (num_partitions_ > 0) {
    const char* partition_map = flat_map_ + header_size_;
    uint32_t partition_idx = FindPartitionForTarget(partition_map, num_partitions_, target);
    
    if (partition_idx > 0) {
      uint32_t partition_delta = ReadVarUint(partition_map + (partition_idx * 4), 4);
      const char* partition_ptr = data_start_ + (partition_idx * kPartitionSize);
      cumulative_position_ = partition_delta;
      
      while (static_cast<uint8_t>(*partition_ptr) != 0x00 &&
             (static_cast<uint8_t>(*partition_ptr) & 0x03) != 0x03) {
        partition_ptr++;
      }
      
      if (static_cast<uint8_t>(*partition_ptr) == 0x00) {
        current_start_ptr_ = nullptr;
        current_end_ptr_ = nullptr;
        return false;
      }
      
      current_start_ptr_ = partition_ptr;
      current_end_ptr_ = partition_ptr;
    }
  }

  NextPosition();

  while (IsValid()) {
    if (current_position_ >= target) {
      return current_position_ == target;
    }
    NextPosition();
  }
  return false;
}

Position FlatPositionMapIterator::GetPosition() const {
  return current_position_;
}

uint64_t FlatPositionMapIterator::GetFieldMask() const {
  if (field_mask_bytes_ == 0) return 1;
  return current_field_mask_;
}

//=============================================================================
// Public Query Methods
//=============================================================================

uint32_t FlatPositionMap::CountPositions() const {
  if (data_ == nullptr) return 0;
  uint32_t num_positions, num_partitions;
  ParseHeader(data_, num_positions, num_partitions);
  return num_positions;
}

size_t FlatPositionMap::CountTermFrequency() const {
  if (data_ == nullptr) return 0;

  size_t total_frequency = 0;
  FlatPositionMapIterator iter(*this);
  while (iter.IsValid()) {
    total_frequency += __builtin_popcountll(iter.GetFieldMask());
    iter.NextPosition();
  }
  return total_frequency;
}

}  // namespace valkey_search::indexes::text
