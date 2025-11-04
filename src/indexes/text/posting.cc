/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/posting.h"

#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "absl/log/check.h"
#include "src/index_schema.h"

namespace valkey_search::indexes::text {

// Internal FieldMask classes - not part of external interface

// Template implementation for field mask with optimized storage
template <typename MaskType, size_t MAX_FIELDS>
class FieldMaskImpl : public FieldMask {
 public:
  explicit FieldMaskImpl(size_t num_fields = MAX_FIELDS);
  void SetField(size_t field_index) override;
  void ClearField(size_t field_index) override;
  bool HasField(size_t field_index) const override;
  void SetAllFields() override;
  void ClearAllFields() override;
  size_t CountSetFields() const override;
  uint64_t AsUint64() const override;
  size_t MaxFields() const override { return num_fields_; }

 private:
  MaskType mask_;
  size_t num_fields_;
};

// Empty placeholder type that takes no space
struct EmptyFieldMask {};

// Optimized implementations for different field counts
using SingleFieldMask = FieldMaskImpl<EmptyFieldMask, 1>;
using ByteFieldMask = FieldMaskImpl<uint8_t, 8>;
using Uint64FieldMask = FieldMaskImpl<uint64_t, 64>;

// Field Mask Implementation

// Factory method to create optimal field mask based on field count
std::unique_ptr<FieldMask> FieldMask::Create(size_t num_fields) {
  CHECK(num_fields > 0) << "num_fields must be greater than 0";
  CHECK(num_fields <= 64) << "Too many text fields (max 64 supported)";

  // Select most memory-efficient implementation
  if (num_fields <= 1) {
    return std::make_unique<SingleFieldMask>();  // EmptyFieldMask (no storage)
  } else if (num_fields <= 8) {
    return std::make_unique<ByteFieldMask>(num_fields);  // uint8_t (1 byte)
  } else {
    return std::make_unique<Uint64FieldMask>(num_fields);  // uint64_t (8 bytes)
  }
}

// Initialize field mask with specified field count
template <typename MaskType, size_t MAX_FIELDS>
FieldMaskImpl<MaskType, MAX_FIELDS>::FieldMaskImpl(size_t num_fields)
    : num_fields_(num_fields) {
  CHECK(num_fields <= MAX_FIELDS)
      << "Field count exceeds maximum for this mask type";

  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ = MaskType{};
  }
}

// Set a specific field bit to true
template <typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetField(size_t field_index) {
  CHECK(field_index < num_fields_) << "Field index out of range";

  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ |= (MaskType(1) << field_index);
  }
}

// Clear a specific field bit
template <typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearField(size_t field_index) {
  CHECK(field_index < num_fields_) << "Field index out of range";

  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ &= ~(MaskType(1) << field_index);
  }
}

// Check if a specific field bit is set
template <typename MaskType, size_t MAX_FIELDS>
bool FieldMaskImpl<MaskType, MAX_FIELDS>::HasField(size_t field_index) const {
  if (field_index >= num_fields_) {
    return false;
  }

  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    return true;  // Single field case: presence of object implies field is set
  } else {
    return (mask_ & (MaskType(1) << field_index)) != 0;
  }
}

// Set all field bits to true
template <typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::SetAllFields() {
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    // No-op: field is already implicitly set by object presence
  } else if (num_fields_ == MAX_FIELDS && MAX_FIELDS == 64) {
    mask_ = ~MaskType(
        0);  // Special case: avoid undefined behavior for 64-bit shift
  } else {
    mask_ = (MaskType(1) << num_fields_) - 1;  // Set num_fields_ bits
  }
}

// Clear all field bits
template <typename MaskType, size_t MAX_FIELDS>
void FieldMaskImpl<MaskType, MAX_FIELDS>::ClearAllFields() {
  if constexpr (!std::is_same_v<MaskType, EmptyFieldMask>) {
    mask_ = MaskType{};  // Zero-initialize
  }
}

// Count number of set field bits
template <typename MaskType, size_t MAX_FIELDS>
size_t FieldMaskImpl<MaskType, MAX_FIELDS>::CountSetFields() const {
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    return 1;  // Single field case: presence of object implies field is set
  } else if constexpr (std::is_same_v<MaskType, uint8_t>) {
    return __builtin_popcount(
        static_cast<unsigned int>(mask_));  // Count bits in uint8_t
  } else if constexpr (std::is_same_v<MaskType, uint64_t>) {
    return __builtin_popcountll(mask_);  // Count bits in uint64_t
  } else {
    CHECK(false) << "Unsupported mask type for CountSetFields";
  }
}

// Convert field mask to standard uint64_t representation
template <typename MaskType, size_t MAX_FIELDS>
uint64_t FieldMaskImpl<MaskType, MAX_FIELDS>::AsUint64() const {
  if constexpr (std::is_same_v<MaskType, EmptyFieldMask>) {
    return 1ULL;  // Single field case: presence of object implies field is set
  } else {
    return static_cast<uint64_t>(mask_);  // Cast integer types to uint64_t
  }
}

// Explicit template instantiations
template class FieldMaskImpl<EmptyFieldMask, 1>;
template class FieldMaskImpl<uint8_t, 8>;
template class FieldMaskImpl<uint64_t, 64>;

// Basic Postings Object Implementation

// Check if posting list contains any documents
bool Postings::IsEmpty() const { return key_to_positions_.empty(); }

void Postings::InsertKey(const Key& key, PositionMap&& pos_map) {
  // TODO: Compress the positions map.
  key_to_positions_[key] = std::move(pos_map);
}

// Remove a document key and all its positions
void Postings::RemoveKey(const Key& key) { key_to_positions_.erase(key); }

// Get total number of document keys
size_t Postings::GetKeyCount() const { return key_to_positions_.size(); }

// Get total number of position entries across all keys
size_t Postings::GetPostingCount() const {
  size_t total = 0;
  for (const auto& [key, positions] : key_to_positions_) {
    total += positions.size();
  }
  return total;
}

// Get total term frequency (sum of field occurrences across all positions)
size_t Postings::GetTotalTermFrequency() const {
  size_t total_frequency = 0;
  for (const auto& [key, positions] : key_to_positions_) {
    for (const auto& [position, field_mask] : positions) {
      // Use efficient bit manipulation to count set fields (much faster!)
      total_frequency += field_mask->CountSetFields();
    }
  }
  return total_frequency;
}

// Defragment posting list
Postings* Postings::Defrag() { return this; }

// Iterators Implementation

// Get a Key iterator
Postings::KeyIterator Postings::GetKeyIterator() const {
  KeyIterator iterator;
  iterator.key_map_ = &key_to_positions_;
  iterator.current_ = iterator.key_map_->begin();
  iterator.end_ = iterator.key_map_->end();
  return iterator;
}

// KeyIterator implementations
bool Postings::KeyIterator::IsValid() const {
  CHECK(key_map_ != nullptr) << "KeyIterator is invalid";
  return current_ != end_;
}

void Postings::KeyIterator::NextKey() {
  CHECK(key_map_ != nullptr) << "KeyIterator is invalid";
  if (current_ != end_) {
    ++current_;
  }
}

bool Postings::KeyIterator::ContainsFields(uint64_t field_mask) const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";

  // Check all positions for this key to see if any of the requested fields are
  // set
  for (const auto& [position, position_field_mask] : current_->second) {
    // Safety check: Ensure field_mask is not null
    if (position_field_mask == nullptr) {
      CHECK(false) << "position_field_mask is null";
      return false;
    }

    // Convert position field mask to uint64_t and compare if any of the fields
    // from field_mask are present in position_field_mask
    uint64_t position_mask = position_field_mask->AsUint64();
    if ((position_mask & field_mask) != 0) {
      return true;
    }
  }

  return false;
}

bool Postings::KeyIterator::SkipForwardKey(const Key& key) {
  CHECK(key_map_ != nullptr) << "KeyIterator is invalid";

  // Use lower_bound for efficient binary search since map is ordered
  current_ = key_map_->lower_bound(key);

  // Return true if we landed on exact key match
  return (current_ != end_ && current_->first == key);
}

const Key& Postings::KeyIterator::GetKey() const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";
  return current_->first;
}

Postings::PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";

  PositionIterator pos_iterator;
  pos_iterator.position_map_ = &current_->second;
  pos_iterator.current_ = pos_iterator.position_map_->begin();
  pos_iterator.end_ = pos_iterator.position_map_->end();
  return pos_iterator;
}

// PositionIterator implementations
bool Postings::PositionIterator::IsValid() const {
  CHECK(position_map_ != nullptr) << "PositionIterator is invalid";
  return current_ != end_;
}

void Postings::PositionIterator::NextPosition() {
  CHECK(position_map_ != nullptr) << "PositionIterator is invalid";
  if (current_ != end_) {
    ++current_;
  }
}

bool Postings::PositionIterator::SkipForwardPosition(const Position& position) {
  CHECK(position_map_ != nullptr) << "PositionIterator is invalid";

  // Use lower_bound for efficient binary search since map is ordered
  current_ = position_map_->lower_bound(position);

  // Return true if we landed on exact position match
  return (current_ != end_ && current_->first == position);
}

const Position& Postings::PositionIterator::GetPosition() const {
  CHECK(position_map_ != nullptr && current_ != end_)
      << "PositionIterator is invalid or exhausted";
  return current_->first;
}

uint64_t Postings::PositionIterator::GetFieldMask() const {
  CHECK(position_map_ != nullptr && current_ != end_)
      << "PositionIterator is invalid or exhausted";
  return current_->second->AsUint64();
}

}  // namespace valkey_search::indexes::text
