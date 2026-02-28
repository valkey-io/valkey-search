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
#include "src/indexes/text/flat_position_map.h"

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

// Destructor: clean up all FlatPositionMaps
Postings::~Postings() {
  for (auto& [key, flat_map] : key_to_positions_) {
    FlatPositionMap::Destroy(flat_map);
  }
}

// Check if posting list contains any documents
bool Postings::IsEmpty() const { return key_to_positions_.empty(); }

// TODO: develop a better strategy to track terms
unsigned int count_num_terms(const PositionMap& pos_map) {
  unsigned int num_terms = 0;
  for (const auto& [_, field_mask] : pos_map) {
    num_terms += field_mask->CountSetFields();
  }
  return num_terms;
}

void Postings::InsertKey(const Key& key, FlatPositionMap* flat_map) {
  // Insert pre-created FlatPositionMap pointer into map
  // Metadata updates are done by the caller before this point
  key_to_positions_.emplace(key, flat_map);
}

// Remove a document key and all its positions
void Postings::RemoveKey(const Key& key, TextIndexMetadata* metadata) {
  auto node = key_to_positions_.extract(key);
  if (node.empty()) return;

  FlatPositionMap* flat_map = node.mapped();

  // Use member functions to get counts
  size_t position_count = flat_map->CountPositions();
  size_t term_frequency = flat_map->CountTermFrequency();

  metadata->total_positions -= position_count;
  metadata->total_term_frequency -= term_frequency;

  // Destroy and remove from map
  FlatPositionMap::Destroy(flat_map);
}

// Get total number of document keys
size_t Postings::GetKeyCount() const { return key_to_positions_.size(); }

// Get total number of position entries across all keys
size_t Postings::GetPositionCount() const {
  size_t total = 0;
  for (const auto& [key, flat_map] : key_to_positions_) {
    total += flat_map->CountPositions();
  }
  return total;
}

// Get total term frequency (sum of field occurrences across all positions)
size_t Postings::GetTotalTermFrequency() const {
  size_t total_frequency = 0;
  for (const auto& [key, flat_map] : key_to_positions_) {
    total_frequency += flat_map->CountTermFrequency();
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

  FlatPositionMap* flat_map = current_->second;

  // Check all positions for this key to see if any of the requested fields are
  // set
  PositionIterator iter(*flat_map);
  while (iter.IsValid()) {
    uint64_t position_mask = iter.GetFieldMask();
    if ((position_mask & field_mask) != 0) {
      return true;
    }
    iter.NextPosition();
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

PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";

  FlatPositionMap* flat_map = current_->second;
  return PositionIterator(*flat_map);
}

}  // namespace valkey_search::indexes::text
