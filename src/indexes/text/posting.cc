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

// Simple FieldMask implementation
class FieldMaskImpl final : public FieldMask {
 public:
  explicit FieldMaskImpl(size_t num_fields) : num_fields_(num_fields) {}
  void SetField(size_t field_index) override;
  void ClearField(size_t field_index) override;
  bool HasField(size_t field_index) const override;
  void SetAllFields() override;
  void ClearAllFields() override;
  size_t CountSetFields() const override;
  uint64_t AsUint64() const override;
  size_t MaxFields() const override { return num_fields_; }

 private:
  uint64_t mask_{0};
  size_t num_fields_;
};

std::unique_ptr<FieldMask> FieldMask::Create(size_t num_fields) {
  CHECK(num_fields > 0) << "num_fields must be greater than 0";
  CHECK(num_fields <= 64) << "Too many text fields (max 64 supported)";
  return std::make_unique<FieldMaskImpl>(num_fields);
}

void FieldMaskImpl::SetField(size_t field_index) {
  CHECK(field_index < num_fields_) << "Field index out of range";
  mask_ |= (1ULL << field_index);
}

void FieldMaskImpl::ClearField(size_t field_index) {
  CHECK(field_index < num_fields_) << "Field index out of range";
  mask_ &= ~(1ULL << field_index);
}

bool FieldMaskImpl::HasField(size_t field_index) const {
  if (field_index >= num_fields_) return false;
  return (mask_ & (1ULL << field_index)) != 0;
}

void FieldMaskImpl::SetAllFields() {
  mask_ = (num_fields_ == 64) ? ~0ULL : ((1ULL << num_fields_) - 1);
}

void FieldMaskImpl::ClearAllFields() { mask_ = 0; }

size_t FieldMaskImpl::CountSetFields() const {
  return __builtin_popcountll(mask_);
}

uint64_t FieldMaskImpl::AsUint64() const { return mask_; }

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

void Postings::InsertKey(const Key& key, PositionMap&& pos_map,
                         TextIndexMetadata* metadata, size_t num_text_fields) {
  metadata->total_positions += pos_map.size();
  metadata->total_term_frequency += count_num_terms(pos_map);

  // Create FlatPositionMap and insert pointer into map
  FlatPositionMap* flat_map = FlatPositionMap::Create(pos_map, num_text_fields);
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
