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
#include <optional>

#include "absl/log/check.h"
#include "src/index_schema.h"
#include "src/indexes/text/flat_position_map.h"

namespace valkey_search::indexes::text {

// FieldMask Implementation

FieldMask::FieldMask(size_t num_fields) : mask_(0) {
  CHECK(num_fields > 0 && num_fields <= 64)
      << "num_fields must be between 1 and 64";
  num_fields_ = static_cast<uint8_t>(num_fields);
}

void FieldMask::SetField(size_t field_index) {
  CHECK(field_index < num_fields_) << "Field index out of range";
  mask_ |= (1ULL << field_index);
}

size_t FieldMask::CountSetFields() const { return __builtin_popcountll(mask_); }

uint64_t FieldMask::GetMask() const { return mask_; }

// Basic Postings Object Implementation

// Destructor: clean up all FlatPositionMaps
Postings::~Postings() {
  for (auto& [key, value] : key_to_positions_) {
    FlatPositionMap::Destroy(value.map);
  }
}

// Check if posting list contains any documents
bool Postings::IsEmpty() const { return key_to_positions_.empty(); }

void Postings::InsertKey(const Key& key, FlatPositionMap* flat_map, uint32_t tf,
                         uint32_t doc_len) {
  key_to_positions_.emplace(key, PostingValue{flat_map, {tf, doc_len}});
}

// Remove a document key and all its positions
void Postings::RemoveKey(const Key& key, TextIndexMetadata* metadata) {
  auto node = key_to_positions_.extract(key);
  if (node.empty()) return;

  FlatPositionMap* flat_map = node.mapped().map;

  metadata->total_positions -= flat_map->CountPositions();
  metadata->total_term_frequency -= node.mapped().doc_stats.tf;

  // Destroy and remove from map
  FlatPositionMap::Destroy(flat_map);
}

// Get total number of document keys
size_t Postings::GetKeyCount() const { return key_to_positions_.size(); }

// Get total number of position entries across all keys
size_t Postings::GetPositionCount() const {
  size_t total = 0;
  for (const auto& [key, value] : key_to_positions_) {
    total += value.map->CountPositions();
  }
  return total;
}

// Get total term frequency (sum of field occurrences across all positions)
size_t Postings::GetTotalTermFrequency() const {
  size_t total_frequency = 0;
  for (const auto& [key, value] : key_to_positions_) {
    total_frequency += value.doc_stats.tf;
  }
  return total_frequency;
}

namespace {

// Does any position for this key fall in a field in `field_mask`?
// NOTE: We could make a space tradeoff and store a union of the field masks
// upon creation for every PostingValue to avoid iteration cost.
bool ContainsFields(const PostingValue& value, uint64_t field_mask) {
  CHECK(value.map != nullptr)
      << "Posting list contains a key with no FlatPositionMap";
  // Every key present has >=1 position, so "any field" needs no scan.
  if (field_mask == ~0ULL) return true;
  PositionIterator iter(*value.map);
  while (iter.IsValid()) {
    if ((iter.GetFieldMask() & field_mask) != 0) {
      return true;
    }
    iter.NextPosition();
  }
  return false;
}

}  // namespace

std::optional<PostingValue> Postings::GetPostingValue(
    BorrowedInternedStringPtr key, uint64_t field_mask) const {
  auto it = key_to_positions_.find(key);
  if (it == key_to_positions_.end() ||
      !text::ContainsFields(it->second, field_mask)) {
    return std::nullopt;
  }
  return it->second;
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
  return text::ContainsFields(current_->second, field_mask);
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

const FlatPositionMap& Postings::KeyIterator::GetPositionMap() const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";
  return *current_->second.map;
}

PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  return {GetPositionMap()};
}

size_t Postings::KeyIterator::GetTermFrequency() const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";
  return current_->second.doc_stats.tf;
}

uint32_t Postings::KeyIterator::GetDocLen() const {
  CHECK(key_map_ != nullptr && current_ != end_)
      << "KeyIterator is invalid or exhausted";
  return current_->second.doc_stats.doc_len;
}

}  // namespace valkey_search::indexes::text
