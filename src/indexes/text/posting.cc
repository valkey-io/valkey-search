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
  for (auto it = key_to_positions_.GetIterator(); it.IsValid(); it.Next()) {
    FlatPositionMap::Destroy(it.GetValue().map);
  }
}

// Check if posting list contains any documents
bool Postings::IsEmpty() const { return key_to_positions_.empty(); }

void Postings::InsertKey(const Key& key, FlatPositionMap* flat_map, uint32_t tf,
                         uint32_t doc_len) {
  key_to_positions_.Insert(key, PostingValue{flat_map, tf, doc_len});
}

// Remove a document key and all its positions
void Postings::RemoveKey(const Key& key, TextIndexMetadata* metadata) {
  PostingValue value;
  if (!key_to_positions_.Erase(key, &value)) return;

  metadata->total_positions -= value.map->CountPositions();
  metadata->total_term_frequency -= value.tf;

  FlatPositionMap::Destroy(value.map);
}

// Get total number of document keys
size_t Postings::GetKeyCount() const { return key_to_positions_.size(); }

// Get total number of position entries across all keys
size_t Postings::GetPositionCount() const {
  size_t total = 0;
  for (auto it = key_to_positions_.GetIterator(); it.IsValid(); it.Next()) {
    total += it.GetValue().map->CountPositions();
  }
  return total;
}

// Get total term frequency (sum of field occurrences across all positions)
size_t Postings::GetTotalTermFrequency() const {
  size_t total_frequency = 0;
  for (auto it = key_to_positions_.GetIterator(); it.IsValid(); it.Next()) {
    total_frequency += it.GetValue().tf;
  }
  return total_frequency;
}

std::optional<PostingValue> Postings::LookupKey(
    BorrowedInternedStringPtr key) const {
  const PostingValue* value = key_to_positions_.Find(key);
  if (value == nullptr) {
    return std::nullopt;
  }
  return *value;
}

// Defragment posting list
Postings* Postings::Defrag() { return this; }

// Iterators Implementation

// Get a Key iterator
Postings::KeyIterator Postings::GetKeyIterator() const {
  KeyIterator iterator;
  iterator.it_ = key_to_positions_.GetIterator();
  return iterator;
}

// KeyIterator implementations
bool Postings::KeyIterator::IsValid() const { return it_.IsValid(); }

void Postings::KeyIterator::NextKey() { it_.Next(); }

bool Postings::KeyIterator::ContainsFields(uint64_t field_mask) const {
  CHECK(it_.IsValid()) << "KeyIterator is invalid or exhausted";

  CHECK(it_.GetValue().map != nullptr)
      << "Posting list contains a key with no FlatPositionMap";

  // When querying all fields (~0ULL), any non-zero position mask will match,
  // and every key in the posting list has at least one position entry.
  if (field_mask == ~0ULL) return true;

  FlatPositionMap* flat_map = it_.GetValue().map;

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
  return it_.SkipForward(key);
}

const Key& Postings::KeyIterator::GetKey() const {
  CHECK(it_.IsValid()) << "KeyIterator is invalid or exhausted";
  return it_.GetKey();
}

PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  CHECK(it_.IsValid()) << "KeyIterator is invalid or exhausted";

  FlatPositionMap* flat_map = it_.GetValue().map;
  return PositionIterator(*flat_map);
}

size_t Postings::KeyIterator::GetTermFrequency() const {
  CHECK(it_.IsValid()) << "KeyIterator is invalid or exhausted";
  return it_.GetValue().tf;
}

uint32_t Postings::KeyIterator::GetDocLen() const {
  CHECK(it_.IsValid()) << "KeyIterator is invalid or exhausted";
  return it_.GetValue().doc_len;
}

}  // namespace valkey_search::indexes::text