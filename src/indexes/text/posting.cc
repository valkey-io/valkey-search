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
  for (auto& [key, flat_map] : flat_entries_) {
    FlatPositionMap::Destroy(flat_map);
  }
  for (auto& [key, flat_map] : tree_entries_) {
    FlatPositionMap::Destroy(flat_map);
  }
}

// Check if posting list contains any documents
bool Postings::IsEmpty() const {
  return flat_entries_.empty() && tree_entries_.empty();
}

void Postings::InsertKey(const Key& key, FlatPositionMap* flat_map) {
  if (!tree_entries_.empty()) {
    tree_entries_.emplace(key, flat_map);
    return;
  }

  auto comp = [](const std::pair<Key, FlatPositionMap*>& item, const Key& val) {
    return item.first < val;
  };
  auto it = std::lower_bound(flat_entries_.begin(), flat_entries_.end(), key, comp);
  if (it != flat_entries_.end() && it->first == key) {
    FlatPositionMap::Destroy(it->second);
    it->second = flat_map;
    return;
  }
  flat_entries_.insert(it, {key, flat_map});

  if (flat_entries_.size() > kFlatThreshold) {
    for (auto& entry : flat_entries_) {
      tree_entries_.emplace(entry.first, entry.second);
    }
    flat_entries_.clear();
    flat_entries_.shrink_to_fit();
  }
}

// Remove a document key and all its positions
void Postings::RemoveKey(const Key& key, TextIndexMetadata* metadata) {
  FlatPositionMap* flat_map = nullptr;

  if (!tree_entries_.empty()) {
    auto node = tree_entries_.extract(key);
    if (!node.empty()) {
      flat_map = node.mapped();
    }
  } else {
    auto comp = [](const std::pair<Key, FlatPositionMap*>& item, const Key& val) {
      return item.first < val;
    };
    auto it = std::lower_bound(flat_entries_.begin(), flat_entries_.end(), key, comp);
    if (it != flat_entries_.end() && it->first == key) {
      flat_map = it->second;
      flat_entries_.erase(it);
    }
  }

  if (!flat_map) return;

  size_t position_count = flat_map->CountPositions();
  size_t term_frequency = flat_map->CountTermFrequency();

  metadata->total_positions -= position_count;
  metadata->total_term_frequency -= term_frequency;

  FlatPositionMap::Destroy(flat_map);
}

// Get total number of document keys
size_t Postings::GetKeyCount() const {
  return tree_entries_.empty() ? flat_entries_.size() : tree_entries_.size();
}

// Get total number of position entries across all keys
size_t Postings::GetPositionCount() const {
  size_t total = 0;
  if (!tree_entries_.empty()) {
    for (const auto& [key, flat_map] : tree_entries_) {
      total += flat_map->CountPositions();
    }
  } else {
    for (const auto& [key, flat_map] : flat_entries_) {
      total += flat_map->CountPositions();
    }
  }
  return total;
}

// Get total term frequency (sum of field occurrences across all positions)
size_t Postings::GetTotalTermFrequency() const {
  size_t total_frequency = 0;
  if (!tree_entries_.empty()) {
    for (const auto& [key, flat_map] : tree_entries_) {
      total_frequency += flat_map->CountTermFrequency();
    }
  } else {
    for (const auto& [key, flat_map] : flat_entries_) {
      total_frequency += flat_map->CountTermFrequency();
    }
  }
  return total_frequency;
}

// Defragment posting list
Postings* Postings::Defrag() { return this; }

// Iterators Implementation

Postings::KeyIterator Postings::GetKeyIterator() const {
  KeyIterator iterator;
  iterator.postings_ = this;
  if (!tree_entries_.empty()) {
    iterator.is_flat_ = false;
    iterator.tree_it_ = tree_entries_.begin();
  } else {
    iterator.is_flat_ = true;
    iterator.vec_idx_ = 0;
  }
  return iterator;
}

bool Postings::KeyIterator::IsValid() const {
  if (!postings_) return false;
  if (is_flat_) {
    return vec_idx_ < postings_->flat_entries_.size();
  } else {
    return tree_it_ != postings_->tree_entries_.end();
  }
}

void Postings::KeyIterator::NextKey() {
  if (!IsValid()) return;
  if (is_flat_) {
    vec_idx_++;
  } else {
    ++tree_it_;
  }
}

bool Postings::KeyIterator::ContainsFields(uint64_t field_mask) const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";

  FlatPositionMap* flat_map = is_flat_ ? postings_->flat_entries_[vec_idx_].second : tree_it_->second;
  CHECK(flat_map != nullptr) << "Posting list contains a key with no FlatPositionMap";

  if (field_mask == ~0ULL) return true;

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
  if (!postings_) return false;

  if (is_flat_) {
    auto comp = [](const std::pair<Key, FlatPositionMap*>& item, const Key& val) {
      return item.first < val;
    };
    auto it = std::lower_bound(postings_->flat_entries_.begin() + vec_idx_, postings_->flat_entries_.end(), key, comp);
    vec_idx_ = std::distance(postings_->flat_entries_.begin(), it);
    return (vec_idx_ < postings_->flat_entries_.size() && postings_->flat_entries_[vec_idx_].first == key);
  } else {
    tree_it_ = postings_->tree_entries_.lower_bound(key);
    return (tree_it_ != postings_->tree_entries_.end() && tree_it_->first == key);
  }
}

const Key& Postings::KeyIterator::GetKey() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  return is_flat_ ? postings_->flat_entries_[vec_idx_].first : tree_it_->first;
}

PositionIterator Postings::KeyIterator::GetPositionIterator() const {
  CHECK(IsValid()) << "KeyIterator is invalid or exhausted";
  FlatPositionMap* flat_map = is_flat_ ? postings_->flat_entries_[vec_idx_].second : tree_it_->second;
  return PositionIterator(*flat_map);
}

}  // namespace valkey_search::indexes::text