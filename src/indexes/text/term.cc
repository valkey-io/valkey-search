/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/term.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(const WordIterator& word_iter, bool exact,
                           const absl::string_view data,
                           const uint32_t field_mask,
                           const InternedStringSet* untracked_keys)
    : exact_(exact),
      data_(data),
      field_mask_(field_mask),
      word_iter_(word_iter),
      current_key_(nullptr),
      current_position_(std::nullopt),
      untracked_keys_(untracked_keys) {
  // This check for matching words is done for exact.
  while (!word_iter_.Done()) {
    if (exact_ && word_iter_.GetWord() == data) {
      key_iterators_.emplace_back(word_iter_.GetTarget()->GetKeyIterator());
    }
    word_iter_.Next();
  }
  // Prime the first key and position if they exist.
  if (!key_iterators_.empty()) {
    TermIterator::NextKey();
  }
}

uint64_t TermIterator::FieldMask() const { return field_mask_; }

bool TermIterator::DoneKeys() const {
  for (const auto& key_iter : key_iterators_) {
    if (key_iter.IsValid()) return false;
  }
  return true;
}

const InternedStringPtr& TermIterator::CurrentKey() const {
  CHECK(current_key_ != nullptr);
  return current_key_;
}

bool TermIterator::NextKey() {
  if (current_key_) {
    for (auto& key_iter : key_iterators_) {
      if (key_iter.IsValid() &&
          key_iter.GetKey()->Str() == current_key_->Str()) {
        key_iter.NextKey();
      }
    }
  }
  current_key_ = nullptr;
  current_position_ = std::nullopt;
  for (auto& key_iter : key_iterators_) {
    while (key_iter.IsValid() && !key_iter.ContainsFields(field_mask_)) {
      key_iter.NextKey();
    }
    if (key_iter.IsValid()) {
      auto key = key_iter.GetKey();
      if (!current_key_ || key->Str() < current_key_->Str()) {
        pos_iterators_.clear();
        pos_iterators_.emplace_back(key_iter.GetPositionIterator());
        current_key_ = key;
      } else if (key->Str() == current_key_->Str()) {
        pos_iterators_.emplace_back(key_iter.GetPositionIterator());
      }
    }
  }
  if (!current_key_) {
    return false;
  }
  // No need to check since we know that at least one position exists based on
  // ContainsFields.
  NextPosition();
  return true;
}

bool TermIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  if (current_key_ && current_key_->Str() >= target_key->Str()) {
    return true;
  }
  // Use SkipForwardKey to efficiently seek all iterators to target_key or
  // beyond
  for (auto& key_iter : key_iterators_) {
    key_iter.SkipForwardKey(target_key);
  }
  // Now find the minimum key using the same logic as NextKey
  current_key_ = nullptr;
  current_position_ = std::nullopt;
  for (auto& key_iter : key_iterators_) {
    while (key_iter.IsValid() && !key_iter.ContainsFields(field_mask_)) {
      key_iter.NextKey();
    }
    if (key_iter.IsValid()) {
      auto key = key_iter.GetKey();
      if (!current_key_ || key->Str() < current_key_->Str()) {
        // Test this candidate key by collecting its position iterators
        pos_iterators_.clear();
        pos_iterators_.emplace_back(key_iter.GetPositionIterator());
        current_key_ = key;
      } else if (key->Str() == current_key_->Str()) {
        pos_iterators_.emplace_back(key_iter.GetPositionIterator());
      }
    }
  }
  if (!current_key_) {
    return false;
  }
  // No need to check since we know that at least one position exists based on
  // ContainsFields.
  NextPosition();
  return true;
}

bool TermIterator::DonePositions() const {
  for (const auto& pos_iter : pos_iterators_) {
    if (pos_iter.IsValid()) return false;
  }
  return true;
}

std::pair<uint32_t, uint32_t> TermIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return std::make_pair(current_position_.value(), current_position_.value());
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    for (auto& pos_iter : pos_iterators_) {
      if (pos_iter.IsValid() &&
          pos_iter.GetPosition() == current_position_.value()) {
        pos_iter.NextPosition();
      }
    }
  }
  uint32_t min_position = UINT32_MAX;
  bool found = false;
  for (auto& pos_iter : pos_iterators_) {
    while (pos_iter.IsValid() && !(pos_iter.GetFieldMask() & field_mask_)) {
      pos_iter.NextPosition();
    }
    if (pos_iter.IsValid()) {
      uint32_t position = pos_iter.GetPosition();
      if (position < min_position) {
        min_position = position;
        found = true;
      }
    }
  }
  if (!found) {
    current_position_ = std::nullopt;
    return false;
  }
  current_position_ = min_position;
  return true;
}

}  // namespace valkey_search::indexes::text
