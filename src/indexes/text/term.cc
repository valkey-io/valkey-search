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
      target_posting_(nullptr),
      key_iter_(),
      pos_iter_(),
      current_key_(nullptr),
      current_position_(std::nullopt),
      untracked_keys_(untracked_keys),
      nomatch_(false)
{
  if (word_iter_.Done()) {
    nomatch_ = true;
    return;
  }
  if (data_ != word_iter_.GetWord()) {
    nomatch_ = true;
    return;
  }
  target_posting_ = word_iter_.GetTarget();
  key_iter_ = target_posting_->GetKeyIterator();
  // Prime the first key and position if they exist.
  TermIterator::NextKey();
}

// Usage: Need to check if not DoneKeys, and only then call NextKey
bool TermIterator::NextKey() {
  if (current_key_) {
    key_iter_.NextKey();
  }
  // Loop until we find a key that satisfies the field mask
  while (key_iter_.IsValid()) {
    if (key_iter_.ContainsFields(field_mask_)) {
      current_key_ = key_iter_.GetKey();
      pos_iter_ = key_iter_.GetPositionIterator();
      current_position_ = std::nullopt;
      // We need to call NextPosition here if we dont want garbage values.
      if (TermIterator::NextPosition()) {
        return true;  // We have a key and a position
      }
    }
    key_iter_.NextKey();
  }
  // No more valid keys
  current_key_ = nullptr;
  return false;
}

const InternedStringPtr& TermIterator::CurrentKey() const {
  CHECK(current_key_ != nullptr);
  return current_key_;
}

bool TermIterator::NextPosition() {
  if (current_position_.has_value()) {
    pos_iter_.NextPosition();
  }
  // Loop until we find a position that satisfies the field mask
  while (pos_iter_.IsValid()) {
    if (pos_iter_.GetFieldMask() & field_mask_) {
      current_position_ = pos_iter_.GetPosition();
      return true;
    }
    pos_iter_.NextPosition();
  }
  // No more valid positions
  current_position_ = std::nullopt;
  return false;
}

std::pair<uint32_t, uint32_t> TermIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return std::make_pair(current_position_.value(), current_position_.value());
}

bool TermIterator::DoneKeys() const {
  if (nomatch_) {
    return true;
  }
  return !key_iter_.IsValid();
}

bool TermIterator::DonePositions() const {
  if (nomatch_) {
    return true;
  }
  return !pos_iter_.IsValid();
}

uint64_t TermIterator::FieldMask() const { return field_mask_; }

}  // namespace valkey_search::indexes::text
