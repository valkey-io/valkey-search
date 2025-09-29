/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/wildcard.h"

namespace valkey_search::indexes::text {

WildCardIterator::WildCardIterator(const WordIterator& word_iter,
                                   const text::WildCardOperation operation,
                                   const absl::string_view data,
                                   const uint32_t field_mask,
                                   const InternedStringSet* untracked_keys)
    : operation_(operation),
      data_(data),
      field_mask_(field_mask),
      word_iter_(word_iter),
      target_posting_(nullptr),
      key_iter_(),
      pos_iter_(),
      current_key_(nullptr),
      current_position_(std::nullopt),
      untracked_keys_(untracked_keys) {
  if (word_iter_.Done()) {
    return;
  }
  target_posting_ = word_iter_.GetTarget();
  key_iter_ = target_posting_->GetKeyIterator();
  // Prime the first key and position if they exist.
  WildCardIterator::NextKey();
}

uint64_t WildCardIterator::FieldMask() const { return field_mask_; }

bool WildCardIterator::DoneKeys() const {
  if (word_iter_.Done()) {
    return true;
  }
  return !key_iter_.IsValid();
}

const InternedStringPtr& WildCardIterator::CurrentKey() const {
  CHECK(current_key_ != nullptr);
  return current_key_;
}

bool WildCardIterator::NextKey() {
  if (current_key_) {
    key_iter_.NextKey();
  }
  // Loop until we find a key that satisfies the field mask
  while (!word_iter_.Done()) {
    while (key_iter_.IsValid()) {
      if (key_iter_.ContainsFields(field_mask_)) {
        current_key_ = key_iter_.GetKey();
        pos_iter_ = key_iter_.GetPositionIterator();
        current_position_ = std::nullopt;
        if (WildCardIterator::NextPosition()) {
          return true;
        }
      }
      key_iter_.NextKey();
    }
    // Current posting exhausted. Move to next word
    word_iter_.Next();
    if (!word_iter_.Done()) {
      target_posting_ = word_iter_.GetTarget();
      key_iter_ = target_posting_->GetKeyIterator();
    }
  }
  // No more valid keys
  current_key_ = nullptr;
  return false;
}

bool WildCardIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  // If current key is already >= target_key, no need to seek
  if (current_key_ && current_key_->Str() >= target_key->Str()) {
    return true;
  }
  // Seek through words and keys to find target_key or beyond
  // We optimize by not checking position / field constraints
  // until we find the right key.
  while (!word_iter_.Done()) {
    // Seek key iterator to target_key or beyond
    while (key_iter_.IsValid() &&
           key_iter_.GetKey()->Str() < target_key->Str()) {
      key_iter_.NextKey();
    }
    // Find next valid key/position combination using existing logic
    while (key_iter_.IsValid()) {
      if (key_iter_.ContainsFields(field_mask_)) {
        current_key_ = key_iter_.GetKey();
        pos_iter_ = key_iter_.GetPositionIterator();
        current_position_ = std::nullopt;
        if (NextPosition()) {
          return true;
        }
      }
      key_iter_.NextKey();
    }
    // Current posting exhausted. Move to next word
    word_iter_.Next();
    if (!word_iter_.Done()) {
      target_posting_ = word_iter_.GetTarget();
      key_iter_ = target_posting_->GetKeyIterator();
    }
  }
  current_key_ = nullptr;
  return false;
}

bool WildCardIterator::DonePositions() const { return !pos_iter_.IsValid(); }

std::pair<uint32_t, uint32_t> WildCardIterator::CurrentPosition() const {
  CHECK(current_position_.has_value());
  return std::make_pair(current_position_.value(), current_position_.value());
}

bool WildCardIterator::NextPosition() {
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

}  // namespace valkey_search::indexes::text
