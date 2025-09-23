/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/term.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(const WordIterator& word_iter,
                           bool exact,
                           const absl::string_view data,
                           const uint32_t field_mask,
                           const InternedStringSet* untracked_keys)
    : exact_(exact),
      data_(data),
      field_mask_(field_mask),
      word_iter_(word_iter),
      target_posting_(nullptr),      // initialize shared_ptr to null
      key_iter_(),                   // default-initialize iterator
      pos_iter_(),                   // default-initialize iterator
      current_key_(nullptr),         // no key yet
      current_position_(std::nullopt),
      current_field_mask_(std::nullopt),
      untracked_keys_(untracked_keys),
      nomatch_(false)                // start as "not done"
{
  VMSDK_LOG(WARNING, nullptr) << "TI::init{" << data_ << "}. nomatch_: " << nomatch_;
  if (word_iter_.Done()) {
    VMSDK_LOG(WARNING, nullptr) << "TI::nomatch1{" << data_ << "}";
    nomatch_ = true;
    return;
  }
  if (data_ != word_iter_.GetWord()) {
    VMSDK_LOG(WARNING, nullptr) << "TI::nomatch2{" << data_ << "}";
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
  VMSDK_LOG(WARNING, nullptr) << "TI::NextKey{" << word_iter_.GetWord() << "}";
  if (current_key_) {
      key_iter_.NextKey();
  }
  // Loop until we find a key that satisfies the field mask
  while (key_iter_.IsValid()) {
    if (key_iter_.ContainsFields(field_mask_)) {
        current_key_ = key_iter_.GetKey();
        pos_iter_ = key_iter_.GetPositionIterator();
        current_position_ = std::nullopt;
        current_field_mask_ = std::nullopt;
        // We need to call NextPosition here if we dont want garbage values.
        VMSDK_LOG(WARNING, nullptr) << "TI::NextKey{" << word_iter_.GetWord() << "} - Found key. CurrentKey: " << current_key_->Str() << " Position: " << pos_iter_.GetPosition();
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

const InternedStringPtr& TermIterator::CurrentKey() {
  CHECK(current_key_ != nullptr);
  VMSDK_LOG(WARNING, nullptr) << "TI::CurrentKey{" << word_iter_.GetWord() << "}. Key: " <<  current_key_->Str();
  return current_key_;
}

bool TermIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "TI::NextPosition{" << word_iter_.GetWord() << "}";
  if (current_position_.has_value()) {
      pos_iter_.NextPosition();
  }
  // Loop until we find a position that satisfies the field mask
  while (pos_iter_.IsValid()) {
    if (pos_iter_.GetFieldMask() & field_mask_) {
      current_position_ = pos_iter_.GetPosition();
      current_field_mask_ = pos_iter_.GetFieldMask();
      return true;
    }
    pos_iter_.NextPosition();
  }
  // No more valid positions
  current_position_ = std::nullopt;
  current_field_mask_ = std::nullopt;
  return false;
}

std::pair<uint32_t, uint32_t> TermIterator::CurrentPosition() {
  VMSDK_LOG(WARNING, nullptr) << "TI::CurrentPosition{" << word_iter_.GetWord() << "}";
  CHECK(current_position_.has_value());
  return std::make_pair(current_position_.value(), current_position_.value());
}

uint64_t TermIterator::CurrentFieldMask() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::CurrentFieldMask{" << word_iter_.GetWord() << "}";
  CHECK(current_field_mask_.has_value());
  return current_field_mask_.value();
}

bool TermIterator::DoneKeys() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::DoneKeys{" << data_ << "}";
  if (nomatch_) {
      VMSDK_LOG(WARNING, nullptr) << "TI::DoneKeys{" << data_ << "} Done due to nomatch_";
    return true;
  }
  return !key_iter_.IsValid();
}

bool TermIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::DonePositions{" << data_ << "}";
  if (nomatch_) {
    return true;
  }
  return !pos_iter_.IsValid();
}

}  // namespace valkey_search::indexes::text
