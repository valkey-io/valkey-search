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
      target_posting_(nullptr),      // initialize shared_ptr to null
      key_iter_(),                   // default-initialize iterator
      pos_iter_(),                   // default-initialize iterator
      current_key_(nullptr),         // no key yet
      current_position_(std::nullopt),
      current_field_mask_(std::nullopt),
      untracked_keys_(untracked_keys),
      nomatch_(false)                // start as "not done"
{
  VMSDK_LOG(WARNING, nullptr) << "WI::init{" << data_ << "}. nomatch_: " << nomatch_;
  if (word_iter_.Done()) {
    VMSDK_LOG(WARNING, nullptr) << "WI::nomatch1{" << data_ << "}";
    nomatch_ = true;
    return;
  }
  target_posting_ = word_iter_.GetTarget();
  key_iter_ = target_posting_->GetKeyIterator();
  // Prime the first key and position if they exist.
  if (!WildCardIterator::NextKey()) {
    VMSDK_LOG(WARNING, nullptr) << "WI::nomatch2{" << word_iter_.GetWord() << "}";
    nomatch_ = true;
    return;
  }
  if (!WildCardIterator::NextPosition()) {
    VMSDK_LOG(WARNING, nullptr) << "WI::nomatch3{" << word_iter_.GetWord() << "}";
    nomatch_ = true;
  }
}

bool WildCardIterator::NextKey() {
  VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << word_iter_.GetWord() << "}";
  if (nomatch_) return false;
  if (current_key_) {
      key_iter_.NextKey();
  }
  // Loop until we find a key that satisfies the field mask
  while (!word_iter_.Done()) {
    while (key_iter_.IsValid()) {
      if (key_iter_.ContainsFields(field_mask_)) {
          current_key_ = key_iter_.GetKey();
          pos_iter_ = key_iter_.GetPositionIterator();
          VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << word_iter_.GetWord() << "} - Found key. CurrentKey: " << current_key_->Str() << " Position: " << pos_iter_.GetPosition();;
          return true;
      }
      key_iter_.NextKey();
    }
    // Current posting exhausted. Move to next word
    word_iter_.Next();
  }
  // No more valid keys
  current_key_ = nullptr;
  nomatch_ = true;
  return false;
}

const InternedStringPtr& WildCardIterator::CurrentKey() {
  VMSDK_LOG(WARNING, nullptr) << "WI::CurrentKey{" << word_iter_.GetWord() << "}";
  CHECK(current_key_ != nullptr);
  return current_key_;
}

// Note: Right now, we expect the caller site to move to the next key when the positions are exhausted.
// Need to think about whether this is the right contract.
bool WildCardIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "WI::NextPosition{" << word_iter_.GetWord() << "}";
  if (nomatch_) return false;
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
  nomatch_ = true;
  return false;
}

std::pair<uint32_t, uint32_t> WildCardIterator::CurrentPosition() {
  VMSDK_LOG(WARNING, nullptr) << "WI::CurrentPosition{" << word_iter_.GetWord() << "}";
  CHECK(current_position_.has_value());
  // return current_position_.value();
  return std::make_pair(current_position_.value(), current_position_.value());
}

uint64_t WildCardIterator::CurrentFieldMask() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::CurrentFieldMask{" << word_iter_.GetWord() << "}";
  CHECK(current_field_mask_.has_value());
  return current_field_mask_.value();
} 

bool WildCardIterator::DoneKeys() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::DoneKeys{" << word_iter_.GetWord() << "}";
  if (nomatch_) {
    return true;
  }
  return word_iter_.Done() && !key_iter_.IsValid();
}

bool WildCardIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::Done{" << word_iter_.GetWord() << "}";
  if (nomatch_) {
    return true;
  }
  return !pos_iter_.IsValid();
}

}  // namespace valkey_search::indexes::text
