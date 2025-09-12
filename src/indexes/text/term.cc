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
      untracked_keys_(untracked_keys),
      nomatch_(false)                // start as "not done"
{
  VMSDK_LOG(WARNING, nullptr) << "TI::init{" << data_ << "}. nomatch_: " << nomatch_;
  if (word_iter_.Done()) {
    VMSDK_LOG(WARNING, nullptr) << "TI::nomatch1{" << data_ << "}";
    nomatch_ = true;
    return;
  }
  target_posting_ = word_iter_.GetTarget();
  key_iter_ = target_posting_->GetKeyIterator();
  // TODO: Check if this is a degenerate case of the Done function or if it can be done to be done. 
  if (!TermIterator::NextKey()) {
    VMSDK_LOG(WARNING, nullptr) << "TI::nomatch2{" << word_iter_.GetWord() << "}";
    nomatch_ = true;
  }
}

bool TermIterator::NextKey() {
  VMSDK_LOG(WARNING, nullptr) << "TI::NextKey{" << word_iter_.GetWord() << "}";
  if (nomatch_) return false;
  if (current_key_) {
      key_iter_.NextKey();
  }
  // Loop until we find a key that satisfies the field mask
  while (key_iter_.IsValid()) {
    if (key_iter_.ContainsFields(field_mask_)) {
        current_key_ = key_iter_.GetKey();
        pos_iter_ = key_iter_.GetPositionIterator();
        VMSDK_LOG(WARNING, nullptr) << "TI::NextKey{" << word_iter_.GetWord() << "} - Found key. CurrentKey: " << current_key_->Str() << " Position: " << pos_iter_.GetPosition();;
        // pos_iter_.NextPosition();
        return true;
    }
    key_iter_.NextKey();
  }
  // No more valid keys
  current_key_ = nullptr;
  nomatch_ = true;
  return false;
}

const InternedStringPtr& TermIterator::CurrentKey() {
  VMSDK_LOG(WARNING, nullptr) << "TI::CurrentKey{" << word_iter_.GetWord() << "}";
  return current_key_;
}

// Note: Right now, we expect the caller site to move to the next key when the positions are exhausted.
// Need to think about whether this is the right contract.
bool TermIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "TI::NextPosition{" << word_iter_.GetWord() << "}";
  if (nomatch_) return false;
  if (!pos_iter_.IsValid()) {
    return false;
  }
  pos_iter_.NextPosition();
  return true;
}

uint32_t TermIterator::CurrentPosition() {
  VMSDK_LOG(WARNING, nullptr) << "TI::CurrentPosition{" << word_iter_.GetWord() << "}";
  return pos_iter_.GetPosition();
}

uint64_t TermIterator::GetFieldMask() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::GetFieldMask{" << word_iter_.GetWord() << "}";
  return pos_iter_.GetFieldMask();
} 

bool TermIterator::DoneKeys() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::Done{" << word_iter_.GetWord() << "}";
  return nomatch_ || !key_iter_.IsValid();
}

bool TermIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::Done{" << word_iter_.GetWord() << "}";
  return nomatch_ || !pos_iter_.IsValid();
}

bool TermIterator::Done() const {
  VMSDK_LOG(WARNING, nullptr) << "TI::Done{" << word_iter_.GetWord() << "}";
  return nomatch_ || !key_iter_.IsValid();
}

void TermIterator::Next() {
  // Unified iteration: advance by position first
  // if (nomatch_) return;
  // NextPosition();
  // If no positions left at all, nomatch_ would be set by NextKey/NextWord path
}

// bool TermIterator::Done() const {
//   if (nomatch_ || word_.GetWord() != data_) {
//     return true;
//   }
//   // Check if key iterator is valid
//   return !key_iter_.IsValid();
// }

// void TermIterator::Next() {
  // // On a Begin() call, we initialize the target_posting_ and key_iter_.
  // if (begin_) {
  //   if (word_.Done()) {
  //     nomatch_ = true;
  //     return;
  //   }
  //   target_posting_ = word_.GetTarget();
  //   key_iter_ = target_posting_->GetKeyIterator();
  //   begin_ = false;  // Set to false after the first call to Next.
  // } else {
  //   key_iter_.NextKey();
  // }
  // // Advance until we find a valid key or reach the end
  // while (!Done() && !key_iter_.ContainsFields(field_mask_)) {
  //   key_iter_.NextKey();
  // }
// }

// const InternedStringPtr& TermIterator::operator*() const {
//   // Return the current key from the key iterator of the posting object.
//   return key_iter_.GetKey();
// }

}  // namespace valkey_search::indexes::text
