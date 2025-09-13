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
  // TODO: Check if this is a degenerate case of the Done function or if it can be done to be done. 
  if (!WildCardIterator::NextKey()) {
    VMSDK_LOG(WARNING, nullptr) << "WI::nomatch2{" << word_iter_.GetWord() << "}";
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
  return current_key_;
}

// Note: Right now, we expect the caller site to move to the next key when the positions are exhausted.
// Need to think about whether this is the right contract.
bool WildCardIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "WI::NextPosition{" << word_iter_.GetWord() << "}";
  if (nomatch_) return false;
  if (!pos_iter_.IsValid()) {
    return false;
  }
  pos_iter_.NextPosition();
  return true;
}

uint32_t WildCardIterator::CurrentPosition() {
  VMSDK_LOG(WARNING, nullptr) << "WI::CurrentPosition{" << word_iter_.GetWord() << "}";
  return pos_iter_.GetPosition();
}

uint64_t WildCardIterator::GetFieldMask() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::GetFieldMask{" << word_iter_.GetWord() << "}";
  return pos_iter_.GetFieldMask();
} 

bool WildCardIterator::DoneKeys() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::Done{" << word_iter_.GetWord() << "}";
  if (nomatch_) {
    return true;
  }
  return word_iter_.Done() && !key_iter_.IsValid();
}

bool WildCardIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::Done{" << word_iter_.GetWord() << "}";
  return nomatch_ || !pos_iter_.IsValid();
}

bool WildCardIterator::Done() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::Done{" << word_iter_.GetWord() << "}";
  return nomatch_ || !key_iter_.IsValid();
}

void WildCardIterator::Next() {
  // Unified iteration: advance by position first
  // if (nomatch_) return;
  // NextPosition();
  // If no positions left at all, nomatch_ would be set by NextKey/NextWord path
}

// bool WildCardIterator::Done() const {
//   if (nomatch_) {
//     return true;
//   }
//   return word_.Done() && !key_iter_.IsValid();
// }

// void WildCardIterator::Next() {
//   // Note: Currently only supports Prefix search.
//   if (begin_) {
//     if (word_.Done()) {
//       nomatch_ = true;
//       return;
//     }
//     target_posting_ = word_.GetTarget();
//     key_iter_ = target_posting_->GetKeyIterator();
//     begin_ = false;
//   } else if (key_iter_.IsValid()) {
//     key_iter_.NextKey();
//   }
//   while (!word_.Done()) {
//     while (key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
//       key_iter_.NextKey();
//     }
//     // If we found a valid key, stop
//     if (key_iter_.IsValid()) {
//       return;
//     }
//     // Current posting exhausted. Move to next word
//     word_.Next();
//     if (!word_.Done()) {
//       target_posting_ = word_.GetTarget();
//       key_iter_ = target_posting_->GetKeyIterator();
//     }
//   }
// }

// WildCardIterator::WildCardIterator(const WordIterator& word,
//                                    const text::WildCardOperation op,
//                                    const FieldMaskPredicate field_mask,
//                                    const InternedStringSet* untracked_keys)
//     : word_(word),
//       operation_(op),
//       field_mask_(field_mask),
//       untracked_keys_(untracked_keys) {}

// bool WildCardIterator::Done() const {
//   if (nomatch_) {
//     return true;
//   }
//   return word_.Done() && !key_iter_.IsValid();
// }

// void WildCardIterator::Next() {
//   // Note: Currently only supports Prefix search.
//   if (begin_) {
//     if (word_.Done()) {
//       nomatch_ = true;
//       return;
//     }
//     target_posting_ = word_.GetTarget();
//     key_iter_ = target_posting_->GetKeyIterator();
//     begin_ = false;
//   } else if (key_iter_.IsValid()) {
//     key_iter_.NextKey();
//   }
//   while (!word_.Done()) {
//     while (key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
//       key_iter_.NextKey();
//     }
//     // If we found a valid key, stop
//     if (key_iter_.IsValid()) {
//       return;
//     }
//     // Current posting exhausted. Move to next word
//     word_.Next();
//     if (!word_.Done()) {
//       target_posting_ = word_.GetTarget();
//       key_iter_ = target_posting_->GetKeyIterator();
//     }
//   }
// }

// const InternedStringPtr& WildCardIterator::operator*() const {
//   // Return the current key from the key iterator of the posting object.
//   return key_iter_.GetKey();
// }

}  // namespace valkey_search::indexes::text
