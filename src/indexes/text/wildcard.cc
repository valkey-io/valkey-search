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
      // pos_iter_(),
      current_key_(nullptr),
      current_position_(std::nullopt),
      untracked_keys_(untracked_keys) {
  VMSDK_LOG(WARNING, nullptr) << "WI::init{" << data_ << "}";
  while (!word_iter_.Done()) {
    key_iterators_.emplace_back(word_iter_.GetTarget()->GetKeyIterator());
    word_iter_.Next();
  }
  if (!key_iterators_.empty()) {
    WildCardIterator::NextKey();
  }
}

uint64_t WildCardIterator::FieldMask() const { return field_mask_; }

bool WildCardIterator::DoneKeys() const {
  for (const auto& key_iter : key_iterators_) {
    if (key_iter.IsValid()) return false;
  }
  return true;
}

const InternedStringPtr& WildCardIterator::CurrentKey() const {
  CHECK(current_key_ != nullptr);
  VMSDK_LOG(WARNING, nullptr) << "WI::CurrentKey{" << data_ << "}. Key: " <<  current_key_->Str();
  return current_key_;
}

bool WildCardIterator::NextKey() {
  VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << data_ << "}";
  if (current_key_) {
    for (auto& key_iter : key_iterators_) {
      VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << data_ << "}. Move to next key. Early Key";
      if (key_iter.IsValid() && key_iter.GetKey()->Str() == current_key_->Str()) {
        key_iter.NextKey();
      }
    }
  }
  current_key_ = nullptr;
  current_position_ = std::nullopt;
  for (auto& key_iter : key_iterators_) {
    while (key_iter.IsValid() && !key_iter.ContainsFields(field_mask_)) {
      VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << data_ << "}. Invalid field. Key: "
                                 << key_iter.GetKey()->Str();
      key_iter.NextKey();
    }
    if (key_iter.IsValid()) {
      auto key = key_iter.GetKey();
      if (!current_key_ || key->Str() < current_key_->Str()) {
        pos_iterators_.clear();
        pos_iterators_.emplace_back(key_iter.GetPositionIterator());
        current_key_ = key;
        VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << data_ << "} - Found better key. CurrentKey: " << current_key_->Str();
      } else if (key->Str() == current_key_->Str()) {
        pos_iterators_.emplace_back(key_iter.GetPositionIterator());
        VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << data_ << "} - Found same key. CurrentKey: " << current_key_->Str();
      }
    }
  }
  if (!current_key_) {
    VMSDK_LOG(WARNING, nullptr) << "WI::NextKey{" << data_ << "} - No more keys.";
    return false;
  }
  NextPosition();
  return true;
}

// bool WildCardIterator::NextKey() {
//   if (current_key_) {
//     for (auto& key_iter : key_iterators_) {
//       if (key_iter.GetKey()->Str() == current_key_->Str()) {
//         key_iter.NextKey();
//       }
//     }
//   }
//   current_key_ = nullptr;
//   InternedStringPtr min_key = nullptr;
//   for (size_t i = 0; i < key_iterators_.size(); ++i) {
//     auto& key_iter = key_iterators_[i];
//     while (key_iter.IsValid() && !key_iter.ContainsFields(field_mask_)) {
//       key_iter.NextKey();
//     }
//     if (key_iter.IsValid()) {
//       auto key = key_iter.GetKey();
//       if (!min_key || key->Str() < min_key->Str()) {
//         // current_word_index_ = i;
//         current_key_ = key;
//         pos_iter_ = key_iter.GetPositionIterator();
//         current_position_ = std::nullopt;
//         if (NextPosition()) {
//           min_key = key;
//           min_index = i;
//         }
//       }
//     }
//   }
//   if (current_key_ == nullptr) {
//     return false;
//   }
//   return true;
// }

bool WildCardIterator::SeekForwardKey(const InternedStringPtr& target_key) {
  VMSDK_LOG(WARNING, nullptr) << "WI::SeekForwardKey{" << data_ << "} - TargetKey: " << target_key->Str();
  if (current_key_ && current_key_->Str() >= target_key->Str()) {
    VMSDK_LOG(WARNING, nullptr) << "WI::SeekForwardKey{" << data_ << "} - Already at or past target key. CurrentKey: " << current_key_->Str();
    return true;
  }
  // Use SkipForwardKey to efficiently seek all iterators to target_key or beyond
  for (auto& key_iter : key_iterators_) {
    key_iter.SkipForwardKey(target_key);
  }
  // Now find the minimum key using the same logic as NextKey
  current_key_ = nullptr;
  current_position_ = std::nullopt;
  for (auto& key_iter : key_iterators_) {
    // Should understand better what `key_iter.ContainsFields(field_mask_)` means....
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
    VMSDK_LOG(WARNING, nullptr) << "WI::SeekForwardKey{" << data_ << "} - No more keys.";
    return false;
  }
  // IMO, it can be replaced with just `NextPosition();`
  if (!NextPosition()) {
    current_key_ = nullptr;
    return false;
  }
  VMSDK_LOG(WARNING, nullptr) << "WI::SeekForwardKey{" << data_ << "} - Done Seeking. CurrentKey: " << current_key_->Str();
  return true;
}

bool WildCardIterator::DonePositions() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::DonePositions{" << data_ << "}";
  for (const auto& pos_iter : pos_iterators_) {
    if (pos_iter.IsValid()) return false;
  }
  return true;
}

std::pair<uint32_t, uint32_t> WildCardIterator::CurrentPosition() const {
  VMSDK_LOG(WARNING, nullptr) << "WI::CurrentPosition{" << data_ << "}";
  CHECK(current_position_.has_value());
  return std::make_pair(current_position_.value(), current_position_.value());
}

bool WildCardIterator::NextPosition() {
  VMSDK_LOG(WARNING, nullptr) << "WI::NextPosition{" << data_ << "}";
  if (current_position_.has_value()) {
    for (auto& pos_iter : pos_iterators_) {
      if (pos_iter.IsValid() && pos_iter.GetPosition() == current_position_.value()) {
        VMSDK_LOG(WARNING, nullptr) << "WI::NextPosition{" << data_
                            << "} Moving to next. Cur Position: " << current_position_.value();
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
  VMSDK_LOG(WARNING, nullptr) << "WI::NextPosition{" << data_ << "}. Valid pos";
  current_position_ = min_position;
  return true;
}

}  // namespace valkey_search::indexes::text


// /*
//  * Copyright (c) 2025, valkey-search contributors
//  * All rights reserved.
//  * SPDX-License-Identifier: BSD 3-Clause
//  *
//  */

// #include "src/indexes/text/wildcard.h"

// namespace valkey_search::indexes::text {

// WildCardIterator::WildCardIterator(const WordIterator& word_iter,
//                                    const text::WildCardOperation operation,
//                                    const absl::string_view data,
//                                    const uint32_t field_mask,
//                                    const InternedStringSet* untracked_keys)
//     : operation_(operation),
//       data_(data),
//       field_mask_(field_mask),
//       word_iter_(word_iter),
//       target_posting_(nullptr),
//       key_iter_(),
//       pos_iter_(),
//       current_key_(nullptr),
//       current_position_(std::nullopt),
//       untracked_keys_(untracked_keys) {
//   if (word_iter_.Done()) {
//     return;
//   }
//   target_posting_ = word_iter_.GetTarget();
//   key_iter_ = target_posting_->GetKeyIterator();
//   // Prime the first key and position if they exist.
//   WildCardIterator::NextKey();
// }

// uint64_t WildCardIterator::FieldMask() const { return field_mask_; }

// bool WildCardIterator::DoneKeys() const {
//   if (word_iter_.Done()) {
//     return true;
//   }
//   return !key_iter_.IsValid();
// }

// const InternedStringPtr& WildCardIterator::CurrentKey() const {
//   CHECK(current_key_ != nullptr);
//   return current_key_;
// }

// bool WildCardIterator::NextKey() {
//   if (current_key_) {
//     key_iter_.NextKey();
//   }
//   // Loop until we find a key that satisfies the field mask
//   while (!word_iter_.Done()) {
//     while (key_iter_.IsValid()) {
//       if (key_iter_.ContainsFields(field_mask_)) {
//         current_key_ = key_iter_.GetKey();
//         pos_iter_ = key_iter_.GetPositionIterator();
//         current_position_ = std::nullopt;
//         if (WildCardIterator::NextPosition()) {
//           return true;
//         }
//       }
//       key_iter_.NextKey();
//     }
//     // Current posting exhausted. Move to next word
//     word_iter_.Next();
//     if (!word_iter_.Done()) {
//       target_posting_ = word_iter_.GetTarget();
//       key_iter_ = target_posting_->GetKeyIterator();
//     }
//   }
//   // No more valid keys
//   current_key_ = nullptr;
//   return false;
// }

// bool WildCardIterator::SeekForwardKey(const InternedStringPtr& target_key) {
//   // If current key is already >= target_key, no need to seek
//   if (current_key_ && current_key_->Str() >= target_key->Str()) {
//     return true;
//   }
//   // Seek through words and keys to find target_key or beyond
//   // We optimize by not checking position / field constraints
//   // until we find the right key.
//   while (!word_iter_.Done()) {
//     // Seek key iterator to target_key or beyond
//     while (key_iter_.IsValid() &&
//            key_iter_.GetKey()->Str() < target_key->Str()) {
//       key_iter_.NextKey();
//     }
//     // Find next valid key/position combination using existing logic
//     while (key_iter_.IsValid()) {
//       if (key_iter_.ContainsFields(field_mask_)) {
//         current_key_ = key_iter_.GetKey();
//         pos_iter_ = key_iter_.GetPositionIterator();
//         current_position_ = std::nullopt;
//         if (NextPosition()) {
//           return true;
//         }
//       }
//       key_iter_.NextKey();
//     }
//     // Current posting exhausted. Move to next word
//     word_iter_.Next();
//     if (!word_iter_.Done()) {
//       target_posting_ = word_iter_.GetTarget();
//       key_iter_ = target_posting_->GetKeyIterator();
//     }
//   }
//   current_key_ = nullptr;
//   return false;
// }

// bool WildCardIterator::DonePositions() const { return !pos_iter_.IsValid(); }

// std::pair<uint32_t, uint32_t> WildCardIterator::CurrentPosition() const {
//   CHECK(current_position_.has_value());
//   return std::make_pair(current_position_.value(), current_position_.value());
// }

// bool WildCardIterator::NextPosition() {
//   if (current_position_.has_value()) {
//     pos_iter_.NextPosition();
//   }
//   // Loop until we find a position that satisfies the field mask
//   while (pos_iter_.IsValid()) {
//     if (pos_iter_.GetFieldMask() & field_mask_) {
//       current_position_ = pos_iter_.GetPosition();
//       return true;
//     }
//     pos_iter_.NextPosition();
//   }
//   // No more valid positions
//   current_position_ = std::nullopt;
//   return false;
// }

// }  // namespace valkey_search::indexes::text
