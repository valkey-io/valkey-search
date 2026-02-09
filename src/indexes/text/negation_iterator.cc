/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/negation_iterator.h"

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "vmsdk/src/log.h"

namespace valkey_search::indexes::text {

NegationIterator::NegationIterator(
    std::unique_ptr<TextIterator> wrapped_iter,
    const InternedStringSet* schema_tracked_keys,
    const InternedStringSet* schema_untracked_keys,
    FieldMaskPredicate query_field_mask)
    : wrapped_iter_(std::move(wrapped_iter)),
      schema_tracked_keys_(schema_tracked_keys),
      schema_untracked_keys_(schema_untracked_keys),
      query_field_mask_(query_field_mask) {
  VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] Constructor: tracked_keys=" 
                              << (schema_tracked_keys_ ? schema_tracked_keys_->size() : 0)
                              << ", untracked_keys=" << (schema_untracked_keys_ ? schema_untracked_keys_->size() : 0)
                              << ", query_field_mask=" << query_field_mask_;
  
  // Log tracked keys
  if (schema_tracked_keys_) {
    std::string tracked_keys_str;
    for (const auto& key : *schema_tracked_keys_) {
      if (!tracked_keys_str.empty()) tracked_keys_str += ", ";
      tracked_keys_str += std::string(key->Str());
    }
    VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] Tracked keys: {" << tracked_keys_str << "}";
  }
  
  // Log untracked keys
  if (schema_untracked_keys_) {
    std::string untracked_keys_str;
    for (const auto& key : *schema_untracked_keys_) {
      if (!untracked_keys_str.empty()) untracked_keys_str += ", ";
      untracked_keys_str += std::string(key->Str());
    }
    VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] Untracked keys: {" << untracked_keys_str << "}";
  }
  
  // Check for overlaps
  // if (schema_tracked_keys_ && schema_untracked_keys_) {
  //   std::string overlap_str;
  //   for (const auto& key : *schema_tracked_keys_) {
  //     if (schema_untracked_keys_->contains(key)) {
  //       if (!overlap_str.empty()) overlap_str += ", ";
  //       overlap_str += std::string(key->Str());
  //     }
  //   }
  //   if (!overlap_str.empty()) {
  //     VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] OVERLAP DETECTED! Keys in BOTH sets: {" << overlap_str << "}";
  //   }
  // }
  
  // Build set of keys that match the wrapped iterator
  // Formula: negation = (tracked_keys - matched_keys) + untracked_keys
  BuildMatchedKeysSet();
  VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] After BuildMatchedKeysSet: matched_keys=" << matched_keys_.size();
  
  // Log matched keys
  std::string matched_keys_str;
  for (const auto& key : matched_keys_) {
    if (!matched_keys_str.empty()) matched_keys_str += ", ";
    matched_keys_str += key;
  }
  VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] Matched keys: {" << matched_keys_str << "}";

  // OLD: Direct iteration over unordered sets (causes intermittent failures)
  // if (schema_tracked_keys_ && !schema_tracked_keys_->empty()) {
  //   tracked_iter_ = schema_tracked_keys_->begin();
  //   AdvanceToNextValidKey();
  // } else if (schema_untracked_keys_ && !schema_untracked_keys_->empty()) {
  //   untracked_iter_ = schema_untracked_keys_->begin();
  //   current_key_ = *untracked_iter_.value();
  // }

  // NEW: Build sorted vector for deterministic, lexically-ordered iteration
  // Required by SeekForwardKey which assumes sorted order for ProximityIterator intersection
  
  // Collect negated keys: (tracked_keys - matched_keys) + untracked_keys
  // Use set to track already-added keys to avoid duplicates
  absl::flat_hash_set<std::string> added_keys;
  
  if (schema_tracked_keys_) {
    for (const auto& key : *schema_tracked_keys_) {
      if (!matched_keys_.contains(std::string(key->Str()))) {
        if (added_keys.insert(std::string(key->Str())).second) {
          sorted_negated_keys_.push_back(key);
        }
      }
    }
  }
  if (schema_untracked_keys_) {
    for (const auto& key : *schema_untracked_keys_) {
      if (added_keys.insert(std::string(key->Str())).second) {
        sorted_negated_keys_.push_back(key);
      }
    }
  }
  
  // Sort keys lexically for correct SeekForwardKey behavior
  std::sort(sorted_negated_keys_.begin(), sorted_negated_keys_.end(),
            [](const Key& a, const Key& b) { return a->Str() < b->Str(); });
  
  // Log sorted result
  std::string sorted_keys_str;
  for (const auto& key : sorted_negated_keys_) {
    if (!sorted_keys_str.empty()) sorted_keys_str += ", ";
    sorted_keys_str += std::string(key->Str());
  }
  VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] Sorted negated keys (" << sorted_negated_keys_.size() << "): {" << sorted_keys_str << "}";
  
  // Initialize iteration at first key
  current_index_ = 0;
  if (!sorted_negated_keys_.empty()) {
    current_key_ = sorted_negated_keys_[0];
  }
}

void NegationIterator::BuildMatchedKeysSet() {
  if (!wrapped_iter_) {
    return;
  }

  // Collect all keys that match the wrapped iterator
  // These are the keys we need to EXCLUDE from the result
  while (!wrapped_iter_->DoneKeys()) {
    const Key& key = wrapped_iter_->CurrentKey();
    matched_keys_.insert(std::string(key->Str()));
    wrapped_iter_->NextKey();
  }
}

// OLD: AdvanceToNextValidKey - no longer needed with sorted vector approach
// void NegationIterator::AdvanceToNextValidKey() {
//   // Advance through tracked keys, skipping matched ones
//   while (tracked_iter_.has_value() &&
//          tracked_iter_.value() != schema_tracked_keys_->end()) {
//     const Key& key = *tracked_iter_.value();
//     if (!matched_keys_.contains(std::string(key->Str()))) {
//       // Found a non-matching key (part of tracked_keys - matched_keys)
//       current_key_ = key;
//       return;
//     }
//     ++tracked_iter_.value();
//   }
//
//   // Finished tracked keys, move to untracked keys if available
//   if (tracked_iter_.has_value() &&
//       tracked_iter_.value() == schema_tracked_keys_->end()) {
//     if (schema_untracked_keys_ && !schema_untracked_keys_->empty()) {
//       untracked_iter_ = schema_untracked_keys_->begin();
//       current_key_ = *untracked_iter_.value();
//       return;
//     }
//   }
//
//   // No more keys
//   current_key_ = nullptr;
// }

FieldMaskPredicate NegationIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool NegationIterator::DoneKeys() const {
  // OLD: Check unordered set iterators
  // bool tracked_done = !tracked_iter_.has_value() ||
  //                     tracked_iter_.value() == schema_tracked_keys_->end();
  // bool untracked_done = !untracked_iter_.has_value() ||
  //                       untracked_iter_.value() == schema_untracked_keys_->end();
  // return tracked_done && untracked_done;
  
  // NEW: Check sorted vector index
  return current_index_ >= sorted_negated_keys_.size();
}

const Key& NegationIterator::CurrentKey() const {
  CHECK(!DoneKeys()) << "CurrentKey() called on exhausted iterator";
  VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] CurrentKey() returning: " << (current_key_ ? std::string(current_key_->Str()) : "null")
                              << " at index=" << current_index_;
  return current_key_;
}

bool NegationIterator::NextKey() {
  if (DoneKeys()) {
    return false;
  }
  
  // VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] NextKey() called, current_index=" << current_index_
  //                             << ", current_key=" << (current_key_ ? std::string(current_key_->Str()) : "null");

  // OLD: Iterate through unordered sets
  // if (tracked_iter_.has_value() &&
  //     tracked_iter_.value() != schema_tracked_keys_->end()) {
  //   ++tracked_iter_.value();
  //   AdvanceToNextValidKey();
  // } else if (untracked_iter_.has_value() &&
  //            untracked_iter_.value() != schema_untracked_keys_->end()) {
  //   ++untracked_iter_.value();
  //   if (untracked_iter_.value() != schema_untracked_keys_->end()) {
  //     current_key_ = *untracked_iter_.value();
  //   } else {
  //     current_key_ = nullptr;
  //   }
  // }

  // NEW: Advance in sorted vector
  ++current_index_;
  if (current_index_ < sorted_negated_keys_.size()) {
    current_key_ = sorted_negated_keys_[current_index_];
  } else {
    current_key_ = nullptr;
  }
  
  // VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] NextKey() result, new current_index=" << current_index_
  //                             << ", new current_key=" << (current_key_ ? std::string(current_key_->Str()) : "null");

  return !DoneKeys();
}

bool NegationIterator::SeekForwardKey(const Key& target_key) {
  if (DoneKeys()) {
    return false;
  }

  // OLD: Linear scan (works with sorted keys)
  // while (!DoneKeys() && CurrentKey()->Str() < target_key->Str()) {
  //   if (!NextKey()) return false;
  // }
  // return !DoneKeys();

  // NEW: Binary search in sorted vector for better performance
  auto it = std::lower_bound(
      sorted_negated_keys_.begin() + current_index_,
      sorted_negated_keys_.end(),
      target_key,
      [](const Key& a, const Key& b) { return a->Str() < b->Str(); });
  
  if (it != sorted_negated_keys_.end()) {
    current_index_ = std::distance(sorted_negated_keys_.begin(), it);
    current_key_ = sorted_negated_keys_[current_index_];
    // VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] SeekForwardKey to " << std::string(target_key->Str())
    //                             << " found at index " << current_index_
    //                             << ", key=" << std::string(current_key_->Str());
    return true;
  }
  
  // No key >= target_key found
  current_index_ = sorted_negated_keys_.size();
  current_key_ = nullptr;
  // VMSDK_LOG(WARNING, nullptr) << "[NEGATION_ITER] SeekForwardKey to " << std::string(target_key->Str())
  //                             << " not found, exhausted";
  return false;
}

bool NegationIterator::DonePositions() const {
  // Negation only has a single dummy position per key
  return DoneKeys();
}

const PositionRange& NegationIterator::CurrentPosition() const {
  CHECK(!DonePositions()) << "CurrentPosition() called on exhausted iterator";
  return dummy_position_;
}

bool NegationIterator::NextPosition() {
  // Negation only has one position per key, so immediately exhausted
  return false;
}

bool NegationIterator::SeekForwardPosition(Position target_position) {
  // Negation doesn't support position seeking
  return false;
}

FieldMaskPredicate NegationIterator::CurrentFieldMask() const {
  // Return the query field mask for all negated results
  return query_field_mask_;
}

bool NegationIterator::IsIteratorValid() const {
  return !DoneKeys();
}

}  // namespace valkey_search::indexes::text
