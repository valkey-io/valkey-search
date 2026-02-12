/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/negation_iterator.h"

#include <algorithm>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"

namespace valkey_search::indexes::text {

NegationIterator::NegationIterator(
    std::unique_ptr<TextIterator> wrapped_iter,
    std::shared_ptr<TextIndexSchema> text_index_schema,
    uint64_t query_field_mask)
    : wrapped_iter_(std::move(wrapped_iter)),
      text_index_schema_(text_index_schema),
      query_field_mask_(query_field_mask) {
  BuildResults();
}

void NegationIterator::BuildResults() {
  // Step 1: Collect matched keys from wrapped iterator
  absl::flat_hash_set<Key> matched_keys;
  while (!wrapped_iter_->DoneKeys()) {
    matched_keys.insert(wrapped_iter_->CurrentKey());
    wrapped_iter_->NextKey();
  }

  // Step 2: Get tracked keys and compute (tracked - matched)
  text_index_schema_->WithPerKeyTextIndexes([&](const auto& per_key_indexes) {
    for (const auto& [key, _] : per_key_indexes) {
      if (matched_keys.find(key) == matched_keys.end()) {
        result_keys_.push_back(key);
      }
    }
  });

  // Step 3: Add all untracked keys
  const auto& untracked = text_index_schema_->GetSchemaUntrackedKeys();
  for (const auto& key : untracked) {
    result_keys_.push_back(key);
  }

  // Step 4: Sort for deterministic ordering
  std::sort(result_keys_.begin(), result_keys_.end(),
            [](const Key& a, const Key& b) {
              return a->Str() < b->Str();
            });
}

//Key-level iteration

FieldMaskPredicate NegationIterator::QueryFieldMask() const {
  return query_field_mask_;
}

bool NegationIterator::DoneKeys() const {
  return current_offset_ >= result_keys_.size();
}

const Key& NegationIterator::CurrentKey() const {
  CHECK(!DoneKeys()) << "CurrentKey() called on exhausted NegationIterator";
  return result_keys_[current_offset_];
}

bool NegationIterator::NextKey() {
  if (DoneKeys()) {
    return false;
  }
  ++current_offset_;
  return !DoneKeys();
}

bool NegationIterator::SeekForwardKey(const Key& target_key) {
  if (DoneKeys()) {
    return false;
  }

  // Binary search for first key >= target_key
  auto it = std::lower_bound(
      result_keys_.begin() + current_offset_, result_keys_.end(), target_key,
      [](const Key& a, const Key& b) { return a->Str() < b->Str(); });

  if (it == result_keys_.end()) {
    current_offset_ = result_keys_.size();  // Mark as done
    return false;
  }

  current_offset_ = std::distance(result_keys_.begin(), it);
  return true;
}

// position-level iteration (stubs - document-level only)

bool NegationIterator::DonePositions() const {
  return true;  // Always done - negation has no positions
}

const PositionRange& NegationIterator::CurrentPosition() const {
  return dummy_range_;  // Dummy value, shouldn't be called
}

bool NegationIterator::NextPosition() {
  return false;  // Can't advance, no positions
}

bool NegationIterator::SeekForwardPosition(Position target_position) {
  return false;  // Can't seek, no positions
}

FieldMaskPredicate NegationIterator::CurrentFieldMask() const {
  return query_field_mask_;  // Return query field mask
}

bool NegationIterator::IsIteratorValid() const {
  return !DoneKeys();  // Valid if we have a current key
}

}  // namespace valkey_search::indexes::text
