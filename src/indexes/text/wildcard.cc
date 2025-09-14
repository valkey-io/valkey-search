/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/wildcard.h"

namespace valkey_search::indexes::text {

WildCardIterator::WildCardIterator(const WordIterator& word,
                                   const text::WildCardOperation op,
                                   const FieldMaskPredicate field_mask,
                                   const InternedStringSet* untracked_keys)
    : word_(word),
      operation_(op),
      field_mask_(field_mask),
      untracked_keys_(untracked_keys) {}

bool WildCardIterator::Done() const {
  if (nomatch_) {
    return true;
  }
  return word_.Done() && !key_iter_.IsValid();
}

void WildCardIterator::Next() {
  // Note: Currently only supports Prefix search.
  if (begin_) {
    if (word_.Done()) {
      nomatch_ = true;
      return;
    }
    target_posting_ = word_.GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;
  } else if (key_iter_.IsValid()) {
    key_iter_.NextKey();
  }
  while (!word_.Done()) {
    while (key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
      key_iter_.NextKey();
    }
    // If we found a valid key, stop
    if (key_iter_.IsValid()) {
      return;
    }
    // Current posting exhausted. Move to next word
    word_.Next();
    if (!word_.Done()) {
      target_posting_ = word_.GetTarget();
      key_iter_ = target_posting_->GetKeyIterator();
    }
  }
}

const InternedStringPtr& WildCardIterator::operator*() const {
  // Return the current key from the key iterator of the posting object.
  return key_iter_.GetKey();
}

}  // namespace valkey_search::indexes::text
