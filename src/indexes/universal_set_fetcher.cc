/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/universal_set_fetcher.h"

#include "src/index_schema.h"

namespace valkey_search::indexes {

UniversalSetFetcher::UniversalSetFetcher(const IndexSchema* index_schema) {
  // Snapshot key list under schema_mutex_'s reader lock (ForEachKey acquires
  // it internally). Holding raw map iterators across the read phase would be
  // unsafe — a snapshot is robust and the cost is negligible vs. the
  // downstream query work.
  keys_.reserve(index_schema->GetIndexKeyInfoSize());
  (void)index_schema->ForEachKey(
      [this](const InternedStringPtr& key, const KeyAttrValue& /*kav*/) {
        keys_.push_back(key);
        return absl::OkStatus();
      });
}

std::unique_ptr<EntriesFetcherIteratorBase> UniversalSetFetcher::Begin() {
  return std::make_unique<Iterator>(&keys_);
}

// --- Iterator Implementation ---

UniversalSetFetcher::Iterator::Iterator(
    const std::vector<InternedStringPtr>* keys)
    : keys_(keys) {}

bool UniversalSetFetcher::Iterator::Done() const {
  return i_ >= keys_->size();
}

void UniversalSetFetcher::Iterator::Next() {
  if (i_ < keys_->size()) {
    ++i_;
  }
}

const InternedStringPtr& UniversalSetFetcher::Iterator::operator*() const {
  return (*keys_)[i_];
}

}  // namespace valkey_search::indexes
