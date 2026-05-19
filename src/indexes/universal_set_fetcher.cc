/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/universal_set_fetcher.h"

#include <algorithm>

#include "src/index_schema.h"

namespace valkey_search::indexes {

UniversalSetFetcher::UniversalSetFetcher(const IndexSchema* index_schema)
    : index_schema_(index_schema), size_(index_schema->GetIndexKeyInfoSize()) {}

std::unique_ptr<EntriesFetcherIteratorBase> UniversalSetFetcher::Begin() {
  return std::make_unique<Iterator>(index_schema_);
}

// --- Iterator Implementation ---

UniversalSetFetcher::Iterator::Iterator(const IndexSchema* index_schema) {
  const auto& index_key_info = index_schema->GetIndexKeyInfo();
  sorted_keys_.reserve(index_key_info.size());
  for (const auto& [key, _] : index_key_info) {
    sorted_keys_.push_back(key);
  }
  std::sort(sorted_keys_.begin(), sorted_keys_.end());
  current_idx_ = 0;
}

bool UniversalSetFetcher::Iterator::Done() const {
  return current_idx_ >= sorted_keys_.size();
}

void UniversalSetFetcher::Iterator::Next() {
  if (current_idx_ < sorted_keys_.size()) {
    ++current_idx_;
  }
}

const InternedStringPtr& UniversalSetFetcher::Iterator::operator*() const {
  return sorted_keys_[current_idx_];
}

bool UniversalSetFetcher::Iterator::SeekForwardKey(
    const InternedStringPtr& target) {
  while (current_idx_ < sorted_keys_.size() &&
         sorted_keys_[current_idx_] < target) {
    ++current_idx_;
  }
  return current_idx_ < sorted_keys_.size();
}

}  // namespace valkey_search::indexes
