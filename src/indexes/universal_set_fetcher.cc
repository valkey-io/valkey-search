/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/universal_set_fetcher.h"

#include "src/index_schema.h"

namespace valkey_search::indexes {

UniversalSetFetcher::UniversalSetFetcher(const IndexSchema* index_schema)
    : index_schema_(index_schema), size_(index_schema->GetIndexKeyInfoSize()) {}

std::unique_ptr<EntriesFetcherIteratorBase> UniversalSetFetcher::Begin() {
  return std::make_unique<Iterator>(index_schema_);
}

UniversalSetFetcher::Iterator::Iterator(const IndexSchema* index_schema) {
  const auto& sorted_keys = index_schema->GetSortedKeys();
  current_it_ = sorted_keys.begin();
  end_it_ = sorted_keys.end();
}

bool UniversalSetFetcher::Iterator::Done() const {
  return current_it_ == end_it_;
}

void UniversalSetFetcher::Iterator::Next() {
  if (current_it_ != end_it_) ++current_it_;
}

const InternedStringPtr& UniversalSetFetcher::Iterator::operator*() const {
  return *current_it_;
}

bool UniversalSetFetcher::Iterator::SeekForwardKey(
    const InternedStringPtr& target) {
  while (current_it_ != end_it_ && *current_it_ < target) {
    ++current_it_;
  }
  return current_it_ != end_it_;
}

}  // namespace valkey_search::indexes
