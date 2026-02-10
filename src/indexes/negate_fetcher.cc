/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/negate_fetcher.h"

#include "src/index_schema.h"

namespace valkey_search::indexes {

NegateEntriesFetcher::NegateEntriesFetcher(
    std::queue<std::unique_ptr<EntriesFetcherBase>> inner_fetchers,
    const IndexSchema* index_schema)
    : index_schema_(index_schema) {
  // Materialize all matched keys from inner fetchers
  while (!inner_fetchers.empty()) {
    auto fetcher = std::move(inner_fetchers.front());
    inner_fetchers.pop();
    
    auto iter = fetcher->Begin();
    while (!iter->Done()) {
      const auto& key = **iter;
      matched_keys_.insert(key->Str().data());
      iter->Next();
    }
  }
  
  // Size = total keys - matched keys
  size_t total_keys = index_schema_->GetIndexKeyInfoSize();
  size_ = total_keys > matched_keys_.size() 
      ? total_keys - matched_keys_.size() 
      : 0;
}

std::unique_ptr<EntriesFetcherIteratorBase> NegateEntriesFetcher::Begin() {
  return std::make_unique<Iterator>(matched_keys_, index_schema_);
}

NegateEntriesFetcher::Iterator::Iterator(
    const absl::flat_hash_set<const char*>& matched_keys,
    const IndexSchema* index_schema)
    : matched_keys_(matched_keys),
      index_schema_(index_schema),
      done_(false) {
  // Get iterator to index_key_info_
  auto& index_key_info = index_schema_->GetIndexKeyInfo();
  db_key_iter_ = new decltype(index_key_info.begin())(index_key_info.begin());
  
  // Advance to first unmatched key
  AdvanceToNextUnmatched();
}

bool NegateEntriesFetcher::Iterator::Done() const {
  return done_;
}

void NegateEntriesFetcher::Iterator::Next() {
  if (done_) return;
  
  auto& iter = *static_cast<
      decltype(index_schema_->GetIndexKeyInfo().begin())*>(db_key_iter_);
  ++iter;
  AdvanceToNextUnmatched();
}

const InternedStringPtr& NegateEntriesFetcher::Iterator::operator*() const {
  auto& iter = *static_cast<
      decltype(index_schema_->GetIndexKeyInfo().begin())*>(db_key_iter_);
  return iter->first;
}

void NegateEntriesFetcher::Iterator::AdvanceToNextUnmatched() {
  auto& index_key_info = index_schema_->GetIndexKeyInfo();
  auto& iter = *static_cast<
      decltype(index_key_info.begin())*>(db_key_iter_);
  
  while (iter != index_key_info.end()) {
    const auto& key = iter->first;
    // Skip if key is in matched set
    if (matched_keys_.contains(key->Str().data())) {
      ++iter;
      continue;
    }
    // Found unmatched key
    return;
  }
  
  // Reached end
  done_ = true;
  delete static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
  db_key_iter_ = nullptr;
}

}  // namespace valkey_search::indexes
