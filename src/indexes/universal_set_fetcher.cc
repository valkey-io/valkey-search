/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/universal_set_fetcher.h"

#include "src/index_schema.h"

namespace valkey_search::indexes {

UniversalSetFetcher::UniversalSetFetcher(const IndexSchema* index_schema)
    : index_schema_(index_schema) {
  size_ = index_schema_->GetIndexKeyInfoSize();
}

std::unique_ptr<EntriesFetcherIteratorBase> UniversalSetFetcher::Begin() {
  return std::make_unique<Iterator>(index_schema_);
}

UniversalSetFetcher::Iterator::Iterator(const IndexSchema* index_schema)
    : index_schema_(index_schema), done_(false) {
  auto& index_key_info = index_schema_->GetIndexKeyInfo();
  db_key_iter_ = new decltype(index_key_info.begin())(index_key_info.begin());
  
  // Check if empty
  auto& iter = *static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
  if (iter == index_key_info.end()) {
    done_ = true;
    delete static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
    db_key_iter_ = nullptr;
  }
}

UniversalSetFetcher::Iterator::~Iterator() {
  if (db_key_iter_) {
    auto& index_key_info = index_schema_->GetIndexKeyInfo();
    delete static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
  }
}

bool UniversalSetFetcher::Iterator::Done() const {
  return done_;
}

void UniversalSetFetcher::Iterator::Next() {
  if (done_) return;
  
  auto& index_key_info = index_schema_->GetIndexKeyInfo();
  auto& iter = *static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
  ++iter;
  
  if (iter == index_key_info.end()) {
    done_ = true;
    delete static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
    db_key_iter_ = nullptr;
  }
}

const InternedStringPtr& UniversalSetFetcher::Iterator::operator*() const {
  auto& index_key_info = index_schema_->GetIndexKeyInfo();
  auto& iter = *static_cast<decltype(index_key_info.begin())*>(db_key_iter_);
  return iter->first;
}

}  // namespace valkey_search::indexes
