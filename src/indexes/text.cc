/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"
#include "src/indexes/text_index.h"
#include "src/indexes/text/lexer.h"
#include "vmsdk/src/log.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex& text_index_proto, 
           const data_model::IndexSchema* index_schema_proto)
    : IndexBase(IndexerType::kText),
      text_impl_(std::make_unique<text::TextFieldIndex>(
          text_index_proto,
          index_schema_proto,
          // Pass field identifier if available, otherwise empty string
          (index_schema_proto && !index_schema_proto->name().empty()) 
              ? index_schema_proto->name() : "")) {}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key,
                                     absl::string_view data) {
  absl::MutexLock lock(&index_mutex_);
  // TODO: stub
  return text_impl_->AddRecord(key, data);
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key,
                                        DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  // TODO: stub
  return text_impl_->RemoveRecord(key, deletion_type);
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  absl::MutexLock lock(&index_mutex_);
  // TODO: stub
  return text_impl_->ModifyRecord(key, data);
}

int Text::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  // TODO: stub
  return text_impl_->RespondWithInfo(ctx);
}

bool Text::IsTracked(const InternedStringPtr& key) const {
  // TODO: stub
  return text_impl_->IsTracked(key);
}

uint64_t Text::GetRecordCount() const {
  // TODO: stub
  return text_impl_->GetRecordCount();
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  // TODO: stub
  return text_impl_->ToProto();
}

InternedStringPtr Text::GetRawValue(const InternedStringPtr& key) const {
  // TODO:stub
  static InternedStringPtr empty;
  return empty;
}

// EntriesFetcherIterator implementations
bool Text::EntriesFetcherIterator::Done() const {
  // TODO:stub
  return true;
}

void Text::EntriesFetcherIterator::Next() {
  // TODO:stub
}

const InternedStringPtr& Text::EntriesFetcherIterator::operator*() const {
  // TODO:stub
  static InternedStringPtr empty;
  return empty;
}

// EntriesFetcher implementations
size_t Text::EntriesFetcher::Size() const {
  // TODO:stub
  return 0;
}

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  // TODO:stub
  return std::make_unique<EntriesFetcherIterator>();
}

std::unique_ptr<Text::EntriesFetcher> Text::Search(
    const query::TextPredicate& predicate, bool negate) const {
  // TODO: stub
  return std::make_unique<EntriesFetcher>();
}

} // namespace valkey_search::indexes

namespace valkey_search {
namespace text {

absl::StatusOr<bool> TextFieldIndex::AddRecord(const InternedStringPtr& key,
                                               absl::string_view data) {
  auto lexer_result = lexer_.ProcessString(data);
  if (!lexer_result.ok()) {
    // Use debug logging for non-parseable data (very rare, not actionable)
    // Increased interval to once per minute to prevent log spam
    VMSDK_LOG_EVERY_N_SEC(DEBUG, nullptr, 60)
        << "Text field analysis failed for key " << key->Str() << ": " 
        << lexer_result.status().message();
    return lexer_result.status();
  }
  
  // Get the processed terms
  const auto& terms = lexer_result->GetProcessedTerms();
  
  // Use info-level logging for normal operation, with rate limiting
  // TODO: This logging is for testing purpose, will be removed or updated to
  // DEBUG level as we have other change below working as anticipated
  VMSDK_LOG_EVERY_N_SEC(NOTICE, nullptr, 1) 
      << "Text field [" 
      << (field_identifier_.empty() ? "unknown" : field_identifier_)
      // Access field configuration flags safely
      << (text_index_proto_.nostem() ? " NOSTEM" : "")
      << (text_index_proto_.suffix_tree() ? " SUFFIX" : "")
      // Include language information if index_schema_proto_ is available and not using NOSTEM
      << (!text_index_proto_.nostem() && index_schema_proto_ && index_schema_proto_->language() != data_model::LANGUAGE_UNSPECIFIED ? 
            (" LANG:" + data_model::Language_Name(index_schema_proto_->language())) : "")
      << "] processed document '" << key->Str() 
      << "' into " << terms.size() << " terms";
  
  // TODO: Once RadixTree and Postings are implemented:
  // 1. Store terms in prefix tree (text_->prefix_) with appropriate locking
  // 2. Store terms in suffix tree if text_index_proto_.suffix_tree() is enabled with appropriate locking
  // 3. Update postings for each term to track key and positions (if with_offsets_ is true)
  // Note: Updates to prefix/suffix tree data structures must be coordinated
  // with their respective locking mechanisms to ensure thread safety
  
  // For now, return success regardless of word count
  // Empty documents and documents with no indexable words are valid cases
  // This is consistent with other index types that can have empty records
  return true;
}

}  // namespace text
}  // namespace valkey_search
