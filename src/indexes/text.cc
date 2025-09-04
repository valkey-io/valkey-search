/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"

#include <stdexcept>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/posting.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex& text_index_proto,
           std::shared_ptr<text::TextIndexSchema> text_index_schema)
    : IndexBase(IndexerType::kText),
      text_index_schema_(text_index_schema),
      text_field_number_(text_index_schema->AllocateTextFieldNumber()),
      with_suffix_trie_(text_index_proto.with_suffix_trie()),
      no_stem_(text_index_proto.no_stem()),
      min_stem_size_(text_index_proto.min_stem_size()) {}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key,
                                     absl::string_view data) {
  valkey_search::indexes::text::Lexer lexer;

  auto tokens =
      lexer.Tokenize(data, text_index_schema_->GetPunctuationBitmap(),
                     text_index_schema_->GetStemmer(), !no_stem_,
                     min_stem_size_, text_index_schema_->GetStopWordsSet());

  if (!tokens.ok()) {
    if (tokens.status().code() == absl::StatusCode::kInvalidArgument) {
      return false;  // UTF-8 errors → hash_indexing_failures
    }
    return tokens.status();
  }

  for (uint32_t position = 0; position < tokens->size(); ++position) {
    const auto& token = (*tokens)[position];
    text_index_schema_->text_index_->prefix_.Mutate(
        token,
        [&](std::optional<std::shared_ptr<text::Postings>> existing)
            -> std::optional<std::shared_ptr<text::Postings>> {
          std::shared_ptr<text::Postings> postings;
          if (existing.has_value()) {
            postings = existing.value();
          } else {
            // Create new Postings object with schema configuration
            bool save_positions = text_index_schema_->GetWithOffsets();
            uint8_t num_text_fields = text_index_schema_->num_text_fields_;
            postings = std::make_shared<text::Postings>(save_positions,
                                                        num_text_fields);
          }

          postings->InsertPosting(key, text_field_number_, position);
          return postings;
        });
  }

  return true;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key,
                                        DeletionType deletion_type) {
  throw std::runtime_error("Text::RemoveRecord not implemented");
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  throw std::runtime_error("Text::ModifyRecord not implemented");
}

int Text::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
  ValkeyModule_ReplyWithSimpleString(ctx, "WITH_SUFFIX_TRIE");
  ValkeyModule_ReplyWithBool(ctx, with_suffix_trie_);
  
  // Show only one: if no_stem is specified, show no_stem, otherwise show min_stem_size
  if (no_stem_) {
    ValkeyModule_ReplyWithSimpleString(ctx, "NO_STEM");
    ValkeyModule_ReplyWithBool(ctx, no_stem_);
    return 8;
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "MIN_STEM_SIZE");
    ValkeyModule_ReplyWithLongLong(ctx, min_stem_size_);
    return 8;
  }
}

bool Text::IsTracked(const InternedStringPtr& key) const { return false; }

uint64_t Text::GetRecordCount() const {
  // TODO: Implement proper record count tracking when key management is added
  return 0;
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto text_index = std::make_unique<data_model::TextIndex>();
  text_index->set_with_suffix_trie(with_suffix_trie_);
  text_index->set_no_stem(no_stem_);
  text_index->set_min_stem_size(min_stem_size_);
  index_proto->set_allocated_text_index(text_index.release());
  return index_proto;
}

// Size is needed for Inline queries (for approximation of qualified entries)
// and for multi sub query operations (with AND/OR). This should be implemented
// as part of either Inline support OR multi sub query search.
size_t Text::CalculateSize(const query::TextPredicate& predicate) const {
  return 0;
}

std::unique_ptr<Text::EntriesFetcher> Text::Search(
    const query::TextPredicate& predicate, bool negate) const {
  auto fetcher = std::make_unique<EntriesFetcher>(
      CalculateSize(predicate), text_index_schema_->text_index_,
      negate ? &untracked_keys_ : nullptr);
  fetcher->predicate_ = &predicate;
  // TODO : We only support single field queries for now. Change below when we
  // support multiple and all fields.
  fetcher->field_mask_ = 1ULL << text_field_number_;
  return fetcher;
}

size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  if (auto term = dynamic_cast<const query::TermPredicate*>(predicate_)) {
    auto iter = text_index_->prefix_.GetWordIterator(term->GetTextString());
    auto itr = std::make_unique<text::TermIterator>(
        iter, term->GetTextString(), field_mask_, untracked_keys_);
    itr->Next();
    return itr;
  } else if (auto prefix =
                 dynamic_cast<const query::PrefixPredicate*>(predicate_)) {
    auto iter = text_index_->prefix_.GetWordIterator(prefix->GetTextString());
    auto itr = std::make_unique<text::WildCardIterator>(
        iter, text::WildCardOperation::kPrefix, field_mask_, untracked_keys_);
    itr->Next();
    return itr;
  }
  CHECK(false) << "Unsupported TextPredicate operation";
  return nullptr;
}

}  // namespace valkey_search::indexes
