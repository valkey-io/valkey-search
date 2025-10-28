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
    text_index_schema_->GetTextIndex()->prefix_.Mutate(
        token,
        [&](std::optional<std::shared_ptr<text::Postings>> existing)
            -> std::optional<std::shared_ptr<text::Postings>> {
          std::shared_ptr<text::Postings> postings;
          if (existing.has_value()) {
            postings = existing.value();
          } else {
            // Create new Postings object with schema configuration
            bool save_positions = text_index_schema_->GetWithOffsets();
            uint8_t num_text_fields = text_index_schema_->GetNumTextFields();
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
  ValkeyModule_ReplyWithSimpleString(ctx, with_suffix_trie_ ? "1" : "0");

  // Show only one: if no_stem is specified, show no_stem, otherwise show
  // min_stem_size
  if (no_stem_) {
    ValkeyModule_ReplyWithSimpleString(ctx, "NO_STEM");
    ValkeyModule_ReplyWithSimpleString(ctx, "1");
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "MIN_STEM_SIZE");
    ValkeyModule_ReplyWithLongLong(ctx, min_stem_size_);
  }
  // Text fields do not include a size field right now (unlike
  // numeric/tag/vector fields)
  return 6;
}

bool Text::IsTracked(const InternedStringPtr& key) const {
  // TODO
  return false;
}

uint64_t Text::GetRecordCount() const {
  // TODO: keep track of number of keys indexed for this attribute
  return 0;
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto* text_index = index_proto->mutable_text_index();
  text_index->set_with_suffix_trie(with_suffix_trie_);
  text_index->set_no_stem(no_stem_);
  text_index->set_min_stem_size(min_stem_size_);
  return index_proto;
}

// Size is needed for Inline queries (for approximation of qualified entries)
// and for multi sub query operations (with AND/OR). This should be implemented
// as part of either Inline support OR multi sub query search.
size_t Text::CalculateSize(const query::TextPredicate& predicate) const {
  return 0;
}

// std::unique_ptr<Text::EntriesFetcher> Text::Search(
//     const query::TextPredicate& predicate, bool negate) const {
//   auto fetcher = std::make_unique<EntriesFetcher>(
//       CalculateSize(predicate), text_index_schema_->GetTextIndex(),
//       negate ? &untracked_keys_ : nullptr);
//   fetcher->predicate_ = &predicate;
//   fetcher->field_mask_ = predicate.GetFieldMask();
//   return fetcher;
// }

size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  auto iter = predicate_->BuildTextIterator(this);
  return std::make_unique<text::TextFetcher>(std::move(iter));
}

}  // namespace valkey_search::indexes

// Implement the TextPredicate BuildTextIterator virtual method
namespace valkey_search::query {

void* TextPredicate::Search(bool negate) const {
  auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
      0, GetTextIndexSchema()->GetTextIndex(),
      nullptr, GetFieldMask());
  fetcher->predicate_ = this;
  return fetcher.release();
}

std::unique_ptr<indexes::text::TextIterator> TermPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->prefix_.GetWordIterator(GetTextString());
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  while (!word_iter.Done()) {
    if (word_iter.GetWord() == GetTextString()) {
      key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    }
    word_iter.Next();
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_);
}

std::unique_ptr<indexes::text::TextIterator> PrefixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->prefix_.GetWordIterator(GetTextString());
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  while (!word_iter.Done()) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_);
}

std::unique_ptr<indexes::text::TextIterator> SuffixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  CHECK(fetcher->text_index_->suffix_.has_value())
      << "Text index does not have suffix trie enabled.";
  std::string reversed_word(GetTextString().rbegin(), GetTextString().rend());
  auto word_iter =
      fetcher->text_index_->suffix_->GetWordIterator(reversed_word);
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  while (!word_iter.Done()) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_);
}

std::unique_ptr<indexes::text::TextIterator>
ProximityPredicate::BuildTextIterator(const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  std::vector<std::unique_ptr<indexes::text::TextIterator>> vec;
  vec.reserve(terms_.size());
  for (const auto& term : terms_) {
    vec.emplace_back(term->BuildTextIterator(fetcher));
  }
  return std::make_unique<indexes::text::ProximityIterator>(
      std::move(vec), slop_, inorder_, fetcher->field_mask_,
      fetcher->untracked_keys_);
}

std::unique_ptr<indexes::text::TextIterator> InfixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  CHECK(false) << "Unsupported TextPredicate type";
}

std::unique_ptr<indexes::text::TextIterator> FuzzyPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  CHECK(false) << "Unsupported TextPredicate type";
}

}  // namespace valkey_search::query