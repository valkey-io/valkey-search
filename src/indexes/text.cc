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
      min_stem_size_(text_index_proto.min_stem_size()) {
  // The schema level wants to know if suffix search is enabled for at least one
  // attribute to determine how it initializes its data structures.
  if (with_suffix_trie_) {
    text_index_schema_->EnableSuffix();
  }
}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr& key,
                                     absl::string_view data) {
  // TODO: Key Tracking

  return text_index_schema_->StageAttributeData(key, data, text_field_number_,
                                                !no_stem_, min_stem_size_,
                                                with_suffix_trie_);
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key,
                                        DeletionType deletion_type) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey(), so there is no need to touch the index
  // structures here

  // TODO: key tracking

  return true;
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  // TODO: key tracking

  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey() at this point, so we simply add the new key
  // data
  return text_index_schema_->StageAttributeData(key, data, text_field_number_,
                                                !no_stem_, min_stem_size_,
                                                with_suffix_trie_);
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

size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  auto iter = predicate_->BuildTextIterator(this);
  return std::make_unique<text::TextFetcher>(std::move(iter));
}

}  // namespace valkey_search::indexes

// Implement the TextPredicate BuildTextIterator virtual method
namespace valkey_search::query {

void* TextPredicate::Search(bool negate) const {
  //
  size_t estimated_size = 0;
  auto temp_fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
      0, GetTextIndexSchema()->GetTextIndex(), nullptr, GetFieldMask());
  temp_fetcher->predicate_ = this;

  auto iterator = BuildTextIterator(temp_fetcher.get());
  while (!iterator->DoneKeys()) {
    estimated_size++;
    iterator->NextKey();
  }

  // Log with predicate type detection
  const char* predicate_type = "Unknown";
  std::string predicate_value;

  // Remove this temporary logging
  if (auto* term = dynamic_cast<const TermPredicate*>(this)) {
    predicate_type = "Term";
    predicate_value = std::string(term->GetTextString());
  } else if (auto* prefix = dynamic_cast<const PrefixPredicate*>(this)) {
    predicate_type = "Prefix";
    predicate_value = std::string(prefix->GetTextString());
  } else if (auto* suffix = dynamic_cast<const SuffixPredicate*>(this)) {
    predicate_type = "Suffix";
    predicate_value = std::string(suffix->GetTextString());
  }

  VMSDK_LOG(WARNING, nullptr)
      << "TextPredicate::Search - " << predicate_type << "Predicate"
      << (predicate_value.empty() ? "" : " for '") << predicate_value
      << (predicate_value.empty() ? "" : "'")
      << " has estimated_size: " << estimated_size;

  // For all other predicates (Prefix, Suffix, Proximity, etc.), size remains 0
  auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
      estimated_size, GetTextIndexSchema()->GetTextIndex(), nullptr,
      GetFieldMask());
  fetcher->predicate_ = this;
  return fetcher.release();
}

std::unique_ptr<indexes::text::TextIterator> TermPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->GetPrefix().GetWordIterator(GetTextString());
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  while (!word_iter.Done()) {
    if (word_iter.GetWord() == GetTextString()) {
      key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    }
    word_iter.Next();
  }
  // We do not perform positional checks on the initial background search.
  bool require_positions = false;
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      require_positions);
}

std::unique_ptr<indexes::text::TextIterator> PrefixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->GetPrefix().GetWordIterator(GetTextString());
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  while (!word_iter.Done()) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
  }
  // We do not perform positional checks on the initial background search.
  bool require_positions = false;
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      require_positions);
}

std::unique_ptr<indexes::text::TextIterator> SuffixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  CHECK(fetcher->text_index_->GetSuffix().has_value())
      << "Text index does not have suffix trie enabled.";
  std::string reversed_word(GetTextString().rbegin(), GetTextString().rend());
  auto word_iter =
      fetcher->text_index_->GetSuffix().value().get().GetWordIterator(
          reversed_word);
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  while (!word_iter.Done()) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
  }
  // We do not perform positional checks on the initial background search.
  bool require_positions = false;
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      require_positions);
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
