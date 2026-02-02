/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"

#include <stdexcept>

#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/lexer.h"
#include "src/valkey_search_options.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex& text_index_proto,
           std::shared_ptr<text::TextIndexSchema> text_index_schema)
    : IndexBase(IndexerType::kText),
      text_index_schema_(text_index_schema),
      text_field_number_(text_index_schema->AllocateTextFieldNumber()),
      with_suffix_trie_(text_index_proto.with_suffix_trie()),
      no_stem_(text_index_proto.no_stem()) {
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
                                                !no_stem_, with_suffix_trie_);
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
                                                !no_stem_, with_suffix_trie_);
}

int Text::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
  ValkeyModule_ReplyWithSimpleString(ctx, "WITH_SUFFIX_TRIE");
  ValkeyModule_ReplyWithSimpleString(ctx, with_suffix_trie_ ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "NO_STEM");
  ValkeyModule_ReplyWithSimpleString(ctx, no_stem_ ? "1" : "0");
  return 6;
}

bool Text::IsTracked(const InternedStringPtr& key) const {
  // TODO
  return false;
}

size_t Text::GetTrackedKeyCount() const {
  // TODO: keep track of number of keys indexed for this attribute
  return 0;
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto* text_index = index_proto->mutable_text_index();
  text_index->set_with_suffix_trie(with_suffix_trie_);
  text_index->set_no_stem(no_stem_);
  return index_proto;
}

// Size is needed for Inline queries (for approximation of qualified entries)
// and for multi sub query operations (with AND/OR). This should be implemented
// as part of either Inline support OR multi sub query search.
size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  auto iter = predicate_->BuildTextIterator(this);
  return std::make_unique<text::TextFetcher>(std::move(iter));
}

}  // namespace valkey_search::indexes

namespace valkey_search::query {

// EntriesFetcher for proximity iterators used in the exact phrase optimization
class ProximityFetcher : public indexes::EntriesFetcherBase {
 public:
  ProximityFetcher(std::unique_ptr<indexes::text::TextIterator> iter,
                   size_t size)
      : iter_(std::move(iter)), size_(size) {}
  size_t Size() const override { return size_; }
  std::unique_ptr<indexes::EntriesFetcherIteratorBase> Begin() override {
    return std::make_unique<indexes::text::TextFetcher>(std::move(iter_));
  }

 private:
  std::unique_ptr<indexes::text::TextIterator> iter_;
  size_t size_;
};

std::unique_ptr<indexes::EntriesFetcherBase> BuildExactPhraseFetcher(
    const ComposedPredicate* composed_predicate) {
  absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                      indexes::text::kProximityTermsInlineCapacity>
      iters;
  FieldMaskPredicate field_mask = ~0ULL;
  size_t min_size = SIZE_MAX;
  for (const auto& child : composed_predicate->GetChildren()) {
    CHECK(child->GetType() == PredicateType::kText);
    auto text_pred = dynamic_cast<const TextPredicate*>(child.get());
    auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
        0, text_pred->GetTextIndexSchema()->GetTextIndex(), nullptr,
        text_pred->GetFieldMask(), true);
    fetcher->predicate_ = text_pred;
    min_size = std::min(min_size, fetcher->Size());
    iters.push_back(text_pred->BuildTextIterator(fetcher.get()));
    field_mask &= text_pred->GetFieldMask();
  }
  auto proximity_iter = std::make_unique<indexes::text::ProximityIterator>(
      std::move(iters), composed_predicate->GetSlop(),
      composed_predicate->GetInorder(), field_mask, nullptr, false);
  return std::make_unique<ProximityFetcher>(std::move(proximity_iter),
                                            min_size);
}

std::unique_ptr<indexes::EntriesFetcherBase> BuildComposedAndFetcher(
    const ComposedPredicate* composed_predicate) {
  absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                      indexes::text::kProximityTermsInlineCapacity>
      iters;
  size_t min_size = SIZE_MAX;
  for (const auto& child : composed_predicate->GetChildren()) {
    CHECK(child->GetType() == PredicateType::kText);
    auto text_pred = dynamic_cast<const TextPredicate*>(child.get());
    auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
        0, text_pred->GetTextIndexSchema()->GetTextIndex(), nullptr,
        text_pred->GetFieldMask(), false);
    fetcher->predicate_ = text_pred;
    min_size = std::min(min_size, fetcher->Size());
    iters.push_back(text_pred->BuildTextIterator(fetcher.get()));
  }
  auto proximity_iter = std::make_unique<indexes::text::ProximityIterator>(
      std::move(iters), std::nullopt, false, ~0ULL, nullptr, true);
  return std::make_unique<ProximityFetcher>(std::move(proximity_iter),
                                            min_size);
}

void* TextPredicate::Search(bool negate) const {
  size_t estimated_size = EstimateSize();
  // We do not perform positional checks on the initial term/prefix/suffix/fuzzy
  // predicate fetchers from the entries fetcher search.
  // This is yet another optimization that can be done in the future to complete
  // the text search during the initial entries fetcher search itself for
  // proximity queries.
  bool require_positions = false;
  auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
      estimated_size, GetTextIndexSchema()->GetTextIndex(), nullptr,
      GetFieldMask(), require_positions);
  fetcher->predicate_ = this;
  return fetcher.release();
}

std::unique_ptr<indexes::text::TextIterator> TermPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;

  // Collect all words to search: original word first
  absl::InlinedVector<absl::string_view,
                      indexes::text::kWordExpansionInlineCapacity>
      words_to_search;
  words_to_search.push_back(GetTextString());

  // Get stem variants if not exact match
  std::string stemmed;
  uint64_t stem_field_mask =
      fetcher->field_mask_ & GetTextIndexSchema()->GetStemTextFieldMask();
  if (!IsExact() && stem_field_mask != 0) {
    stemmed = GetTextIndexSchema()->GetAllStemVariants(
        GetTextString(), words_to_search,
        GetTextIndexSchema()->GetMinStemSize(), stem_field_mask, false);
    if (stemmed != GetTextString()) {
      words_to_search.push_back(stemmed);
    }
  }

  // Search for all words (original word first, then stem variants)
  bool found_original = false;
  for (const auto& word : words_to_search) {
    auto word_iter = fetcher->text_index_->GetPrefix().GetWordIterator(word);
    if (!word_iter.Done() && word_iter.GetWord() == word) {
      key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
      if (word == GetTextString()) {  // First word is the original
        found_original = true;
      }
    }
  }

  // TermIterator will use query_field_mask when has_original is true,
  // and stem_field_mask for stem variants (has_original becomes false after
  // first pass)
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      fetcher->require_positions_, stem_field_mask, found_original);
}

std::unique_ptr<indexes::text::TextIterator> PrefixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->GetPrefix().GetWordIterator(GetTextString());
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  while (!word_iter.Done() && word_count < max_words) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
    ++word_count;
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      fetcher->require_positions_);
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
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  while (!word_iter.Done() && word_count < max_words) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
    ++word_count;
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      fetcher->require_positions_);
}

std::unique_ptr<indexes::text::TextIterator> InfixPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  CHECK(false) << "Unsupported TextPredicate type";
}

std::unique_ptr<indexes::text::TextIterator> FuzzyPredicate::BuildTextIterator(
    const void* fetcher_ptr) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  auto key_iterators = indexes::text::FuzzySearch::Search(
      fetcher->text_index_->GetPrefix(), GetTextString(), GetDistance(),
      max_words);
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      fetcher->require_positions_);
}

// Size apis for estimation
size_t TermPredicate::EstimateSize() const {
  // TODO: Implementation
  return 0;
}

size_t PrefixPredicate::EstimateSize() const {
  // TODO: Implementation
  return 0;
}

size_t SuffixPredicate::EstimateSize() const {
  // TODO: Implementation
  return 0;
}

size_t InfixPredicate::EstimateSize() const {
  // TODO: Implementation
  return 0;
}

size_t FuzzyPredicate::EstimateSize() const {
  // TODO: Implementation
  return 0;
}
}  // namespace valkey_search::query
