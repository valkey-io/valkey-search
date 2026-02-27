/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"

#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/term.h"
#include "src/valkey_search_options.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex &text_index_proto,
           std::shared_ptr<text::TextIndexSchema> text_index_schema)
    : IndexBase(IndexerType::kText),
      text_index_schema_(text_index_schema),
      text_field_number_(text_index_schema->AllocateTextFieldNumber()),
      with_suffix_trie_(text_index_proto.with_suffix_trie()),
      no_stem_(text_index_proto.no_stem()),
      weight_(text_index_proto.weight()) {
  // The schema level wants to know if suffix search is enabled for at least one
  // attribute to determine how it initializes its data structures.
  if (with_suffix_trie_) {
    text_index_schema_->EnableSuffix();
  }
}

absl::StatusOr<bool> Text::AddRecord(const InternedStringPtr &key,
                                     absl::string_view data) {
  absl::MutexLock lock(&index_mutex_);
  auto result = text_index_schema_->StageAttributeData(
      key, data, text_field_number_, !no_stem_, with_suffix_trie_);
  if (result.ok() && *result) {
    auto [_, succ] = tracked_keys_.insert(key);
    if (!succ) {
      return absl::AlreadyExistsError(
          absl::StrCat("Key `", key->Str(), "` already exists"));
    }
    untracked_keys_.erase(key);
  } else {
    untracked_keys_.insert(key);
  }
  return result;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr &key,
                                        DeletionType deletion_type) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey(), so there is no need to touch the index
  // structures here
  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    untracked_keys_.erase(key);
  } else {
    untracked_keys_.insert(key);
  }
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    return false;
  }
  tracked_keys_.erase(key);
  return true;
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr &key,
                                        absl::string_view data) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey() at this point, so we simply add the new key
  // data
  bool need_remove = false;
  {
    absl::MutexLock lock(&index_mutex_);
    auto it = tracked_keys_.find(key);
    if (it == tracked_keys_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Key `", key->Str(), "` not found"));
    }
    auto result = text_index_schema_->StageAttributeData(
        key, data, text_field_number_, !no_stem_, with_suffix_trie_);
    if (!result.ok() || !*result) {
      need_remove = true;
    }
  }
  if (need_remove) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return false;
  }
  return true;
}

int Text::RespondWithInfo(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "TEXT");
  ValkeyModule_ReplyWithSimpleString(ctx, "WITH_SUFFIX_TRIE");
  ValkeyModule_ReplyWithSimpleString(ctx, with_suffix_trie_ ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "NO_STEM");
  ValkeyModule_ReplyWithSimpleString(ctx, no_stem_ ? "1" : "0");
  ValkeyModule_ReplyWithSimpleString(ctx, "WEIGHT");
  ValkeyModule_ReplyWithSimpleString(ctx,
                                     absl::StrFormat("%g", weight_).data());
  return 8;
}

bool Text::IsTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

size_t Text::GetTrackedKeyCount() const {
  // keep track of number of keys indexed for this attribute
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto *text_index = index_proto->mutable_text_index();
  text_index->set_with_suffix_trie(with_suffix_trie_);
  text_index->set_no_stem(no_stem_);
  text_index->set_weight(weight_);
  return index_proto;
}

// Size is needed for Inline queries (for approximation of qualified entries)
// and for multi sub query operations (with AND/OR). This should be implemented
// as part of either Inline support OR multi sub query search.
size_t Text::EntriesFetcher::Size() const { return size_; }

std::unique_ptr<EntriesFetcherIteratorBase> Text::EntriesFetcher::Begin() {
  auto iter = predicate_->BuildTextIterator(text_index_, field_mask_,
                                            require_positions_);
  return std::make_unique<text::TextFetcher>(std::move(iter));
}

}  // namespace valkey_search::indexes

namespace valkey_search::query {

namespace {

// Helper to search for a word in the text index and add its key iterator
// Returns true if the word was found and added
bool TryAddWordKeyIterator(
    const indexes::text::TextIndex *text_index, absl::string_view word,
    absl::InlinedVector<indexes::text::Postings::KeyIterator,
                        indexes::text::kWordExpansionInlineCapacity>
        &key_iterators) {
  auto word_iter = text_index->GetPrefix().GetWordIterator(word);
  if (!word_iter.Done() && word_iter.GetWord() == word) {
    key_iterators.emplace_back(word_iter.GetPostingsTarget()->GetKeyIterator());
    return true;
  }
  return false;
}

}  // namespace

std::unique_ptr<indexes::text::TextIterator> TermPredicate::BuildTextIterator(
    const std::shared_ptr<indexes::text::TextIndex> &text_index,
    FieldMaskPredicate field_mask, bool require_positions) const {
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  absl::string_view text_string = GetTextString();
  // Search for the original word - may or may not exist in corpus
  bool found_original =
      TryAddWordKeyIterator(text_index.get(), text_string, key_iterators);
  // Get stem variants if not exact term search
  uint64_t stem_field_mask =
      field_mask & GetTextIndexSchema()->GetStemTextFieldMask();
  if (!IsExact() && stem_field_mask != 0) {
    // Collect stem variant words (words that also stem to the same form)
    absl::InlinedVector<absl::string_view,
                        indexes::text::kStemVariantsInlineCapacity>
        stem_variants;
    std::string stemmed = GetTextIndexSchema()->GetAllStemVariants(
        text_string, stem_variants, stem_field_mask, false);
    // Search for the stemmed word itself - may or may not exist in corpus
    if (stemmed != text_string) {
      TryAddWordKeyIterator(text_index.get(), stemmed, key_iterators);
    }
    // Search for stem variants - these should all exist from ingestion
    for (const auto &variant : stem_variants) {
      bool found =
          TryAddWordKeyIterator(text_index.get(), variant, key_iterators);
      CHECK(found) << "Word in stem tree not found in index - ingestion issue";
    }
  }
  // TermIterator will use query_field_mask when has_original is true,
  // and stem_field_mask for stem variants (has_original becomes false after
  // first pass)
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, require_positions, stem_field_mask,
      found_original);
}

std::unique_ptr<indexes::text::TextIterator> PrefixPredicate::BuildTextIterator(
    const std::shared_ptr<indexes::text::TextIndex> &text_index,
    FieldMaskPredicate field_mask, bool require_positions) const {
  auto word_iter = text_index->GetPrefix().GetWordIterator(GetTextString());
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  while (!word_iter.Done() && word_count < max_words) {
    key_iterators.emplace_back(word_iter.GetPostingsTarget()->GetKeyIterator());
    word_iter.Next();
    ++word_count;
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, require_positions);
}

std::unique_ptr<indexes::text::TextIterator> SuffixPredicate::BuildTextIterator(
    const std::shared_ptr<indexes::text::TextIndex> &text_index,
    FieldMaskPredicate field_mask, bool require_positions) const {
  CHECK(text_index->GetSuffix().has_value())
      << "Text index does not have suffix trie enabled.";
  std::string reversed_word(GetTextString().rbegin(), GetTextString().rend());
  auto word_iter =
      text_index->GetSuffix().value().get().GetWordIterator(reversed_word);
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  while (!word_iter.Done() && word_count < max_words) {
    key_iterators.emplace_back(word_iter.GetPostingsTarget()->GetKeyIterator());
    word_iter.Next();
    ++word_count;
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, require_positions);
}

std::unique_ptr<indexes::text::TextIterator> InfixPredicate::BuildTextIterator(
    const std::shared_ptr<indexes::text::TextIndex> &text_index,
    FieldMaskPredicate field_mask, bool require_positions) const {
  CHECK(false) << "Unsupported TextPredicate type";
}

std::unique_ptr<indexes::text::TextIterator> FuzzyPredicate::BuildTextIterator(
    const std::shared_ptr<indexes::text::TextIndex> &text_index,
    FieldMaskPredicate field_mask, bool require_positions) const {
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  auto key_iterators = indexes::text::FuzzySearch::Search(
      text_index->GetPrefix(), GetTextString(), GetDistance(), max_words);
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, require_positions);
}

/* Size APIs for pre-filter or inline filter planning */

size_t TermPredicate::EstimateSize() const {
  auto iter =
      text_index_schema_->GetTextIndex()->GetPrefix().GetWordIterator(term_);
  if (!iter.Done() && iter.GetWord() == term_) {
    return iter.GetPostingsTarget()->GetKeyCount();
  }
  return 0;
}

size_t PrefixPredicate::EstimateSize() const {
  return text_index_schema_->GetTextIndex()->GetPrefix().GetSubtreeItemCount(
      term_);
}

size_t SuffixPredicate::EstimateSize() const {
  auto suffix_tree = text_index_schema_->GetTextIndex()->GetSuffix();
  CHECK(suffix_tree) << "Suffix estimation not supported";
  return text_index_schema_->GetTextIndex()
      ->GetSuffix()
      .value()
      .get()
      .GetSubtreeItemCount(term_);
}

size_t InfixPredicate::EstimateSize() const {
  // TODO: Implement once infix is supported
  // Right now we return the upper bound
  return text_index_schema_->GetTrackedKeyCount();
}

size_t FuzzyPredicate::EstimateSize() const {
  // TODO: Implement proper heuristic
  // Right now we return the upper bound
  return text_index_schema_->GetTrackedKeyCount();
}
}  // namespace valkey_search::query
