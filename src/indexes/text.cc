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
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/negation_iterator.h"

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
  auto result = text_index_schema_->StageAttributeData(
      key, data, text_field_number_, !no_stem_, min_stem_size_,
      with_suffix_trie_);

  if (!result.ok()) {
    return result;
  }

  absl::MutexLock lock(&index_mutex_);
  if (result.value()) {
    // Has indexable content
    tracked_keys_.insert(key);
    untracked_keys_.erase(key);
  } else {
    // No indexable content
    untracked_keys_.insert(key);
  }

  return result;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr& key,
                                        DeletionType deletion_type) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey(), so there is no need to touch the index
  // structures here

  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    // If key is DELETED, remove it from untracked_keys_.
    untracked_keys_.erase(key);
  } else {
    // If key doesn't have text but exists, insert it to untracked_keys_.
    untracked_keys_.insert(key);
  }

  // Check if key was tracked before erasing
  bool was_tracked = tracked_keys_.contains(key);
  tracked_keys_.erase(key);

  return was_tracked;
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr& key,
                                        absl::string_view data) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey() at this point, so we simply add the new key
  // data
  auto result = text_index_schema_->StageAttributeData(
      key, data, text_field_number_, !no_stem_, min_stem_size_,
      with_suffix_trie_);

  if (!result.ok()) {
    return result;
  }

  absl::MutexLock lock(&index_mutex_);
  if (result.value()) {
    // Has indexable content
    tracked_keys_.insert(key);
    untracked_keys_.erase(key);
  } else {
    // No indexable content
    untracked_keys_.insert(key);
    tracked_keys_.erase(key);
  }

  return result;
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
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

size_t Text::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
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
  if (is_negated_phrase_) {
    absl::InlinedVector<std::unique_ptr<text::TextIterator>,
                        text::kProximityTermsInlineCapacity>
        child_iterators;
    child_iterators.reserve(phrase_children_.size());

    for (auto& child_fetcher : phrase_children_) {
      auto* text_fetcher = dynamic_cast<EntriesFetcher*>(child_fetcher.get());
      CHECK(text_fetcher);
      child_iterators.push_back(
          text_fetcher->predicate_->BuildTextIterator(text_fetcher, true));
    }

    auto proximity_iter = std::make_unique<text::ProximityIterator>(
        std::move(child_iterators), phrase_slop_, phrase_inorder_, field_mask_,
        untracked_keys_);

    auto negated_iter = std::make_unique<text::NegationTextIterator>(
        std::move(proximity_iter), tracked_keys_, untracked_keys_, field_mask_);

    return std::make_unique<text::TextFetcher>(std::move(negated_iter));
  }

  auto iter = predicate_->BuildTextIterator(this, false);

  if (negate_ && tracked_keys_) {
    iter = std::make_unique<text::NegationTextIterator>(
        std::move(iter), tracked_keys_, untracked_keys_, field_mask_);
  }

  return std::make_unique<text::TextFetcher>(std::move(iter));
}

}  // namespace valkey_search::indexes

// Implement the TextPredicate BuildTextIterator virtual method
namespace valkey_search::query {

void* TextPredicate::Search(bool negate,
                            const IndexSchema* index_schema) const {
  size_t estimated_size = EstimateSize();

  // Get aggregated tracked/untracked keys for negation support
  const InternedStringSet* tracked_keys_ptr = nullptr;
  const InternedStringSet* untracked_keys_ptr = nullptr;
  std::unique_ptr<InternedStringSet> aggregated_tracked;
  std::unique_ptr<InternedStringSet> aggregated_untracked;

  if (negate && index_schema) {
    aggregated_tracked =
        std::make_unique<InternedStringSet>(index_schema->GetKeysByFieldMask(
            GetFieldMask(), IndexSchema::KeySetType::kTracked));
    tracked_keys_ptr = aggregated_tracked.get();

    aggregated_untracked =
        std::make_unique<InternedStringSet>(index_schema->GetKeysByFieldMask(
            GetFieldMask(), IndexSchema::KeySetType::kUntracked));
    untracked_keys_ptr = aggregated_untracked.get();

    // For negation, size = (tracked_keys - matched_keys) + untracked_keys
    estimated_size = aggregated_tracked->size() + aggregated_untracked->size();
  }

  auto fetcher = std::make_unique<indexes::Text::EntriesFetcher>(
      estimated_size, GetTextIndexSchema()->GetTextIndex(), tracked_keys_ptr,
      untracked_keys_ptr, GetFieldMask(), negate);
  fetcher->predicate_ = this;

  // Transfer ownership of aggregated keys to keep them alive
  if (aggregated_tracked) {
    fetcher->owned_tracked_keys_ = std::move(aggregated_tracked);
  }
  if (aggregated_untracked) {
    fetcher->owned_untracked_keys_ = std::move(aggregated_untracked);
  }

  return fetcher.release();
}

std::unique_ptr<indexes::text::TextIterator> TermPredicate::BuildTextIterator(
    const void* fetcher_ptr, bool require_positions) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->GetPrefix().GetWordIterator(GetTextString());
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  while (!word_iter.Done()) {
    if (word_iter.GetWord() == GetTextString()) {
      key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    }
    word_iter.Next();
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      require_positions);
}

std::unique_ptr<indexes::text::TextIterator> PrefixPredicate::BuildTextIterator(
    const void* fetcher_ptr, bool require_positions) const {
  const auto* fetcher =
      static_cast<const indexes::Text::EntriesFetcher*>(fetcher_ptr);
  auto word_iter =
      fetcher->text_index_->GetPrefix().GetWordIterator(GetTextString());
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  while (!word_iter.Done()) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      require_positions);
}

std::unique_ptr<indexes::text::TextIterator> SuffixPredicate::BuildTextIterator(
    const void* fetcher_ptr, bool require_positions) const {
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
  while (!word_iter.Done()) {
    key_iterators.emplace_back(word_iter.GetTarget()->GetKeyIterator());
    word_iter.Next();
  }
  return std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), fetcher->field_mask_, fetcher->untracked_keys_,
      require_positions);
}

std::unique_ptr<indexes::text::TextIterator> InfixPredicate::BuildTextIterator(
    const void* fetcher_ptr, bool require_positions) const {
  CHECK(false) << "Unsupported TextPredicate type";
}

std::unique_ptr<indexes::text::TextIterator> FuzzyPredicate::BuildTextIterator(
    const void* fetcher_ptr, bool require_positions) const {
  CHECK(false) << "Unsupported TextPredicate type";
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
