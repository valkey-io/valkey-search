/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text.h"

#include <cstdint>
#include <new>

#include "absl/container/inlined_vector.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/indexes/key_attr_value.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/term.h"
#include "src/valkey_search_options.h"

namespace valkey_search::indexes {

Text::Text(const data_model::TextIndex &text_index_proto,
           std::shared_ptr<text::TextIndexSchema> text_index_schema)
    : TypedIndex<Text, TextSlot>(IndexerType::kText),
      text_field_number_(text_index_schema->AllocateTextFieldNumber()),
      text_index_schema_(text_index_schema),
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
  auto result = text_index_schema_->StageAttributeData(
      key, data, text_field_number_, !no_stem_, with_suffix_trie_);

  absl::MutexLock lock(&index_mutex_);
  if (result.ok() && *result) {
    std::byte *storage = OccupySlot(key, data.size());
    new (storage) TextSlot{
        {/*occupied=*/1u, /*user_data_len=*/static_cast<uint32_t>(data.size())}};
  } else {
    if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
  }
  return result;
}

absl::StatusOr<bool> Text::RemoveRecord(const InternedStringPtr &key,
                                        DeletionType deletion_type) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey() at this point.
  absl::MutexLock lock(&index_mutex_);
  KeyAttrValue *kav = schema_->FindKAV(key);
  if (kav == nullptr) {
    return false;
  }
  Slot &slot = kav->slots[pos_];
  if (IsOccupied(slot)) {
    VacateSlot(key, /*relink=*/deletion_type != DeletionType::kRecord);
    return true;
  }
  if (deletion_type == DeletionType::kRecord) {
    if (schema_->IsLinked(pos_, key)) {
      schema_->UnlinkMissing(pos_, key);
    }
  } else {
    if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
  }
  return false;
}

absl::StatusOr<bool> Text::ModifyRecord(const InternedStringPtr &key,
                                        absl::string_view data) {
  // The old key value has already been removed from the index by a call to
  // TextIndexSchema::DeleteKey() at this point, so we simply add the new key
  // data
  auto result = text_index_schema_->StageAttributeData(
      key, data, text_field_number_, !no_stem_, with_suffix_trie_);

  absl::MutexLock lock(&index_mutex_);
  KeyAttrValue *kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  Slot &slot = kav->slots[pos_];
  if (!result.ok() || !*result) {
    if (IsOccupied(slot)) {
      VacateSlot(key, /*relink=*/true);
    } else if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
    return false;
  }
  if (!IsOccupied(slot)) {
    return absl::NotFoundError(
        absl::StrCat("Key `", key->Str(), "` not found"));
  }
  ResizeSlot(key, data.size());
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
  const KeyAttrValue *kav = schema_->FindKAV(key);
  return kav != nullptr && IsOccupied(kav->slots[pos_]);
}

bool Text::IsUnTracked(const InternedStringPtr &key) const {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue *kav = schema_->FindKAV(key);
  return kav != nullptr && !IsOccupied(kav->slots[pos_]);
}

void Text::UnTrack(const InternedStringPtr &key) {
  absl::MutexLock lock(&index_mutex_);
  const KeyAttrValue *kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  CHECK(!IsOccupied(kav->slots[pos_]));
  if (!schema_->IsLinked(pos_, key)) {
    schema_->LinkMissing(pos_, key);
  }
}

size_t Text::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return schema_->OccupiedCount(pos_);
}

size_t Text::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return schema_->MissingListAt(pos_).size;
}

absl::Status Text::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  return schema_->ForEachKey(
      [&](const InternedStringPtr &key, const KeyAttrValue &kav) {
        if (IsOccupied(kav.slots[pos_])) {
          return fn(key);
        }
        return absl::OkStatus();
      });
}

absl::Status Text::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (auto it = MissingListBegin(schema_, pos_); !it.Done(); it.Next()) {
    VMSDK_RETURN_IF_ERROR(fn(it.Key()));
  }
  return absl::OkStatus();
}

std::unique_ptr<data_model::Index> Text::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto *text_index = index_proto->mutable_text_index();
  text_index->set_with_suffix_trie(with_suffix_trie_);
  text_index->set_no_stem(no_stem_);
  text_index->set_weight(weight_);
  return index_proto;
}

uint32_t Text::GetMutationWeight() const {
  return options::GetMutationWeightText().GetValue();
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
  bool found_original;
  uint64_t stem_field_mask =
      field_mask & GetTextIndexSchema()->GetStemTextFieldMask();

  // Search for the original word - may or may not exist in corpus
  found_original =
      TryAddWordKeyIterator(text_index.get(), text_string, key_iterators);

  // Get stem variants if not exact term search
  if (!IsExact() && stem_field_mask != 0) {
    // Collect stem variant words (words that also stem to the same form)
    absl::InlinedVector<absl::string_view,
                        indexes::text::kStemVariantsInlineCapacity>
        stem_variants;
    std::string stemmed = GetTextIndexSchema()->GetAllStemVariants(
        text_string, stem_variants, stem_field_mask, true);
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

/*
 * Size APIs for pre-filter or inline filter planning.
 *
 * Size estimation is only done at the schema-level right now. It does not
 * account for a field specifier in the text query and may over-estimate
 * because of it if there are multiple text fields in the schema.
 *
 * When a query has a vector component, we perform a more sophisticated size
 * estimation with a tree traversal to help make the correct in-line vs
 * pre-filter query planning decision. Otherwise we simply grab a rough upper
 * bound using the total number of tracked keys since the estimation will only
 * be used to reserve space in a collection.
 */

size_t TermPredicate::EstimateSize(bool is_vec_query) const {
  if (is_vec_query) {
    auto iter =
        text_index_schema_->GetTextIndex()->GetPrefix().GetWordIterator(term_);
    if (!iter.Done() && iter.GetWord() == term_) {
      return iter.GetPostingsTarget()->GetKeyCount();
    }
    return 0;
  } else {
    return text_index_schema_->GetTrackedKeyCount();
  }
}

size_t PrefixPredicate::EstimateSize(bool is_vec_query) const {
  if (is_vec_query) {
    return text_index_schema_->GetTextIndex()->GetPrefix().GetSubtreeItemCount(
        term_);
  } else {
    return text_index_schema_->GetTrackedKeyCount();
  }
}

size_t SuffixPredicate::EstimateSize(bool is_vec_query) const {
  if (is_vec_query) {
    auto suffix_tree = text_index_schema_->GetTextIndex()->GetSuffix();
    CHECK(suffix_tree) << "Suffix estimation not supported";
    return suffix_tree.value().get().GetSubtreeItemCount(term_);
  } else {
    return text_index_schema_->GetTrackedKeyCount();
  }
}

size_t InfixPredicate::EstimateSize(bool is_vec_query) const {
  // TODO: Implement once infix is supported
  // Right now we return the upper bound
  return text_index_schema_->GetTrackedKeyCount();
}

size_t FuzzyPredicate::EstimateSize(bool is_vec_query) const {
  // TODO: Implement proper heuristic
  // Right now we return the upper bound
  return text_index_schema_->GetTrackedKeyCount();
}
}  // namespace valkey_search::query
