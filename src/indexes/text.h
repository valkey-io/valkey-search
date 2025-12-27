/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_H_

#include <memory>

namespace valkey_search::indexes::text {
// Inline capacity for word expansion key iterators
constexpr size_t kWordExpansionInlineCapacity = 200;
// Inline capacity for proximity terms
constexpr size_t kProximityTermsInlineCapacity = 64;
}  // namespace valkey_search::indexes::text

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/proximity.h"
#include "src/indexes/text/term.h"
#include "src/indexes/text/text_fetcher.h"
#include "src/indexes/text/text_index.h"
#include "src/query/predicate.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

/**
 * Text per-field index implementation for full-text search functionality.
 */
class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto,
                std::shared_ptr<text::TextIndexSchema> text_index_schema);

  std::shared_ptr<text::TextIndexSchema> GetTextIndexSchema() const {
    return text_index_schema_;
  }
  uint32_t GetMinStemSize() const { return min_stem_size_; }
  bool IsStemmingEnabled() const { return !no_stem_; }
  bool WithSuffixTrie() const { return with_suffix_trie_; }
  absl::StatusOr<bool> AddRecord(const InternedStringPtr& key,
                                 absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key,
      DeletionType deletion_type = DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> ModifyRecord(const InternedStringPtr& key,
                                    absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  int RespondWithInfo(ValkeyModuleCtx* ctx) const override;
  bool IsTracked(const InternedStringPtr& key) const override;
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const override {
    return absl::OkStatus();
  }

  inline absl::Status ForEachTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn)
      const override {
    absl::MutexLock lock(&index_mutex_);
    for (const auto& key : tracked_keys_) {
      VMSDK_RETURN_IF_ERROR(fn(key));
    }
    return absl::OkStatus();
  }

  size_t GetUnTrackedKeyCount() const override {
    absl::MutexLock lock(&index_mutex_);
    return untracked_keys_.size();
  }

  bool IsUnTracked(const InternedStringPtr& key) const override {
    absl::MutexLock lock(&index_mutex_);
    return untracked_keys_.contains(key);
  }

  absl::Status ForEachUnTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn)
      const override {
    absl::MutexLock lock(&index_mutex_);
    for (const auto& key : untracked_keys_) {
      VMSDK_RETURN_IF_ERROR(fn(key));
    }
    return absl::OkStatus();
  }

  size_t GetTrackedKeyCount() const override;
  std::unique_ptr<data_model::Index> ToProto() const override;

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 public:
  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(size_t size,
                   const std::shared_ptr<text::TextIndex>& text_index,
                   const InternedStringSet* tracked_keys,
                   const InternedStringSet* untracked_keys,
                   text::FieldMaskPredicate field_mask, bool negate)
        : size_(size),
          text_index_(text_index),
          tracked_keys_(tracked_keys),
          untracked_keys_(untracked_keys),
          field_mask_(field_mask),
          negate_(negate) {}

    EntriesFetcher(size_t size,
                   const std::shared_ptr<text::TextIndex>& text_index,
                   const InternedStringSet* tracked_keys,
                   const InternedStringSet* untracked_keys,
                   text::FieldMaskPredicate field_mask,
                   std::vector<std::unique_ptr<EntriesFetcherBase>> children,
                   std::optional<uint32_t> slop, bool inorder)
        : size_(size),
          text_index_(text_index),
          tracked_keys_(tracked_keys),
          untracked_keys_(untracked_keys),
          field_mask_(field_mask),
          negate_(true),
          is_negated_phrase_(true),
          phrase_children_(std::move(children)),
          phrase_slop_(slop),
          phrase_inorder_(inorder) {}

    size_t Size() const override;

    std::unique_ptr<text::TextIterator> BuildTextIterator(
        const query::TextPredicate* predicate, bool require_positions = false);

    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

    size_t size_;
    const InternedStringSet* tracked_keys_;
    const InternedStringSet* untracked_keys_;
    std::shared_ptr<text::TextIndex> text_index_;
    const query::TextPredicate* predicate_;
    text::FieldMaskPredicate field_mask_;
    bool negate_;
    std::unique_ptr<InternedStringSet> owned_tracked_keys_;
    std::unique_ptr<InternedStringSet> owned_untracked_keys_;

    bool is_negated_phrase_{false};
    std::vector<std::unique_ptr<EntriesFetcherBase>> phrase_children_;
    std::optional<uint32_t> phrase_slop_;
    bool phrase_inorder_{true};
  };

  // Calculate size based on the predicate.
  size_t CalculateSize(const query::TextPredicate& predicate) const;

  size_t GetTextFieldNumber() const { return text_field_number_; }

 private:
  // Each text field index within the schema is assigned a unique number, this
  // is used by the Postings object to identify fields.
  size_t text_field_number_;

  // Reference to the shared text index schema
  std::shared_ptr<text::TextIndexSchema> text_index_schema_;

  InternedStringSet tracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  InternedStringSet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);

  bool with_suffix_trie_;
  bool no_stem_;
  uint32_t min_stem_size_;

  // TODO: Map to track which keys are indexed and their raw data

  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
