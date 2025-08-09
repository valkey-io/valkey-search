/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_H_

#include <memory>
#include <optional>
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text/text_index.h"
#include "src/index_schema.pb.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "src/indexes/text/phrase.h"
#include "src/query/predicate.h" 

namespace valkey_search::indexes {

using WordIterator = text::RadixTree<std::shared_ptr<text::Postings>, false>::WordIterator;

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto,
                std::shared_ptr<valkey_search::indexes::text::TextIndexSchema> text_index_schema);
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

  inline void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const override {
    absl::MutexLock lock(&index_mutex_);
    // TODO: Implement proper key tracking
  }
  uint64_t GetRecordCount() const override;
  std::unique_ptr<data_model::Index> ToProto() const override;

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 public:
  // Abstract for Text. Every text operation will have a specific implementation.
  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
  };

  // Common EntriesFetcher impl for all Text operations.
  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(size_t size,
                const std::shared_ptr<text::TextIndex>& text_index,
                const InternedStringSet* untracked_keys = nullptr)
        : size_(size),
          text_index_(text_index),
          untracked_keys_(untracked_keys) {}

    size_t Size() const override;

    // Factory method that creates the appropriate iterator
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

    size_t size_;
    const InternedStringSet* untracked_keys_;
    std::shared_ptr<text::TextIndex> text_index_;
    query::TextPredicate::Operation operation_;
    absl::string_view data_;
    bool no_field_{false};
  };

  // Calculate size based on the predicate.
  size_t CalculateSize(const query::TextPredicate& predicate) const;

  virtual std::unique_ptr<EntriesFetcher> Search(const query::TextPredicate& predicate,
                                                 bool negate) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  // Each text field index within the schema is assigned a unique number, this is used
  // by the Postings object to identify fields.
  size_t text_field_number_;
  
  // Reference to the shared text index schema
  std::shared_ptr<text::TextIndexSchema> text_index_schema_;
  InternedStringSet untracked_keys_;
  // TextIndex proto-derived configuration fields
  bool with_suffix_trie_;
  bool no_stem_;
  int32_t min_stem_size_;
  
  // TODO: Map to track which keys are indexed and their raw data

  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
