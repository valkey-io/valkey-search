/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_H_

#include "absl/functional/any_invocable.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text/text_index.h"
#include "src/query/predicate.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto,
                std::shared_ptr<text::TextIndex> text_index);
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

 private:
  // Each text field is assigned a unique number within the containing index,
  // this is used by the Postings object to identify fields.
  // size_t text_field_number_; - TODO
  // 
  std::shared_ptr<text::TextIndex> text_index_; // Class 2
  // untracked keys is needed to support negate filtering
  InternedStringSet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);

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
  // Abstract for Text. Every text type will have a specific implementation.
  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
  };
  // TODO: remove once actual TextPredicate is implemented
  // struct TextPredicate {};
  // Common for all Text types.
  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(size_t size,
                const InternedStringSet* untracked_keys = nullptr)
    : size_(size), untracked_keys_(untracked_keys) {}

    size_t Size() const override;

    // Factory method that creates the appropriate iterator
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

    size_t size_;
    const InternedStringSet* untracked_keys_;
    query::TextPredicate::Operation operation_;
    absl::string_view data_;
    bool no_field_{false};
  };

  // Calculate size based on the predicate.
  size_t CalculateSize(const query::TextPredicate& predicate) const;

  // This is needed for the FT.SEARCH command's core search fn.
  virtual std::unique_ptr<EntriesFetcher> Search(const query::TextPredicate& predicate,
                                                 bool negate) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
