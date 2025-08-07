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
  std::shared_ptr<text::TextIndex> text_index_;

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
  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
  };

  // TODO: remove once actual TextPredicate is implemented
  struct TextPredicate {};

  virtual std::unique_ptr<EntriesFetcher> Search(const TextPredicate& predicate,
                                                 bool negate) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
