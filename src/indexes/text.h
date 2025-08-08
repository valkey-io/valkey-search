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


namespace valkey_search::indexes {

class Text : public IndexBase {
 public:
  explicit Text(const data_model::TextIndex& text_index_proto,
                std::shared_ptr<TextIndexSchema> text_index_schema,
                size_t text_field_number);
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
  // Each text field is assigned a unique number within the containing index, this is used
  // by the Postings object to identify fields.
  size_t text_field_number_;
  
  // Reference to the shared text index schema
  std::shared_ptr<TextIndexSchema> text_index_schema_;
  
  // Map to track which keys are indexed and their raw data
  absl::flat_hash_map<InternedStringPtr, InternedStringPtr> tracked_tags_by_keys_
      ABSL_GUARDED_BY(index_mutex_);
  
  mutable absl::Mutex index_mutex_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_H_
