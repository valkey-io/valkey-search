/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_NUMERIC_H_
#define VALKEYSEARCH_SRC_INDEXES_NUMERIC_H_
#include <cstddef>
#include <memory>
#include <optional>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/query/predicate.h"
#include "src/rdb_serialization.h"
#include "src/utils/numeric_btree.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

class Numeric : public IndexBase {
 public:
  explicit Numeric(const data_model::NumericIndex& numeric_index_proto);
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
  int RespondWithInfo(ValkeyModuleCtx* ctx) const override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const override {
    return absl::OkStatus();
  }

  size_t GetTrackedKeyCount() const override ABSL_LOCKS_EXCLUDED(index_mutex_);
  size_t GetUnTrackedKeyCount() const override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  bool IsTracked(const InternedStringPtr& key) const override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  bool IsUnTracked(const InternedStringPtr& key) const override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  void UnTrack(const InternedStringPtr& key) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);

  absl::Status ForEachTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn)
      const override ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::Status ForEachUnTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn)
      const override ABSL_LOCKS_EXCLUDED(index_mutex_);

  std::unique_ptr<data_model::Index> ToProto() const override;

  uint32_t GetMutationWeight() const override;

  const double* GetValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  using TreeType = utils::NumericBTree;
  using TreeIterator = TreeType::Iterator;
  using KeySet = BagOfInternedStringPtrs;

  // A half-open range [first, last) over the order-statistic B+-tree.
  struct EntriesRange {
    TreeIterator first;
    TreeIterator last;
  };

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    EntriesFetcherIterator(
        const EntriesRange& entries_range,
        const std::optional<EntriesRange>& additional_entries_range,
        const KeySet* untracked_keys);
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    const EntriesRange& entries_range_;
    TreeIterator entries_iter_;
    const std::optional<EntriesRange>& additional_entries_range_;
    TreeIterator additional_entries_iter_;
    const KeySet* untracked_keys_;
    std::optional<KeySet::const_iterator> untracked_keys_iter_;
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(
        const EntriesRange& entries_range, size_t size,
        std::optional<EntriesRange> additional_entries_range = std::nullopt,
        const KeySet* untracked_keys = nullptr)
        : entries_range_(entries_range),
          size_(size),
          additional_entries_range_(additional_entries_range),
          untracked_keys_(untracked_keys) {}
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
    EntriesRange entries_range_;
    size_t size_{0};
    std::optional<EntriesRange> additional_entries_range_;
    const KeySet* untracked_keys_;
  };

  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::NumericPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  mutable absl::Mutex index_mutex_;
  InternedStringHashMap<double> tracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  // untracked keys is needed to support negate filtering
  KeySet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  std::unique_ptr<TreeType> index_ ABSL_GUARDED_BY(index_mutex_);
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_NUMERIC_H_
