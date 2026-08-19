/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TAG_H_
#define VALKEYSEARCH_SRC_INDEXES_TAG_H_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text/rax/rax.h"
#include "src/query/predicate.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

// Tag index backed by an in-tree vs_rax radix tree.
//
// Storage model:
//   - tracked_tags_by_keys_: doc-key → interned raw tag string.
//   - untracked_keys_: doc-keys present in the dataset but without any tag.
//   - tree_ (rax): per-normalized-tag posting list. The rax key bytes are
//     the lowercased tag (or raw bytes when case-sensitive); the rax value
//     slot's 8 bytes ARE the storage of a BagOfInternedStringPtrs holding
//     the doc-keys posted to that tag. The bag picks one of four
//     representations (Single / Array4 / Array8 / Set) by size; storage = 0
//     means the rax key has been erased.
class Tag : public IndexBase {
 public:
  using KeySet = BagOfInternedStringPtrs;

  explicit Tag(const data_model::TagIndex& tag_index_proto);
  ~Tag() override;

  absl::StatusOr<RecordResult> AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<bool> RemoveRecord(
      const InternedStringPtr& key,
      DeletionType deletion_type = DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::StatusOr<RecordResult> ModifyRecord(const InternedStringPtr& key,
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

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Returns the parsed tag set for `key`, or nullopt if `key` is not tracked.
  // The returned set's string_views point into the interned raw tag string
  // held by the tracked entry; valid for the duration of the call under the
  // index's read-side invariant.
  std::optional<absl::flat_hash_set<absl::string_view>> GetValue(
      const InternedStringPtr& key,
      bool& case_sensitive) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Iterator yielded by EntriesFetcher::Begin(). Walks a vector of rax slots
  // (each slot's 8 bytes encode a BagOfInternedStringPtrs); for negated
  // queries, also walks an extras vector of untracked keys.
  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    EntriesFetcherIterator(const std::vector<void*>& slots,
                           const std::vector<InternedStringPtr>& extras);
    ~EntriesFetcherIterator() override;
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    void AdvanceToNextNonEmpty();

    const std::vector<void*>& slots_;
    const std::vector<InternedStringPtr>& extras_;
    size_t slot_idx_{0};
    bool slots_done_{false};
    size_t extras_idx_{0};
    // Adopts the current slot's bag storage; Release()d before moving on so
    // the live storage stays in the rax slot.
    BagOfInternedStringPtrs bag_;
    BagOfInternedStringPtrs::const_iterator bag_it_;
    BagOfInternedStringPtrs::const_iterator bag_end_;
    InternedStringPtr current_;
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(std::vector<void*> matched_slots,
                   std::vector<InternedStringPtr> extras, size_t size)
        : size_(size),
          matched_slots_(std::move(matched_slots)),
          extras_(std::move(extras)) {}
    size_t Size() const override { return size_; }
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
    size_t size_;
    std::vector<void*> matched_slots_;
    std::vector<InternedStringPtr> extras_;
  };

  // Kept virtual so unit tests can mock Search; no production subclass.
  virtual std::unique_ptr<EntriesFetcherBase> Search(
      const query::TagPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

  char GetSeparator() const { return separator_; }
  bool IsCaseSensitive() const { return case_sensitive_; }
  static absl::StatusOr<absl::flat_hash_set<absl::string_view>> ParseSearchTags(
      absl::string_view data, char separator);
  static absl::flat_hash_set<absl::string_view> ParseRecordTags(
      absl::string_view data, char separator);
  // Unescape a tag string (e.g. escaped pipe becomes literal pipe)
  static std::string UnescapeTag(absl::string_view tag);

 private:
  void IndexTagForKey(absl::string_view tag, const InternedStringPtr& key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(index_mutex_);
  void DeindexTagForKey(absl::string_view tag, const InternedStringPtr& key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(index_mutex_);
  // Normalize a tag for storage / lookup: lowercase if !case_sensitive_,
  // pass-through otherwise.
  std::string Normalize(absl::string_view tag) const;

  mutable absl::Mutex index_mutex_;
  struct TagInfo {
    InternedStringPtr raw_tag_string;
  };
  InternedStringHashMap<TagInfo> tracked_tags_by_keys_
      ABSL_GUARDED_BY(index_mutex_);
  KeySet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  const char separator_;
  const bool case_sensitive_;
  rax* tree_ ABSL_GUARDED_BY(index_mutex_);
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TAG_H_
