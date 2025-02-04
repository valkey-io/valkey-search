/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TAG_H_
#define VALKEYSEARCH_SRC_INDEXES_TAG_H_
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/query/predicate.h"
#include "src/rdb_io_stream.h"
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

class Tag : public IndexBase {
 public:
  explicit Tag(const data_model::TagIndex& tag_index_proto);
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
  int RespondWithInfo(RedisModuleCtx* ctx) const override;
  bool IsTracked(const InternedStringPtr& key) const override;
  absl::Status SaveIndex(RDBOutputStream& rdb_stream) const override {
    return absl::OkStatus();
  }

  inline void ForEachTrackedKey(
      absl::AnyInvocable<void(const InternedStringPtr&)> fn) const override {
    absl::MutexLock lock(&index_mutex_);
    for (const auto& [key, _] : tracked_tags_by_keys_) {
      fn(key);
    }
  }
  uint64_t GetRecordCount() const override;
  std::unique_ptr<data_model::Index> ToProto() const override;

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  const absl::flat_hash_set<absl::string_view>* GetValue(
      const InternedStringPtr& key,
      bool& case_sensitive) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  using PatriciaTreeIndex =
      PatriciaTree<InternedStringPtr, InternedStringPtrHash,
                   InternedStringPtrEqual>;
  using PatriciaNodeIndex =
      PatriciaNode<InternedStringPtr, InternedStringPtrHash,
                   InternedStringPtrEqual>;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    EntriesFetcherIterator(const PatriciaTreeIndex& tree,
                           absl::flat_hash_set<PatriciaNodeIndex*>& entries,
                           const InternedStringSet& untracked_keys,
                           bool negate);
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    PatriciaTreeIndex::PrefixSubTreeIterator tree_iter_;
    absl::flat_hash_set<PatriciaNodeIndex*>& entries_;
    PatriciaNodeIndex* next_node_{nullptr};
    InternedStringSet::const_iterator next_iter_;
    const InternedStringSet& untracked_keys_;
    bool negate_;
    std::optional<InternedStringSet::const_iterator> untracked_keys_iter_;
    void NextNegate();
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(const PatriciaTreeIndex& tree,
                   absl::flat_hash_set<PatriciaNodeIndex*> entries, size_t size,
                   bool negate, const InternedStringSet& untracked_keys)
        : tree_(tree),
          size_(size),
          entries_(entries),
          negate_(negate),
          untracked_keys_(untracked_keys){};
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
    const PatriciaTreeIndex& tree_;
    size_t size_{0};
    absl::flat_hash_set<PatriciaNodeIndex*> entries_;
    bool negate_;
    const InternedStringSet& untracked_keys_;
  };

  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::TagPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  char GetSeparator() const { return separator_; }
  bool IsCaseSensitive() const { return case_sensitive_; }
  static absl::StatusOr<absl::flat_hash_set<absl::string_view>> ParseSearchTags(
      absl::string_view data, char separator);
  static absl::flat_hash_set<absl::string_view> ParseRecordTags(
      absl::string_view data, char separator);
  vmsdk::UniqueRedisString NormalizeStringRecord(
      vmsdk::UniqueRedisString input) const override;

 private:
  mutable absl::Mutex index_mutex_;
  struct TagInfo {
    InternedStringPtr raw_tag_string;
    absl::flat_hash_set<absl::string_view> tags;
  };
  // Map of tracked keys to their tags.
  InternedStringMap<TagInfo> tracked_tags_by_keys_
      ABSL_GUARDED_BY(index_mutex_);
  // untracked and tracked_ keys are mutually exclusive.
  InternedStringSet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  const char separator_;
  const bool case_sensitive_;
  PatriciaTreeIndex tree_ ABSL_GUARDED_BY(index_mutex_);
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TAG_H_
