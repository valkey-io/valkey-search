/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TAG_H_
#define VALKEYSEARCH_SRC_INDEXES_TAG_H_
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/key_attr_value.h"
#include "src/query/predicate.h"
#include "src/rdb_serialization.h"
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

// MissingListIterator is declared in index_base.h (above #include).

// Per-key tag bookkeeping. Heap-allocated; pointed to by TagSlot::info.
// Owned by the slot — DestructTyped frees it on whole-key delete.
struct TagInfo {
  InternedStringPtr raw_tag_string;
  absl::flat_hash_set<absl::string_view> tags;
};

// Per-slot payload for the Tag index. SlotBase (4B) + padding (4B) +
// TagInfo* (8B) = 16B total, naturally aligned.
struct TagSlot : SlotBase {
  TagInfo* info;
};

static_assert(sizeof(TagSlot) == 16, "TagSlot must fit exactly in a 16-byte Slot");
static_assert(alignof(TagSlot) == 8, "TagSlot must be 8-byte aligned");

class Tag : public TypedIndex<Tag, TagSlot> {
 public:
  using SlotT = TagSlot;

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

  // Whole-key delete safety net (TypedIndex CRTP). Removes this key's tags
  // from the patricia tree and deletes the TagInfo* held by the slot.
  void DestructTyped(const InternedStringPtr& key, TagSlot& slot) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);

  InternedStringPtr GetRawValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  const absl::flat_hash_set<absl::string_view>* GetValue(
      const InternedStringPtr& key,
      bool& case_sensitive) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  using PatriciaTreeIndex = PatriciaTree<InternedStringPtr>;
  using PatriciaNodeIndex = PatriciaNode<InternedStringPtr>;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    EntriesFetcherIterator(const PatriciaTreeIndex& tree,
                           absl::flat_hash_set<PatriciaNodeIndex*>& entries,
                           const IndexSchema* schema_for_missing, uint16_t pos,
                           bool negate);
    ~EntriesFetcherIterator() override;
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    const PatriciaTreeIndex& tree_;
    std::optional<PatriciaTreeIndex::PrefixSubTreeIterator> negate_root_iter_;
    absl::flat_hash_set<PatriciaNodeIndex*>& entries_;
    PatriciaNodeIndex* next_node_{nullptr};
    InternedStringSet::const_iterator next_iter_;
    const IndexSchema* schema_for_missing_;
    uint16_t pos_;
    bool negate_;
    std::unique_ptr<MissingListIterator> missing_iter_;
    bool missing_started_{false};
    void NextNegate();
    void EnsureNegateRootIter();
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(const PatriciaTreeIndex& tree,
                   absl::flat_hash_set<PatriciaNodeIndex*> entries, size_t size,
                   bool negate, const IndexSchema* schema_for_missing,
                   uint16_t pos)
        : tree_(tree),
          size_(size),
          entries_(entries),
          negate_(negate),
          schema_for_missing_(schema_for_missing),
          pos_(pos) {}
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
    const PatriciaTreeIndex& tree_;
    size_t size_{0};
    absl::flat_hash_set<PatriciaNodeIndex*> entries_;
    bool negate_;
    const IndexSchema* schema_for_missing_;
    uint16_t pos_;
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
  static std::string UnescapeTag(absl::string_view tag);

 private:
  mutable absl::Mutex index_mutex_;
  const char separator_;
  const bool case_sensitive_;
  PatriciaTreeIndex tree_ ABSL_GUARDED_BY(index_mutex_);
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_TAG_H_
