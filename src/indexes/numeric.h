/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_NUMERIC_H_
#define VALKEYSEARCH_SRC_INDEXES_NUMERIC_H_
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/hash/hash.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/indexes/key_attr_value.h"
#include "src/query/predicate.h"
#include "src/rdb_serialization.h"
#include "src/utils/segment_tree.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

// MissingListIterator is declared in index_base.h (above #include).

template <typename T, typename Hasher = absl::Hash<T>,
          typename Equalizer = std::equal_to<T>>
class BTreeNumeric {
 public:
  using SetType = absl::flat_hash_set<T, Hasher, Equalizer>;
  using ConstIterator =
      typename absl::btree_map<double, SetType>::const_iterator;

  void Add(const T& value, double key) {
    btree_[key].insert(value);
    segment_tree_.Add(key);
  }

  void Modify(const T& value, double old_key, double new_key) {
    Remove(value, old_key);
    Add(value, new_key);
  }

  void Remove(const T& value, double key) {
    btree_[key].erase(value);
    if (btree_[key].empty()) {
      btree_.erase(key);
    }
    segment_tree_.Remove(key);
  }
  const absl::btree_map<double, SetType>& GetBtree() const { return btree_; }

  size_t GetCount(double start, double end, bool start_inclusive,
                  bool end_inclusive) {
    return segment_tree_.Count(start, end, start_inclusive, end_inclusive);
  }

 private:
  // Right now we have both BTree and Segment Tree. The BTree is used to
  // maintain the keys and the values. The segment tree is used to maintain the
  // count of the keys in the range.
  //
  // Note on overhead: SegmentTree is roughly 80 bytes per entry (40 B per node,
  // 2x nodes per entries with a balanced tree).
  //
  // TODO: Consider using a single data structure to maintain both
  // the keys and the count.
  absl::btree_map<double, SetType> btree_;
  utils::SegmentTree segment_tree_;
};

// Per-slot payload for the Numeric index. SlotBase is 4 bytes; the compiler
// inserts 4 bytes of padding so `value` is 8-byte aligned and the total slot
// size is the canonical 16 bytes. NOT packed — naturally-aligned access to
// `value` is the common case at query time.
struct NumericSlot : SlotBase {
  double value;
};

static_assert(sizeof(NumericSlot) == 16,
              "NumericSlot must fit exactly in a 16-byte Slot");
static_assert(alignof(NumericSlot) == 8, "NumericSlot must be 8-byte aligned");

class Numeric : public TypedIndex<Numeric, NumericSlot> {
 public:
  using SlotT = NumericSlot;

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

  // Slot teardown for whole-key delete (TypedIndex CRTP). Removes this
  // index's value from the btree/segment tree using the SlotT's stored
  // value. Does NOT touch slot bytes — VacateSlot overwrites them.
  void DestructTyped(const InternedStringPtr& key, NumericSlot& slot) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);

  std::unique_ptr<data_model::Index> ToProto() const override;

  uint32_t GetMutationWeight() const override;

  // Returns a pointer to this key's stored numeric value if currently tracked,
  // else nullptr. Reads the slot bytes via SlotAs; lock-free under the read
  // epoch (the Numeric index is not mutated while time_sliced_mutex_ is in
  // read mode — see numeric.cc).
  const double* GetValue(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  using BTreeNumericIndex = BTreeNumeric<InternedStringPtr>;
  using EntriesRange = std::pair<BTreeNumericIndex::ConstIterator,
                                 BTreeNumericIndex::ConstIterator>;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    EntriesFetcherIterator(
        const EntriesRange& entries_range,
        const std::optional<EntriesRange>& additional_entries_range,
        const IndexSchema* schema_for_missing, uint16_t pos);
    ~EntriesFetcherIterator() override;
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    static bool NextKeys(
        const Numeric::EntriesRange& range,
        BTreeNumericIndex::ConstIterator& iter,
        std::optional<InternedStringSet::const_iterator>& keys_iter);
    const EntriesRange& entries_range_;
    BTreeNumericIndex::ConstIterator entries_iter_;
    std::optional<InternedStringSet::const_iterator> entry_keys_iter_;
    const std::optional<EntriesRange>& additional_entries_range_;
    BTreeNumericIndex::ConstIterator additional_entries_iter_;
    std::optional<InternedStringSet::const_iterator>
        additional_entry_keys_iter_;
    // For negation queries, after the btree ranges are exhausted, walk the
    // per-attribute missing list. Both nullptrs when the fetcher is not a
    // negation. `missing_iter_` is heap-allocated to keep MissingListIterator
    // a complete type only in the .cc.
    const IndexSchema* schema_for_missing_{nullptr};
    uint16_t pos_{0};
    std::unique_ptr<MissingListIterator> missing_iter_;
    bool missing_started_{false};
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(
        const EntriesRange& entries_range, size_t size,
        std::optional<EntriesRange> additional_entries_range = std::nullopt,
        const IndexSchema* schema_for_missing = nullptr, uint16_t pos = 0)
        : entries_range_(entries_range),
          size_(size),
          additional_entries_range_(additional_entries_range),
          schema_for_missing_(schema_for_missing),
          pos_(pos) {}
    size_t Size() const override;
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
    EntriesRange entries_range_;
    size_t size_{0};
    std::optional<EntriesRange> additional_entries_range_;
    const IndexSchema* schema_for_missing_{nullptr};
    uint16_t pos_{0};
  };

  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::NumericPredicate& predicate,
      bool negate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  mutable absl::Mutex index_mutex_;
  std::unique_ptr<BTreeNumericIndex> index_ ABSL_GUARDED_BY(index_mutex_);
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_NUMERIC_H_
