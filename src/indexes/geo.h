/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * 52-bit geohash GEO index. Mirrors src/indexes/numeric.{h,cc} but keys on
 * uint64_t (Morton-interleaved geohash, top-aligned to 52 bits) instead of
 * double. The current 52-bit Mercator-strip algorithm has the same polar
 * caveats as valkey-core's GEO* commands; documented at the field-type level.
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_GEO_H_
#define VALKEYSEARCH_SRC_INDEXES_GEO_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/hash/hash.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/geohash.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {
class GeoPredicate;
}

namespace valkey_search::indexes {

// Sorted multimap from 52-bit geohash score to the set of keys at that score.
// The btree gives ordered range iteration, the inner flat_hash_set gives O(1)
// duplicate-suppression. Direct analog of BTreeNumeric, but keyed on uint64_t.
template <typename T,
          typename Hasher = absl::Hash<T>,
          typename Eq = std::equal_to<T>>
class BTreeGeo {
 public:
  using SetType = absl::flat_hash_set<T, Hasher, Eq>;
  using ConstIterator =
      typename absl::btree_map<uint64_t, SetType>::const_iterator;

  void Add(const T& value, uint64_t score) {
    btree_[score].insert(value);
  }

  void Remove(const T& value, uint64_t score) {
    auto it = btree_.find(score);
    if (it == btree_.end()) {
      return;
    }
    it->second.erase(value);
    if (it->second.empty()) {
      btree_.erase(it);
    }
  }

  void Modify(const T& value, uint64_t old_score, uint64_t new_score) {
    if (old_score == new_score) {
      return;
    }
    Remove(value, old_score);
    Add(value, new_score);
  }

  const absl::btree_map<uint64_t, SetType>& GetBtree() const { return btree_; }

 private:
  absl::btree_map<uint64_t, SetType> btree_;
};

class Geo : public IndexBase {
 public:
  explicit Geo(const data_model::GeoIndex& geo_index_proto);

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

  // Returns the cached score (encoded position) for a tracked key, or nullptr.
  // Caller must hold the read-side time slice (see Numeric::GetValue).
  const uint64_t* GetScore(const InternedStringPtr& key) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Decodes a tracked key's score back to (lon, lat). Returns nullopt if
  // the key isn't tracked.
  std::optional<std::array<double, 2>> GetLonLat(
      const InternedStringPtr& key) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

  using BTreeGeoIndex = BTreeGeo<InternedStringPtr>;

  // Each ScoreRange is [min, max) over uint64 scores, mirroring the half-open
  // semantics of valkey-core's scoresOfGeoHashBox.
  using ScoreRange = std::pair<uint64_t, uint64_t>;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    EntriesFetcherIterator(
        std::vector<ScoreRange> ranges,
        const BTreeGeoIndex* index,
        geohash::Shape shape);
    bool Done() const override;
    void Next() override;
    const InternedStringPtr& operator*() const override;

   private:
    // Walks ranges_ until it lands on a candidate key whose decoded position
    // is actually inside the shape. Sets done_ when exhausted.
    void Advance();

    std::vector<ScoreRange> ranges_;
    const BTreeGeoIndex* index_;
    geohash::Shape shape_;

    size_t range_idx_{0};
    BTreeGeoIndex::ConstIterator score_iter_;
    BTreeGeoIndex::ConstIterator score_end_;
    std::optional<absl::flat_hash_set<InternedStringPtr>::const_iterator>
        key_iter_;
    bool done_{false};
    bool started_{false};
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    EntriesFetcher(std::vector<ScoreRange> ranges, size_t size_estimate,
                   const BTreeGeoIndex* index, geohash::Shape shape)
        : ranges_(std::move(ranges)),
          size_(size_estimate),
          index_(index),
          shape_(shape) {}
    size_t Size() const override { return size_; }
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override;

   private:
    std::vector<ScoreRange> ranges_;
    size_t size_;
    const BTreeGeoIndex* index_;
    geohash::Shape shape_;
  };

  // Build an EntriesFetcher that yields exactly the tracked keys whose
  // position lies within the predicate's circular search shape. The fetcher
  // performs the geohash 9-box cover, the score-range scans, and the exact
  // haversine post-filter internally — callers see only points truly inside
  // the radius.
  virtual std::unique_ptr<EntriesFetcher> Search(
      const query::GeoPredicate& predicate) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Parses "lon,lat" (HASH wire format). Returns nullopt on malformed input
  // or coordinates outside the supported WGS-84 strip.
  static std::optional<std::array<double, 2>> ParseLonLat(
      absl::string_view data);

 private:
  mutable absl::Mutex index_mutex_;
  InternedStringHashMap<uint64_t> tracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  InternedStringSet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  std::unique_ptr<BTreeGeoIndex> index_ ABSL_GUARDED_BY(index_mutex_);
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_GEO_H_
