/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_GEOSHAPE_H_
#define VALKEYSEARCH_SRC_INDEXES_GEOSHAPE_H_

#include <cstddef>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/indexes/geoshape_rtree.h"
#include "src/indexes/index_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::query {
class GeoShapePredicate;
}

namespace valkey_search::indexes {

// Represents a 2D point
struct GeoPoint {
  double x;
  double y;
};

// Represents a polygon as a vector of points (ring)
struct GeoPolygon {
  std::vector<GeoPoint> points;
};

// A geometry can be either a point or a polygon
using Geometry = std::variant<GeoPoint, GeoPolygon>;

// Spatial operators for GEOSHAPE queries
enum class SpatialOp { kWithin, kContains, kIntersects, kDisjoint };

// Parse WKT string into a Geometry
absl::StatusOr<Geometry> ParseWKT(absl::string_view wkt);

// Spatial predicate evaluation
bool GeometryWithin(const Geometry& shape, const Geometry& query_shape);
bool GeometryContains(const Geometry& shape, const Geometry& query_shape);
bool GeometryIntersects(const Geometry& shape, const Geometry& query_shape);

// Compute bounding box for a geometry
BBox GeometryBBox(const Geometry& geom);

class GeoShape : public IndexBase {
 public:
  explicit GeoShape(const data_model::GeoShapeIndex& geoshape_index_proto);

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

  data_model::GeoShapeCoordSystem GetCoordSystem() const {
    return coord_system_;
  }

  // Evaluate a spatial predicate against a stored geometry for a given key
  bool EvaluatePredicate(const InternedStringPtr& key, SpatialOp op,
                         const Geometry& query_shape) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Search: returns matching keys using R-tree for candidate filtering
  std::vector<InternedStringPtr> SearchCandidates(
      SpatialOp op,
      const Geometry& query_shape) const ABSL_NO_THREAD_SAFETY_ANALYSIS;

  class EntriesFetcherIterator : public EntriesFetcherIteratorBase {
   public:
    explicit EntriesFetcherIterator(std::vector<InternedStringPtr> keys)
        : keys_(std::move(keys)), pos_(0) {}
    bool Done() const override { return pos_ >= keys_.size(); }
    void Next() override { ++pos_; }
    const InternedStringPtr& operator*() const override { return keys_[pos_]; }

   private:
    std::vector<InternedStringPtr> keys_;
    size_t pos_;
  };

  class EntriesFetcher : public EntriesFetcherBase {
   public:
    explicit EntriesFetcher(std::vector<InternedStringPtr> keys)
        : keys_(std::move(keys)) {}
    size_t Size() const override { return keys_.size(); }
    std::unique_ptr<EntriesFetcherIteratorBase> Begin() override {
      return std::make_unique<EntriesFetcherIterator>(keys_);
    }

   private:
    std::vector<InternedStringPtr> keys_;
  };

 private:
  mutable absl::Mutex index_mutex_;
  data_model::GeoShapeCoordSystem coord_system_;
  InternedStringHashMap<Geometry> tracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  InternedStringSet untracked_keys_ ABSL_GUARDED_BY(index_mutex_);
  RTree<InternedStringPtr> rtree_ ABSL_GUARDED_BY(index_mutex_);
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_GEOSHAPE_H_
