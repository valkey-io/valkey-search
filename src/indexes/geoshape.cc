/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/geoshape.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

namespace {

// Point-in-polygon test using ray casting algorithm
bool PointInPolygon(const GeoPoint& point, const GeoPolygon& polygon) {
  bool inside = false;
  size_t n = polygon.points.size();
  for (size_t i = 0, j = n - 1; i < n; j = i++) {
    double xi = polygon.points[i].x, yi = polygon.points[i].y;
    double xj = polygon.points[j].x, yj = polygon.points[j].y;
    if (((yi > point.y) != (yj > point.y)) &&
        (point.x < (xj - xi) * (point.y - yi) / (yj - yi) + xi)) {
      inside = !inside;
    }
  }
  return inside;
}

// Check if all points of polygon A are inside polygon B
bool PolygonWithinPolygon(const GeoPolygon& a, const GeoPolygon& b) {
  for (const auto& pt : a.points) {
    if (!PointInPolygon(pt, b)) return false;
  }
  return true;
}

// Cross product of vectors (p1->p2) and (p1->p3)
double Cross(const GeoPoint& p1, const GeoPoint& p2, const GeoPoint& p3) {
  return (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x);
}

// Check if point p is on segment (a, b)
bool OnSegment(const GeoPoint& a, const GeoPoint& b, const GeoPoint& p) {
  return std::min(a.x, b.x) <= p.x && p.x <= std::max(a.x, b.x) &&
         std::min(a.y, b.y) <= p.y && p.y <= std::max(a.y, b.y);
}

// Check if segments (a1,a2) and (b1,b2) intersect
bool SegmentsIntersect(const GeoPoint& a1, const GeoPoint& a2,
                       const GeoPoint& b1, const GeoPoint& b2) {
  double d1 = Cross(b1, b2, a1);
  double d2 = Cross(b1, b2, a2);
  double d3 = Cross(a1, a2, b1);
  double d4 = Cross(a1, a2, b2);
  if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
      ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) {
    return true;
  }
  if (d1 == 0 && OnSegment(b1, b2, a1)) return true;
  if (d2 == 0 && OnSegment(b1, b2, a2)) return true;
  if (d3 == 0 && OnSegment(a1, a2, b1)) return true;
  if (d4 == 0 && OnSegment(a1, a2, b2)) return true;
  return false;
}

// Check if two polygons have intersecting edges
bool PolygonsEdgesIntersect(const GeoPolygon& a, const GeoPolygon& b) {
  for (size_t i = 0; i < a.points.size() - 1; ++i) {
    for (size_t j = 0; j < b.points.size() - 1; ++j) {
      if (SegmentsIntersect(a.points[i], a.points[i + 1], b.points[j],
                            b.points[j + 1])) {
        return true;
      }
    }
  }
  return false;
}

absl::StatusOr<GeoPoint> ParsePointCoords(absl::string_view coords) {
  coords = absl::StripAsciiWhitespace(coords);
  std::vector<absl::string_view> parts =
      absl::StrSplit(coords, ' ', absl::SkipWhitespace());
  if (parts.size() != 2) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid point coordinates: ", coords));
  }
  double x, y;
  if (!absl::SimpleAtod(parts[0], &x) || !absl::SimpleAtod(parts[1], &y)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid coordinate values: ", coords));
  }
  return GeoPoint{x, y};
}

absl::StatusOr<GeoPolygon> ParsePolygonCoords(absl::string_view coords) {
  // coords should be like "(x1 y1, x2 y2, ...)"
  coords = absl::StripAsciiWhitespace(coords);
  if (coords.empty() || coords.front() != '(' || coords.back() != ')') {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid polygon ring: ", coords));
  }
  coords = coords.substr(1, coords.size() - 2);
  GeoPolygon polygon;
  std::vector<absl::string_view> point_strs =
      absl::StrSplit(coords, ',', absl::SkipWhitespace());
  for (const auto& pt_str : point_strs) {
    VMSDK_ASSIGN_OR_RETURN(auto pt, ParsePointCoords(pt_str));
    polygon.points.push_back(pt);
  }
  if (polygon.points.size() < 4) {
    return absl::InvalidArgumentError(
        "Polygon must have at least 4 points (closed ring)");
  }
  return polygon;
}

}  // namespace

BBox GeometryBBox(const Geometry& geom) {
  BBox bbox = MakeBBox();
  if (std::holds_alternative<GeoPoint>(geom)) {
    const auto& pt = std::get<GeoPoint>(geom);
    bbox.Expand(pt.x, pt.y);
  } else {
    const auto& poly = std::get<GeoPolygon>(geom);
    for (const auto& pt : poly.points) {
      bbox.Expand(pt.x, pt.y);
    }
  }
  return bbox;
}

absl::StatusOr<Geometry> ParseWKT(absl::string_view wkt) {
  wkt = absl::StripAsciiWhitespace(wkt);
  auto upper = absl::AsciiStrToUpper(wkt);

  if (upper.starts_with("POINT")) {
    auto paren_start = wkt.find('(');
    auto paren_end = wkt.rfind(')');
    if (paren_start == absl::string_view::npos ||
        paren_end == absl::string_view::npos) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid POINT WKT: ", wkt));
    }
    auto coords = wkt.substr(paren_start + 1, paren_end - paren_start - 1);
    VMSDK_ASSIGN_OR_RETURN(auto pt, ParsePointCoords(coords));
    return Geometry{pt};
  }

  if (upper.starts_with("POLYGON")) {
    // Find the outer ring: POLYGON ((x1 y1, x2 y2, ...))
    auto first_paren = wkt.find('(');
    auto last_paren = wkt.rfind(')');
    if (first_paren == absl::string_view::npos ||
        last_paren == absl::string_view::npos) {
      return absl::InvalidArgumentError(
          absl::StrCat("Invalid POLYGON WKT: ", wkt));
    }
    // Strip outer parens of POLYGON ((...))
    auto inner = wkt.substr(first_paren + 1, last_paren - first_paren - 1);
    inner = absl::StripAsciiWhitespace(inner);
    // Find the ring
    auto ring_start = inner.find('(');
    auto ring_end = inner.rfind(')');
    if (ring_start == absl::string_view::npos ||
        ring_end == absl::string_view::npos) {
      // Try without inner parens (single ring)
      VMSDK_ASSIGN_OR_RETURN(auto polygon,
                             ParsePolygonCoords(absl::StrCat("(", inner, ")")));
      return Geometry{polygon};
    }
    auto ring = inner.substr(ring_start, ring_end - ring_start + 1);
    VMSDK_ASSIGN_OR_RETURN(auto polygon, ParsePolygonCoords(ring));
    return Geometry{polygon};
  }

  return absl::InvalidArgumentError(
      absl::StrCat("Unsupported WKT geometry type: ", wkt));
}

bool GeometryWithin(const Geometry& shape, const Geometry& query_shape) {
  // shape is WITHIN query_shape means shape is completely inside query_shape
  if (std::holds_alternative<GeoPolygon>(query_shape)) {
    const auto& query_poly = std::get<GeoPolygon>(query_shape);
    if (std::holds_alternative<GeoPoint>(shape)) {
      return PointInPolygon(std::get<GeoPoint>(shape), query_poly);
    }
    if (std::holds_alternative<GeoPolygon>(shape)) {
      return PolygonWithinPolygon(std::get<GeoPolygon>(shape), query_poly);
    }
  }
  if (std::holds_alternative<GeoPoint>(query_shape)) {
    if (std::holds_alternative<GeoPoint>(shape)) {
      const auto& a = std::get<GeoPoint>(shape);
      const auto& b = std::get<GeoPoint>(query_shape);
      return a.x == b.x && a.y == b.y;
    }
    return false;  // polygon cannot be within a point
  }
  return false;
}

bool GeometryContains(const Geometry& shape, const Geometry& query_shape) {
  // shape CONTAINS query_shape means query_shape is within shape
  return GeometryWithin(query_shape, shape);
}

bool GeometryIntersects(const Geometry& shape, const Geometry& query_shape) {
  // Two geometries intersect if they share any point
  if (GeometryWithin(shape, query_shape) ||
      GeometryContains(shape, query_shape)) {
    return true;
  }
  // Check edge intersections for polygon-polygon case
  if (std::holds_alternative<GeoPolygon>(shape) &&
      std::holds_alternative<GeoPolygon>(query_shape)) {
    return PolygonsEdgesIntersect(std::get<GeoPolygon>(shape),
                                  std::get<GeoPolygon>(query_shape));
  }
  return false;
}

// GeoShape class implementation

GeoShape::GeoShape(const data_model::GeoShapeIndex& geoshape_index_proto)
    : IndexBase(IndexerType::kGeoShape),
      coord_system_(geoshape_index_proto.coord_system()) {}

absl::StatusOr<bool> GeoShape::AddRecord(const InternedStringPtr& key,
                                         absl::string_view data) {
  auto geom = ParseWKT(data);
  absl::MutexLock lock(&index_mutex_);
  if (!geom.ok()) {
    untracked_keys_.insert(key);
    return false;
  }
  auto [_, succ] = tracked_keys_.insert({key, *geom});
  if (!succ) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key `", key->Str(), "` already exists"));
  }
  untracked_keys_.erase(key);
  rtree_.Insert(GeometryBBox(*geom), key);
  return true;
}

absl::StatusOr<bool> GeoShape::ModifyRecord(const InternedStringPtr& key,
                                            absl::string_view data) {
  auto geom = ParseWKT(data);
  if (!geom.ok()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return false;
  }
  absl::MutexLock lock(&index_mutex_);
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Key `", key->Str(), "` not found"));
  }
  rtree_.Remove(GeometryBBox(it->second), key);
  it->second = *geom;
  rtree_.Insert(GeometryBBox(*geom), key);
  return true;
}

absl::StatusOr<bool> GeoShape::RemoveRecord(const InternedStringPtr& key,
                                            DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    untracked_keys_.erase(key);
  } else {
    untracked_keys_.insert(key);
  }
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) {
    return false;
  }
  rtree_.Remove(GeometryBBox(it->second), key);
  tracked_keys_.erase(it);
  return true;
}

int GeoShape::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "GEOSHAPE");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(ctx,
                                std::to_string(tracked_keys_.size()).c_str());
  return 4;
}

std::unique_ptr<data_model::Index> GeoShape::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto geoshape_index = std::make_unique<data_model::GeoShapeIndex>();
  geoshape_index->set_coord_system(coord_system_);
  index_proto->set_allocated_geoshape_index(geoshape_index.release());
  return index_proto;
}

uint32_t GeoShape::GetMutationWeight() const {
  // Use same weight as numeric for now
  return 1;
}

bool GeoShape::EvaluatePredicate(const InternedStringPtr& key, SpatialOp op,
                                 const Geometry& query_shape) const {
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) return false;
  const auto& shape = it->second;
  switch (op) {
    case SpatialOp::kWithin:
      return GeometryWithin(shape, query_shape);
    case SpatialOp::kContains:
      return GeometryContains(shape, query_shape);
    case SpatialOp::kIntersects:
      return GeometryIntersects(shape, query_shape);
    case SpatialOp::kDisjoint:
      return !GeometryIntersects(shape, query_shape);
  }
  return false;
}

std::vector<InternedStringPtr> GeoShape::SearchCandidates(
    SpatialOp op, const Geometry& query_shape) const {
  BBox query_bbox = GeometryBBox(query_shape);
  std::vector<InternedStringPtr> results;

  if (op == SpatialOp::kDisjoint) {
    // For DISJOINT, we need all keys — can't prune with R-tree bbox overlap
    for (const auto& [key, geom] : tracked_keys_) {
      if (!GeometryIntersects(geom, query_shape)) {
        results.push_back(key);
      }
    }
  } else {
    // For WITHIN/CONTAINS/INTERSECTS, R-tree bbox overlap is a necessary
    // condition — only check candidates whose bbox intersects query bbox
    rtree_.Query(query_bbox, [&](const InternedStringPtr& key, const BBox&) {
      auto it = tracked_keys_.find(key);
      if (it == tracked_keys_.end()) return;
      bool match = false;
      switch (op) {
        case SpatialOp::kWithin:
          match = GeometryWithin(it->second, query_shape);
          break;
        case SpatialOp::kContains:
          match = GeometryContains(it->second, query_shape);
          break;
        case SpatialOp::kIntersects:
          match = GeometryIntersects(it->second, query_shape);
          break;
        default:
          break;
      }
      if (match) results.push_back(key);
    });
  }
  return results;
}

size_t GeoShape::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
}

size_t GeoShape::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.size();
}

bool GeoShape::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

bool GeoShape::IsUnTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.contains(key);
}

void GeoShape::UnTrack(const InternedStringPtr& key) {
  absl::MutexLock lock(&index_mutex_);
  CHECK(!tracked_keys_.contains(key));
  untracked_keys_.insert(key);
}

absl::Status GeoShape::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto& [key, _] : tracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

absl::Status GeoShape::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto& key : untracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::indexes
