/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/indexes/geo.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/index_base.h"
#include "src/query/predicate.h"
#include "src/utils/geohash.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

std::optional<std::array<double, 2>> Geo::ParseLonLat(absl::string_view data) {
  // Wire format mirrors valkey-core GEOADD: "lon,lat".
  std::vector<absl::string_view> parts = absl::StrSplit(data, ',');
  if (parts.size() != 2) return std::nullopt;
  double lon, lat;
  if (!absl::SimpleAtod(parts[0], &lon)) return std::nullopt;
  if (!absl::SimpleAtod(parts[1], &lat)) return std::nullopt;
  if (lon < geohash::kLonMin || lon > geohash::kLonMax) return std::nullopt;
  if (lat < geohash::kLatMin || lat > geohash::kLatMax) return std::nullopt;
  return std::array<double, 2>{lon, lat};
}

namespace {
std::optional<uint64_t> EncodeScore(double lon, double lat) {
  geohash::Bits bits;
  if (!geohash::EncodeWGS84(lon, lat, geohash::kStepMax, &bits)) {
    return std::nullopt;
  }
  return geohash::Align52Bits(bits);
}
}  // namespace

Geo::Geo(const data_model::GeoIndex& /*geo_index_proto*/)
    : IndexBase(IndexerType::kGeo),
      index_(std::make_unique<BTreeGeoIndex>()) {}

absl::StatusOr<bool> Geo::AddRecord(const InternedStringPtr& key,
                                    absl::string_view data) {
  auto lonlat = ParseLonLat(data);
  absl::MutexLock lock(&index_mutex_);
  if (!lonlat.has_value()) {
    untracked_keys_.insert(key);
    return false;
  }
  auto score = EncodeScore((*lonlat)[0], (*lonlat)[1]);
  if (!score.has_value()) {
    untracked_keys_.insert(key);
    return false;
  }
  auto [_, ok] = tracked_keys_.insert({key, *score});
  if (!ok) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key `", key->Str(), "` already exists"));
  }
  untracked_keys_.erase(key);
  index_->Add(key, *score);
  return true;
}

absl::StatusOr<bool> Geo::ModifyRecord(const InternedStringPtr& key,
                                       absl::string_view data) {
  auto lonlat = ParseLonLat(data);
  if (!lonlat.has_value()) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kIdentifier);
    return false;
  }
  auto new_score = EncodeScore((*lonlat)[0], (*lonlat)[1]);
  if (!new_score.has_value()) {
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
  index_->Modify(it->first, it->second, *new_score);
  it->second = *new_score;
  return true;
}

absl::StatusOr<bool> Geo::RemoveRecord(const InternedStringPtr& key,
                                       DeletionType deletion_type) {
  absl::MutexLock lock(&index_mutex_);
  if (deletion_type == DeletionType::kRecord) {
    untracked_keys_.erase(key);
  } else {
    untracked_keys_.insert(key);
  }
  auto it = tracked_keys_.find(key);
  if (it == tracked_keys_.end()) return false;
  index_->Remove(it->first, it->second);
  tracked_keys_.erase(it);
  return true;
}

int Geo::RespondWithInfo(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "GEO");
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  absl::MutexLock lock(&index_mutex_);
  ValkeyModule_ReplyWithCString(ctx,
                                std::to_string(tracked_keys_.size()).c_str());
  return 4;
}

std::unique_ptr<data_model::Index> Geo::ToProto() const {
  auto index_proto = std::make_unique<data_model::Index>();
  auto geo_index = std::make_unique<data_model::GeoIndex>();
  index_proto->set_allocated_geo_index(geo_index.release());
  return index_proto;
}

const uint64_t* Geo::GetScore(const InternedStringPtr& key) const {
  if (auto it = tracked_keys_.find(key); it != tracked_keys_.end()) {
    return &it->second;
  }
  return nullptr;
}

std::optional<std::array<double, 2>> Geo::GetLonLat(
    const InternedStringPtr& key) const {
  const uint64_t* score = GetScore(key);
  if (score == nullptr) return std::nullopt;
  // Reverse Align52Bits: the score is the hash bits left-shifted by zero
  // (step = 26, 52 - 26*2 = 0), so the bits == score and step = 26.
  geohash::Bits bits{*score, geohash::kStepMax};
  std::array<double, 2> xy;
  if (!geohash::DecodeToLonLatWGS84(bits, &xy)) return std::nullopt;
  return xy;
}

namespace {

// Coalesce a set of [lo, hi) ranges in place: sort by lo, then merge
// overlapping/adjacent. Up to 9 boxes from the geohash neighborhood often
// share boundaries (especially for huge radii where neighbors collapse to
// the same hash); coalescing avoids redundant scans and double-counting.
void CoalesceRanges(std::vector<Geo::ScoreRange>* ranges) {
  if (ranges->size() < 2) return;
  std::sort(ranges->begin(), ranges->end());
  size_t out = 0;
  for (size_t i = 1; i < ranges->size(); ++i) {
    if ((*ranges)[i].first <= (*ranges)[out].second) {
      (*ranges)[out].second =
          std::max((*ranges)[out].second, (*ranges)[i].second);
    } else {
      ++out;
      (*ranges)[out] = (*ranges)[i];
    }
  }
  ranges->resize(out + 1);
}

}  // namespace

}  // namespace valkey_search::indexes

// Search() and the iterator are defined after query::GeoPredicate in the
// rest of this TU below; predicate.h provides the declaration we need.

namespace valkey_search::indexes {

Geo::EntriesFetcherIterator::EntriesFetcherIterator(
    std::vector<ScoreRange> ranges, const BTreeGeoIndex* index,
    geohash::Shape shape)
    : ranges_(std::move(ranges)), index_(index), shape_(shape) {
  if (ranges_.empty()) {
    done_ = true;
  }
}

void Geo::EntriesFetcherIterator::Advance() {
  const auto& btree = index_->GetBtree();
  while (range_idx_ < ranges_.size()) {
    if (!started_) {
      score_iter_ = btree.lower_bound(ranges_[range_idx_].first);
      score_end_ = btree.lower_bound(ranges_[range_idx_].second);
      key_iter_.reset();
      started_ = true;
    }
    while (score_iter_ != score_end_) {
      if (!key_iter_.has_value()) {
        key_iter_ = score_iter_->second.begin();
      } else {
        ++key_iter_.value();
      }
      while (key_iter_.value() == score_iter_->second.end()) {
        ++score_iter_;
        if (score_iter_ == score_end_) break;
        key_iter_ = score_iter_->second.begin();
      }
      if (score_iter_ == score_end_) break;

      // Decode and exact-distance filter.
      geohash::Bits bits{score_iter_->first, geohash::kStepMax};
      std::array<double, 2> xy;
      if (!geohash::DecodeToLonLatWGS84(bits, &xy)) {
        // Should be unreachable for well-formed scores.
        continue;
      }
      double distance = 0;
      bool inside = false;
      if (shape_.type == geohash::ShapeType::kCircular) {
        inside = geohash::DistanceIfInRadius(
            shape_.xy[0], shape_.xy[1], xy[0], xy[1],
            shape_.u.radius * shape_.conversion, &distance);
      } else {
        inside = geohash::DistanceIfInRectangle(
            shape_.u.rect.width * shape_.conversion,
            shape_.u.rect.height * shape_.conversion,
            shape_.xy[0], shape_.xy[1], xy[0], xy[1], &distance);
      }
      if (inside) return;  // current cursor points at a valid match
    }
    ++range_idx_;
    started_ = false;
  }
  done_ = true;
}

bool Geo::EntriesFetcherIterator::Done() const { return done_; }

void Geo::EntriesFetcherIterator::Next() { Advance(); }

const InternedStringPtr& Geo::EntriesFetcherIterator::operator*() const {
  return *key_iter_.value();
}

std::unique_ptr<EntriesFetcherIteratorBase> Geo::EntriesFetcher::Begin() {
  auto it = std::make_unique<EntriesFetcherIterator>(ranges_, index_, shape_);
  it->Next();  // Position on first match (or done_).
  return it;
}

std::unique_ptr<Geo::EntriesFetcher> Geo::Search(
    const query::GeoPredicate& predicate) const {
  // Build the search shape from the predicate.
  geohash::Shape shape{};
  shape.type = geohash::ShapeType::kCircular;
  shape.xy[0] = predicate.GetLon();
  shape.xy[1] = predicate.GetLat();
  shape.conversion = predicate.GetUnitConversionMeters();
  shape.u.radius = predicate.GetRadius();

  auto cells = geohash::CalculateAreasByShapeWGS84(&shape);

  std::array<geohash::Bits, 9> boxes = {
      cells.hash,
      cells.neighbors.north,      cells.neighbors.south,
      cells.neighbors.east,       cells.neighbors.west,
      cells.neighbors.north_east, cells.neighbors.north_west,
      cells.neighbors.south_east, cells.neighbors.south_west,
  };

  std::vector<ScoreRange> ranges;
  ranges.reserve(boxes.size());
  for (const auto& b : boxes) {
    if (geohash::HashIsZero(b)) continue;
    ranges.push_back(geohash::ScoreRangeOfBox(b));
  }
  CoalesceRanges(&ranges);

  // Coarse pre-filter cardinality: total btree entries inside the score
  // ranges. This is an upper bound on the result count (the exact filter
  // may reject some). For now we just sum tracked_keys_.size() scaled by
  // the union range; a btree distance() walk is O(log N * |ranges|) and
  // can be added if the planner needs tighter estimates.
  size_t size_estimate = tracked_keys_.size();

  return std::make_unique<EntriesFetcher>(std::move(ranges), size_estimate,
                                          index_.get(), shape);
}

size_t Geo::GetTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.size();
}

size_t Geo::GetUnTrackedKeyCount() const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.size();
}

bool Geo::IsTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return tracked_keys_.contains(key);
}

bool Geo::IsUnTracked(const InternedStringPtr& key) const {
  absl::MutexLock lock(&index_mutex_);
  return untracked_keys_.contains(key);
}

void Geo::UnTrack(const InternedStringPtr& key) {
  absl::MutexLock lock(&index_mutex_);
  CHECK(!tracked_keys_.contains(key));
  untracked_keys_.insert(key);
}

absl::Status Geo::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto& [key, _] : tracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

absl::Status Geo::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr&)> fn) const {
  absl::MutexLock lock(&index_mutex_);
  for (const auto& key : untracked_keys_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

}  // namespace valkey_search::indexes
