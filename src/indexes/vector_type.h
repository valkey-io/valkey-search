/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_TYPE_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_TYPE_H_

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

// IEEE 754 bit layout of a distance type, for classifying values by their bits:
// -ffast-math makes comparisons with infinity and NaN unreliable.
template <typename F>
struct FloatBits;
template <>
struct FloatBits<float> {
  using Bits = uint32_t;
  static constexpr Bits kExponentMask = 0x7f800000U;
  static constexpr Bits kSignBit = 0x80000000U;
};
template <>
struct FloatBits<double> {
  using Bits = uint64_t;
  static constexpr Bits kExponentMask = 0x7ff0000000000000ULL;
  static constexpr Bits kSignBit = 0x8000000000000000ULL;
};

// Intermediate layer between the type-erased VectorBase and the
// algorithm-specific leaves (VectorHNSW<T>, VectorFlat<T>). Carries all
// state and behavior that depends on the element type T but not on the
// choice of ANN algorithm:
//
//   * the hnswlib::SpaceInterface<DistanceT> owned by the index (both
//     algorithms construct it the same way, from dimensions + distance
//     metric via CreateSpace<T>),
//   * GetDataTypeSize() = sizeof(T),
//   * GetVectorDataType() = the compile-time-mapped enum for T,
//   * ConstructRecord() over T-width elements, with the header for T,
//   * distances outside the ANN algorithms (range search, pre-filtering,
//     recomputation), computed in DistanceT like the algorithms' own,
//   * the FT.INFO data_type label and the vector_data_type proto value,
//   * NormalizeStringRecord's per-T byte width + format conversion for
//     the ASCII-to-binary query encoder path (float direct, float16 via
//     static_cast, bfloat16 via float_to_bfloat16).
//
// Downstream callers keep talking to VectorBase* -- the type-erased
// seam is preserved.
//
// Note the space is templated on the *distance* dtype (double for FLOAT64,
// float otherwise), never on T. Only the concrete space class chosen by
// CreateSpace<T> knows the storage layout. Templating the space on T (as
// upstream's float-only code did) conflates the two and breaks for 2-byte
// storage.
template <typename T>
class VectorType : public VectorBase {
 public:
  // Type of the reciprocal magnitude kept in each record's header.
  using MagnitudeT = MagnitudeOf<T>;
  // Type the ANN algorithms compute and compare distances in.
  using DistanceT = MagnitudeOf<T>;
  // Record type the ANN algorithms store.
  using StoredVector = TypedVectorRecord<MagnitudeT>;

  ~VectorType() override = default;

  data_model::VectorDataType GetVectorDataType() const override;

  // Convert an ASCII "[1.0, 2.0, ...]" query into a binary payload sized
  // sizeof(T) per element, with the format conversion appropriate to T.
  vmsdk::UniqueValkeyString NormalizeStringAttribute(
      vmsdk::UniqueValkeyString attribute) const override;
  VectorRecord ConstructRecord(absl::string_view record,
                               Allocator* allocator) const override;
  absl::StatusOr<std::optional<double>> IsWithinVectorRange(
      const InternedStringPtr& key, absl::string_view query,
      double radius) const override;
  bool IsRecordWithinVectorRange(absl::string_view record,
                                 absl::string_view query,
                                 double radius) const override;
  absl::StatusOr<double> RecomputeDistance(
      absl::string_view record, absl::string_view query) const override;
  std::unique_ptr<NearestKeys> CreateNearestKeys(absl::string_view query,
                                                 uint64_t count) override;

 protected:
  VectorType(IndexerType indexer_type, int dimensions,
             data_model::AttributeDataType attribute_data_type,
             absl::string_view attribute_identifier, int db_num)
      : VectorBase(indexer_type, dimensions, sizeof(T),
                   StoredVector::kStorageHeaderSize, attribute_data_type,
                   attribute_identifier, db_num) {}

  size_t GetDataTypeSize() const override { return sizeof(T); }

  // Build `space_` from the distance metric and stamp distance_metric_
  // and normalize_ on the base. Called by the leaves' Create() /
  // LoadFromRDB() factories after construction.
  void Init(data_model::DistanceMetric distance_metric);

  // Emit the two-string ("data_type", "<label>") reply pair used inside
  // both leaves' RespondWithInfoImpl.
  void EmitDataTypeInfo(ValkeyModuleCtx* ctx) const;

  // Set proto->vector_data_type = enum-for-T. Used by both leaves'
  // ToProtoImpl.
  void SetProtoDataType(data_model::VectorIndex* vector_index_proto) const;

  virtual DistanceT ComputeDistance(absl::string_view query,
                                    const VectorRecord& vector_record,
                                    MagnitudeT query_magnitude) const = 0;

  // Reciprocal magnitude of `query` if the index normalizes, else 1.
  MagnitudeT QueryReciprocalMagnitude(absl::string_view query) const {
    if (!normalize_) {
      return kDefaultMagnitude;
    }
    return CalcReciprocalMagnitude(reinterpret_cast<const T*>(query.data()),
                                   query.size() / sizeof(T));
  }

  // `radius` rounded to DistanceT, so a distance that equals the radius as
  // written stays within it. Radii are capped at the largest double (see
  // VectorRangePredicate::SetRadius); a float index caps them at the largest
  // float, which still bounds every finite float distance.
  DistanceT RangeRadius(double radius) const {
    if constexpr (std::is_same_v<DistanceT, double>) {
      return radius;
    } else {
      return static_cast<DistanceT>(
          std::min(radius, double{std::numeric_limits<DistanceT>::max()}));
    }
  }

  // Clamps a cosine distance to [0, 2]; no tolerance window, since one wide
  // enough to absorb FP noise also swallows real near-duplicates/antipodes.
  // Non-finite values are classified from their bits (-ffast-math breaks
  // isnan/isfinite) and reported as +inf, outside every radius; an IP -inf
  // is kept as-is, within every radius, matching Redis.
  DistanceT ClampCosineDistance(DistanceT dist) const {
    using Layout = FloatBits<DistanceT>;
    const auto bits = std::bit_cast<typename Layout::Bits>(dist);
    if ((bits & Layout::kExponentMask) == Layout::kExponentMask) {
      return !normalize_ && bits == (Layout::kExponentMask | Layout::kSignBit)
                 ? dist
                 : std::bit_cast<DistanceT>(Layout::kExponentMask);
    }
    if (!normalize_) {
      return dist;
    }
    return std::clamp(dist, DistanceT{0}, DistanceT{2});
  }

  // Clamped distance for a range search; `query` from NormalizeQueryIfNeeded,
  // `query_magnitude` its reciprocal magnitude (1 unless normalize_).
  DistanceT RangeDistance(absl::string_view query, MagnitudeT query_magnitude,
                          const VectorRecord& record) const {
    if (normalize_) {
      query_magnitude *=
          StoredVector::ReciprocalMagnitudeOf(record.GetRawVector());
    }
    return ClampCosineDistance(ComputeDistance(query, record, query_magnitude));
  }

  // Every tracked key within `radius` of `query`, unordered, for FLAT/HNSW
  // alike. Lock-free: phase-based locking keeps queries and mutations
  // mutually exclusive, so key_to_metadata_mutex_ is not needed here.
  std::vector<Neighbor> SearchRangeExhaustive(
      absl::string_view query, double radius, cancel::Token& cancellation_token,
      hnswlib::BaseFilterFunctor* filter = nullptr) const;

  // Holds an optionally-normalized query vector. `view` is always valid and
  // points either into `storage` (if normalization was applied) or into the
  // original caller-owned buffer.
  struct NormalizedQuery {
    std::vector<char> storage;  // owns normalized data when normalize_ is true
    absl::string_view view;     // always usable as the query input
  };

  // Returns a normalized copy of `query` when the index uses cosine distance,
  // or a zero-copy view into the original buffer otherwise.
  NormalizedQuery NormalizeQueryIfNeeded(absl::string_view query) const {
    if (normalize_) {
      auto norm = NormalizeVector<T>(query);
      absl::string_view v(norm.data(), norm.size());
      return {std::move(norm), v};
    }
    return {{}, query};
  }

  // Owned by VectorType<T>. The leaves' algo_ refers to it by raw
  // pointer via space_.get(); since C++ destroys derived-class members
  // before base-class members, algo_ is torn down first -- the
  // destruction order is correct without any extra scaffolding.
  std::unique_ptr<hnswlib::SpaceInterface<DistanceT>> space_;

 private:
  class NearestKeysImpl;

  // Distance and internal label for `key`, or an error if untracked.
  // Search-phase only: lock-free, like GetVectorDuringSearch.
  absl::StatusOr<std::pair<DistanceT, hnswlib::labeltype>>
  ComputeDistanceFromRecord(const InternedStringPtr& key,
                            absl::string_view query,
                            MagnitudeT query_magnitude) const;

  // Distance from `query` to the vector in `record`, raw bytes rather than
  // anything the index holds. Backs RecomputeDistance.
  absl::StatusOr<DistanceT> ComputeDistanceToBytes(
      absl::string_view record, absl::string_view query) const;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_TYPE_H_
