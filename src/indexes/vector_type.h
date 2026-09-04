/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_TYPE_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_TYPE_H_

#include <cstddef>
#include <memory>
#include <type_traits>

#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

// Compile-time mapping from T to its data_model::VectorDataType enum.
// If a new element type is added, add its arm here plus one arm in
// CreateSpace above and NormalizeStringRecord below; both leaves'
// ToProtoImpl / RespondWithInfoImpl stay unchanged.
template <typename T>
constexpr data_model::VectorDataType VectorDataTypeEnumFor() {
  if constexpr (std::is_same_v<T, float>) {
    return data_model::VECTOR_DATA_TYPE_FLOAT32;
  } else if constexpr (std::is_same_v<T, float16>) {
    return data_model::VECTOR_DATA_TYPE_FLOAT16;
  } else if constexpr (std::is_same_v<T, bfloat16>) {
    return data_model::VECTOR_DATA_TYPE_BFLOAT16;
  } else {
    // Force a compile error rather than a silent runtime UNSPECIFIED --
    // adding a new T without adding an arm here is a bug we want caught
    // at build time.
    static_assert(sizeof(T) == 0, "no VectorDataType enum for this T");
  }
}

// Intermediate layer between the type-erased VectorBase and the
// algorithm-specific leaves (VectorHNSW<T>, VectorFlat<T>). Carries all
// state and behavior that depends on the element type T but not on the
// choice of ANN algorithm:
//
//   * the hnswlib::SpaceInterface<float> owned by the index (both
//     algorithms construct it the same way, from dimensions + distance
//     metric via CreateSpace<T>),
//   * GetDataTypeSize() = sizeof(T),
//   * GetVectorDataType() = the compile-time-mapped enum for T,
//   * ComputeReciprocalMagnitude() over T-width elements,
//   * the FT.INFO data_type label and the vector_data_type proto value,
//   * NormalizeStringRecord's per-T byte width + format conversion for
//     the ASCII-to-binary query encoder path (float direct, float16 via
//     static_cast, bfloat16 via float_to_bfloat16).
//
// Downstream callers keep talking to VectorBase* -- the type-erased
// seam is preserved.
//
// Note the space is templated on the *distance* dtype, which is always
// float, never on T. Only the concrete space class chosen by CreateSpace<T>
// knows the storage layout. Templating the space on T (as upstream's
// float-only code did) conflates the two and breaks for 2-byte storage.
template <typename T>
class VectorType : public VectorBase {
 public:
  ~VectorType() override = default;

  // Convert an ASCII "[1.0, 2.0, ...]" query into a binary payload sized
  // sizeof(T) per element, with the format conversion appropriate to T.
  vmsdk::UniqueValkeyString NormalizeStringRecord(
      vmsdk::UniqueValkeyString record) const override;

 protected:
  VectorType(IndexerType indexer_type, int dimensions,
             data_model::AttributeDataType attribute_data_type,
             absl::string_view attribute_identifier, int db_num)
      : VectorBase(indexer_type, dimensions, sizeof(T),
                   VectorDataTypeEnumFor<T>(), attribute_data_type,
                   attribute_identifier, db_num) {}

  size_t GetDataTypeSize() const override { return sizeof(T); }
  float ComputeReciprocalMagnitude(absl::string_view record) const override;

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

  // Owned by VectorType<T>. The leaves' algo_ refers to it by raw
  // pointer via space_.get(); since C++ destroys derived-class members
  // before base-class members, algo_ is torn down first -- the
  // destruction order is correct without any extra scaffolding.
  std::unique_ptr<hnswlib::SpaceInterface<float>> space_;
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_TYPE_H_
