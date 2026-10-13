/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_type.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Forward-declare the SVS opaque index handle so consumers of this header do
// not need the SVS C API include path. The full typedef lives in
// third_party/svs/bindings/c/include/svs/c/svs_c.h and is included only from
// vector_svs.cc.
extern "C" {
struct svs_index;
typedef struct svs_index *svs_index_h;
}

namespace valkey_search::indexes {

// Build-time configuration retained on the class so the first HSET can
// bootstrap the SVS index once it has an actual vector to build with;
// the SVS C API's svs_index_build_dynamic requires num_vectors > 0.
struct SVSBuildConfig {
  uint32_t graph_max_degree{0};
  uint32_t construction_window_size{0};
  uint32_t search_window_size{0};
  float alpha{0.0f};
  data_model::SVSCompressionType compression{data_model::SVS_COMPRESSION_NONE};
};

template <typename T>
class VectorSVS : public VectorType<T> {
 protected:
  // VectorType<T> is a dependent base, so inherited names are not found by
  // unqualified lookup. Re-declare the ones used below.
  using VectorType<T>::resize_mutex_;
  using VectorType<T>::dimensions_;
  using VectorType<T>::normalize_;
  using VectorType<T>::space_;
  using VectorType<T>::CreateReply;
  using VectorType<T>::GetCapacity;
  using VectorType<T>::GetDataTypeSize;
  using VectorType<T>::GetVectorAllocator;
  using VectorType<T>::GetVectorDataSize;
  using VectorType<T>::GetVectorDataType;
  using VectorType<T>::IsValidSizeVector;
  using VectorType<T>::EmitDataTypeInfo;
  using VectorType<T>::SetProtoDataType;
  using VectorType<T>::Init;

 public:
  static absl::StatusOr<std::shared_ptr<VectorSVS<T>>> Create(
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type,
      int db_num) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  static absl::StatusOr<std::shared_ptr<VectorSVS<T>>> LoadFromRDB(
      ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      SupplementalContentChunkIter &&iter,
      int db_num) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  ~VectorSVS() override;

  size_t GetCapacity() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Defaults must match VectorBase::Search exactly.
  absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<size_t> ef_runtime = std::nullopt,
      bool enable_partial_results = false) override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 protected:
  absl::Status AddRecordImpl(
      uint64_t internal_id,
      std::shared_ptr<const VectorRecord> &&vector_record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status RemoveRecordImpl(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status ModifyRecordImpl(
      uint64_t internal_id,
      std::shared_ptr<const VectorRecord> &&vector_record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  void ToProtoImpl(data_model::VectorIndex *vector_index_proto) const override;
  int RespondWithInfoImpl(ValkeyModuleCtx *ctx) const override;
  absl::Status SaveIndexImpl(RDBChunkOutputStream chunked_out) const override;
  float ComputeDistance(
      absl::string_view query, const VectorRecord *vector_record,
      float query_magnitude) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  std::shared_ptr<const VectorRecord> &GetVectorLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  // SVS has no tombstones, so every label LoadTrackedKeys re-encounters is
  // new to this map; try_emplace instead of the base class's CHECK.
  std::shared_ptr<const VectorRecord> &GetOrCreateVectorLockFree(
      uint64_t internal_id) override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  std::shared_ptr<const VectorRecord> &GetVector(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  uint64_t GetMaxLoadedLabel() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  size_t GetLabelCount() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  VectorSVS(int dimensions, absl::string_view attribute_identifier,
            data_model::AttributeDataType attribute_data_type, int db_num);

  svs_index_h svs_index_ ABSL_GUARDED_BY(resize_mutex_){nullptr};
  SVSBuildConfig build_config_;
  mutable absl::flat_hash_map<uint64_t, std::shared_ptr<const VectorRecord>>
      label_to_record_ ABSL_GUARDED_BY(resize_mutex_);
};

}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
