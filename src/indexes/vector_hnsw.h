/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_type.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

class QueryVector {
 public:
  // `data_type` selects the element width used when normalizing; QueryVector
  // is shared across all VectorHNSW<T> instantiations, so it cannot take the
  // storage type as a template parameter.
  QueryVector(const std::shared_ptr<const VectorRecord> &vector_record,
              size_t vector_record_size, bool normalize,
              data_model::VectorDataType data_type =
                  data_model::VECTOR_DATA_TYPE_FLOAT32);
  const char *GetRawVector() const { return vector_record_->GetRawVector(); }
  float GetReciprocalMagnitude() const {
    return vector_record_->GetReciprocalMagnitude();
  }
  const char *GetNormalizedVector() const { return normalized_vector_.data(); }

  std::shared_ptr<const VectorRecord> GetVectorRecord() const {
    return vector_record_;
  }

 private:
  std::shared_ptr<const VectorRecord> vector_record_;
  std::vector<char> normalized_vector_;
};

template <typename T>
class VectorHNSW : public VectorType<T> {
 protected:
  // VectorType<T> is a dependent base, so inherited names are not found by
  // unqualified lookup. Re-declare the ones used below (including inside
  // thread-safety annotations, which cannot be written as this->member).
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
  using HNSWIndex =
      hnswlib::HierarchicalNSW<float, QueryVector,
                               std::shared_ptr<const VectorRecord>>;

  static absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> Create(
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type,
      int db_num) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  static absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> LoadFromRDB(
      ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      SupplementalContentChunkIter &&iter,
      int db_num) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  ~VectorHNSW() override = default;

  const hnswlib::SpaceInterface<float> *GetSpace() const {
    return this->space_.get();
  }

  size_t GetCapacity() const override ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return algo_->max_elements_;
  }
  // Reading immutable index parameters does not require a mutex. Bypassing
  // thread-safety analysis because while the pointer algo_ is guarded to
  // protect mutative operations, these specific fields are strictly constant
  // after construction.
  int GetM() const ABSL_NO_THREAD_SAFETY_ANALYSIS { return algo_->M_; }
  int GetEfConstruction() const ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return algo_->ef_construction_;
  }
  size_t GetEfRuntime() const ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return algo_->ef_;
  }

  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  // Defaults must match VectorBase::Search exactly -- see the note there.
  absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<size_t> ef_runtime = std::nullopt,
      bool enable_partial_results = false) override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 protected:
  absl::Status ResizeIfFull() ABSL_LOCKS_EXCLUDED(resize_mutex_);
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
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  float ComputeDistance(absl::string_view query,
                        const VectorRecord *vector_record,
                    float query_magnitude) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  std::shared_ptr<const VectorRecord> &GetVectorLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS {
    auto *ptr = algo_->GetPointLockFree(internal_id);
    CHECK(ptr != nullptr) << "Internal ID not found in label_lookup: "
                          << internal_id;
    return *ptr;
  }
  std::shared_ptr<const VectorRecord> &GetVector(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS {
    auto *ptr = algo_->GetPoint(internal_id);
    CHECK(ptr != nullptr) << "Internal ID not found in label_lookup: "
                          << internal_id;
    return *ptr;
  }
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  uint64_t GetMaxLoadedLabel() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  size_t GetLabelCount() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  VectorHNSW(int dimensions, absl::string_view attribute_identifier,
             data_model::AttributeDataType attribute_data_type, int db_num);
  absl::Status AlgoDeleteRecord(uint64_t label)
      ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_);

  std::unique_ptr<HNSWIndex> algo_ ABSL_GUARDED_BY(resize_mutex_);
};

}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
