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

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

class InputVector {
 public:
  InputVector(const std::shared_ptr<const VectorRecord> &vector_record,
              size_t vector_record_size, bool normalize);
  inline const char *GetRawVector() const {
    return vector_record_->GetRawVector();
  }
  inline float GetReciprocalMagnitude() const {
    return vector_record_->GetReciprocalMagnitude();
  }
  inline const char *GetNormalizedVector() const {
    return normalized_vector_.data();
  }

  inline std::shared_ptr<const VectorRecord> GetVectorRecord() const {
    return vector_record_;
  }

 private:
  std::shared_ptr<const VectorRecord> vector_record_;
  std::vector<char> normalized_vector_;
};

template <typename T>
class VectorHNSW : public VectorBase {
 public:
  using HNSWIndex =
      hnswlib::HierarchicalNSW<T, InputVector,
                               std::shared_ptr<const VectorRecord>>;

  static absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> Create(
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  static absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> LoadFromRDB(
      ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      SupplementalContentChunkIter &&iter) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  ~VectorHNSW() override = default;
  size_t GetDataTypeSize() const override { return sizeof(T); }

  const hnswlib::SpaceInterface<float> *GetSpace() const {
    return space_.get();
  }

  size_t GetCapacity() const override
      ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->max_elements_;
  }
  int GetM() const ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->M_;
  }
  int GetEfConstruction() const ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->ef_construction_;
  }
  size_t GetEfRuntime() const ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_) {
    return algo_->ef_;
  }

  absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<size_t> ef_runtime = std::nullopt,
      bool enable_partial_results = false) ABSL_LOCKS_EXCLUDED(resize_mutex_);

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
  T ComputeDistance(absl::string_view query, const VectorRecord *vector_record,
                    float query_magnitude) const override;
  std::shared_ptr<const VectorRecord> &GetVectorLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS {
    auto *ptr = algo_->getPoint(internal_id);
    CHECK(ptr != nullptr) << "Internal ID not found in label_lookup: "
                          << internal_id;
    return *ptr;
  }
  std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const override;
  uint64_t GetMaxInternalLabel() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  size_t GetLabelCount() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  VectorHNSW(int dimensions, absl::string_view attribute_identifier,
             data_model::AttributeDataType attribute_data_type);
  absl::Status AlgoDeleteRecord(uint64_t label)
      ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_);

  std::unique_ptr<HNSWIndex> algo_ ABSL_GUARDED_BY(resize_mutex_);
  std::unique_ptr<hnswlib::SpaceInterface<T>> space_;
};

}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
