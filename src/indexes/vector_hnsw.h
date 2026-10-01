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
  // Holds a query or inserted vector and caches its raw data pointer and
  // reciprocal magnitude so HNSW distance evaluations do not reload the record
  // header on every candidate.
  QueryVector(const VectorRecord &vector_record, size_t vector_record_size = 0,
              bool normalize = false,
              data_model::VectorDataType data_type =
                  data_model::VECTOR_DATA_TYPE_FLOAT32) noexcept
      : record_ref_(&vector_record),
        raw_vector_(vector_record.GetRawVector()),
        reciprocal_magnitude_(vector_record.GetReciprocalMagnitude()) {}

  QueryVector(VectorRecord &&vector_record, size_t vector_record_size = 0,
              bool normalize = false,
              data_model::VectorDataType data_type =
                  data_model::VECTOR_DATA_TYPE_FLOAT32) noexcept
      : owned_record_(std::move(vector_record)),
        raw_vector_(owned_record_.GetRawVector()),
        reciprocal_magnitude_(owned_record_.GetReciprocalMagnitude()) {}

  QueryVector(const QueryVector &other) noexcept
      : record_ref_(other.record_ref_),
        owned_record_(other.owned_record_),
        raw_vector_(other.raw_vector_),
        reciprocal_magnitude_(other.reciprocal_magnitude_) {}

  QueryVector(QueryVector &&other) noexcept = default;
  QueryVector &operator=(QueryVector &&other) noexcept = default;
  QueryVector &operator=(const QueryVector &other) noexcept {
    if (this != &other) {
      record_ref_ = other.record_ref_;
      owned_record_ = other.owned_record_;
      raw_vector_ = other.raw_vector_;
      reciprocal_magnitude_ = other.reciprocal_magnitude_;
    }
    return *this;
  }

  const VectorRecord &GetRecord() const noexcept {
    return record_ref_ != nullptr ? *record_ref_ : owned_record_;
  }

  const char *GetRawVector() const noexcept { return raw_vector_; }
  float GetReciprocalMagnitude() const noexcept {
    return reciprocal_magnitude_;
  }

  VectorRecord GetVectorRecord() const noexcept { return GetRecord(); }

 private:
  const VectorRecord *record_ref_{nullptr};
  VectorRecord owned_record_;
  const char *raw_vector_{nullptr};
  float reciprocal_magnitude_{1.0f};
};

template <typename T>
class VectorHNSW : public VectorType<T> {
 protected:
  // VectorType<T> is a dependent base, so inherited names are not found by
  // unqualified lookup. Re-declare the ones used below (including inside
  // thread-safety annotations, which cannot be written as member).
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
  using HNSWIndex = hnswlib::HierarchicalNSW<float, QueryVector, VectorRecord>;

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
    return space_.get();
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

  absl::StatusOr<std::vector<Neighbor>> SearchRange(
      absl::string_view query, float radius, cancel::Token &cancellation_token,
      float epsilon = 0.0f,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr)
      ABSL_LOCKS_EXCLUDED(resize_mutex_) override;

 protected:
  absl::Status ResizeIfFull() ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status AddRecordImpl(uint64_t internal_id,
                             VectorRecord &&vector_record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);

  absl::Status RemoveRecordImpl(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status ModifyRecordImpl(uint64_t internal_id,
                                VectorRecord &&vector_record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  void ToProtoImpl(data_model::VectorIndex *vector_index_proto) const override;
  int RespondWithInfoImpl(ValkeyModuleCtx *ctx) const override;
  absl::Status SaveIndexImpl(RDBChunkOutputStream chunked_out) const override;
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  float ComputeDistance(
      absl::string_view query, const VectorRecord &vector_record,
      float query_magnitude) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  VectorRecord &GetVectorLockFree(uint64_t internal_id) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    auto *ptr = algo_->GetPointLockFree(internal_id);
    CHECK(ptr != nullptr) << "Internal ID not found in label_lookup: "
                          << internal_id;
    return *ptr;
  }
  VectorRecord &GetVector(uint64_t internal_id) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
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
