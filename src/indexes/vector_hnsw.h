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
#include <deque>
#include <memory>
#include <optional>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

template <typename T>
class VectorHNSWField : public VectorBaseField {
 public:
  static absl::StatusOr<std::shared_ptr<VectorHNSWField<T>>> Create(
      const data_model::VectorIndex& vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  static absl::StatusOr<std::shared_ptr<VectorHNSWField<T>>> LoadFromRDB(
      ValkeyModuleCtx* ctx, const AttributeDataType* attribute_data_type,
      const data_model::VectorIndex& vector_index_proto,
      absl::string_view attribute_identifier,
      SupplementalContentChunkIter&& iter) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  ~VectorHNSWField() override = default;
  size_t GetDataTypeSize() const override { return sizeof(T); }

  const hnswlib::SpaceInterface<float>* GetSpace() const {
    return space_.get();
  }

  int GetDimensions() const { return dimensions_; }
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

  absl::StatusOr<std::deque<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token& cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<size_t> ef_runtime = std::nullopt)
      ABSL_LOCKS_EXCLUDED(resize_mutex_);

 protected:
  absl::Status ResizeIfFull() ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status AddRecordImpl(uint64_t internal_id,
                             absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);

  absl::Status RemoveRecordImpl(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status ModifyRecordImpl(uint64_t internal_id,
                                absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  void ToProtoImpl(data_model::VectorIndex* vector_index_proto) const override;
  int RespondWithInfoImpl(ValkeyModuleCtx* ctx) const override;
  absl::Status SaveIndexImpl(RDBChunkOutputStream chunked_out) const override;
  absl::StatusOr<std::pair<float, hnswlib::labeltype>>
  ComputeDistanceFromRecordImpl(uint64_t internal_id, absl::string_view query)
      const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  char* GetValueImpl(uint64_t internal_id) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return algo_->getPoint(internal_id);
  }
  bool IsVectorMatch(uint64_t internal_id,
                     const InternedStringPtr& vector) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);
  void TrackVector(uint64_t internal_id,
                   const InternedStringPtr& vector) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);
  void UnTrackVector(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);

 private:
  VectorHNSWField(int dimensions, absl::string_view attribute_identifier,
                  data_model::AttributeDataType attribute_data_type);
  std::unique_ptr<hnswlib::HierarchicalNSW<T>> algo_
      ABSL_GUARDED_BY(resize_mutex_);
  std::unique_ptr<hnswlib::SpaceInterface<T>> space_;
  mutable absl::Mutex resize_mutex_;
  mutable absl::Mutex tracked_vectors_mutex_;
  std::deque<InternedStringPtr> tracked_vectors_
      ABSL_GUARDED_BY(tracked_vectors_mutex_);
};

}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_HNSW_H_
