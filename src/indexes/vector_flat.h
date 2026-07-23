/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_FLAT_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_FLAT_H_

#include <cstddef>
#include <cstdint>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/bruteforce.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

template <typename T>
class VectorFlat : public VectorBase {
 public:
  using FlatIndex =
      hnswlib::BruteforceSearch<T, std::shared_ptr<const VectorRecord>>;

  static absl::StatusOr<std::shared_ptr<VectorFlat<T>>> Create(
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  static absl::StatusOr<std::shared_ptr<VectorFlat<T>>> LoadFromRDB(
      ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
      const data_model::VectorIndex &vector_index_proto,
      absl::string_view attribute_identifier,
      SupplementalContentChunkIter &&iter) ABSL_NO_THREAD_SAFETY_ANALYSIS;
  ~VectorFlat() override = default;
  size_t GetDataTypeSize() const override { return sizeof(T); }

  const hnswlib::SpaceInterface<float> *GetSpace() const {
    return space_.get();
  }
  int GetBlockSize() const { return block_size_; }
  // Lock-free search optimization: Reading capacity is thread-safe because
  // ChunkedArray::element_count_ is implemented as an atomic variable.
  size_t GetCapacity() const override ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return algo_->data_->getCapacity();
  }
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr)
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
  T ComputeDistance(absl::string_view query, const VectorRecord *vector_record,
                    float query_magnitude) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  std::shared_ptr<const VectorRecord> &GetVectorLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  VectorFlat(int dimensions, data_model::DistanceMetric distance_metric,
             uint32_t block_size, absl::string_view attribute_identifier,
             data_model::AttributeDataType attribute_data_type);

 private:
  std::unique_ptr<FlatIndex> algo_ ABSL_GUARDED_BY(resize_mutex_);
  std::unique_ptr<hnswlib::SpaceInterface<T>> space_;
  const uint32_t block_size_;
};
}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_FLAT_H_
