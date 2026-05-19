/*
 * Copyright (c) 2025, valkey-search contributors
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
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#include <svs/runtime/api_defs.h>
#include <svs/runtime/dynamic_vamana_index.h>
#include <svs/runtime/training.h>

namespace valkey_search::indexes {

// Lifecycle of a VectorSVS index. Most compression kinds are kReady from
// construction. LeanVec defers SVS-graph construction until the buffered
// vectors reach the training threshold; until then the index is kStaging
// and search is rejected.
enum class SVSIndexState { kStaging, kReady };

// SVS Vamana build parameters exposed to FT.CREATE
struct SVSBuildConfig {
  size_t graph_max_degree = 64;
  size_t construction_window_size = 128;
  float alpha = 1.2f;
  size_t search_window_size = 10;
  data_model::SVSCompressionType compression =
      data_model::SVS_COMPRESSION_NONE;
  // LeanVec-only: target reduced dimensionality for the projection. 0 means
  // unset (only valid when compression is non-LeanVec).
  size_t leanvec_dims = 0;
  // LeanVec-only: number of vectors to accumulate before training matrices
  // and constructing the index. Default 10000.
  size_t leanvec_training_threshold = 10000;
};

// True when the compression type uses LeanVec, which requires a training
// set before the SVS index can be constructed.
inline bool IsLeanVecCompression(data_model::SVSCompressionType c) {
  return c == data_model::SVS_COMPRESSION_LEANVEC4X4 ||
         c == data_model::SVS_COMPRESSION_LEANVEC4X8 ||
         c == data_model::SVS_COMPRESSION_LEANVEC8X8;
}

template <typename T>
class VectorSVS : public VectorBase {
 public:
  static absl::StatusOr<std::shared_ptr<VectorSVS<T>>> Create(
      const data_model::VectorIndex& vector_index_proto,
      absl::string_view attribute_identifier,
      data_model::AttributeDataType attribute_data_type)
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

  ~VectorSVS() override;

  size_t GetDataTypeSize() const override { return sizeof(T); }
  int GetDimensions() const { return dimensions_; }
  size_t GetCapacity() const override ABSL_LOCKS_EXCLUDED(index_mutex_) {
    absl::ReaderMutexLock lock(&index_mutex_);
    return num_elements_;
  }

  absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token& cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<unsigned> search_window_size = std::nullopt)
      ABSL_LOCKS_EXCLUDED(index_mutex_);

 protected:
  absl::Status AddRecordImpl(uint64_t internal_id,
                             absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::Status RemoveRecordImpl(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  absl::Status ModifyRecordImpl(uint64_t internal_id,
                                absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  void ToProtoImpl(data_model::VectorIndex* vector_index_proto) const override;
  int RespondWithInfoImpl(ValkeyModuleCtx* ctx) const override;
  absl::Status SaveIndexImpl(RDBChunkOutputStream chunked_out) const override;
  absl::StatusOr<std::pair<float, hnswlib::labeltype>>
  ComputeDistanceFromRecordImpl(uint64_t internal_id,
                                absl::string_view query) const override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  char* GetValueImpl(uint64_t internal_id) const override
      ABSL_LOCKS_EXCLUDED(index_mutex_);
  void TrackVector(uint64_t internal_id,
                   const InternedStringPtr& vector) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);
  bool IsVectorMatch(uint64_t internal_id,
                     const InternedStringPtr& vector) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);
  void UnTrackVector(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(tracked_vectors_mutex_);

 private:
  VectorSVS(int dimensions, data_model::DistanceMetric distance_metric,
            const SVSBuildConfig& build_config,
            absl::string_view attribute_identifier,
            data_model::AttributeDataType attribute_data_type);

  // Flush buffered vectors to SVS graph
  absl::Status FlushBuffer() ABSL_EXCLUSIVE_LOCKS_REQUIRED(index_mutex_);

  // Train LeanVec matrices on the buffered vectors, build a
  // DynamicVamanaIndexLeanVec, and ingest the buffer as the first batch.
  // Transitions index_state_ from kStaging to kReady. Called from
  // AddRecordImpl when the buffer reaches leanvec_training_threshold.
  absl::Status TrainAndBuildLeanVecIndex()
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(index_mutex_);

  // SVS index (owned, destroyed via DynamicVamanaIndex::destroy)
  svs::runtime::v0::DynamicVamanaIndex* svs_index_
      ABSL_GUARDED_BY(index_mutex_){nullptr};
  SVSBuildConfig build_config_;
  size_t num_elements_ ABSL_GUARDED_BY(index_mutex_){0};

  // kStaging until LeanVec training completes; kReady from the start for
  // non-LeanVec compression types.
  SVSIndexState index_state_ ABSL_GUARDED_BY(index_mutex_){
      SVSIndexState::kReady};
  // LeanVec compression matrices (raw owning pointer; destroyed via
  // svs::runtime::v0::LeanVecTrainingData::destroy after the index is
  // built or in the destructor).
  svs::runtime::v0::LeanVecTrainingData* leanvec_training_data_
      ABSL_GUARDED_BY(index_mutex_){nullptr};

  mutable absl::Mutex index_mutex_;
  mutable absl::Mutex tracked_vectors_mutex_;
  absl::flat_hash_map<uint64_t, InternedStringPtr> tracked_vectors_
      ABSL_GUARDED_BY(tracked_vectors_mutex_);

  // Store raw vector data for retrieval and distance computation
  absl::flat_hash_map<uint64_t, std::vector<char>> raw_vectors_
      ABSL_GUARDED_BY(index_mutex_);

  // Space interface for distance computation in pre-filter path
  std::unique_ptr<hnswlib::SpaceInterface<T>> space_;

  // Buffering for benchmarking (simple 10K batch approach)
  static constexpr size_t kBufferSize = 10000;
  struct PendingInsert {
    uint64_t internal_id;
    std::vector<char> data;
  };
  std::vector<PendingInsert> pending_buffer_ ABSL_GUARDED_BY(index_mutex_);
  bool buffer_flushing_ ABSL_GUARDED_BY(index_mutex_){false};
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
