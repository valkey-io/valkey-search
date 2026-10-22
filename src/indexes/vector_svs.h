/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/node_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
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
struct svs_threadpool_interface_ops;
struct svs_threadpool_interface;
}

namespace valkey_search::indexes {

#if defined(__linux__) && defined(__x86_64__)
// Sequential threadpool vtable installed on every SVS builder; exposed for
// the unit tests.
extern svs_threadpool_interface_ops kSvsThreadpoolOps;
extern svs_threadpool_interface kSvsThreadpoolIface;
#endif

// Build-time configuration retained on the class so the first HSET can
// bootstrap the SVS index once it has an actual vector to build with;
// the SVS C API's svs_index_build_dynamic requires num_vectors > 0.
struct SVSBuildConfig {
  uint32_t graph_max_degree{0};
  uint32_t construction_window_size{0};
  uint32_t search_window_size{0};
  float alpha{0.0f};
  data_model::SVSCompressionType compression{data_model::SVS_COMPRESSION_NONE};
  // Element type of the input vectors; with COMPRESSION NONE, SVS stores
  // that type.
  data_model::VectorDataType input_type{data_model::VECTOR_DATA_TYPE_FLOAT32};
  // SVS allocation block for data and graph, sized from INITIAL_CAP.
  size_t blocksize_bytes{0};
  // INITIAL_CAP as given at FT.CREATE, reported by GetCapacity.
  uint32_t initial_cap{0};
  // For a compression that trains on the data (SQ8): the index stays
  // uncompressed until it holds this many vectors, then converts. 0 compresses
  // from the first add.
  uint32_t training_threshold{0};
};

// Routes SVS's default logger to the Valkey log. Call once at module load.
void InitSvsLogging();

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
  // Rejects vectors with non-finite components: SVS's graph search stalls on
  // NaN distances.
  bool IsValidVectorData(absl::string_view record) const override;
  absl::StatusOr<std::vector<Neighbor>> SearchRange(
      absl::string_view query, float radius, cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr) override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;

 protected:
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
  float ComputeDistance(
      absl::string_view query, const VectorRecord &vector_record,
      float query_magnitude) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  VectorRecord &GetVectorLockFree(uint64_t internal_id) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  // LoadFromRDB leaves this map empty (saves consolidate SVS's soft
  // deletes away), so every label LoadTrackedKeys re-encounters is new to
  // it; try_emplace instead of the base class's CHECK.
  VectorRecord &GetOrCreateVectorLockFree(uint64_t internal_id) override
      ABSL_LOCKS_EXCLUDED(label_mutex_);
  VectorRecord &GetVector(uint64_t internal_id) const override
      ABSL_NO_THREAD_SAFETY_ANALYSIS;
  std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  uint64_t GetMaxLoadedLabel() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  // Rebuilds the SVS index from the tracked vectors when LoadFromRDB skipped
  // a payload written by a different SVS version.
  absl::Status OnTrackedKeysLoaded() override
      ABSL_LOCKS_EXCLUDED(resize_mutex_, label_mutex_);
  size_t GetLabelCount() const override ABSL_NO_THREAD_SAFETY_ANALYSIS;

 private:
  VectorSVS(int dimensions, absl::string_view attribute_identifier,
            data_model::AttributeDataType attribute_data_type, int db_num);

#if defined(__linux__) && defined(__x86_64__)
  // Up to `count` nearest neighbors of `query` as (distance, label), shared by
  // Search and SearchRange. `filter` may be null and is not consumed.
  absl::StatusOr<std::priority_queue<std::pair<float, hnswlib::labeltype>>>
  SearchKnn(absl::string_view query, uint64_t count,
            hnswlib::BaseFilterFunctor *filter,
            std::optional<size_t> ef_runtime) ABSL_NO_THREAD_SAFETY_ANALYSIS;

  // Adds one vector, building the index from it when there is no handle yet
  // (writer lock only for that first build, double-checked).
  absl::Status InsertVector(uint64_t internal_id, const float *fp32)
      ABSL_LOCKS_EXCLUDED(resize_mutex_);
  absl::Status AddPoint(uint64_t internal_id, const float *fp32)
      ABSL_SHARED_LOCKS_REQUIRED(resize_mutex_);
  // The configuration of the storage svs_index_ uses now: build_config_, or
  // its uncompressed form while compression_pending_.
  SVSBuildConfig StorageConfig() const;
  // Converts the index to build_config_'s compression once it holds
  // training_threshold vectors. SVS keeps the graph and calibrates the
  // compression on every vector in the index. A failed conversion is logged,
  // counted, and retried on the next add.
  void MaybeConvert() ABSL_LOCKS_EXCLUDED(resize_mutex_, label_mutex_);
  // SVS reuses deleted slots only after consolidating and compacting. Does
  // both once enough slots have been deleted, so update-heavy workloads stay
  // bounded.
  void MaybeConsolidate() ABSL_LOCKS_EXCLUDED(resize_mutex_, label_mutex_);
#endif

  // Concurrency: the index is built with SVS_SYNC_KIND_FINE_GRAIN and guards
  // itself, so SVS calls run under the reader side of resize_mutex_ (as in
  // VectorHNSW); the writer side is taken only to install the first handle.
  // label_to_record_ is guarded by label_mutex_ (acquired after resize_mutex_,
  // never held across an SVS call) and is a node_hash_map so references from
  // GetVector survive concurrent inserts. Search relies on the index-schema
  // phase gate, so GetVectorLockFree stays lock-free and the delete-then-add
  // gap inside modify is never observed.
  svs_index_h svs_index_ ABSL_GUARDED_BY(resize_mutex_){nullptr};
  // Mutations hold the reader side of resize_mutex_ when SVS guards the index
  // itself (built with SVS_SYNC_KIND_FINE_GRAIN). An index restored from RDB
  // comes back with SVS_SYNC_KIND_NONE at the pinned revision (the stream load
  // takes no parameters), so its mutations take the writer side instead and
  // stay serialized, as before. Set once in LoadFromRDB before the instance is
  // published; read on every mutation.
  std::atomic<bool> mutations_exclusive_{false};
  // True while svs_index_ (or the index the next add builds) is uncompressed,
  // awaiting MaybeConvert. Written under the writer side of resize_mutex_;
  // atomic so adds can check it without the lock.
  std::atomic<bool> compression_pending_{false};
  // Set by LoadFromRDB when the saved SVS payload was skipped; consumed by
  // OnTrackedKeysLoaded on the main thread before the index is published.
  bool rebuild_from_keyspace_{false};
  // Slots deleted in SVS since the last consolidation (or save, which also
  // consolidates).
  mutable std::atomic<size_t> deleted_since_consolidate_{0};
  SVSBuildConfig build_config_;
  mutable absl::Mutex label_mutex_ ABSL_ACQUIRED_AFTER(resize_mutex_);
  mutable absl::node_hash_map<uint64_t, VectorRecord> label_to_record_
      ABSL_GUARDED_BY(label_mutex_);

  friend class VectorSVSTestPeer;
};

}  // namespace valkey_search::indexes
#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_SVS_H_
