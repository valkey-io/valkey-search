/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_hnsw.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <queue>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search.h"
#include "valkey_search_options.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Note that the ordering matters here - we want to minimize the memory
// overrides to just the hnswlib code.
// clang-format off
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/hnswlib.h"
// clang-format on

namespace hnswlib_helpers {

template <typename T>
std::optional<hnswlib::tableint> GetInternalIdLockFree(
    hnswlib::HierarchicalNSW<T> *algo, uint64_t internal_id) {
  auto search = algo->label_lookup_.find(internal_id);
  if (search == algo->label_lookup_.end() ||
      algo->isMarkedDeleted(search->second)) {
    return std::nullopt;
  }
  return search->second;
}

template <typename T>
std::optional<hnswlib::tableint> GetInternalId(
    hnswlib::HierarchicalNSW<T> *algo, uint64_t internal_id) {
  std::unique_lock<std::mutex> lock_table(algo->label_lookup_lock);
  return GetInternalIdLockFree(algo, internal_id);
}

template <typename T>
std::optional<hnswlib::tableint> GetInternalIdDuringSearch(
    hnswlib::HierarchicalNSW<T> *algo, uint64_t internal_id) {
  return GetInternalIdLockFree(algo, internal_id);
}
}  // namespace hnswlib_helpers

namespace valkey_search::indexes {

template <typename T>
absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> VectorHNSW<T>::Create(
    const data_model::VectorIndex &vector_index_proto,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type) {
  try {
    auto index = std::shared_ptr<VectorHNSW<T>>(
        new VectorHNSW<T>(vector_index_proto.dimension_count(),
                          attribute_identifier, attribute_data_type));
    index->Init(vector_index_proto.dimension_count(),
                vector_index_proto.distance_metric(), index->space_);
    const auto &hnsw_proto = vector_index_proto.hnsw_algorithm();
    index->algo_ = std::make_unique<hnswlib::HierarchicalNSW<T>>(
        index->space_.get(), vector_index_proto.initial_cap(), hnsw_proto.m(),
        hnsw_proto.ef_construction());
    index->algo_->setEf(hnsw_proto.ef_runtime());
    // Notes:
    // 1. Not allowing replace delete is aligned with RediSearch
    // 2. Consider making allow_replace_deleted_ configurable
    index->algo_->allow_replace_deleted_ = false;
    return index;
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_create_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("HNSWLib error while creating a record: ", e.what()));
  }
}

template <typename T>
void VectorHNSW<T>::TrackVector(uint64_t internal_id,
                                const InternedStringPtr &vector) {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_.push_back(vector);
}

template <typename T>
bool VectorHNSW<T>::IsVectorMatch(uint64_t internal_id,
                                  const InternedStringPtr &vector) {
  absl::ReaderMutexLock lock(&resize_mutex_);
  {
    std::unique_lock<std::mutex> lock_label(
        algo_->getLabelOpMutex(internal_id));
    auto id = hnswlib_helpers::GetInternalId(algo_.get(), internal_id);
    if (!id.has_value()) {
      return false;
    }
    char *data_ptrv = algo_->getDataByInternalId(*id);
    size_t dim = *((size_t *)algo_->dist_func_param_);
    absl::string_view record(data_ptrv, dim * sizeof(T));
    return vector->Str() == record;
  }
}
// UnTrackVector does not delete the vector in VectorHNSW, as vectors are never
// physically removed from the graph—only marked as deleted.
template <typename T>
void VectorHNSW<T>::UnTrackVector(uint64_t internal_id) {}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> VectorHNSW<T>::LoadFromRDB(
    ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
    const data_model::VectorIndex &vector_index_proto,
    absl::string_view attribute_identifier,
    SupplementalContentChunkIter &&iter) {
  try {
    auto index = std::shared_ptr<VectorHNSW<T>>(new VectorHNSW<T>(
        vector_index_proto.dimension_count(), attribute_identifier,
        attribute_data_type->ToProto()));
    index->Init(vector_index_proto.dimension_count(),
                vector_index_proto.distance_metric(), index->space_);

    index->algo_ =
        std::make_unique<hnswlib::HierarchicalNSW<T>>(index->space_.get());
    // initial_cap needs to be provided to retain the original initial_cap if
    // the index being loaded is empty.

    RDBChunkInputStream input(std::move(iter));
    VMSDK_RETURN_IF_ERROR(
        index->algo_->LoadIndex(input, index->space_.get(),
                                vector_index_proto.initial_cap(), index.get()));
    // ef_runtime is not persisted in the index contents
    index->algo_->setEf(vector_index_proto.hnsw_algorithm().ef_runtime());
    // Notes:
    // 1. Not allowing replace delete is aligned with RediSearch
    // 2. Consider making allow_replace_deleted_ configurable
    index->algo_->allow_replace_deleted_ = false;
    return index;
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_create_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("HNSWLib error while loading an index: ", e.what()));
  }
}

template <typename T>
VectorHNSW<T>::VectorHNSW(int dimensions,
                          absl::string_view attribute_identifier,
                          data_model::AttributeDataType attribute_data_type)
    : VectorBase(IndexerType::kHNSW, dimensions, attribute_data_type,
                 attribute_identifier) {}

template <typename T>
absl::Status VectorHNSW<T>::AddRecordImpl(uint64_t internal_id,
                                          absl::string_view record) {
  do {
    try {
      absl::ReaderMutexLock lock(&resize_mutex_);

      algo_->addPoint((T *)record.data(), internal_id);
      return absl::OkStatus();
    } catch (const std::exception &e) {
      std::string error_msg = e.what();
      if (absl::StrContains(
              error_msg,
              "The number of elements exceeds the specified limit")) {
        VMSDK_RETURN_IF_ERROR(ResizeIfFull());
        continue;
      }
      ++Metrics::GetStats().hnsw_add_exceptions_cnt;
      return absl::InternalError(
          absl::StrCat("Error while adding a record: ", e.what()));
    }
  } while (true);
}

template <typename T>
int VectorHNSW<T>::RespondWithInfoImpl(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "data_type");
  if constexpr (std::is_same_v<T, float>) {
    ValkeyModule_ReplyWithSimpleString(
        ctx,
        LookupKeyByValue(*kVectorDataTypeByStr,
                         data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32)
            .data());
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "UNKNOWN");
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  ValkeyModule_ReplyWithArray(ctx, 8);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      LookupKeyByValue(*kVectorAlgoByStr,
                       data_model::VectorIndex::AlgorithmCase::kHnswAlgorithm)
          .data());
  ValkeyModule_ReplyWithSimpleString(ctx, "m");
  absl::ReaderMutexLock lock(&resize_mutex_);
  ValkeyModule_ReplyWithLongLong(ctx, GetM());
  ValkeyModule_ReplyWithSimpleString(ctx, "ef_construction");
  ValkeyModule_ReplyWithLongLong(ctx, GetEfConstruction());
  ValkeyModule_ReplyWithSimpleString(ctx, "ef_runtime");
  ValkeyModule_ReplyWithLongLong(ctx, GetEfRuntime());
  return 4;
}

template <typename T>
absl::Status VectorHNSW<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&resize_mutex_);
  return algo_->SaveIndex(chunked_out);
}

template <typename T>
absl::Status VectorHNSW<T>::ResizeIfFull() {
  {
    absl::ReaderMutexLock lock(&resize_mutex_);
    if (algo_->getCurrentElementCount() < algo_->getMaxElements() ||
        (algo_->allow_replace_deleted_ && algo_->getDeletedCount() > 0)) {
      return absl::OkStatus();
    }
  }
  try {
    absl::WriterMutexLock lock(&resize_mutex_);
    if (algo_->getCurrentElementCount() == algo_->getMaxElements() &&
        (!algo_->allow_replace_deleted_ || algo_->getDeletedCount() == 0)) {
      vmsdk::StopWatch stop_watch;
      auto max_elements = algo_->getMaxElements();
      // Notes
      // 1. Currently HNSWLib doesn't provide a way to shrink an index after
      // it was expanded.
      // 2. Once multithreaded is supported we'll have to make sure that no
      // thread is reading/writing during resize
      auto block_size = ValkeySearch::Instance().GetHNSWBlockSize();
      algo_->resizeIndex(algo_->getMaxElements() + block_size);
      VMSDK_LOG(WARNING, nullptr)
          << "Resizing HNSW Index, current size: " << max_elements
          << ", expand by: " << block_size << ", resize time took: "
          << absl::FormatDuration(stop_watch.Duration());
    }
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_add_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while adding a record: ", e.what()));
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status VectorHNSW<T>::ModifyRecordImpl(uint64_t internal_id,
                                             absl::string_view record) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);
    // TODO - an alternative approach is to call HierarchicalNSW::updatePoint.
    // The concern with calling updatePoint is that it might have implications
    // on the search accuracy. Need to revisit this in the future.
    algo_->markDelete(internal_id);
    algo_->addPoint((T *)record.data(), internal_id);
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_modify_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while modifying a record: ", e.what()));
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status VectorHNSW<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);
    algo_->markDelete(internal_id);
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_remove_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while removing a record: ", e.what()));
  }
  return absl::OkStatus();
}

// Paper over the impedance mismatch between the
// cancel::Token and hnswlib::BaseCancellationFunctor.
class CancelCondition : public hnswlib::BaseCancellationFunctor {
 public:
  explicit CancelCondition(cancel::Token &token) : token_(token) {}
  bool isCancelled() override { return token_->IsCancelled(); }

 private:
  cancel::Token &token_;
};

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorHNSW<T>::Search(
    absl::string_view query, uint64_t count, cancel::Token &cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter,
    std::optional<size_t> ef_runtime, bool enable_partial_results) {
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Error parsing vector similarity query: query vector blob size (",
        query.size(), ") does not match index's expected size (",
        dimensions_ * GetDataTypeSize(), ")."));
  }
  auto perform_search = [this, count, &filter, enable_partial_results,
                         &ef_runtime,
                         &cancellation_token](absl::string_view query)
                            ABSL_NO_THREAD_SAFETY_ANALYSIS
      -> absl::StatusOr<std::priority_queue<std::pair<T, hnswlib::labeltype>>> {
    try {
      CancelCondition cancel_condition(cancellation_token);
      auto res = algo_->searchKnn((T *)query.data(), count, ef_runtime,
                                  filter.get(), &cancel_condition);
      if (!enable_partial_results && cancellation_token->IsCancelled()) {
        return absl::CancelledError(
            "Search operation cancelled due to timeout");
      }
      return res;
    } catch (const std::exception &e) {
      Metrics::GetStats().hnsw_search_exceptions_cnt.fetch_add(
          1, std::memory_order_relaxed);
      return absl::InternalError(e.what());
    }
  };
  if (normalize_) {
    auto norm_record = NormalizeEmbedding(query, GetDataTypeSize());
    VMSDK_ASSIGN_OR_RETURN(
        auto search_result,
        perform_search(absl::string_view((const char *)norm_record.data(),
                                         norm_record.size())));
    return CreateReply(search_result);
  }
  VMSDK_ASSIGN_OR_RETURN(auto search_result, perform_search(query));
  return CreateReply(search_result);
}

template <typename T>
void VectorHNSW<T>::ToProtoImpl(
    data_model::VectorIndex *vector_index_proto) const {
  data_model::VectorDataType data_type;
  if constexpr (std::is_same_v<T, float>) {
    data_type = data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32;
  } else {
    DCHECK(false) << "Unsupported type: " << typeid(T).name();
    data_type = data_model::VectorDataType::VECTOR_DATA_TYPE_UNSPECIFIED;
  }
  vector_index_proto->set_vector_data_type(data_type);
  absl::ReaderMutexLock lock(&resize_mutex_);
  auto hnsw_algorithm_proto = std::make_unique<data_model::HNSWAlgorithm>();
  hnsw_algorithm_proto->set_ef_construction(GetEfConstruction());
  hnsw_algorithm_proto->set_ef_runtime(GetEfRuntime());
  hnsw_algorithm_proto->set_m(GetM());
  vector_index_proto->set_allocated_hnsw_algorithm(
      hnsw_algorithm_proto.release());
}

template <typename T>
absl::StatusOr<std::pair<float, hnswlib::labeltype>>
VectorHNSW<T>::ComputeDistanceFromRecordImpl(uint64_t internal_id,
                                             absl::string_view query) const {
  auto id =
      hnswlib_helpers::GetInternalIdDuringSearch(algo_.get(), internal_id);
  if (!id.has_value()) {
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }
  return (std::pair<float, hnswlib::labeltype>){
      algo_->fstdistfunc_((T *)query.data(), algo_->getDataByInternalId(*id),
                          algo_->dist_func_param_),
      internal_id};
}

template class VectorHNSW<float>;

}  // namespace valkey_search::indexes
