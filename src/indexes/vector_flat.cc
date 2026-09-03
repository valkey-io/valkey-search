/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_flat.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <string>
#include <type_traits>
#include <utility>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Note that the ordering matters here - we want to minimize the memory
// overrides to just the hnswlib code.
// clang-format off
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#include "third_party/hnswlib/bruteforce.h"
#include "third_party/hnswlib/hnswlib.h"
// clang-format on

namespace valkey_search::indexes {

template <typename T>
absl::StatusOr<std::shared_ptr<VectorFlat<T>>> VectorFlat<T>::Create(
    const data_model::VectorIndex &vector_index_proto,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type, int db_num) {
  try {
    auto index = std::shared_ptr<VectorFlat<T>>(
        new VectorFlat<T>(vector_index_proto.dimension_count(),
                          vector_index_proto.distance_metric(),
                          vector_index_proto.flat_algorithm().block_size(),
                          attribute_identifier, attribute_data_type, db_num));
    index->Init(vector_index_proto.dimension_count(),
                vector_index_proto.distance_metric(), index->space_);
    index->algo_ =
        std::make_unique<FlatIndex>(index->space_.get(), index->normalize_,
                                    vector_index_proto.initial_cap());
    return index;
  } catch (const std::exception &e) {
    ++Metrics::GetStats().flat_create_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while creating a FLAT index: ", e.what()));
  }
}

template <typename T>
std::optional<hnswlib::tableint> VectorFlat<T>::GetAlgoIdLockFree(
    uint64_t internal_id) const {
  auto search = algo_->dict_external_to_internal.find(internal_id);
  if (search == algo_->dict_external_to_internal.end()) {
    return std::nullopt;
  }
  return search->second;
}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorFlat<T>>> VectorFlat<T>::LoadFromRDB(
    ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
    const data_model::VectorIndex &vector_index_proto,
    absl::string_view attribute_identifier, SupplementalContentChunkIter &&iter,
    int db_num) {
  try {
    auto index = std::shared_ptr<VectorFlat<T>>(
        new VectorFlat<T>(vector_index_proto.dimension_count(),
                          vector_index_proto.distance_metric(),
                          vector_index_proto.flat_algorithm().block_size(),
                          attribute_identifier, attribute_data_type->ToProto(),
                          db_num),
        vmsdk::DestructByMainThread<VectorFlat<T>>{});
    index->Init(vector_index_proto.dimension_count(),
                vector_index_proto.distance_metric(), index->space_);
    index->algo_ =
        std::make_unique<FlatIndex>(index->space_.get(), index->normalize_);
    RDBChunkInputStream input(std::move(iter));

    auto generator = [allocator = index->GetVectorAllocator()](
                         absl::string_view vector_data) {
      T reciprocal_magnitude = CalcReciprocalMagnitude(
          reinterpret_cast<const T *>(vector_data.data()),
          vector_data.size() / sizeof(T));
      return VectorRecord::Construct(
          vector_data, reciprocal_magnitude,
          static_cast<FixedSizeAllocator *>(allocator));
    };
    VMSDK_RETURN_IF_ERROR(
        index->algo_->LoadIndex(input, index->space_.get(), generator));
    return index;
  } catch (const std::exception &e) {
    ++Metrics::GetStats().flat_create_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while loading a FLAT index: ", e.what()));
  }
}

template <typename T>
VectorFlat<T>::VectorFlat(
    int dimensions, valkey_search::data_model::DistanceMetric distance_metric,
    uint32_t block_size, absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type, int db_num)
    : VectorBase(IndexerType::kFlat, dimensions, attribute_data_type,
                 attribute_identifier, db_num),
      block_size_(block_size) {}

template <typename T>
absl::Status VectorFlat<T>::ResizeIfFull() {
  {
    absl::ReaderMutexLock lock(&resize_mutex_);
    if (algo_->cur_element_count_ < GetCapacity()) {
      return absl::OkStatus();
    }
  }
  absl::WriterMutexLock lock(&resize_mutex_);
  std::unique_lock<std::mutex> index_lock(algo_->index_lock);
  if (algo_->cur_element_count_ == GetCapacity()) {
    if (block_size_ == 0) {
      return absl::InternalError("Cannot resize FLAT index: block_size is 0");
    }
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "Resizing FLAT Index, current size: " << GetCapacity()
        << ", expand by: " << block_size_;
    algo_->resizeIndex(GetCapacity() + block_size_);
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status VectorFlat<T>::AddRecordImpl(
    uint64_t internal_id, std::shared_ptr<const VectorRecord> &&vector_record) {
  do {
    try {
      absl::ReaderMutexLock lock(&resize_mutex_);
      algo_->addPoint(std::move(vector_record), internal_id);
    } catch (const std::exception &e) {
      ++Metrics::GetStats().flat_add_exceptions_cnt;
      std::string error_msg = e.what();
      if (absl::StrContains(
              error_msg,
              "The number of elements exceeds the specified limit")) {
        VMSDK_RETURN_IF_ERROR(ResizeIfFull());
        continue;
      }
      return absl::InternalError(
          absl::StrCat("Error while adding a record: ", e.what()));
    }
    return absl::OkStatus();
  } while (true);
}

template <typename T>
absl::Status VectorFlat<T>::ModifyRecordImpl(
    uint64_t internal_id, std::shared_ptr<const VectorRecord> &&vector_record) {
  absl::ReaderMutexLock lock(&resize_mutex_);
  std::shared_ptr<const VectorRecord> *stored_record =
      algo_->GetPoint(internal_id);
  if (!stored_record) {
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }

  *stored_record = std::move(vector_record);

  return absl::OkStatus();
}

template <typename T>
absl::Status VectorFlat<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);
    algo_->removePoint(internal_id);
  } catch (const std::exception &e) {
    ++Metrics::GetStats().flat_remove_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while removing a FLAT record: ", e.what()));
  }
  return absl::OkStatus();
}

// Paper over the impedance mismatch between the
// cancel::Token and hnswlib::BaseCancellationFunctor.
class CancelCondition : public hnswlib::BaseCancellationFunctor {
 public:
  explicit CancelCondition(cancel::Token &token) : token_(token) {
    CHECK(&token);
  }
  bool isCancelled() override { return token_->IsCancelled(); }

 private:
  cancel::Token &token_;
};

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorFlat<T>::Search(
    absl::string_view query, uint64_t count, cancel::Token &cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter,
    bool enable_partial_results) {
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Error parsing vector similarity query: query vector blob size (",
        query.size(), ") does not match index's expected size (",
        dimensions_ * GetDataTypeSize(), ")."));
  }
  float reciprocal_magnitude =
      normalize_
          ? CalcReciprocalMagnitude(
                reinterpret_cast<const float *>(query.data()), dimensions_)
          : 1.0f;

  try {
    CancelCondition canceler(cancellation_token);
    auto embedding = VectorRecord::Construct(query, reciprocal_magnitude);
    auto res = algo_->searchKnn(
        embedding,
        std::min(count, static_cast<uint64_t>(algo_->cur_element_count_)),
        filter.get(), &canceler);

    if (!enable_partial_results && cancellation_token->IsCancelled()) {
      return absl::CancelledError(query::kTimeoutMsg);
    }
    return CreateReply(res);
  } catch (const std::exception &e) {
    Metrics::GetStats().flat_search_exceptions_cnt.fetch_add(
        1, std::memory_order_relaxed);
    return absl::InternalError(e.what());
  }
}

template <typename T>
T VectorFlat<T>::ComputeDistance(absl::string_view query,
                                 const VectorRecord *vector_record,
                                 float query_magnitude) const {
  return algo_->fstdistfunc_(query.data(), vector_record->GetRawVector(),
                             algo_->dist_func_param_, query_magnitude);
}

// Linear scan over all tracked keys. This is O(N) but correct for flat
// indexes which have no graph structure to exploit.
template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorFlat<T>::SearchRange(
    absl::string_view query, float radius, cancel::Token &cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter) {
  auto nq = NormalizeQueryIfNeeded(query);

  std::vector<Neighbor> neighbors;
  // Pre-allocate to reduce re-allocations during the linear scan.
  // The actual match count is unknown ahead of time, so use a modest initial
  // capacity that covers typical result sets without over-allocating.
  neighbors.reserve(128);
  auto status =
      ForEachTrackedKey([&](const InternedStringPtr &key) -> absl::Status {
        if (cancellation_token->IsCancelled()) {
          return absl::CancelledError("SearchRange cancelled");
        }
        auto dist_result = ComputeDistanceFromRecord(key, nq.view);
        if (!dist_result.ok()) {
          return absl::OkStatus();
        }
        if (filter && !(*filter)(dist_result->second)) {
          return absl::OkStatus();
        }
        float clamped_dist = ClampCosineDistance(dist_result->first);
        if (clamped_dist <= radius) {
          neighbors.emplace_back(key, clamped_dist);
        }
        return absl::OkStatus();
      });
  // Cancellation is expected (the inner lambda returns CancelledError to
  // exit the iteration loop early); only propagate real errors.
  if (!status.ok() && !absl::IsCancelled(status)) {
    return status;
  }
  return neighbors;
}

template <typename T>
void VectorFlat<T>::ToProtoImpl(
    data_model::VectorIndex *vector_index_proto) const {
  data_model::VectorDataType data_type;
  if constexpr (std::is_same_v<T, float>) {
    data_type = data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32;
  } else {
    DCHECK(false) << "Unsupported type: " << typeid(T).name();
    data_type = data_model::VectorDataType::VECTOR_DATA_TYPE_UNSPECIFIED;
  }
  vector_index_proto->set_vector_data_type(data_type);

  auto flat_algorithm_proto = std::make_unique<data_model::FlatAlgorithm>();
  flat_algorithm_proto->set_block_size(block_size_);
  vector_index_proto->set_allocated_flat_algorithm(
      flat_algorithm_proto.release());
}

template <typename T>
int VectorFlat<T>::RespondWithInfoImpl(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "data_type");
  if constexpr (std::is_same_v<T, float>) {
    ValkeyModule_ReplyWithSimpleString(
        ctx,
        std::string(LookupKeyByValue(
                        *kVectorDataTypeByStr,
                        data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32))
            .c_str());
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "UNKNOWN");
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  ValkeyModule_ReplyWithArray(ctx, 4);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      std::string(LookupKeyByValue(
                      *kVectorAlgoByStr,
                      data_model::VectorIndex::AlgorithmCase::kFlatAlgorithm))
          .c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "block_size");
  ValkeyModule_ReplyWithLongLong(ctx, block_size_);

  return 4;
}

template <typename T>
absl::Status VectorFlat<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&resize_mutex_);
  auto serializer = [normalize = normalize_, vector_size = GetVectorDataSize()](
                        const std::shared_ptr<const VectorRecord> &record) {
    if (normalize) {
      return NormalizeVector(
          absl::string_view(record->GetRawVector(), vector_size));
    }
    return std::vector<char>(record->GetRawVector(),
                             record->GetRawVector() + vector_size);
  };
  return algo_->SaveIndex(chunked_out, serializer);
}

template <typename T>
std::shared_ptr<const VectorRecord> &VectorFlat<T>::GetVectorLockFree(
    uint64_t internal_id) const {
  return *algo_->GetPointLockFree(internal_id);
}

template <typename T>
std::shared_ptr<const VectorRecord> &VectorFlat<T>::GetVector(
    uint64_t internal_id) const {
  return *algo_->GetPoint(internal_id);
}

template class VectorFlat<float>;

}  // namespace valkey_search::indexes
