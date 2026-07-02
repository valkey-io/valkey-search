/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_flat.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <string>
#include <string_view>
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
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
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
    data_model::AttributeDataType attribute_data_type) {
  try {
    auto index = std::shared_ptr<VectorFlat<T>>(
        new VectorFlat<T>(vector_index_proto.dimension_count(),
                          vector_index_proto.distance_metric(),
                          vector_index_proto.flat_algorithm().block_size(),
                          attribute_identifier, attribute_data_type),
        vmsdk::DestructByMainThread<VectorFlat<T>>{});
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
    absl::string_view attribute_identifier,
    SupplementalContentChunkIter &&iter) {
  try {
    auto index = std::shared_ptr<VectorFlat<T>>(
        new VectorFlat<T>(vector_index_proto.dimension_count(),
                          vector_index_proto.distance_metric(),
                          vector_index_proto.flat_algorithm().block_size(),
                          attribute_identifier, attribute_data_type->ToProto()),
        vmsdk::DestructByMainThread<VectorFlat<T>>{});
    index->Init(vector_index_proto.dimension_count(),
                vector_index_proto.distance_metric(), index->space_);
    index->algo_ =
        std::make_unique<FlatIndex>(index->space_.get(), index->normalize_);
    RDBChunkInputStream input(std::move(iter));

    auto generator = [](absl::string_view vector_data) {
      return std::shared_ptr<VectorRecord>(
          nullptr);  // Placeholder, will be replaced in LoadTrackedKeys.
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
    data_model::AttributeDataType attribute_data_type)
    : VectorBase(IndexerType::kFlat, dimensions, attribute_data_type,
                 attribute_identifier),
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
    uint64_t internal_id, const std::shared_ptr<VectorRecord> &vector_record,
    [[maybe_unused]] const std::vector<char> &norm_record) {
  do {
    try {
      absl::ReaderMutexLock lock(&resize_mutex_);
      algo_->addPoint(vector_record, internal_id);
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
    uint64_t internal_id, const std::shared_ptr<VectorRecord> &vector_record,
    [[maybe_unused]] const std::vector<char> &norm_record) {
  absl::ReaderMutexLock lock(&resize_mutex_);
  std::unique_lock<std::mutex> index_lock(algo_->index_lock);
  std::shared_ptr<VectorRecord> *stored_record =
      algo_->getPointByExternalId(internal_id);
  if (!stored_record) {
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }

  *stored_record = vector_record;

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
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter) {
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Error parsing vector similarity query: query vector blob size (",
        query.size(), ") does not match index's expected size (",
        dimensions_ * GetDataTypeSize(), ")."));
  }
  float magnitude =
      normalize_ ? CalcMagnitude(reinterpret_cast<const float *>(query.data()),
                                 dimensions_)
                 : 1.0f;

  try {
    CancelCondition canceler(cancellation_token);
    auto embedding = VectorRecord::Construct(query, magnitude);
    auto res = algo_->searchKnn(
        embedding,
        std::min(count, static_cast<uint64_t>(algo_->cur_element_count_)),
        filter.get(), &canceler);

    // if (cancellation_token->IsCancelled()) {
    //   return absl::CancelledError("Search operation cancelled");
    // }
    return CreateReply(res);
  } catch (const std::exception &e) {
    Metrics::GetStats().flat_search_exceptions_cnt.fetch_add(
        1, std::memory_order_relaxed);
    return absl::InternalError(e.what());
  }
}

template <typename T>
T VectorFlat<T>::ComputeDistance(absl::string_view query,
                                 VectorRecord *vector_record,
                                 float query_magnitude) const {
  return algo_->fstdistfunc_(query.data(), vector_record->GetRawVector(),
                             algo_->dist_func_param_, query_magnitude);
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
        LookupKeyByValue(*kVectorDataTypeByStr,
                         data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32)
            .data());
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "UNKNOWN");
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  ValkeyModule_ReplyWithArray(ctx, 4);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      LookupKeyByValue(*kVectorAlgoByStr,
                       data_model::VectorIndex::AlgorithmCase::kFlatAlgorithm)
          .data());
  ValkeyModule_ReplyWithSimpleString(ctx, "block_size");
  ValkeyModule_ReplyWithLongLong(ctx, block_size_);

  return 4;
}

template <typename T>
absl::Status VectorFlat<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&resize_mutex_);
  auto serializer = [normalize = normalize_, vector_size = GetVectorDataSize()](
                        const std::shared_ptr<VectorRecord> &record) {
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
std::shared_ptr<VectorRecord> &VectorFlat<T>::GetVectorLockFree(
    uint64_t internal_id) const {
  return *algo_->getPointByExternalId(internal_id);
}

template class VectorFlat<float>;

}  // namespace valkey_search::indexes
