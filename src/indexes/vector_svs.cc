/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_svs.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

namespace {
constexpr absl::string_view kSkeletonMsg =
    "SVS_VAMANA is not implemented on this build.";
}  // namespace

template <typename T>
VectorSVS<T>::VectorSVS(int dimensions,
                        absl::string_view attribute_identifier,
                        data_model::AttributeDataType attribute_data_type,
                        int db_num)
    : VectorType<T>(IndexerType::kSVS, dimensions, attribute_data_type,
                    attribute_identifier, db_num) {}

template <typename T>
VectorSVS<T>::~VectorSVS() = default;

template <typename T>
absl::StatusOr<std::shared_ptr<VectorSVS<T>>> VectorSVS<T>::Create(
    const data_model::VectorIndex & /*vector_index_proto*/,
    absl::string_view /*attribute_identifier*/,
    data_model::AttributeDataType /*attribute_data_type*/, int /*db_num*/) {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorSVS<T>>> VectorSVS<T>::LoadFromRDB(
    ValkeyModuleCtx * /*ctx*/,
    const AttributeDataType * /*attribute_data_type*/,
    const data_model::VectorIndex & /*vector_index_proto*/,
    absl::string_view /*attribute_identifier*/,
    SupplementalContentChunkIter && /*iter*/, int /*db_num*/) {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
size_t VectorSVS<T>::GetCapacity() const {
  return 0;
}

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(
    absl::string_view /*query*/, uint64_t /*count*/,
    cancel::Token & /*cancellation_token*/,
    std::unique_ptr<hnswlib::BaseFilterFunctor> /*filter*/,
    std::optional<size_t> /*ef_runtime*/, bool /*enable_partial_results*/) {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
absl::Status VectorSVS<T>::AddRecordImpl(
    uint64_t /*internal_id*/,
    std::shared_ptr<const VectorRecord> && /*vector_record*/) {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
absl::Status VectorSVS<T>::RemoveRecordImpl(uint64_t /*internal_id*/) {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
absl::Status VectorSVS<T>::ModifyRecordImpl(
    uint64_t /*internal_id*/,
    std::shared_ptr<const VectorRecord> && /*vector_record*/) {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
void VectorSVS<T>::ToProtoImpl(
    data_model::VectorIndex * /*vector_index_proto*/) const {
  LOG(FATAL) << kSkeletonMsg;
}

template <typename T>
int VectorSVS<T>::RespondWithInfoImpl(ValkeyModuleCtx * /*ctx*/) const {
  LOG(FATAL) << kSkeletonMsg;
}

template <typename T>
absl::Status VectorSVS<T>::SaveIndexImpl(
    RDBChunkOutputStream /*chunked_out*/) const {
  return absl::UnimplementedError(kSkeletonMsg);
}

template <typename T>
float VectorSVS<T>::ComputeDistance(absl::string_view /*query*/,
                                    const VectorRecord * /*vector_record*/,
                                    float /*query_magnitude*/) const {
  LOG(FATAL) << kSkeletonMsg;
}

template <typename T>
std::shared_ptr<const VectorRecord> &VectorSVS<T>::GetVectorLockFree(
    uint64_t /*internal_id*/) const {
  LOG(FATAL) << kSkeletonMsg;
}

template <typename T>
std::shared_ptr<const VectorRecord> &VectorSVS<T>::GetVector(
    uint64_t /*internal_id*/) const {
  LOG(FATAL) << kSkeletonMsg;
}

template <typename T>
std::optional<hnswlib::tableint> VectorSVS<T>::GetAlgoIdLockFree(
    uint64_t /*internal_id*/) const {
  return std::nullopt;
}

template <typename T>
uint64_t VectorSVS<T>::GetMaxLoadedLabel() const {
  return 0;
}

template <typename T>
size_t VectorSVS<T>::GetLabelCount() const {
  return 0;
}

template class VectorSVS<float>;
template class VectorSVS<float16>;
template class VectorSVS<bfloat16>;

}  // namespace valkey_search::indexes
