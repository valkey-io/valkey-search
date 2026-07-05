/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_type.h"

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "absl/log/check.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "src/index_schema.pb.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/vector_base.h"
#include "third_party/hnswlib/hnswlib.h"
#include "third_party/hnswlib/space_ip.h"
#include "third_party/hnswlib/space_ip_bfloat16.h"
#include "third_party/hnswlib/space_ip_fp16.h"
#include "third_party/hnswlib/space_l2.h"
#include "third_party/hnswlib/space_l2_bfloat16.h"
#include "third_party/hnswlib/space_l2_fp16.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

namespace {

// Constructs the right hnswlib space for T + metric. Kept private to
// this TU -- callers reach it via VectorType<T>::Init below. Note that
// SpaceInterface is templated on the distance dtype (float); the storage
// element type is baked into the concrete space class chosen here.
template <typename StorageT>
std::unique_ptr<hnswlib::SpaceInterface<float>> CreateSpace(
    int dimensions, data_model::DistanceMetric distance_metric) {
  const bool is_ip =
      distance_metric == data_model::DistanceMetric::DISTANCE_METRIC_COSINE ||
      distance_metric == data_model::DistanceMetric::DISTANCE_METRIC_IP;
  if constexpr (std::is_same_v<StorageT, float>) {
    if (is_ip) {
      return std::make_unique<hnswlib::InnerProductSpace>(dimensions);
    }
    return std::make_unique<hnswlib::L2Space>(dimensions);
  } else if constexpr (std::is_same_v<StorageT, float16>) {
    if (is_ip) {
      return std::make_unique<hnswlib::InnerProductSpaceFP16>(dimensions);
    }
    return std::make_unique<hnswlib::L2SpaceFP16>(dimensions);
  } else if constexpr (std::is_same_v<StorageT, bfloat16>) {
    if (is_ip) {
      return std::make_unique<hnswlib::InnerProductSpaceBF16>(dimensions);
    }
    return std::make_unique<hnswlib::L2SpaceBF16>(dimensions);
  }
  DCHECK(false) << "no matching space for T";
  return std::make_unique<hnswlib::L2Space>(dimensions);
}

// Compile-time mapping from T to its data_model::VectorDataType enum.
// If a new element type is added, add its arm here plus one arm in
// CreateSpace above and NormalizeStringRecord below; both leaves'
// ToProtoImpl / RespondWithInfoImpl stay unchanged.
template <typename T>
constexpr data_model::VectorDataType VectorDataTypeEnumFor() {
  if constexpr (std::is_same_v<T, float>) {
    return data_model::VECTOR_DATA_TYPE_FLOAT32;
  } else if constexpr (std::is_same_v<T, float16>) {
    return data_model::VECTOR_DATA_TYPE_FLOAT16;
  } else if constexpr (std::is_same_v<T, bfloat16>) {
    return data_model::VECTOR_DATA_TYPE_BFLOAT16;
  } else {
    // Force a compile error rather than a silent runtime UNSPECIFIED --
    // adding a new T without adding an arm here is a bug we want caught
    // at build time.
    static_assert(sizeof(T) == 0, "no VectorDataType enum for this T");
  }
}

}  // namespace

template <typename T>
data_model::VectorDataType VectorType<T>::GetVectorDataType() const {
  return VectorDataTypeEnumFor<T>();
}

template <typename T>
void VectorType<T>::Init(data_model::DistanceMetric distance_metric) {
  space_ = CreateSpace<T>(dimensions_, distance_metric);
  distance_metric_ = distance_metric;
  if (distance_metric == data_model::DistanceMetric::DISTANCE_METRIC_COSINE) {
    normalize_ = true;
  }
}

template <typename T>
void VectorType<T>::EmitDataTypeInfo(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "data_type");
  ValkeyModule_ReplyWithSimpleString(
      ctx, LookupKeyByValue(*kVectorDataTypeByStr, VectorDataTypeEnumFor<T>())
               .data());
}

template <typename T>
void VectorType<T>::SetProtoDataType(
    data_model::VectorIndex *vector_index_proto) const {
  vector_index_proto->set_vector_data_type(VectorDataTypeEnumFor<T>());
}

template <typename T>
vmsdk::UniqueValkeyString VectorType<T>::NormalizeStringRecord(
    vmsdk::UniqueValkeyString record) const {
  auto record_str = vmsdk::ToStringView(record.get());
  if (absl::ConsumePrefix(&record_str, "[")) {
    absl::ConsumeSuffix(&record_str, "]");
  }
  std::vector<std::string> float_strings =
      absl::StrSplit(record_str, ',', absl::SkipWhitespace());
  std::string binary_string;
  binary_string.reserve(float_strings.size() * sizeof(T));
  for (const auto &float_str : float_strings) {
    float value;
    if (!absl::SimpleAtof(float_str, &value)) {
      return nullptr;
    }
    if constexpr (std::is_same_v<T, float>) {
      binary_string += std::string((char *)&value, sizeof(T));
    } else if constexpr (std::is_same_v<T, float16>) {
      float16 fp16_value = static_cast<float16>(value);
      binary_string += std::string((char *)&fp16_value, sizeof(float16));
    } else if constexpr (std::is_same_v<T, bfloat16>) {
      bfloat16 bf16_value = float_to_bfloat16(value);
      binary_string += std::string((char *)&bf16_value, sizeof(bfloat16));
    } else {
      static_assert(sizeof(T) == 0,
                    "NormalizeStringRecord not yet wired for this T");
    }
  }
  return vmsdk::MakeUniqueValkeyString(binary_string);
}

template class VectorType<float>;
template class VectorType<float16>;
template class VectorType<bfloat16>;

}  // namespace valkey_search::indexes
