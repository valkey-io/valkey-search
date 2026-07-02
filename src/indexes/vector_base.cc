/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_base.h"

#include <sys/types.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/query/predicate.h"
#include "src/rdb_serialization.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_registry.h"
#include "third_party/hnswlib/hnswlib.h"
#include "third_party/hnswlib/space_ip.h"
#include "third_party/hnswlib/space_l2.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

template <typename T>
std::unique_ptr<hnswlib::SpaceInterface<T>> CreateSpace(
    int dimensions, valkey_search::data_model::DistanceMetric distance_metric) {
  if constexpr (std::is_same_v<T, float>) {
    if (distance_metric ==
            valkey_search::data_model::DistanceMetric::DISTANCE_METRIC_COSINE ||
        distance_metric ==
            valkey_search::data_model::DistanceMetric::DISTANCE_METRIC_IP) {
      return std::make_unique<hnswlib::InnerProductSpace>(dimensions);
    } else {
      return std::make_unique<hnswlib::L2Space>(dimensions);
    }
  }
  DCHECK(false) << "no matching spacer";
  return std::make_unique<hnswlib::L2Space>(dimensions);
}

}  // namespace

namespace indexes {
bool PrefilterEvaluator::Evaluate(const query::Predicate &predicate,
                                  const InternedStringPtr &key) {
  key_ = &key;
  auto res = predicate.Evaluate(*this);
  key_ = nullptr;
  return res.matches;
}

query::EvaluationResult PrefilterEvaluator::EvaluateTags(
    const query::TagPredicate &predicate) {
  bool case_sensitive = true;
  auto tags = predicate.GetIndex()->GetValue(*key_, case_sensitive);
  return predicate.Evaluate(tags, case_sensitive);
}

query::EvaluationResult PrefilterEvaluator::EvaluateNumeric(
    const query::NumericPredicate &predicate) {
  CHECK(key_);
  auto value = predicate.GetIndex()->GetValue(*key_);
  return predicate.Evaluate(value);
}

query::EvaluationResult PrefilterEvaluator::EvaluateText(
    const query::TextPredicate &predicate, bool require_positions) {
  CHECK(key_);
  if (!text_index_) {
    return query::EvaluationResult(false);
  }
  return predicate.Evaluate(*text_index_, *key_, require_positions);
}

std::vector<char> NormalizeVector(absl::string_view record,
                                  float reciprocal_magnitude) {
  size_t dimensions = record.size() / sizeof(float);
  float *src = (float *)record.data();
  std::vector<char> ret(record.size());
  float *dst = (float *)&ret[0];
  for (size_t i = 0; i < dimensions; i++) {
    dst[i] = reciprocal_magnitude * src[i];
  }
  return ret;
}

std::vector<char> NormalizeVector(absl::string_view record, float *magnitude) {
  float calc_magnitude =
      CalcMagnitude((float *)record.data(), record.size() / sizeof(float));
  std::vector<char> ret = NormalizeVector(record, 1.0f / calc_magnitude);

  if (magnitude) {
    *magnitude = calc_magnitude;
  }
  return ret;
}

template <typename T>
void VectorBase::Init(int dimensions,
                      valkey_search::data_model::DistanceMetric distance_metric,
                      std::unique_ptr<hnswlib::SpaceInterface<T>> &space) {
  space = CreateSpace<T>(dimensions, distance_metric);
  distance_metric_ = distance_metric;
  if (distance_metric ==
      valkey_search::data_model::DistanceMetric::DISTANCE_METRIC_COSINE) {
    normalize_ = true;
  }
}

absl::StatusOr<bool> VectorBase::AddRecord(const InternedStringPtr &key,
                                           absl::string_view record) {
  if (!IsValidSizeVector(record)) {
    return false;
  }
  float magnitude = CalcMagnitude(
      reinterpret_cast<const float *>(record.data()), dimensions_);

  auto vector_record = VectorRegistry::Instance().DedupConstruct(
      key, attribute_identifier_, record, magnitude, vector_allocator_.get());
  VMSDK_ASSIGN_OR_RETURN(auto internal_id, TrackKey(key, magnitude));
  absl::Status add_result = AddRecordImpl(internal_id, vector_record);
  if (!add_result.ok()) {
    RemoveRecordDueToError(key, internal_id);
    return add_result;
  }
  VectorRegistry::Instance().EngineShare(key, attribute_identifier_,
                                         vector_record, record.size(),
                                         attribute_data_type_);
  return true;
}

absl::StatusOr<uint64_t> VectorBase::GetInternalId(
    const InternedStringPtr &key) const {
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  auto it = tracked_metadata_by_key_.find(key);
  if (it == tracked_metadata_by_key_.end()) {
    return absl::InvalidArgumentError("Record was not found");
  }
  return it->second.internal_id;
}

absl::StatusOr<uint64_t> VectorBase::GetInternalIdDuringSearch(
    const InternedStringPtr &key) const {
  auto it = tracked_metadata_by_key_.find(key);
  if (it == tracked_metadata_by_key_.end()) {
    return absl::InvalidArgumentError("Record was not found");
  }
  return it->second.internal_id;
}

absl::StatusOr<InternedStringPtr> VectorBase::GetKeyDuringSearch(
    uint64_t internal_id) const {
  auto it = key_by_internal_id_.find(internal_id);
  if (it == key_by_internal_id_.end()) {
    return absl::InvalidArgumentError("Record was not found");
  }
  return it->second;
}

absl::StatusOr<bool> VectorBase::ModifyRecord(const InternedStringPtr &key,
                                              absl::string_view record) {
  if (!IsValidSizeVector(record)) {
    auto id_res = GetInternalId(key);
    RemoveRecordDueToError(
        key, id_res.ok() ? std::make_optional(*id_res) : std::nullopt);
    return false;
  }
  float magnitude = CalcMagnitude(
      reinterpret_cast<const float *>(record.data()), dimensions_);
  auto vector_record = VectorRegistry::Instance().DedupConstruct(
      key, attribute_identifier_, record, magnitude, vector_allocator_.get());
  VMSDK_ASSIGN_OR_RETURN(auto internal_id, GetInternalId(key));
  VMSDK_ASSIGN_OR_RETURN(bool res,
                         UpdateMetadata(key, magnitude, vector_record.get()));
  if (!res) {
    return false;
  }

  auto modify_result = ModifyRecordImpl(internal_id, vector_record);
  if (!modify_result.ok()) {
    RemoveRecordDueToError(key, internal_id);
    return modify_result;
  }
  VectorRegistry::Instance().EngineShare(key, attribute_identifier_,
                                         vector_record, record.size(),
                                         attribute_data_type_);
  return true;
}

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorBase::CreateReply(
    std::priority_queue<std::pair<T, hnswlib::labeltype>> &knn_res) {
  std::vector<Neighbor> ret;
  ret.reserve(knn_res.size());
  while (!knn_res.empty()) {
    auto &ele = knn_res.top();
    auto vector_key = GetKeyDuringSearch(ele.second);
    if (!vector_key.ok()) {
      knn_res.pop();
      continue;
    }
    // Insert in desc order.
    ret.emplace_back(Neighbor{vector_key.value(), ele.first});
    knn_res.pop();
  }
  // Reverse to obtain asc order of closest neighbors first.
  std::reverse(ret.begin(), ret.end());
  return ret;
}

absl::StatusOr<std::vector<char>> VectorBase::GetVectorDuringSearch(
    const InternedStringPtr &key) const {
  auto it = tracked_metadata_by_key_.find(key);
  if (it == tracked_metadata_by_key_.end()) {
    return absl::NotFoundError("Record was not found");
  }
  std::vector<char> result;
  const char *value = GetVectorLockFree(it->second.internal_id)->GetRawVector();
  result.assign(value, value + GetVectorDataSize());
  return result;
}

absl::StatusOr<bool> VectorBase::RemoveRecord(
    const InternedStringPtr &key, indexes::DeletionType deletion_type) {
  VMSDK_ASSIGN_OR_RETURN(auto res, UnTrackKey(key));
  VectorRegistry::Instance().Untrack(key, attribute_identifier_);
  if (!res.has_value()) {
    return false;
  }
  VMSDK_RETURN_IF_ERROR(RemoveRecordImpl(res.value()));
  return true;
}

void VectorBase::RemoveRecordDueToError(const InternedStringPtr &key,
                                        std::optional<uint64_t> internal_id) {
  auto res = UnTrackKey(key);
  if (!res.ok()) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "While processing error, failed to untrack the key "
           "with id: "
        << (internal_id.has_value() ? std::to_string(internal_id.value())
                                    : "unknown")
        << ": " << res.status().message();
  }
  if (internal_id.has_value()) {
    auto remove_vector_res = RemoveRecordImpl(internal_id.value());
    if (!remove_vector_res.ok()) {
      VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
          << "While processing error, failed to remove vector with id: "
          << internal_id.value() << ": " << remove_vector_res.message();
    }
  }
  VectorRegistry::Instance().UntrackExpired(key, attribute_identifier_);
}

absl::StatusOr<std::optional<uint64_t>> VectorBase::UnTrackKey(
    const InternedStringPtr &key) {
  if (key->Str().empty()) {
    return std::nullopt;
  }
  absl::WriterMutexLock lock(&key_to_metadata_mutex_);
  auto it = tracked_metadata_by_key_.find(key);
  if (it == tracked_metadata_by_key_.end()) {
    return std::nullopt;
  }
  auto id = it->second.internal_id;
  tracked_metadata_by_key_.erase(it);
  auto key_by_internal_id_it = key_by_internal_id_.find(id);
  if (key_by_internal_id_it == key_by_internal_id_.end()) {
    return absl::InvalidArgumentError(
        "Error while untracking key - key was not found in key_by_internal_id_ "
        "but in internal_by_key_");
  }
  key_by_internal_id_.erase(key_by_internal_id_it);
  return id;
}

absl::StatusOr<uint64_t> VectorBase::TrackKey(const InternedStringPtr &key,
                                              float magnitude) {
  if (key->Str().empty()) {
    return absl::InvalidArgumentError("key can't be empty");
  }
  absl::WriterMutexLock lock(&key_to_metadata_mutex_);
  auto id = inc_id_++;
  auto [_, succ] = tracked_metadata_by_key_.insert(
      {key, {.internal_id = id, .magnitude = magnitude}});

  if (!succ) {
    return absl::InvalidArgumentError(
        absl::StrCat("Embedding id already exists: ", key->Str()));
  }
  key_by_internal_id_.insert({id, key});
  return id;
}

absl::StatusOr<bool> VectorBase::UpdateMetadata(
    const InternedStringPtr &key, float magnitude,
    const VectorRecord *vector_record) {
  if (key->Str().empty()) {
    return absl::InvalidArgumentError("key can't be empty");
  }
  absl::ReaderMutexLock lock(&resize_mutex_);
  const VectorRecord *stored_record;
  {
    absl::WriterMutexLock lock(&key_to_metadata_mutex_);
    auto it = tracked_metadata_by_key_.find(key);
    if (it == tracked_metadata_by_key_.end()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Embedding id not found: ", key->Str()));
    }
    it->second.magnitude = magnitude;
    stored_record = GetVectorLockFree(it->second.internal_id).get();
  }
  // Returns true if the vectors are not matching
  return (std::memcmp(stored_record->GetRawVector(),
                      vector_record->GetRawVector(), GetVectorDataSize()) != 0);
}

int VectorBase::RespondWithInfo(ValkeyModuleCtx *ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "type");
  ValkeyModule_ReplyWithSimpleString(ctx, "VECTOR");
  ValkeyModule_ReplyWithSimpleString(ctx, "index");

  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
  ValkeyModule_ReplyWithSimpleString(ctx, "capacity");
  ValkeyModule_ReplyWithLongLong(ctx, GetCapacity());
  ValkeyModule_ReplyWithSimpleString(ctx, "dimensions");
  ValkeyModule_ReplyWithLongLong(ctx, dimensions_);
  ValkeyModule_ReplyWithSimpleString(ctx, "distance_metric");
  ValkeyModule_ReplyWithSimpleString(
      ctx, LookupKeyByValue(*kDistanceMetricByStr, distance_metric_).data());
  ValkeyModule_ReplyWithSimpleString(ctx, "size");
  {
    absl::MutexLock lock(&key_to_metadata_mutex_);
    ValkeyModule_ReplyWithCString(
        ctx, std::to_string(key_by_internal_id_.size()).c_str());
  }
  int array_len = 8;
  array_len += RespondWithInfoImpl(ctx);
  ValkeyModule_ReplySetArrayLength(ctx, array_len);

  return 4;
}

absl::Status VectorBase::SaveIndex(RDBChunkOutputStream chunked_out) const {
  return SaveIndexImpl(std::move(chunked_out));
}

absl::Status VectorBase::SaveTrackedKeys(
    RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  for (const auto &[key, metadata] : tracked_metadata_by_key_) {
    data_model::TrackedKeyMetadata metadata_pb;
    metadata_pb.set_key(key->Str());
    metadata_pb.set_internal_id(metadata.internal_id);
    metadata_pb.set_magnitude(metadata.magnitude);
    auto metadata_pb_str = metadata_pb.SerializeAsString();
    VMSDK_RETURN_IF_ERROR(
        chunked_out.SaveChunk(metadata_pb_str.data(), metadata_pb_str.size()))
        << "Error saving key_by_internal_id_ entry";
  }
  return absl::OkStatus();
}

absl::Status VectorBase::LoadTrackedKeys(
    ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
    SupplementalContentChunkIter &&iter) {
  absl::WriterMutexLock lock(&key_to_metadata_mutex_);
  while (iter.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto metadata_str, iter.Next(),
                           _ << "Error loading metadata");
    data_model::TrackedKeyMetadata tracked_key_metadata;
    if (!tracked_key_metadata.ParseFromString(metadata_str->binary_content())) {
      return absl::InvalidArgumentError("Error parsing metadata from proto");
    }
    auto interned_key = StringInternStore::Intern(tracked_key_metadata.key());
    tracked_metadata_by_key_.insert(
        {interned_key,
         {.internal_id = tracked_key_metadata.internal_id(),
          .magnitude = tracked_key_metadata.magnitude()}});
    key_by_internal_id_.insert(
        {tracked_key_metadata.internal_id(), interned_key});

    auto key = vmsdk::MakeUniqueValkeyString(interned_key->Str());
    auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
        ctx, key.get(), VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
    CHECK(key_obj) << "Failed to open key during LoadTrackedKeys: "
                   << interned_key->Str();
    auto record = attribute_data_type->GetRecord(
        ctx, key_obj.get(), interned_key->Str(), attribute_identifier_);
    CHECK(record.ok());

    auto record_str = vmsdk::ToStringView(record.value().get());
    const float magnitude = CalcMagnitude(
        reinterpret_cast<const float *>(record_str.data()), dimensions_);
    auto vector_record = VectorRegistry::Instance().DedupConstruct(
        interned_key, attribute_identifier_, record_str, magnitude,
        vector_allocator_.get());
    auto &save_vector = GetVectorLockFree(tracked_key_metadata.internal_id());
    save_vector = vector_record;
  }
  // Use max label from label_lookup_
  inc_id_ = GetMaxInternalLabel();
  ++inc_id_;
  return absl::OkStatus();
}

std::unique_ptr<data_model::Index> VectorBase::ToProto() const {
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  auto index_proto = std::make_unique<data_model::Index>();
  auto vector_index = std::make_unique<data_model::VectorIndex>();
  vector_index->set_normalize(normalize_);
  vector_index->set_distance_metric(distance_metric_);
  vector_index->set_dimension_count(dimensions_);
  vector_index->set_initial_cap(GetCapacity());
  ToProtoImpl(vector_index.get());
  index_proto->set_allocated_vector_index(vector_index.release());
  return index_proto;
}

uint32_t VectorBase::GetMutationWeight() const {
  return options::GetMutationWeightVector().GetValue();
}

absl::StatusOr<std::pair<float, hnswlib::labeltype>>
VectorBase::ComputeDistanceFromRecord(const InternedStringPtr &key,
                                      absl::string_view query,
                                      float query_magnitude) const {
  VMSDK_ASSIGN_OR_RETURN(auto internal_id, GetInternalIdDuringSearch(key));
  const auto &vector_record = GetVectorLockFree(internal_id);
  if (!vector_record) {
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }
  if (normalize_) {
    query_magnitude *= vector_record->GetReciprocalMagnitude();
  }
  return (std::pair<float, hnswlib::labeltype>){
      ComputeDistance(query, vector_record.get(), query_magnitude),
      internal_id};
}

bool VectorBase::AddPrefilteredKey(
    absl::string_view query, float query_magnitude, uint64_t count,
    const InternedStringPtr &key,
    std::priority_queue<std::pair<float, hnswlib::labeltype>> &results,
    absl::flat_hash_set<const char *> &top_keys) const {
  auto result = ComputeDistanceFromRecord(key, query, query_magnitude);
  if (!result.ok()) {
    return false;
  }
  if (results.size() < count) {
    results.emplace(result.value());
    return true;
  }
  if (result.value().first < results.top().first) {
    auto top = results.top();
    auto vector_key = GetKeyDuringSearch(top.second);
    top_keys.erase(vector_key.value()->Str().data());
    results.pop();
    results.emplace(result.value());
    return true;
  }
  return false;
}

vmsdk::UniqueValkeyString VectorBase::NormalizeStringRecord(
    vmsdk::UniqueValkeyString record) const {
  CHECK_EQ(GetDataTypeSize(), sizeof(float));
  auto record_str = vmsdk::ToStringView(record.get());
  if (absl::ConsumePrefix(&record_str, "[")) {
    absl::ConsumeSuffix(&record_str, "]");
  }
  std::vector<std::string> float_strings =
      absl::StrSplit(record_str, ',', absl::SkipWhitespace());
  std::string binary_string;
  binary_string.reserve(float_strings.size() * sizeof(float));
  for (const auto &float_str : float_strings) {
    float value;
    if (!absl::SimpleAtof(float_str, &value)) {
      return nullptr;
    }
    binary_string += std::string((char *)&value, sizeof(float));
  }
  return vmsdk::MakeUniqueValkeyString(binary_string);
}

size_t VectorBase::GetTrackedKeyCount() const {
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  return key_by_internal_id_.size();
}

size_t VectorBase::GetUnTrackedKeyCount() const { return 0; }

bool VectorBase::IsTracked(const InternedStringPtr &key) const {
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  auto it = tracked_metadata_by_key_.find(key);
  return (it != tracked_metadata_by_key_.end());
}

bool VectorBase::IsUnTracked(const InternedStringPtr &key) const {
  return false;
}

absl::Status VectorBase::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  absl::MutexLock lock(&key_to_metadata_mutex_);
  for (const auto &[key, _] : tracked_metadata_by_key_) {
    VMSDK_RETURN_IF_ERROR(fn(key));
  }
  return absl::OkStatus();
}

absl::Status VectorBase::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  return absl::OkStatus();
}

VectorBase::~VectorBase() {
  VectorRegistry::Instance().BatchUntrackExpired(
      attribute_identifier_, std::move(tracked_metadata_by_key_));
}

template void VectorBase::Init<float>(
    int dimensions, data_model::DistanceMetric distance_metric,
    std::unique_ptr<hnswlib::SpaceInterface<float>> &space);

template absl::StatusOr<std::vector<Neighbor>> VectorBase::CreateReply<float>(
    std::priority_queue<std::pair<float, hnswlib::labeltype>> &knn_res);

std::shared_ptr<VectorRecord> VectorRecord::Construct(absl::string_view vector,
                                                      float magnitude,
                                                      Allocator *allocator) {
  size_t total_size = sizeof(VectorRecord) + vector.size();
  void *mem =
      allocator ? allocator->Allocate(total_size) : ::operator new(total_size);
  VectorRecord *ptr = new (mem) VectorRecord(vector, magnitude);
  return {ptr, [allocator_used = (allocator != nullptr)](VectorRecord *p) {
            p->~VectorRecord();
            if (allocator_used) {
              Allocator::Free(reinterpret_cast<char *>(p));
            } else {
              ::operator delete(p);
            }
          }};
}

VectorRecord::VectorRecord(absl::string_view vector, float magnitude)
    : reciprocal_magnitude_(magnitude == 0.0f ? 1.0f : 1.0f / magnitude) {
  std::memcpy(data_, vector.data(), vector.size());
}
}  // namespace indexes

}  // namespace valkey_search
