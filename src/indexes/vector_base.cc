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
#include "src/vector_externalizer.h"
#include "third_party/hnswlib/hnswlib.h"
#include "third_party/hnswlib/space_ip.h"
#include "third_party/hnswlib/space_l2.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
constexpr float kDefaultMagnitude = 1.0f;

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

template <typename T>
void VectorBase::Init(int dimensions,
                      valkey_search::data_model::DistanceMetric distance_metric,
                      std::unique_ptr<hnswlib::SpaceInterface<T>> &space) {
  distance_metric_ = distance_metric;
  if (distance_metric ==
      valkey_search::data_model::DistanceMetric::DISTANCE_METRIC_COSINE) {
    normalize_ = true;
  }
  space = CreateSpace<T>(dimensions, distance_metric);
}

InternedStringPtr VectorBase::InternVector(absl::string_view vector,
                                           const float *magnitude_ptr) const {
  CHECK_EQ(vector.size(), GetVectorDataSize());
  float magnitude = magnitude_ptr
                        ? *magnitude_ptr
                        : CalcMagnitude((float *)vector.data(), dimensions_);
  // The vector magnitude is embedded at the end of the InternedString object to
  // maintain compatibility with hnswlib's vector codec interface (which expects
  // a `T*` type). The vector magnitude is always embedded, regardless of the
  // vector index distance metric, to ensure that only single InternedString
  // object is maintained when the attribute is used across multiple indexes
  // mixing COSINE and non-COSINE distance metrics.
  std::vector<char> record_magnitude(vector.size() + sizeof(float));
  std::memcpy(record_magnitude.data(), vector.data(), vector.size());
  std::memcpy(record_magnitude.data() + vector.size(), &magnitude,
              sizeof(float));
  return StringInternStore::Intern(
      absl::string_view((const char *)record_magnitude.data(),
                        record_magnitude.size()),
      vector_allocator_.get());
}

absl::StatusOr<bool> VectorBase::AddRecord(const InternedStringPtr &key,
                                           absl::string_view record) {
  if (!IsValidSizeVector(record)) {
    return false;
  }
  auto interned_vector = InternVector(record);
  VMSDK_ASSIGN_OR_RETURN(auto internal_id, TrackKey(key, interned_vector));
  absl::Status add_result = AddRecordImpl(internal_id, interned_vector->Str());
  if (!add_result.ok()) {
    auto status = UnTrackKey(key, internal_id);
    if (!status.ok()) {
      VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
          << "While processing error for AddRecord, encountered error in "
             "UntrackKey: "
          << status.message();
    }
    return add_result;
  }
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
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kRecord);
    return false;
  }
  auto interned_vector = InternVector(record);
  VMSDK_ASSIGN_OR_RETURN(auto internal_id, GetInternalId(key));
  VMSDK_ASSIGN_OR_RETURN(bool res, UpdateMetadata(key, interned_vector));
  if (!res) {
    return false;
  }

  auto modify_result = ModifyRecordImpl(internal_id, interned_vector->Str());
  if (!modify_result.ok()) {
    auto status = UnTrackKey(key, internal_id);
    if (!status.ok()) {
      VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
          << "While processing error for ModifyRecord, encountered error "
             "in UntrackKey: "
          << status.message();
    }
    return false;
  }
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

absl::StatusOr<std::vector<char>> VectorBase::GetValue(
    const InternedStringPtr &key) const {
  auto it = tracked_metadata_by_key_.find(key);
  if (it == tracked_metadata_by_key_.end()) {
    return absl::NotFoundError("Record was not found");
  }
  std::vector<char> result;
  char *value = *GetValueImpl(it->second.internal_id);

  result.assign(value, value + GetVectorDataSize());
  return result;
}

absl::StatusOr<bool> VectorBase::RemoveRecord(
    const InternedStringPtr &key,
    [[maybe_unused]] indexes::DeletionType deletion_type) {
  auto internal_id = GetInternalId(key);
  if (!internal_id.ok()) {
    return false;
  }
  VMSDK_RETURN_IF_ERROR(RemoveRecordImpl(internal_id.value()));
  return UnTrackKey(key, internal_id.value()).ok();
}

absl::Status VectorBase::UnTrackKey(const InternedStringPtr &key,
                                    uint64_t internal_id) {
  UnTrackVector(internal_id);
  absl::WriterMutexLock lock(&key_to_metadata_mutex_);
  tracked_metadata_by_key_.erase(key);
  auto key_by_internal_id_it = key_by_internal_id_.find(internal_id);
  if (key_by_internal_id_it == key_by_internal_id_.end()) {
    return absl::InvalidArgumentError(
        "Error while untracking key - key was not found in key_by_internal_id_ "
        "but in internal_by_key_");
  }
  key_by_internal_id_.erase(key_by_internal_id_it);
  return absl::OkStatus();
}

std::vector<char> DenormalizeVector(absl::string_view record, float magnitude) {
  std::vector<char> ret(record.size());

  float *src = (float *)record.data();
  float *dst = (float *)ret.data();
  size_t size = record.size() / sizeof(float);
  for (size_t i = 0; i < size; i++) {
    dst[i] = src[i] * magnitude;
  }
  return ret;
}

std::vector<char> VectorBase::Encode(char *vector, size_t len,
                                     bool is_marked_delete) const {
  if (!normalize_ || is_marked_delete) {
    return {vector, vector + len};
  }
  return NormalizeVector((float *)vector, len / sizeof(float));
}

char *VectorBase::Decode(uint64_t internal_id, char *vector, size_t len) {
  auto vector_str = absl::string_view(vector, len);
  CHECK(IsValidSizeVector(vector_str));
  // For normalized indexes:
  // * Stored vectors are kept denormalized in memory so the module and engine
  //   can share the same buffer efficiently.
  // * Each stored vector also carries its magnitude to avoid recomputing it
  //   during distance calculations.
  // * SaveIndex writes normalized vectors for compatibility, while
  // LoadTrackedKeys
  //   restores them into denormalized form during loading.
  // * Decode cannot denormalize vectors immediately because the actual
  // magnitude
  //   values are only available later in LoadTrackedKeys.
  // * Therefore Decode only prepares the vector for LoadTrackedKeys.
  // * LoadTrackedKeys operates only on live tracked keys, so deleted vectors'
  //   magnitudes cannot be recovered.
  // * A default magnitude value of 1 ensures that normalizing deleted vectors,
  // which are kept in their normalized form, would yield the same vector
  // values.
  auto interned_vector =
      InternVector(vector_str, normalize_ ? &kDefaultMagnitude : nullptr);
  TrackVector(internal_id, interned_vector);
  return (char *)interned_vector->Str().data();
}

float GetMagnitude(const InternedStringPtr &vector) {
  auto vector_str = vector->Str();
  CHECK(vector_str.size() >= sizeof(float));
  return GetMagnitude((float *)vector_str.data(),
                      vector_str.size() / sizeof(float) - 1);
}

absl::StatusOr<uint64_t> VectorBase::TrackKey(const InternedStringPtr &key,
                                              const InternedStringPtr &vector) {
  if (key->Str().empty()) {
    return absl::InvalidArgumentError("key can't be empty");
  }
  absl::WriterMutexLock lock(&key_to_metadata_mutex_);
  auto id = inc_id_++;
  auto [_, succ] = tracked_metadata_by_key_.insert(
      {key, {.internal_id = id, .magnitude = GetMagnitude(vector)}});

  if (!succ) {
    return absl::InvalidArgumentError(
        absl::StrCat("Embedding id already exists: ", key->Str()));
  }
  TrackVector(id, vector);
  key_by_internal_id_.insert({id, key});
  return id;
}
// Return an error if the key is empty or not being tracked.
// Return false if the tracked vector matches the input vector.
// Otherwise, track the new vector and return true.
absl::StatusOr<bool> VectorBase::UpdateMetadata(
    const InternedStringPtr &key, const InternedStringPtr &vector) {
  if (key->Str().empty()) {
    return absl::InvalidArgumentError("key can't be empty");
  }
  uint64_t internal_id;
  {
    absl::WriterMutexLock lock(&key_to_metadata_mutex_);
    auto it = tracked_metadata_by_key_.find(key);
    if (it == tracked_metadata_by_key_.end()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Embedding id not found: ", key->Str()));
    }
    it->second.magnitude = GetMagnitude(vector);
    internal_id = it->second.internal_id;
  }
  if (IsVectorMatch(internal_id, vector)) {
    return false;
  }
  TrackVector(internal_id, vector);
  return true;
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
  VMSDK_RETURN_IF_ERROR(SaveIndexImpl(std::move(chunked_out)));
  return absl::OkStatus();
}

absl::Status VectorBase::SaveTrackedKeys(
    RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  for (const auto &[key, metadata] : tracked_metadata_by_key_) {
    data_model::TrackedKeyMetadata metadata_pb;
    metadata_pb.set_key(key->Str());
    metadata_pb.set_internal_id(metadata.internal_id);
    char **vector_ptr = GetValueImpl(metadata.internal_id);
    float magnitude = ((float *)(*vector_ptr))[dimensions_];
    metadata_pb.set_magnitude(magnitude);

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
  CHECK(GetDataTypeSize() == sizeof(float));
  auto data_type = attribute_data_type->ToProto();
  while (iter.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto metadata_str, iter.Next(),
                           _ << "Error loading metadata");
    data_model::TrackedKeyMetadata tracked_key_metadata;
    if (!tracked_key_metadata.ParseFromString(metadata_str->binary_content())) {
      return absl::InvalidArgumentError("Error parsing metadata from proto");
    }
    auto interned_key = StringInternStore::Intern(tracked_key_metadata.key());

    key_by_internal_id_.insert(
        {tracked_key_metadata.internal_id(), interned_key});
    inc_id_ = std::max(
        inc_id_, static_cast<uint64_t>(tracked_key_metadata.internal_id()));

    auto vec = GetTrackedVector(tracked_key_metadata.internal_id());
    auto magnitude = tracked_key_metadata.magnitude();
    if (normalize_) {
      auto key = vmsdk::MakeUniqueValkeyString(interned_key->Str());
      auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
          ctx, key.get(), VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
      CHECK(key_obj) << "Failed to open key during LoadTrackedKeys: "
                     << interned_key->Str();
      auto record = attribute_data_type->GetRecord(
          ctx, key_obj.get(), interned_key->Str(), attribute_identifier_);
      CHECK(record.ok());
      vec = InternVector(vmsdk::ToStringView(record.value().get()));
      magnitude = GetMagnitude(vec);
      char **load_vec = GetValueImpl(tracked_key_metadata.internal_id());
      *load_vec = (char *)vec->Str().data();
      UnTrackVector(tracked_key_metadata.internal_id(), false);
    }
    tracked_metadata_by_key_.insert(
        {interned_key,
         {.internal_id = tracked_key_metadata.internal_id(),
          .magnitude = magnitude}});
    TrackVector(tracked_key_metadata.internal_id(), vec);
    VectorExternalizer::Instance().Externalize(interned_key,
                                               attribute_identifier_, data_type,
                                               vec, GetVectorDataSize());
  }
  // Use max label from label_lookup_
  inc_id_ = GetMaxInternalLabel();
  ++inc_id_;
  return absl::OkStatus();
}

void VectorBase::TrackVector(uint64_t internal_id,
                             const InternedStringPtr &vector) {
  UnTrackVector(internal_id);
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_[internal_id] = vector;
}

const InternedStringPtr VectorBase::GetTrackedVector(
    uint64_t internal_id) const {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  auto it = tracked_vectors_.find(internal_id);
  if (it == tracked_vectors_.end()) {
    return {};
  }
  return it->second;
}

bool VectorBase::IsVectorMatch(uint64_t internal_id,
                               const InternedStringPtr &vector) const {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  auto it = tracked_vectors_.find(internal_id);
  if (it == tracked_vectors_.end()) {
    return false;
  }
  return it->second->Str() == vector->Str();
}

void VectorBase::UnTrackVector(
    uint64_t internal_id, [[maybe_unused]] bool maintain_strong_reference) {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_.erase(internal_id);
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
                                      absl::string_view query) const {
  VMSDK_ASSIGN_OR_RETURN(auto internal_id, GetInternalIdDuringSearch(key));
  return ComputeDistanceFromRecordImpl(internal_id, query);
}

bool VectorBase::AddPrefilteredKey(
    absl::string_view query, uint64_t count, const InternedStringPtr &key,
    std::priority_queue<std::pair<float, hnswlib::labeltype>> &results,
    absl::flat_hash_set<const char *> &top_keys) const {
  auto result = ComputeDistanceFromRecord(key, query);
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
  return tracked_metadata_by_key_.contains(key);
}

bool VectorBase::IsUnTracked(const InternedStringPtr &key) const {
  return false;
}

void VectorBase::UnTrack(const InternedStringPtr &key) {}

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

template void VectorBase::Init<float>(
    int dimensions, data_model::DistanceMetric distance_metric,
    std::unique_ptr<hnswlib::SpaceInterface<float>> &space);

template absl::StatusOr<std::vector<Neighbor>> VectorBase::CreateReply<float>(
    std::priority_queue<std::pair<float, hnswlib::labeltype>> &knn_res);
}  // namespace indexes

}  // namespace valkey_search
