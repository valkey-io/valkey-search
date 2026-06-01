/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_base.h"

#include <sys/types.h>

#include <algorithm>
#include <cmath>
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
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/key_attr_value.h"
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
constexpr float kDefaultMagnitude = -1.0f;

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
T CopyAndNormalizeEmbedding(T *dst, T *src, size_t size) {
  T magnitude = 0.0f;
  for (size_t i = 0; i < size; i++) {
    magnitude += src[i] * src[i];
  }
  magnitude = std::sqrt(magnitude);
  T norm = (magnitude == 0.0f) ? 1.0f : (1.0f / magnitude);
  for (size_t i = 0; i < size; i++) {
    dst[i] = norm * src[i];
  }
  return magnitude;
}

std::vector<char> NormalizeEmbedding(absl::string_view record, size_t type_size,
                                     float *magnitude) {
  std::vector<char> ret(record.size());
  if (type_size == sizeof(float)) {
    float result = CopyAndNormalizeEmbedding(
        (float *)&ret[0], (float *)record.data(), ret.size() / sizeof(float));
    if (magnitude) {
      *magnitude = result;
    }
    return ret;
  }
  CHECK(false) << "unsupported type size";
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

InternedStringPtr VectorBase::InternVector(absl::string_view record,
                                           std::optional<float> &magnitude) {
  if (!IsValidSizeVector(record)) {
    return {};
  }
  if (normalize_) {
    magnitude = kDefaultMagnitude;
    auto norm_record =
        NormalizeEmbedding(record, GetDataTypeSize(), &magnitude.value());
    return StringInternStore::Intern(
        absl::string_view((const char *)norm_record.data(), norm_record.size()),
        vector_allocator_.get());
  }
  return StringInternStore::Intern(record, vector_allocator_.get());
}

absl::StatusOr<bool> VectorBase::AddRecord(const InternedStringPtr &key,
                                           absl::string_view record) {
  std::optional<float> magnitude;
  auto interned_vector = InternVector(record, magnitude);
  if (!interned_vector) {
    // Unparseable input. If the slot is currently empty link it into the
    // missing list (covers brand-new-key first-notification); if it's
    // already occupied with a previously-tracked vector, leave the existing
    // tracking alone — pre-refactor AddRecord on bad data was always a
    // skip/no-op, never a teardown.
    KeyAttrValue *kav = schema_->FindKAV(key);
    if (kav == nullptr || !IsOccupied(kav->slots[pos_])) {
      if (!schema_->IsLinked(pos_, key)) {
        schema_->LinkMissing(pos_, key);
      }
    }
    return false;
  }
  // Valid input on an already-occupied slot means the caller dispatched to
  // AddRecord when ModifyRecord was expected. Match the pre-refactor
  // TrackKey behavior of returning a non-ok status (which propagates as
  // ExpectedResults::kError in the vector tests).
  if (KeyAttrValue *kav = schema_->FindKAV(key);
      kav != nullptr && IsOccupied(kav->slots[pos_])) {
    return absl::AlreadyExistsError(
        absl::StrCat("Key already exists in vector index: ", key->Str()));
  }
  uint64_t internal_id;
  {
    absl::WriterMutexLock lock(&key_to_metadata_mutex_);
    internal_id = inc_id_++;
    key_by_internal_id_.insert({internal_id, key});
    TrackVector(internal_id, interned_vector);  // virtual to subclass
  }
  // Reserve the slot before calling AddRecordImpl: if the subclass's index
  // insertion fails we need to undo via the same VacateSlot path the normal
  // remove would take.
  std::byte *storage = OccupySlot(key, record.size());
  // Packed struct — initialize with an aggregate that the compiler emits as
  // unaligned stores.
  new (storage) VectorSlot{
      {/*occupied=*/1u, /*user_data_len=*/static_cast<uint32_t>(record.size())},
      internal_id,
      magnitude.value_or(kDefaultMagnitude)};
  absl::Status add_result = AddRecordImpl(internal_id, interned_vector->Str());
  if (!add_result.ok()) {
    {
      absl::WriterMutexLock lock(&key_to_metadata_mutex_);
      key_by_internal_id_.erase(internal_id);
      UnTrackVector(internal_id);
    }
    VacateSlot(key, /*relink=*/false);
    return add_result;
  }
  return true;
}

absl::StatusOr<uint64_t> VectorBase::GetInternalIdFromSlot(
    const InternedStringPtr &key) const {
  KeyAttrValue *kav = schema_->FindKAV(key);
  if (kav == nullptr || !IsOccupied(kav->slots[pos_])) {
    return absl::InvalidArgumentError("Record was not found");
  }
  // The packed VectorSlot puts internal_id at unaligned offset 4 — memcpy
  // out so we don't UB-read on platforms without unaligned-access support.
  uint64_t internal_id;
  std::memcpy(&internal_id,
              kav->slots[pos_].storage + offsetof(VectorSlot, internal_id),
              sizeof(internal_id));
  return internal_id;
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
  // VectorExternalizer tracks added entries. We need to untrack mutations
  // which are processed as modified records.
  std::optional<float> magnitude;
  auto interned_vector = InternVector(record, magnitude);
  if (!interned_vector) {
    [[maybe_unused]] auto res =
        RemoveRecord(key, indexes::DeletionType::kRecord);
    return false;
  }
  // Fetch the existing internal_id from the slot. Also acquire a writer lock
  // on key_to_metadata_mutex_ so the magnitude update and TrackVector are
  // serialized against concurrent removes.
  KeyAttrValue *kav = schema_->FindKAV(key);
  if (kav == nullptr || !IsOccupied(kav->slots[pos_])) {
    return absl::NotFoundError(
        absl::StrCat("Key not tracked in vector index: ", key->Str()));
  }
  uint64_t internal_id;
  std::memcpy(&internal_id,
              kav->slots[pos_].storage + offsetof(VectorSlot, internal_id),
              sizeof(internal_id));
  const float new_magnitude = magnitude.value_or(kDefaultMagnitude);
  {
    absl::WriterMutexLock lock(&key_to_metadata_mutex_);
    if (IsVectorMatch(internal_id, interned_vector)) {
      // Same vector — rewrite the magnitude in the slot, no algorithm update.
      std::memcpy(kav->slots[pos_].storage + offsetof(VectorSlot, magnitude),
                  &new_magnitude, sizeof(new_magnitude));
      return false;
    }
    TrackVector(internal_id, interned_vector);
    std::memcpy(kav->slots[pos_].storage + offsetof(VectorSlot, magnitude),
                &new_magnitude, sizeof(new_magnitude));
  }
  if (auto base = reinterpret_cast<SlotBase *>(kav->slots[pos_].storage);
      base->user_data_len != record.size()) {
    ResizeSlot(key, record.size());
  }
  auto modify_result = ModifyRecordImpl(internal_id, interned_vector->Str());
  if (!modify_result.ok()) {
    // Roll back: delete the key entirely (matches the previous behavior of
    // UnTrackKey on ModifyRecord failure).
    {
      absl::WriterMutexLock lock(&key_to_metadata_mutex_);
      key_by_internal_id_.erase(internal_id);
      UnTrackVector(internal_id);
    }
    VacateSlot(key, /*relink=*/false);
    return modify_result;
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
  KeyAttrValue *kav = schema_->FindKAV(key);
  if (kav == nullptr || !IsOccupied(kav->slots[pos_])) {
    return absl::NotFoundError("Record was not found");
  }
  uint64_t internal_id;
  float magnitude;
  std::memcpy(&internal_id,
              kav->slots[pos_].storage + offsetof(VectorSlot, internal_id),
              sizeof(internal_id));
  std::memcpy(&magnitude,
              kav->slots[pos_].storage + offsetof(VectorSlot, magnitude),
              sizeof(magnitude));
  std::vector<char> result;
  char *value = GetValueImpl(internal_id);
  if (normalize_) {
    if (magnitude < 0) {
      return absl::InternalError("Magnitude is not initialized");
    }
    result = DenormalizeVector(absl::string_view(value, GetVectorDataSize()),
                               GetDataTypeSize(), magnitude);
  } else {
    result.assign(value, value + GetVectorDataSize());
  }
  return result;
}

absl::StatusOr<bool> VectorBase::RemoveRecord(
    const InternedStringPtr &key, indexes::DeletionType deletion_type) {
  if (key->Str().empty()) {
    return false;
  }
  KeyAttrValue *kav = schema_->FindKAV(key);
  if (kav == nullptr) {
    return false;
  }
  Slot &slot = kav->slots[pos_];
  if (IsOccupied(slot)) {
    uint64_t internal_id;
    std::memcpy(&internal_id, slot.storage + offsetof(VectorSlot, internal_id),
                sizeof(internal_id));
    {
      absl::WriterMutexLock lock(&key_to_metadata_mutex_);
      key_by_internal_id_.erase(internal_id);
      UnTrackVector(internal_id);
    }
    VacateSlot(key, /*relink=*/deletion_type != DeletionType::kRecord);
    VMSDK_RETURN_IF_ERROR(RemoveRecordImpl(internal_id));
    return true;
  }
  // Slot already empty. Mirror the link/unlink semantics used by the other
  // indexes so kNone-on-empty links the key into the missing list (covers
  // brand-new-key first-notification with no vector value).
  if (deletion_type == DeletionType::kRecord) {
    if (schema_->IsLinked(pos_, key)) {
      schema_->UnlinkMissing(pos_, key);
    }
  } else {
    if (!schema_->IsLinked(pos_, key)) {
      schema_->LinkMissing(pos_, key);
    }
  }
  return false;
}

void VectorBase::DestructTyped(const InternedStringPtr &key, VectorSlot &slot) {
  // Safety-net path (DestroyKeyAttrValue found an occupied slot at whole-key
  // delete time). Tear down the algorithm-side state; the caller overwrites
  // the slot bytes with a fresh Missing.
  uint64_t internal_id;
  std::memcpy(&internal_id,
              reinterpret_cast<std::byte *>(&slot) +
                  offsetof(VectorSlot, internal_id),
              sizeof(internal_id));
  {
    absl::WriterMutexLock lock(&key_to_metadata_mutex_);
    key_by_internal_id_.erase(internal_id);
    UnTrackVector(internal_id);
  }
  // Best-effort remove from the algorithm — DestroyKeyAttrValue should not
  // be called while the algorithm has an active query, so this is safe.
  auto status = RemoveRecordImpl(internal_id);
  if (!status.ok()) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 1)
        << "DestructTyped: RemoveRecordImpl failed for " << key->Str() << ": "
        << status.message();
  }
}

char *VectorBase::TrackVector(uint64_t internal_id, char *vector, size_t len) {
  auto interned_vector = StringInternStore::Intern(
      absl::string_view(vector, len), vector_allocator_.get());
  TrackVector(internal_id, interned_vector);
  return (char *)interned_vector->Str().data();
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
  ValkeyModule_ReplyWithCString(
      ctx, std::to_string(schema_->OccupiedCount(pos_)).c_str());
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
  // No schema bound → no keys to save (e.g. an empty index saved before
  // being attached to a schema, as some tests do).
  if (schema_ == nullptr) {
    return absl::OkStatus();
  }
  // Walk every key in the schema; the ones with an occupied slot at this
  // index's pos belong to this vector index. Each slot carries the
  // internal_id and magnitude we need for the proto.
  absl::ReaderMutexLock lock(&key_to_metadata_mutex_);
  return schema_->ForEachKey(
      [&](const InternedStringPtr &key, const KeyAttrValue &kav)
          -> absl::Status {
        const Slot &slot = kav.slots[pos_];
        if (!IsOccupied(slot)) {
          return absl::OkStatus();
        }
        uint64_t internal_id;
        float magnitude;
        std::memcpy(&internal_id,
                    slot.storage + offsetof(VectorSlot, internal_id),
                    sizeof(internal_id));
        std::memcpy(&magnitude,
                    slot.storage + offsetof(VectorSlot, magnitude),
                    sizeof(magnitude));
        data_model::TrackedKeyMetadata metadata_pb;
        metadata_pb.set_key(key->Str());
        metadata_pb.set_internal_id(internal_id);
        metadata_pb.set_magnitude(magnitude);
        auto metadata_pb_str = metadata_pb.SerializeAsString();
        VMSDK_RETURN_IF_ERROR(chunked_out.SaveChunk(metadata_pb_str.data(),
                                                    metadata_pb_str.size()))
            << "Error saving key_by_internal_id_ entry";
        return absl::OkStatus();
      });
}

void VectorBase::ExternalizeVector(ValkeyModuleCtx *ctx,
                                   const AttributeDataType *attribute_data_type,
                                   absl::string_view key_cstr,
                                   absl::string_view attribute_identifier) {
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, vmsdk::MakeUniqueValkeyString(key_cstr).get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  if (!key_obj || !attribute_data_type->IsProperType(key_obj.get())) {
    return;
  }
  bool is_module_owned;
  vmsdk::UniqueValkeyString record = VectorExternalizer::Instance().GetRecord(
      ctx, attribute_data_type, key_obj.get(), key_cstr, attribute_identifier,
      is_module_owned);
  CHECK(!is_module_owned);
  std::optional<float> magnitude;
  auto interned_key = StringInternStore::Intern(key_cstr);
  auto interned_vector =
      InternVector(vmsdk::ToStringView(record.get()), magnitude);
  if (interned_vector) {
    VectorExternalizer::Instance().Externalize(
        interned_key, attribute_identifier, attribute_data_type->ToProto(),
        interned_vector, magnitude);
  }
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
    const uint64_t internal_id = tracked_key_metadata.internal_id();
    const float magnitude = tracked_key_metadata.magnitude();
    key_by_internal_id_.insert({internal_id, interned_key});
    // Populate this key's KAV slot for this index's pos. OccupySlot handles
    // EnsureKeyAttrValue + slot bookkeeping; the placement-new below writes
    // the {internal_id, magnitude} payload.
    std::byte *storage =
        OccupySlot(interned_key, /*data_len=*/GetVectorDataSize());
    new (storage) VectorSlot{
        {/*occupied=*/1u,
         /*user_data_len=*/static_cast<uint32_t>(GetVectorDataSize())},
        internal_id,
        magnitude};
    ExternalizeVector(ctx, attribute_data_type, tracked_key_metadata.key(),
                      attribute_identifier_);
  }
  // Use max label from label_lookup_
  inc_id_ = GetMaxInternalLabel();
  ++inc_id_;
  return absl::OkStatus();
}

std::unique_ptr<data_model::Index> VectorBase::ToProto() const {
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
  return schema_->OccupiedCount(pos_);
}

size_t VectorBase::GetUnTrackedKeyCount() const {
  return schema_->MissingListAt(pos_).size;
}

bool VectorBase::IsTracked(const InternedStringPtr &key) const {
  const KeyAttrValue *kav = schema_->FindKAV(key);
  return kav != nullptr && IsOccupied(kav->slots[pos_]);
}

bool VectorBase::IsUnTracked(const InternedStringPtr &key) const {
  const KeyAttrValue *kav = schema_->FindKAV(key);
  return kav != nullptr && !IsOccupied(kav->slots[pos_]);
}

void VectorBase::UnTrack(const InternedStringPtr &key) {
  const KeyAttrValue *kav = schema_->FindKAV(key);
  CHECK(kav != nullptr);
  CHECK(!IsOccupied(kav->slots[pos_]));
  if (!schema_->IsLinked(pos_, key)) {
    schema_->LinkMissing(pos_, key);
  }
}

absl::Status VectorBase::ForEachTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  return schema_->ForEachKey(
      [&](const InternedStringPtr &key, const KeyAttrValue &kav) {
        if (IsOccupied(kav.slots[pos_])) {
          return fn(key);
        }
        return absl::OkStatus();
      });
}

absl::Status VectorBase::ForEachUnTrackedKey(
    absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn) const {
  for (auto it = MissingListBegin(schema_, pos_); !it.Done(); it.Next()) {
    VMSDK_RETURN_IF_ERROR(fn(it.Key()));
  }
  return absl::OkStatus();
}

template void VectorBase::Init<float>(
    int dimensions, data_model::DistanceMetric distance_metric,
    std::unique_ptr<hnswlib::SpaceInterface<float>> &space);

template absl::StatusOr<std::vector<Neighbor>> VectorBase::CreateReply<float>(
    std::priority_queue<std::pair<float, hnswlib::labeltype>> &knn_res);
}  // namespace indexes

}  // namespace valkey_search
