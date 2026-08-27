/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <cstring>
#include <memory>

#include "absl/debugging/leak_check.h"
#include "absl/log/check.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

CONTROLLED_BOOLEAN(ForceHashSharingError, false);

void VectorRegistry::Init(ValkeyModuleCtx *ctx) {
  hash_vector_sharing_ = options::GetEnableVectorSharing().GetValue();
  ctx_ = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
  if (!hash_vector_sharing_) {
    return;
  }
  CHECK(ValkeyModule_GetApi(
            "ValkeyModule_HashSetStringRef",
            reinterpret_cast<void *>(&ValkeyModule_HashSetStringRef)) ==
            VALKEYMODULE_OK &&
        ValkeyModule_GetApi(
            "ValkeyModule_HashHasStringRef",
            reinterpret_cast<void *>(&ValkeyModule_HashHasStringRef)) ==
            VALKEYMODULE_OK)
      << "Valkey version should be 9.0.1 and above";
}

indexes::VectorRecordWithSize ConstructVectorRecord(
    absl::string_view record, const indexes::VectorBase *vector_base) {
  if (record.empty()) {
    return {};
  }
  if (!vector_base->IsValidSizeVector(record)) {
    return {
        .vector_record = indexes::VectorRecord::Construct(record, 0, nullptr),
        .size = record.size()};
  }
  float reciprocal_magnitude = indexes::CalcReciprocalMagnitude(
      reinterpret_cast<const float *>(record.data()),
      record.size() / sizeof(float));
  return {.vector_record = indexes::VectorRecord::Construct(
              record, reciprocal_magnitude, vector_base->GetVectorAllocator()),
          .size = record.size()};
}

indexes::VectorRecordWithSize VectorRegistry::DedupOrConstruct(
    const InternedStringPtr &key, ValkeyModuleString *vector,
    const data_model::AttributeDataType &attribute_data_type, int db_num,
    const indexes::VectorBase *vector_base) {
  vmsdk::VerifyMainThread();
  absl::LeakCheckDisabler disabler;

  if (!vector) {
    if (hash_vector_sharing_) {
      RegistryKey search_key{
          .db_num = db_num,
          .key = key,
          .attribute_identifier = vector_base->GetInternedAttributeIdentifier(),
      };
      tracked_vectors_.erase(search_key);
    }
    return {};
  }

  auto vector_str = vmsdk::ToStringView(vector);
  if (!hash_vector_sharing_) {
    return ConstructVectorRecord(vector_str, vector_base);
  }

  RegistryKey search_key{
      .db_num = db_num,
      .key = key,
      .attribute_identifier = vector_base->GetInternedAttributeIdentifier(),
  };

  if (!vector_base->IsValidSizeVector(vector_str)) {
    if (IsEraseTrackedRecordSafe(
            db_num, key, vector_str,
            vector_base->GetInternedAttributeIdentifier()->Str(),
            attribute_data_type)) {
      tracked_vectors_.erase(search_key);
    }
    return ConstructVectorRecord(vector_str, vector_base);
  }

  indexes::VectorRecordWithSize result;
  auto it = tracked_vectors_.find(search_key);
  if (it != tracked_vectors_.end() && it->second.size == vector_str.size() &&
      std::memcmp(it->second.vector_record->GetRawVector(), vector_str.data(),
                  vector_str.size()) == 0) {
    ++stats_.dedup_cnt;
    result = it->second;
  } else {
    result = ConstructVectorRecord(vector_str, vector_base);
    if (it != tracked_vectors_.end()) {
      it->second = result;
    } else {
      tracked_vectors_.emplace(search_key, result);
    }
  }

  ShareWithValkey(db_num, key,
                  vector_base->GetInternedAttributeIdentifier()->Str(),
                  result.vector_record.get(), vector_base->GetVectorDataSize(),
                  attribute_data_type);
  return result;
}

bool VectorRegistry::IsEraseTrackedRecordSafe(
    int db_num, const InternedStringPtr &key, absl::string_view vector_str,
    absl::string_view attribute_identifier,
    const data_model::AttributeDataType &attribute_data_type) {
  vmsdk::VerifyMainThread();
  if (vector_str.empty()) {
    return true;
  }
  if (!hash_vector_sharing_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return true;
  }
  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  ValkeyModule_SelectDb(ctx_.get(), db_num);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  if (!key_obj) {
    return true;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(attribute_identifier);
  return ValkeyModule_HashHasStringRef(key_obj.get(),
                                       attribute_identifier_str.get()) != 1;
}

bool VectorRegistry::ShareWithValkey(
    int db_num, const InternedStringPtr &key,
    absl::string_view attribute_identifier,
    const indexes::VectorRecord *vector_record, size_t vector_size,
    const data_model::AttributeDataType &attribute_data_type) {
  vmsdk::VerifyMainThread();
  if (!hash_vector_sharing_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }
  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  ValkeyModule_SelectDb(ctx_.get(), db_num);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(attribute_identifier);
  if (ValkeyModule_HashHasStringRef(key_obj.get(),
                                    attribute_identifier_str.get()) == 1) {
    return false;
  }

  if (ForceHashSharingError.GetValue() ||
      ValkeyModule_HashSetStringRef(
          key_obj.get(), attribute_identifier_str.get(),
          vector_record->GetRawVector(), vector_size) != VALKEYMODULE_OK) {
    ++stats_.hash_sharing_errors;
    return false;
  }
  ++stats_.hash_sharing_hits;
  return true;
}

const VectorRegistry::Stats &VectorRegistry::GetStats() const {
  vmsdk::VerifyMainThread();
  stats_.entry_cnt = tracked_vectors_.size();
  return stats_;
}

}  // namespace valkey_search
