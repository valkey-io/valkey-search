/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <cstring>
#include <memory>
#include <utility>

#include "absl/log/check.h"
#include "src/indexes/vector_base.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"

namespace valkey_search {

CONTROLLED_BOOLEAN(ForceHashSharingError, false);

void VectorRegistry::Init(ValkeyModuleCtx *ctx) {
  hash_vector_sharing_ = options::GetEnableVectorSharing().GetValue();
  ctx_ = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
  if (!hash_vector_sharing_) {
    return;
  }
  CHECK(ValkeyModule_GetApi("ValkeyModule_HashSetStringRef",
                            (void **)&ValkeyModule_HashSetStringRef) ==
            VALKEYMODULE_OK &&
        ValkeyModule_GetApi("ValkeyModule_HashHasStringRef",
                            (void **)&ValkeyModule_HashHasStringRef) ==
            VALKEYMODULE_OK)
      << "Valkey version should be 9.0.1 and above";
}

std::pair<std::shared_ptr<indexes::VectorRecord>, size_t>
VectorRegistry::LookupRecord(
    const InternedStringPtr &key,
    const InternedStringPtr &interned_attribute_identifier,
    uint32_t db_num) const {
  RegistryKey search_key{db_num, key, interned_attribute_identifier};
  absl::MutexLock lock(&mutex_);
  auto it = tracked_vectors_.find(search_key);
  if (it != tracked_vectors_.end()) {
    ++stats_.lookup_record_hits;
    return {it->second.vector_record, it->second.vector_record_size};
  }
  ++stats_.lookup_record_misses;
  return {nullptr, 0};
}

std::shared_ptr<indexes::VectorRecord> VectorRegistry::Track(
    const InternedStringPtr &key, const InternedStringPtr &attribute_identifier,
    ValkeyModuleString *vector, Allocator *allocator,
    const data_model::AttributeDataType &attribute_data_type, uint32_t db_num) {
  RegistryKey search_key{db_num, key, attribute_identifier};

  std::shared_ptr<indexes::VectorRecord> vector_record;
  size_t vector_size;
  bool needs_sharing = false;
  {
    absl::MutexLock lock(&mutex_);
    if (!vector) {
      // If the vector is nullptr, it indicates the key/attribute was deleted
      // from Valkey. Therefore, the sharing reference in the Valkey Hash is
      // already gone or invalid, and there is no need to call
      // DetachFromValkeyHash.
      tracked_vectors_.erase(search_key);
      return nullptr;
    }
    auto vector_str = vmsdk::ToStringView(vector);
    auto it = tracked_vectors_.find(search_key);
    if (it != tracked_vectors_.end() &&
        it->second.vector_record_size == vector_str.size() &&
        std::memcmp(it->second.vector_record->GetRawVector(), vector_str.data(),
                    vector_str.size()) == 0) {
      vector_record = it->second.vector_record;
      vector_size = it->second.vector_record_size;
    } else {
      // If the payload changed (mismatch), the Valkey Hash field has already
      // been overwritten with a new raw vector value, so the old sharing
      // reference is already gone. Thus, there is no need to call
      // DetachFromValkeyHash.
      float reciprocal_magnitude = indexes::CalcReciprocalMagnitude(
          reinterpret_cast<const float *>(vector_str.data()),
          vector_str.size() / sizeof(float));
      vector_record = indexes::VectorRecord::Construct(
          vector_str, reciprocal_magnitude, allocator);
      vector_size = vector_str.size();
      tracked_vectors_[search_key] = {vector_record, vector_size};
      needs_sharing = true;
    }
  }
  if (needs_sharing) {
    ShareWithValkeyHash(db_num, key, attribute_identifier->Str(),
                        vector_record.get(), vector_size, attribute_data_type);
  }
  return vector_record;
}

bool VectorRegistry::ShareWithValkeyHash(
    uint32_t db_num, const InternedStringPtr &key,
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
  if (!key_obj) {
    return false;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(attribute_identifier);
  if (ValkeyModule_HashHasStringRef(
          key_obj.get(), attribute_identifier_str.get()) != VALKEYMODULE_OK) {
    return false;
  }

  ValkeyModuleString *record{nullptr};
  ValkeyModule_HashGet(key_obj.get(), VALKEYMODULE_HASH_NONE,
                       attribute_identifier_str.get(), &record, nullptr);
  if (!record) {
    return false;
  }
  vmsdk::UniqueValkeyString record_ptr(record);
  if (vmsdk::ToStringView(record) !=
      absl::string_view(vector_record->GetRawVector(), vector_size)) {
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

void VectorRegistry::BatchUntrackIfUnused(
    const InternedStringPtr &attribute_identifier,
    InternedStringHashMap<indexes::TrackedKeyMetadata>
        &&tracked_metadata_by_key,
    uint32_t db_num) {
  absl::MutexLock lock(&mutex_);
  for (auto &&[key, _] : tracked_metadata_by_key) {
    RegistryKey search_key{db_num, key, attribute_identifier};
    LockFreeUntrackIfUnused(search_key);
  }
}

void VectorRegistry::DetachFromValkeyHash(const RegistryKey &search_key) {
  auto key_str = vmsdk::MakeUniqueValkeyString(search_key.key->Str());
  ValkeyModule_SelectDb(ctx_.get(), search_key.db_num);
  auto open_key = vmsdk::MakeUniqueValkeyOpenKey(ctx_.get(), key_str.get(),
                                                 VALKEYMODULE_WRITE);
  if (!open_key) {
    return;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(search_key.attribute_identifier->Str());
  if (ValkeyModule_KeyType(open_key.get()) != VALKEYMODULE_KEYTYPE_HASH) {
    return;
  }
  if (!ValkeyModule_HashHasStringRef(open_key.get(),
                                     attribute_identifier_str.get())) {
    return;
  }
  auto it = tracked_vectors_.find(search_key);
  if (it == tracked_vectors_.end()) {
    return;
  }
  auto new_val = vmsdk::MakeUniqueValkeyString(absl::string_view(
      it->second.vector_record->GetRawVector(), it->second.vector_record_size));
  ValkeyModuleString *record{nullptr};
  ValkeyModule_HashGet(open_key.get(), VALKEYMODULE_HASH_NONE,
                       attribute_identifier_str.get(), &record, nullptr);
  if (!record) {
    return;
  }
  auto db_val = vmsdk::UniquePtrValkeyString(record);
  if (ValkeyModule_StringCompare(db_val.get(), new_val.get()) != 0) {
    return;
  }
  ValkeyModule_HashSet(open_key.get(), VALKEYMODULE_HASH_NONE,
                       attribute_identifier_str.get(), new_val.get(), nullptr);
}

void VectorRegistry::UntrackIfUnused(
    const InternedStringPtr &key,
    const InternedStringPtr &interned_attribute_identifier, uint32_t db_num) {
  RegistryKey search_key{db_num, key, interned_attribute_identifier};
  absl::MutexLock lock(&mutex_);
  LockFreeUntrackIfUnused(search_key);
}

void VectorRegistry::LockFreeUntrackIfUnused(const RegistryKey &search_key) {
  auto it = tracked_vectors_.find(search_key);
  if (it != tracked_vectors_.end()) {
    if (it->second.vector_record.use_count() == 1) {
      // if hash registration is supported and there are no other references to
      // the vector (except for the one in the hash), we set a non-reference
      // record value to the hash before erasing the entry from the registry.
      if (hash_vector_sharing_) {
        DetachFromValkeyHash(search_key);
      }
      tracked_vectors_.erase(it);
    }
  }
}

const VectorRegistry::Stats &VectorRegistry::GetStats() const {
  vmsdk::VerifyMainThread();
  {
    absl::MutexLock lock(&mutex_);
    stats_.entry_cnt = tracked_vectors_.size();
  }
  return stats_;
}

}  // namespace valkey_search
