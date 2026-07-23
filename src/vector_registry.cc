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
    const InternedStringPtr &interned_attribute_identifier) const {
  RegistryKey search_key = std::make_pair(key, interned_attribute_identifier);
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
    const data_model::AttributeDataType &attribute_data_type) {
  RegistryKey search_key = std::make_pair(key, attribute_identifier);

  std::shared_ptr<indexes::VectorRecord> vector_record;
  size_t vector_size;
  {
    absl::MutexLock lock(&mutex_);
    if (!vector) {
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
      float magnitude = indexes::CalcMagnitude(
          reinterpret_cast<const float *>(vector_str.data()),
          vector_str.size() / sizeof(float));
      vector_record =
          indexes::VectorRecord::Construct(vector_str, magnitude, allocator);
      vector_size = vector_str.size();
      tracked_vectors_[search_key] = {vector_record, vector_size};
    }
  }
  ShareWithValkeyHash(key, attribute_identifier->Str(), vector_record.get(),
                      vector_size, attribute_data_type);
  return vector_record;
}

bool VectorRegistry::ShareWithValkeyHash(
    const InternedStringPtr &key, absl::string_view attribute_identifier,
    const indexes::VectorRecord *vector_record, size_t vector_size,
    const data_model::AttributeDataType &attribute_data_type) {
  vmsdk::VerifyMainThread();
  if (!hash_vector_sharing_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }
  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
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
        &&tracked_metadata_by_key) {
  absl::MutexLock lock(&mutex_);
  for (auto &&[key, _] : tracked_metadata_by_key) {
    RegistryKey search_key = std::make_pair(key, attribute_identifier);
    LockFreeUntrackIfUnused(search_key);
  }
}

void VectorRegistry::DetachFromValkeyHash(const RegistryKey &search_key) {
  auto key_str = vmsdk::MakeUniqueValkeyString(search_key.first->Str());
  auto open_key = vmsdk::MakeUniqueValkeyOpenKey(ctx_.get(), key_str.get(),
                                                 VALKEYMODULE_WRITE);
  if (!open_key) {
    return;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(search_key.second->Str());
  if (ValkeyModule_KeyType(open_key.get()) != VALKEYMODULE_KEYTYPE_HASH) {
    return;
  }
  if (ValkeyModule_HashHasStringRef(
          open_key.get(), attribute_identifier_str.get()) != VALKEYMODULE_OK) {
    return;
  }
  ValkeyModuleString *record{nullptr};
  ValkeyModule_HashGet(open_key.get(), VALKEYMODULE_HASH_NONE,
                       attribute_identifier_str.get(), &record, nullptr);
  if (!record) {
    return;
  }
  ValkeyModule_HashSet(open_key.get(), VALKEYMODULE_HASH_NONE,
                       attribute_identifier_str.get(), record, nullptr);
}

void VectorRegistry::UntrackIfUnused(
    const InternedStringPtr &key,
    const InternedStringPtr &interned_attribute_identifier) {
  RegistryKey search_key = std::make_pair(key, interned_attribute_identifier);
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

VectorRegistry::Stats VectorRegistry::GetStats() const {
  vmsdk::VerifyMainThread();
  absl::MutexLock lock(&mutex_);
  stats_.entry_cnt = tracked_vectors_.size();
  return stats_;
}

}  // namespace valkey_search
