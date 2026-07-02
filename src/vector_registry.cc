/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <cstring>
#include <memory>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "src/indexes/vector_base.h"
#include "src/valkey_search_options.h"

namespace valkey_search {

using indexes::VectorRecord;

void VectorRegistry::Init(ValkeyModuleCtx *ctx) {
  hash_registration_supported_ =
      (ValkeyModule_GetApi("ValkeyModule_HashSetStringRef",
                           (void **)&ValkeyModule_HashSetStringRef) ==
       VALKEYMODULE_OK) &&
      (ValkeyModule_GetApi("ValkeyModule_HashHasStringRef",
                           (void **)&ValkeyModule_HashHasStringRef) ==
       VALKEYMODULE_OK) &&
      options::GetEnableVectorSharing().GetValue();
  ctx_ = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
}

std::shared_ptr<VectorRecord> VectorRegistry::DedupConstruct(
    const InternedStringPtr &key, absl::string_view attribute_identifier,
    absl::string_view vector, float magnitude, Allocator *allocator) {
  RegistryKey search_key =
      std::make_pair(key, std::string(attribute_identifier));

  absl::MutexLock lock(&mutex_);
  auto it = tracked_vectors_.find(search_key);
  if (it != tracked_vectors_.end()) {
    auto vector_record = it->second.vector.record.lock();
    if (vector_record && it->second.vector.record_size == vector.size() &&
        std::memcmp(vector_record->GetRawVector(), vector.data(),
                    vector.size()) == 0) {
      ++stats_.deduplication_hits;
      return vector_record;
    }
  }

  auto vector_record =
      indexes::VectorRecord::Construct(vector, magnitude, allocator);
  ++stats_.deduplication_misses;
  auto &tracked_value = tracked_vectors_[search_key];
  // The old shared externally value must be preserved.
  auto old_shared_externally = tracked_value.shared_externally;
  tracked_value = {{vector_record, vector.size()}, old_shared_externally};
  return vector_record;
}

bool VectorRegistry::EngineShare(
    const InternedStringPtr &key, absl::string_view attribute_identifier,
    const std::shared_ptr<indexes::VectorRecord> &vector_record,
    size_t vector_size,
    const data_model::AttributeDataType &attribute_data_type) {
  if (!hash_registration_supported_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }
  RegistryKey search_key =
      std::make_pair(key, std::string(attribute_identifier));
  bool schedule_run_by_main = false;
  {
    absl::MutexLock lock(&mutex_);
    auto it = tracked_vectors_.find(search_key);
    if ((it != tracked_vectors_.end() && it->second.shared_externally &&
         it->second.shared_externally.get() == vector_record.get())) {
      // Already shared externally or pending to be shared externally, no need
      // to do it again.
      return false;
    }
  }
  {
    absl::MutexLock lock(&pending_mutex_);
    auto it = pending_shared_externally_.find(search_key);
    if (it != pending_shared_externally_.end()) {
      auto vector_record_in_pending = it->second.record.lock();
      if (vector_record_in_pending &&
          vector_record_in_pending.get() == vector_record.get()) {
        return false;
      }
    }
    pending_shared_externally_.insert_or_assign(
        search_key, RegistryVector{vector_record, vector_size});

    if (!scheduled_run_by_main_) {
      scheduled_run_by_main_ = true;
      schedule_run_by_main = true;
    }
  }
  if (schedule_run_by_main) {
    ScheduleAsyncByMain();
  }
  return true;
}

void VectorRegistry::ScheduleAsyncByMain() {
  vmsdk::RunByMain(
      [this]() mutable {
        vmsdk::VerifyMainThread();

        absl::flat_hash_map<RegistryKey, RegistryVector>
            pending_shared_externally;
        {
          absl::MutexLock lock(&pending_mutex_);
          pending_shared_externally.swap(pending_shared_externally_);
        }
        for (const auto &[key, registry_vector] : pending_shared_externally) {
          auto vector_record = registry_vector.record.lock();
          if (!vector_record) {
            continue;
          }
          PerformEngineShare(key, vector_record, registry_vector.record_size);
        }
        bool schedule_again = false;
        {
          absl::MutexLock lock(&pending_mutex_);
          if (pending_shared_externally_.empty()) {
            scheduled_run_by_main_ = false;
            return;
          }
          schedule_again = true;
        }
        if (schedule_again) {
          ScheduleAsyncByMain();
        }
      },
      true);
}

bool VectorRegistry::PerformEngineShare(
    const RegistryKey &key,
    const std::shared_ptr<indexes::VectorRecord> &vector_record,
    size_t vector_size) {
  vmsdk::VerifyMainThread();
  auto key_str = vmsdk::MakeUniqueValkeyString(key.first->Str());
  bool untrack = true;
  absl::Cleanup source_closer = [&untrack, this, &key] {
    if (untrack) {
      absl::MutexLock lock(&mutex_);
      tracked_vectors_.erase(key);
    }
  };
  auto open_key = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.Get().get(), key_str.get(), VALKEYMODULE_WRITE);
  if (!open_key) {
    return false;
  }
  auto attribute_identifier_str = vmsdk::MakeUniqueValkeyString(key.second);
  if (ValkeyModule_KeyType(open_key.get()) != VALKEYMODULE_KEYTYPE_HASH) {
    return false;
  }
  if (ValkeyModule_HashHasStringRef(
          open_key.get(), attribute_identifier_str.get()) != VALKEYMODULE_OK) {
    untrack = false;
    return false;
  }

  ValkeyModuleString *record{nullptr};
  ValkeyModule_HashGet(open_key.get(), VALKEYMODULE_HASH_NONE,
                       attribute_identifier_str.get(), &record, nullptr);
  if (!record) {
    return false;
  }
  vmsdk::UniqueValkeyString record_ptr(record);
  if (vmsdk::ToStringView(record) !=
      absl::string_view(vector_record->GetRawVector(), vector_size)) {
    return false;
  }
  untrack = false;
  if (ValkeyModule_HashSetStringRef(
          open_key.get(), attribute_identifier_str.get(),
          vector_record->GetRawVector(), vector_size) != VALKEYMODULE_OK) {
    ++stats_.hash_extern_errors;
    return false;
  }

  absl::MutexLock lock(&mutex_);
  auto it = tracked_vectors_.find(key);
  if (it == tracked_vectors_.end()) {
    return false;
  }
  if (!it->second.shared_externally) {
    ++stats_.shared_externally_cnt;
  }
  it->second.shared_externally = vector_record;
  return true;
}

void VectorRegistry::BatchUntrackExpired(
    absl::string_view attribute_identifier,
    InternedStringHashMap<indexes::TrackedKeyMetadata>
        &&tracked_metadata_by_key) {
  // if hash registration is supported, a strong reference is held for the
  // vectors in the hash and therefore they cannot expire.
  if (hash_registration_supported_) {
    return;
  }
  absl::MutexLock pending_lock(&pending_mutex_);
  absl::MutexLock lock(&mutex_);
  for (auto &&[key, _] : tracked_metadata_by_key) {
    RegistryKey search_key =
        std::make_pair(key, std::string(attribute_identifier));
    LockFreeUntrackExpiredPending(search_key);
    LockFreeUntrackExpired(search_key);
  }
}

void VectorRegistry::LockFreeUntrackExpiredPending(
    const RegistryKey &search_key) {
  auto it = pending_shared_externally_.find(search_key);
  if (it != pending_shared_externally_.end()) {
    if (it->second.record.expired()) {
      pending_shared_externally_.erase(it);
    }
  }
}

void VectorRegistry::LockFreeUntrackExpired(const RegistryKey &search_key) {
  auto it = tracked_vectors_.find(search_key);
  if (it != tracked_vectors_.end()) {
    if (it->second.vector.record.expired()) {
      tracked_vectors_.erase(it);
    }
  }
}

void VectorRegistry::UntrackExpired(const InternedStringPtr &key,
                                    absl::string_view attribute_identifier) {
  // if hash registration is supported, a strong reference is held for the
  // vectors in the hash and therefore they cannot expire.
  if (hash_registration_supported_) {
    return;
  }
  RegistryKey search_key =
      std::make_pair(key, std::string(attribute_identifier));
  absl::MutexLock pending_lock(&pending_mutex_);
  absl::MutexLock lock(&mutex_);
  LockFreeUntrackExpiredPending(search_key);
  LockFreeUntrackExpired(search_key);
}

void VectorRegistry::Untrack(const InternedStringPtr &key,
                             absl::string_view attribute_identifier) {
  RegistryKey search_key =
      std::make_pair(key, std::string(attribute_identifier));
  {
    absl::MutexLock pending_lock(&pending_mutex_);
    pending_shared_externally_.erase(search_key);
  }
  {
    absl::MutexLock lock(&mutex_);
    auto it = tracked_vectors_.find(search_key);
    if (it != tracked_vectors_.end()) {
      if (it->second.shared_externally) {
        --stats_.shared_externally_cnt;
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
