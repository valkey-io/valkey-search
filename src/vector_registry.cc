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
  
  std::shared_ptr<indexes::VectorRecord> locked_record;
  size_t record_size = 0;
  {
    absl::MutexLock lock(&mutex_);
    auto it = tracked_vectors_.find(search_key);
    if (it != tracked_vectors_.end()) {
      locked_record = it->second.record.lock();
      record_size = it->second.record_size;
    }
  }

  if (locked_record) {
    // If the vector payload matches, successfully reused!
    // We perform memcmp outside the lock to minimize lock contention.
    if (record_size == vector.size() &&
        std::memcmp(locked_record->GetRawVector(), vector.data(),
                    vector.size()) == 0) {
      absl::MutexLock lock(&mutex_);
      ++stats_.deduplication_hits;
      stats_.memory_saved_bytes += vector.size();
      return locked_record;
    }
  }
  
  auto vector_record =
      indexes::VectorRecord::Construct(vector, magnitude, allocator);

  absl::MutexLock lock(&mutex_);
  ++stats_.deduplication_misses;
  auto &tracked_value = tracked_vectors_[search_key];
  auto old_shared_externally = tracked_value.shared_externally;
  tracked_value = {vector_record, old_shared_externally, vector.size()};
  return vector_record;
}

bool VectorRegistry::EngineShare(
    const InternedStringPtr &key, absl::string_view attribute_identifier,
    const indexes::VectorRecord *vector_record, size_t vector_size,
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
    absl::MutexLock lock(&share_externally_mutex_);
    {
      absl::MutexLock lock(&mutex_);
      auto it = tracked_vectors_.find(search_key);
      if ((it != tracked_vectors_.end() &&
           it->second.shared_externally.get() == vector_record) ||
          pending_shared_externally_.contains(search_key)) {
        // Already shared externally or pending to be shared externally, no need
        // to do it again.
        return false;
      };
    }
    pending_shared_externally_.insert(search_key);
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
        ExecuteBatchEngineShare();
      },
      true);
}

void VectorRegistry::ExecuteBatchEngineShare() {
  vmsdk::VerifyMainThread();

  absl::flat_hash_set<RegistryKey> pending_shared_externally;
  {
    absl::MutexLock lock(&share_externally_mutex_);
    pending_shared_externally.swap(pending_shared_externally_);
  }
  for (const auto &key : pending_shared_externally) {
    PerformEngineShare(key);
  }
  absl::MutexLock lock(&share_externally_mutex_);
  for (const auto &key : pending_shared_externally) {
    pending_shared_externally_.erase(key);
  }
  if (pending_shared_externally_.empty()) {
    scheduled_run_by_main_ = false;
    return;
  }
  ScheduleAsyncByMain();
}

bool VectorRegistry::PerformEngineShare(const RegistryKey &key) {
  vmsdk::VerifyMainThread();
  auto key_str = vmsdk::MakeUniqueValkeyString(key.first->Str());
  auto open_key = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.Get().get(), key_str.get(), VALKEYMODULE_WRITE);
  CHECK(open_key);
  auto attribute_identifier_str = vmsdk::MakeUniqueValkeyString(key.second);
  if (ValkeyModule_KeyType(open_key.get()) != VALKEYMODULE_KEYTYPE_HASH) {
    return false;
  }
  if (ValkeyModule_HashHasStringRef(
          open_key.get(), attribute_identifier_str.get()) != VALKEYMODULE_OK) {
    return false;
  }
  std::shared_ptr<indexes::VectorRecord> vector_record;
  size_t vector_size;
  {
    absl::MutexLock lock(&mutex_);
    auto it = tracked_vectors_.find(key);
    if (it == tracked_vectors_.end()) {
      return false;
    }
    vector_record = it->second.record.lock();
    if (!vector_record || vector_record == it->second.shared_externally) {
      return false;
    }
    vector_size = it->second.record_size;
  }

  if (!vector_record) {
    return false;
  }

  ValkeyModuleString *record{nullptr};
  ValkeyModule_HashGet(open_key.get(), VALKEYMODULE_HASH_CFIELDS,
                       attribute_identifier_str.get(), &record, nullptr);
  if (!record ||
      vmsdk::ToStringView(record) !=
          absl::string_view(vector_record->GetRawVector(), vector_size)) {
    return false;
  }
  if (ValkeyModule_HashSetStringRef(
          open_key.get(), attribute_identifier_str.get(),
          vector_record->GetRawVector(), vector_size) != VALKEYMODULE_OK) {
    absl::MutexLock lock(&mutex_);
    ++stats_.hash_extern_errors;
    return false;
  }

  {
    absl::MutexLock lock(&mutex_);
    auto it = tracked_vectors_.find(key);
    if (it != tracked_vectors_.end()) {
      if (!it->second.shared_externally) {
        ++stats_.shared_externally_cnt;
      }
      it->second.shared_externally = vector_record;
    }
  }
  return true;
}

void VectorRegistry::Untrack(const InternedStringPtr &key,
                             absl::string_view attribute_identifier,
                             bool attribute_deleted) {
  RegistryKey search_key =
      std::make_pair(key, std::string(attribute_identifier));
  {
    absl::MutexLock lock(&share_externally_mutex_);
    pending_shared_externally_.erase(search_key);
  }
  absl::MutexLock lock(&mutex_);
  if (attribute_deleted) {
    auto it = tracked_vectors_.find(search_key);
    if (it != tracked_vectors_.end()) {
      if (it->second.shared_externally) {
        --stats_.shared_externally_cnt;
      }
      tracked_vectors_.erase(it);
    }
    return;
  }
  auto it = tracked_vectors_.find(search_key);
  if (it != tracked_vectors_.end()) {
    if (!it->second.shared_externally) {
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

void VectorRegistry::Reset() {
  vmsdk::VerifyMainThread();
  absl::MutexLock lock(&mutex_);
  tracked_vectors_.clear();
}

}  // namespace valkey_search
