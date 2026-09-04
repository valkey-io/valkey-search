/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <cstring>
#include <memory>

#include "absl/log/check.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/keyspace_event_manager.h"
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

  RegistryKey search_key{
      .key = key,
      .attribute_identifier = vector_base->GetInternedAttributeIdentifier(),
  };
  CancelPendingUnshare(db_num, search_key);

  if (!vector) {
    EraseTrackedRecord(db_num, search_key);
    return {};
  }

  auto vector_str = vmsdk::ToStringView(vector);
  if (!vector_base->IsValidSizeVector(vector_str)) {
    if (IsEraseTrackedRecordSafe(
            db_num, key, vector_str,
            vector_base->GetInternedAttributeIdentifier()->Str(),
            attribute_data_type)) {
      EraseTrackedRecord(db_num, search_key);
    }
    return ConstructVectorRecord(vector_str, vector_base);
  }

  indexes::VectorRecordWithSize result;
  auto &db_tracked = tracked_vectors_[db_num];
  auto it = db_tracked.find(search_key);
  if (it != db_tracked.end() && it->second == vector_str) {
    ++stats_.dedup_cnt;
    result = it->second;
  } else {
    result = ConstructVectorRecord(vector_str, vector_base);
    if (it != db_tracked.end()) {
      it->second = result;
    } else {
      db_tracked.emplace(search_key, result);
    }
  }

  ShareWithValkey(db_num, key,
                  vector_base->GetInternedAttributeIdentifier()->Str(),
                  result.vector_record.get(), vector_base->GetVectorDataSize(),
                  attribute_data_type);
  return result;
}

std::optional<indexes::VectorRecordWithSize>
VectorRegistry::ExtractTrackedRecord(
    int db_num, const InternedStringPtr &key,
    const InternedStringPtr &attribute_identifier) {
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return std::nullopt;
  }
  RegistryKey search_key{
      .key = key,
      .attribute_identifier = attribute_identifier,
  };
  auto it = db_it->second.find(search_key);
  if (it == db_it->second.end()) {
    return std::nullopt;
  }
  auto record = it->second;
  db_it->second.erase(it);
  if (db_it->second.empty()) {
    tracked_vectors_.erase(db_it);
  }
  return record;
}

void VectorRegistry::CancelPendingUnshare(int db_num, const RegistryKey &key) {
  if (pending_unshares_.empty()) {
    return;
  }
  auto db_it = pending_unshares_.find(db_num);
  if (db_it == pending_unshares_.end()) {
    return;
  }
  db_it->second.erase(key);
  if (db_it->second.empty()) {
    pending_unshares_.erase(db_it);
  }
}

void VectorRegistry::EraseTrackedRecord(int db_num, const RegistryKey &key) {
  CancelPendingUnshare(db_num, key);
  ExtractTrackedRecord(db_num, key.key, key.attribute_identifier);
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
  if (!key_obj) {
    return false;
  }
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
  size_t total_entries = 0;
  for (const auto &[_, db_map] : tracked_vectors_) {
    total_entries += db_map.size();
  }
  stats_.entry_cnt = total_entries;
  return stats_;
}

void VectorRegistry::OnFlushDB(const ValkeyModuleFlushInfo *flush_info) {
  vmsdk::VerifyMainThread();
  if (!flush_info || flush_info->dbnum == -1) {
    tracked_vectors_.clear();
    pending_unshares_.clear();
    return;
  }
  tracked_vectors_.erase(flush_info->dbnum);
  pending_unshares_.erase(flush_info->dbnum);
}

void VectorRegistry::OnSwapDB(const ValkeyModuleSwapDbInfo *swap_info) {
  vmsdk::VerifyMainThread();
  if (!swap_info || swap_info->dbnum_first == swap_info->dbnum_second) {
    return;
  }
  int db_num1 = swap_info->dbnum_first;
  int db_num2 = swap_info->dbnum_second;
  auto swap_dbs = [db_num1, db_num2](auto &map) {
    auto it1 = map.find(db_num1);
    auto it2 = map.find(db_num2);
    if (it1 == map.end() && it2 == map.end()) {
      return;
    }
    if (it1 != map.end() && it2 != map.end()) {
      std::swap(it1->second, it2->second);
    } else if (it1 != map.end()) {
      auto entries = std::move(it1->second);
      map.erase(it1);
      map.emplace(db_num2, std::move(entries));
    } else {
      auto entries = std::move(it2->second);
      map.erase(it2);
      map.emplace(db_num1, std::move(entries));
    }
  };
  swap_dbs(tracked_vectors_);
  swap_dbs(pending_unshares_);
}

bool VectorRegistry::UnshareWithValkey(
    ValkeyModuleKey *key_obj, absl::string_view attribute_identifier,
    const indexes::VectorRecord *vector_record, size_t vector_size) {
  vmsdk::VerifyMainThread();
  if (!hash_vector_sharing_ || !key_obj ||
      ValkeyModule_KeyType(key_obj) != VALKEYMODULE_KEYTYPE_HASH) {
    return false;
  }
  auto attr_str = vmsdk::MakeUniqueValkeyString(attribute_identifier);
  if (ValkeyModule_HashHasStringRef(key_obj, attr_str.get()) != 1) {
    return false;
  }
  auto raw_vec = vector_record->GetRawVector();
  auto val_str =
      vmsdk::MakeUniqueValkeyString(absl::string_view(raw_vec, vector_size));
  return ValkeyModule_HashSet(key_obj, VALKEYMODULE_HASH_NONE, attr_str.get(),
                              val_str.get(), nullptr) == VALKEYMODULE_OK;
}

void VectorRegistry::MoveKey(
    int src_db_num, const InternedStringPtr &src_key, int dst_db_num,
    ValkeyModuleString *dst_key,
    const absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action>
        &actions) {
  vmsdk::VerifyMainThread();
  if (actions.empty() ||
      tracked_vectors_.find(src_db_num) == tracked_vectors_.end()) {
    return;
  }

  auto interned_dst_key =
      StringInternStore::Intern(vmsdk::ToStringView(dst_key));

  vmsdk::UniqueValkeyOpenKey key_obj;
  for (const auto &[attr, action] : actions) {
    RegistryKey src_search_key{
        .key = src_key,
        .attribute_identifier = attr,
    };
    CancelPendingUnshare(src_db_num, src_search_key);
    auto record = ExtractTrackedRecord(src_db_num, src_key, attr);
    if (!record) {
      continue;
    }
    RegistryKey dst_search_key{
        .key = interned_dst_key,
        .attribute_identifier = attr,
    };
    CancelPendingUnshare(dst_db_num, dst_search_key);
    if (action == Action::kMove) {
      // Transfer the tracked vector record to the destination database and key.
      tracked_vectors_[dst_db_num].insert_or_assign(dst_search_key, *record);
    } else {
      // The attribute is no longer indexed at destination; materialize the
      // vector payload back to Valkey before untracking it.
      if (hash_vector_sharing_) {
        if (!key_obj) {
          ValkeyModule_SelectDb(ctx_.get(), dst_db_num);
          key_obj = vmsdk::MakeUniqueValkeyOpenKey(
              ctx_.get(), dst_key,
              VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
        }
        UnshareWithValkey(key_obj.get(), attr->Str(),
                          record->vector_record.get(), record->size);
      }
      // Clean up any pre-existing tracked entry at destination key if it was
      // overwritten.
      EraseTrackedRecord(dst_db_num, dst_search_key);
    }
  }
}

bool VectorRegistry::HasMatchingVectorIndex(
    int db_num, absl::string_view key,
    const InternedStringPtr &attribute_identifier, ValkeyModuleKey *key_obj,
    size_t vector_size, bool require_tracked) const {
  vmsdk::VerifyMainThread();
  const auto subscriptions =
      KeyspaceEventManager::Instance().GetMatchingSubscriptions(
          key, VALKEYMODULE_NOTIFY_ALL, db_num);
  InternedStringPtr interned_key;
  if (require_tracked) {
    interned_key = StringInternStore::Intern(key);
  }
  for (const auto *sub : subscriptions) {
    if (key_obj && !sub->GetAttributeDataType().IsProperType(key_obj)) {
      continue;
    }
    for (const auto *index : sub->GetVectorIndexes()) {
      if (index->GetInternedAttributeIdentifier() == attribute_identifier) {
        if (index->GetVectorDataSize() != vector_size) {
          continue;
        }
        if (require_tracked && !index->IsTracked(interned_key)) {
          continue;
        }
        return true;
      }
    }
  }
  return false;
}

void VectorRegistry::RemoveIndexKeys(
    int db_num, const InternedStringPtr &attribute_identifier,
    absl::Span<const InternedStringPtr> keys) {
  vmsdk::VerifyMainThread();
  if (keys.empty()) {
    return;
  }
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return;
  }
  const auto &db_tracked = db_it->second;
  auto &db_pending = pending_unshares_[db_num];

  for (const auto &key : keys) {
    RegistryKey rk{
        .key = key,
        .attribute_identifier = attribute_identifier,
    };
    if (db_tracked.contains(rk)) {
      db_pending.insert(std::move(rk));
    }
  }

  if (db_pending.empty()) {
    pending_unshares_.erase(db_num);
    return;
  }
  ProcessPendingUnshares(options::GetVectorUnshareBatchSize().GetValue());
}

void VectorRegistry::RemoveIndexKeys(
    int db_num, const InternedStringPtr &attribute_identifier,
    absl::flat_hash_map<uint64_t, InternedStringPtr> keys) {
  vmsdk::VerifyMainThread();
  if (keys.empty()) {
    return;
  }
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return;
  }
  const auto &db_tracked = db_it->second;
  auto &db_pending = pending_unshares_[db_num];

  for (auto &[_, key] : keys) {
    RegistryKey rk{
        .key = std::move(key),
        .attribute_identifier = attribute_identifier,
    };
    if (db_tracked.contains(rk)) {
      db_pending.insert(std::move(rk));
    }
  }

  if (db_pending.empty()) {
    pending_unshares_.erase(db_num);
    return;
  }
  ProcessPendingUnshares(options::GetVectorUnshareBatchSize().GetValue());
}

size_t VectorRegistry::ProcessPendingUnshares(uint32_t batch_size) {
  vmsdk::VerifyMainThread();
  if (pending_unshares_.empty() || batch_size == 0) {
    return 0;
  }
  size_t processed = 0;
  std::vector<int> empty_dbs;
  for (auto &[db_num, pending_keys] : pending_unshares_) {
    if (processed >= batch_size) {
      break;
    }
    vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);

    while (!pending_keys.empty() && processed < batch_size) {
      auto node = pending_keys.extract(pending_keys.begin());
      const auto &rk = node.value();
      ++processed;

      auto tracked_db_it = tracked_vectors_.find(db_num);
      if (tracked_db_it == tracked_vectors_.end()) {
        continue;
      }
      auto tracked_it = tracked_db_it->second.find(rk);
      if (tracked_it == tracked_db_it->second.end()) {
        continue;
      }
      auto key_str = vmsdk::MakeUniqueValkeyString(rk.key->Str());
      auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
          ctx_.get(), key_str.get(),
          VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
      if (tracked_it->second.vector_record.use_count() > 1 ||
          HasMatchingVectorIndex(db_num, rk.key->Str(), rk.attribute_identifier,
                                 key_obj.get(), tracked_it->second.size,
                                 /*require_tracked=*/true)) {
        continue;
      }
      auto record = tracked_it->second;
      tracked_db_it->second.erase(tracked_it);
      if (tracked_db_it->second.empty()) {
        tracked_vectors_.erase(tracked_db_it);
      }
      if (hash_vector_sharing_ && key_obj) {
        UnshareWithValkey(key_obj.get(), rk.attribute_identifier->Str(),
                          record.vector_record.get(), record.size);
      }
    }

    if (pending_keys.empty()) {
      empty_dbs.push_back(db_num);
    }
  }

  for (int db_num : empty_dbs) {
    pending_unshares_.erase(db_num);
  }

  return processed;
}

size_t VectorRegistry::GetPendingUnsharesCount() const {
  vmsdk::VerifyMainThread();
  size_t count = 0;
  for (const auto &[_, pending_keys] : pending_unshares_) {
    count += pending_keys.size();
  }
  return count;
}

void VectorRegistry::OnServerCronCallback(ValkeyModuleCtx *ctx,
                                          ValkeyModuleEvent eid,
                                          uint64_t subevent, void *data) {
  vmsdk::VerifyMainThread();
  if (pending_unshares_.empty()) {
    return;
  }
  ProcessPendingUnshares(options::GetVectorUnshareBatchSize().GetValue());
}

}  // namespace valkey_search
