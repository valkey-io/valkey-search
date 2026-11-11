/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <cstring>
#include <memory>
#include <optional>
#include <vector>

#include "absl/cleanup/cleanup.h"
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
      << "Failed to resolve required ValkeyModule StringRef APIs "
         "(HashSetStringRef/HashHasStringRef). Valkey version should be 9.1.0 "
         "and above";
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
  float reciprocal_magnitude = vector_base->ComputeReciprocalMagnitude(record);
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
    if (!ShouldRetainMissingVector(db_num, search_key, attribute_data_type)) {
      EraseTrackedRecord(db_num, search_key);
    }
    return {};
  }

  auto vector_str = vmsdk::ToStringView(vector);
  if (!vector_base->IsValidSizeVector(vector_str)) {
    if (IsEraseTrackedRecordSafe(db_num, search_key, vector_str,
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
      tracked_keys_by_db_[db_num][BorrowedInternedStringPtr(key)].insert(
          BorrowedInternedStringPtr(search_key.attribute_identifier));
    }
  }

  ShareWithValkey(db_num, key,
                  vector_base->GetInternedAttributeIdentifier()->Str(),
                  result.vector_record, vector_base->GetVectorDataSize(),
                  attribute_data_type);
  return result;
}

void VectorRegistry::RemoveFromTrackedKeysIndex(
    int db_num, const InternedStringPtr &key,
    const InternedStringPtr &attribute_identifier) {
  auto db_it = tracked_keys_by_db_.find(db_num);
  if (db_it == tracked_keys_by_db_.end()) {
    return;
  }
  auto key_it = db_it->second.find(key);
  if (key_it == db_it->second.end()) {
    return;
  }
  key_it->second.erase(attribute_identifier);
  if (key_it->second.empty()) {
    db_it->second.erase(key_it);
    if (db_it->second.empty()) {
      tracked_keys_by_db_.erase(db_it);
    }
  }
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
  RemoveFromTrackedKeysIndex(db_num, key, attribute_identifier);
  auto record = it->second;
  db_it->second.erase(it);
  if (db_it->second.empty()) {
    tracked_vectors_.erase(db_it);
  }
  return record;
}

absl::flat_hash_map<InternedStringPtr, size_t>
VectorRegistry::GetTrackedAttributes(int db_num, absl::string_view key) const {
  vmsdk::VerifyMainThread();
  absl::flat_hash_map<InternedStringPtr, size_t> result;
  auto db_keys_it = tracked_keys_by_db_.find(db_num);
  if (db_keys_it == tracked_keys_by_db_.end()) {
    return result;
  }
  auto key_it = db_keys_it->second.find(key);
  if (key_it == db_keys_it->second.end()) {
    return result;
  }
  auto db_tracked_it = tracked_vectors_.find(db_num);
  if (db_tracked_it == tracked_vectors_.end()) {
    return result;
  }
  auto interned_key = key_it->first.Materialize();
  for (const auto &borrowed_attr : key_it->second) {
    auto attr = borrowed_attr.Materialize();
    RegistryKey rk{.key = interned_key, .attribute_identifier = attr};
    auto it = db_tracked_it->second.find(rk);
    if (it != db_tracked_it->second.end()) {
      result.emplace(std::move(attr), it->second.size);
    }
  }
  return result;
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

namespace {

enum class HashFieldRefState {
  kMissing,
  kUnshared,
  kShared,
};

HashFieldRefState GetHashFieldRefState(ValkeyModuleKey *key_obj,
                                       ValkeyModuleString *attr_str) {
  int exists = 0;
  if (ValkeyModule_HashGet(key_obj, VALKEYMODULE_HASH_EXISTS, attr_str, &exists,
                           nullptr) != VALKEYMODULE_OK ||
      !exists) {
    return HashFieldRefState::kMissing;
  }
  return ValkeyModule_HashHasStringRef(key_obj, attr_str) == 1
             ? HashFieldRefState::kShared
             : HashFieldRefState::kUnshared;
}

std::optional<long long> ExtractSingleIntegerArrayReply(
    ValkeyModuleCallReply *reply) {
  if (!reply || ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_ARRAY) {
    return std::nullopt;
  }
  ValkeyModuleCallReply *elem = ValkeyModule_CallReplyArrayElement(reply, 0);
  if (!elem || ValkeyModule_CallReplyType(elem) != VALKEYMODULE_REPLY_INTEGER) {
    return std::nullopt;
  }
  return ValkeyModule_CallReplyInteger(elem);
}

long long GetHashFieldExpiryMs(ValkeyModuleCtx *ctx,
                               ValkeyModuleString *key_str,
                               ValkeyModuleString *attr_str) {
  if (!key_str) {
    return -1;
  }
  auto reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, "HPEXPIRETIME", "sccs", key_str, "FIELDS", "1", attr_str));
  return ExtractSingleIntegerArrayReply(reply.get()).value_or(-1);
}

bool RestoreHashFieldExpiryMs(ValkeyModuleCtx *ctx, ValkeyModuleString *key_str,
                              ValkeyModuleString *attr_str,
                              long long expiry_ms) {
  if (expiry_ms <= 0) {
    return true;
  }
  KeyspaceEventManager::Instance().SetSuppressHexpireNotifications(true);
  auto suppress_guard = absl::MakeCleanup([]() {
    KeyspaceEventManager::Instance().SetSuppressHexpireNotifications(false);
  });
  auto expire_reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, "HPEXPIREAT", "slccs", key_str, expiry_ms, "FIELDS", "1", attr_str));
  return ExtractSingleIntegerArrayReply(expire_reply.get()).value_or(0) >= 1;
}

}  // namespace

bool VectorRegistry::ShouldRetainMissingVector(
    int db_num, const RegistryKey &search_key,
    const data_model::AttributeDataType &attribute_data_type) {
  if (!hash_vector_sharing_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end() || !db_it->second.contains(search_key)) {
    return false;
  }
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  auto key_str = vmsdk::MakeUniqueValkeyString(search_key.key->Str());
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  return !key_obj;
}

bool VectorRegistry::IsEraseTrackedRecordSafe(
    int db_num, const RegistryKey &search_key, absl::string_view vector_str,
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
  auto key_str = vmsdk::MakeUniqueValkeyString(search_key.key->Str());
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  if (!key_obj) {
    return false;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(search_key.attribute_identifier->Str());
  return GetHashFieldRefState(key_obj.get(), attribute_identifier_str.get()) !=
         HashFieldRefState::kShared;
}

bool VectorRegistry::ShareWithValkey(
    int db_num, const InternedStringPtr &key,
    absl::string_view attribute_identifier,
    const indexes::VectorRecord &vector_record, size_t vector_size,
    const data_model::AttributeDataType &attribute_data_type) {
  vmsdk::VerifyMainThread();
  if (!hash_vector_sharing_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }
  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  if (!key_obj) {
    return false;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(attribute_identifier);
  if (GetHashFieldRefState(key_obj.get(), attribute_identifier_str.get()) !=
      HashFieldRefState::kUnshared) {
    return false;
  }

  if (ForceHashSharingError.GetValue() ||
      ValkeyModule_HashSetStringRef(
          key_obj.get(), attribute_identifier_str.get(),
          vector_record.GetRawVector(), vector_size) != VALKEYMODULE_OK) {
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
    tracked_keys_by_db_.clear();
    tracked_vectors_.clear();
    pending_unshares_.clear();
    return;
  }
  tracked_keys_by_db_.erase(flush_info->dbnum);
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
  swap_dbs(tracked_keys_by_db_);
  swap_dbs(pending_unshares_);
}

bool VectorRegistry::UnshareWithValkey(
    int db_num, ValkeyModuleString *key_str,
    absl::string_view attribute_identifier,
    const indexes::VectorRecord &vector_record, size_t vector_size) {
  vmsdk::VerifyMainThread();
  if (!hash_vector_sharing_ || !key_str) {
    return false;
  }
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  auto attr_str = vmsdk::MakeUniqueValkeyString(attribute_identifier);
  const long long expiry_ms =
      GetHashFieldExpiryMs(ctx_.get(), key_str, attr_str.get());
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str,
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  if (!key_obj ||
      ValkeyModule_KeyType(key_obj.get()) != VALKEYMODULE_KEYTYPE_HASH ||
      GetHashFieldRefState(key_obj.get(), attr_str.get()) !=
          HashFieldRefState::kShared) {
    return false;
  }
  auto raw_vec = vector_record.GetRawVector();
  auto val_str =
      vmsdk::MakeUniqueValkeyString(absl::string_view(raw_vec, vector_size));
  ValkeyModule_HashSet(key_obj.get(), VALKEYMODULE_HASH_NONE, attr_str.get(),
                       val_str.get(), nullptr);
  key_obj.reset();
  return RestoreHashFieldExpiryMs(ctx_.get(), key_str, attr_str.get(),
                                  expiry_ms);
}

void VectorRegistry::OnKeyDeleted(int db_num, absl::string_view key) {
  vmsdk::VerifyMainThread();
  EraseTrackedRecordsForKey(db_num, key);
}

void VectorRegistry::EraseTrackedRecordsForKey(int db_num,
                                               absl::string_view key) {
  auto db_it = tracked_keys_by_db_.find(db_num);
  if (db_it == tracked_keys_by_db_.end()) {
    return;
  }
  auto key_it = db_it->second.find(key);
  if (key_it == db_it->second.end()) {
    return;
  }
  auto interned_key = key_it->first.Materialize();
  std::vector<InternedStringPtr> attrs;
  attrs.reserve(key_it->second.size());
  for (const auto &borrowed_attr : key_it->second) {
    attrs.push_back(borrowed_attr.Materialize());
  }
  for (const auto &attr : attrs) {
    EraseTrackedRecord(db_num,
                       {.key = interned_key, .attribute_identifier = attr});
  }
}

void VectorRegistry::MoveKey(
    int src_db_num, const InternedStringPtr &src_key, int dst_db_num,
    ValkeyModuleString *dst_key,
    const absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action>
        &actions) {
  vmsdk::VerifyMainThread();
  auto dst_key_view = vmsdk::ToStringView(dst_key);

  // If moving/renaming onto a different key, any pre-existing tracked records
  // at the destination key belonged to the overwritten key and must be purged.
  if (src_db_num != dst_db_num || src_key->Str() != dst_key_view) {
    EraseTrackedRecordsForKey(dst_db_num, dst_key_view);
  }

  if (actions.empty() ||
      tracked_vectors_.find(src_db_num) == tracked_vectors_.end()) {
    return;
  }

  InternedStringPtr interned_dst_key;
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
    if (!pending_unshares_.empty()) {
      if (!interned_dst_key) {
        interned_dst_key = StringInternStore::Intern(dst_key_view);
      }
      CancelPendingUnshare(
          dst_db_num,
          RegistryKey{.key = interned_dst_key, .attribute_identifier = attr});
    }
    if (action == Action::kMove) {
      if (!interned_dst_key) {
        interned_dst_key = StringInternStore::Intern(dst_key_view);
      }
      RegistryKey dst_search_key{
          .key = interned_dst_key,
          .attribute_identifier = attr,
      };
      // Transfer the tracked vector record to the destination database and key.
      tracked_vectors_[dst_db_num].insert_or_assign(dst_search_key, *record);
      tracked_keys_by_db_[dst_db_num]
                         [BorrowedInternedStringPtr(interned_dst_key)]
                             .insert(BorrowedInternedStringPtr(attr));
    } else if (hash_vector_sharing_) {
      // The attribute is no longer indexed at destination; materialize the
      // vector payload back to Valkey before untracking it.
      UnshareWithValkey(dst_db_num, dst_key, attr->Str(), record->vector_record,
                        record->size);
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
  if (in_process_pending_unshares_ || pending_unshares_.empty() ||
      batch_size == 0) {
    return 0;
  }
  in_process_pending_unshares_ = true;
  auto reset_guard =
      absl::MakeCleanup([this]() { in_process_pending_unshares_ = false; });

  using PendingNode = absl::flat_hash_set<RegistryKey>::node_type;
  struct ExtractedPendingItem {
    int db_num;
    PendingNode node;
  };
  std::vector<ExtractedPendingItem> batch;
  batch.reserve(batch_size);
  for (auto db_it = pending_unshares_.begin();
       db_it != pending_unshares_.end() && batch.size() < batch_size;) {
    int db_num = db_it->first;
    auto &pending_keys = db_it->second;
    while (!pending_keys.empty() && batch.size() < batch_size) {
      batch.push_back(ExtractedPendingItem{
          .db_num = db_num,
          .node = pending_keys.extract(pending_keys.begin()),
      });
    }
    if (pending_keys.empty()) {
      pending_unshares_.erase(db_it++);
    } else {
      ++db_it;
    }
  }

  size_t processed = 0;
  std::vector<ExtractedPendingItem> deferred_nodes;
  for (auto &item : batch) {
    int db_num = item.db_num;
    const auto &rk = item.node.value();
    ++processed;

    auto tracked_db_it = tracked_vectors_.find(db_num);
    if (tracked_db_it == tracked_vectors_.end()) {
      continue;
    }
    auto tracked_it = tracked_db_it->second.find(rk);
    if (tracked_it == tracked_db_it->second.end()) {
      continue;
    }
    vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
    auto key_str = vmsdk::MakeUniqueValkeyString(rk.key->Str());
    auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
        ctx_.get(), key_str.get(),
        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
    if (HasMatchingVectorIndex(db_num, rk.key->Str(), rk.attribute_identifier,
                               key_obj.get(), tracked_it->second.size,
                               /*require_tracked=*/true)) {
      continue;
    }
    if (tracked_it->second.vector_record.RefCount() > 1) {
      deferred_nodes.push_back(std::move(item));
      continue;
    }
    if (hash_vector_sharing_) {
      int key_type = key_obj ? ValkeyModule_KeyType(key_obj.get())
                             : VALKEYMODULE_KEYTYPE_EMPTY;
      if (key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
        continue;
      }
      auto attr_str =
          vmsdk::MakeUniqueValkeyString(rk.attribute_identifier->Str());
      const bool is_shared =
          key_type == VALKEYMODULE_KEYTYPE_HASH &&
          GetHashFieldRefState(key_obj.get(), attr_str.get()) ==
              HashFieldRefState::kShared;
      key_obj.reset();
      auto record = tracked_it->second;
      RemoveFromTrackedKeysIndex(db_num, rk.key, rk.attribute_identifier);
      tracked_db_it->second.erase(tracked_it);
      if (tracked_db_it->second.empty()) {
        tracked_vectors_.erase(tracked_db_it);
      }
      if (is_shared) {
        UnshareWithValkey(db_num, key_str.get(), rk.attribute_identifier->Str(),
                          record.vector_record, record.size);
      }
      continue;
    }
    RemoveFromTrackedKeysIndex(db_num, rk.key, rk.attribute_identifier);
    tracked_db_it->second.erase(tracked_it);
    if (tracked_db_it->second.empty()) {
      tracked_vectors_.erase(tracked_db_it);
    }
  }

  for (auto &deferred : deferred_nodes) {
    pending_unshares_[deferred.db_num].insert(std::move(deferred.node));
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
