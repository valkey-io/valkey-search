/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_registry.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/keyspace_event_manager.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
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
    const indexes::VectorBase *vector_base, ValkeyModuleCtx *ctx) {
  vmsdk::VerifyMainThread();

  RegistryKey search_key{
      .key = key,
      .attribute_identifier = vector_base->GetInternedAttributeIdentifier(),
  };

  if (!vector) {
    if (!ShouldRetainMissingVector(ctx, db_num, search_key,
                                   attribute_data_type)) {
      EraseTrackedRecord(db_num, search_key);
    }
    return {};
  }

  CancelPendingUnshare(db_num, search_key);

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
  if (it != db_tracked.end() && it->second.record == vector_str) {
    ++stats_.dedup_cnt;
    result = it->second.record;
  } else {
    result = ConstructVectorRecord(vector_str, vector_base);
    if (it != db_tracked.end()) {
      it->second.record = result;
    } else {
      it = db_tracked
               .emplace(search_key, TrackedVectorEntry{.record = result,
                                                       .is_shared = false})
               .first;
      AddToTrackedKeysIndex(db_num, search_key);
    }
  }

  const bool is_shared = ShareWithValkey(
      db_num, key, vector_base->GetInternedAttributeIdentifier()->Str(),
      result.vector_record, vector_base->GetVectorDataSize(),
      attribute_data_type);
  it->second.is_shared = is_shared;
  return result;
}

void VectorRegistry::AddToTrackedKeysIndex(int db_num, const RegistryKey &key) {
  tracked_keys_by_db_[db_num][BorrowedInternedStringPtr(key.key)].insert(
      BorrowedInternedStringPtr(key.attribute_identifier));
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
    // Nothing else into this db's table is held, so a migration may move
    // its entries.
    db_it->second.erase_and_migrate(key_it);
    if (db_it->second.empty()) {
      tracked_keys_by_db_.erase(db_it);
    }
  }
}

std::optional<VectorRegistry::TrackedVectorEntry>
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
  auto entry = it->second;
  db_it->second.erase_and_migrate(it);
  if (db_it->second.empty()) {
    tracked_vectors_.erase(db_it);
  }
  return entry;
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
      result.emplace(std::move(attr), it->second.record.size);
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
  db_it->second.ready.erase(key);
  db_it->second.deferred.erase(key);
  db_it->second.EraseLookupFailed(key);
  if (db_it->second.empty()) {
    pending_unshares_.erase(db_it);
  }
}

void VectorRegistry::EraseTrackedRecord(int db_num, const RegistryKey &key) {
  CancelPendingUnshare(db_num, key);
  ExtractTrackedRecord(db_num, key.key, key.attribute_identifier);
}

namespace {

// VALKEYMODULE_CTX_FLAGS_SLOT_IMPORT_CLIENT in Valkey 9.1+.
constexpr int kSlotImportClientFlag = (1 << 25);

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

std::optional<long long> GetHashFieldExpiryMs(ValkeyModuleCtx *ctx,
                                              ValkeyModuleString *key_str,
                                              ValkeyModuleString *attr_str) {
  if (!key_str) {
    return std::nullopt;
  }
  auto reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, "HPEXPIRETIME", "sccs", key_str, "FIELDS", "1", attr_str));
  return ExtractSingleIntegerArrayReply(reply.get());
}

bool RestoreHashFieldExpiryMs(ValkeyModuleCtx *ctx, ValkeyModuleString *key_str,
                              ValkeyModuleString *attr_str,
                              long long expiry_ms) {
  if (expiry_ms < 0) {
    return true;
  }
  KeyspaceEventManager::Instance().SetSuppressHexpireNotifications(true);
  auto suppress_guard = absl::MakeCleanup([]() {
    KeyspaceEventManager::Instance().SetSuppressHexpireNotifications(false);
  });
  auto expire_reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, "HPEXPIREAT", "slccs", key_str, expiry_ms, "FIELDS", "1", attr_str));
  // HPEXPIREAT returns 1 when the expiry was applied, 2 if the timestamp
  // crossed into the past on a primary (field expired and deleted immediately),
  // and <= 0 on missing key/field or condition failure.
  const auto reply = ExtractSingleIntegerArrayReply(expire_reply.get());
  if (reply == 1) {
    return true;
  }
  if (reply == 2 &&
      (ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_SLAVE) == 0) {
    // ValkeyModule_Call without "!" does not propagate HPEXPIREAT's internal
    // HDEL/DEL rewrite to replicas or AOF when the timestamp crosses now.
    auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
        ctx, key_str, VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
    const bool key_deleted = !key_obj || ValkeyModule_KeyType(key_obj.get()) ==
                                             VALKEYMODULE_KEYTYPE_EMPTY;
    key_obj.reset();
    const int rep_rc =
        key_deleted
            ? ValkeyModule_Replicate(ctx, "DEL", "s", key_str)
            : ValkeyModule_Replicate(ctx, "HDEL", "ss", key_str, attr_str);
    return rep_rc == VALKEYMODULE_OK;
  }
  return false;
}

}  // namespace

bool VectorRegistry::ShouldRetainMissingVector(
    ValkeyModuleCtx *ctx, int db_num, const RegistryKey &search_key,
    const data_model::AttributeDataType & /*attribute_data_type*/) {
  if (!hash_vector_sharing_) {
    return false;
  }
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return false;
  }
  auto tracked_it = db_it->second.find(search_key);
  if (tracked_it == db_it->second.end() || !tracked_it->second.is_shared) {
    return false;
  }
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  auto key_str = vmsdk::MakeUniqueValkeyString(search_key.key->Str());
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  const int key_type = key_obj ? ValkeyModule_KeyType(key_obj.get())
                               : VALKEYMODULE_KEYTYPE_EMPTY;
  if (key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
    CancelPendingUnshare(db_num, search_key);
    return true;
  }
  if (key_type != VALKEYMODULE_KEYTYPE_HASH) {
    return false;
  }
  const int caller_flags = ValkeyModule_GetContextFlags(ctx ? ctx : ctx_.get());
  constexpr int kAuthoritativeMutationFlags =
      VALKEYMODULE_CTX_FLAGS_REPLICATED | kSlotImportClientFlag |
      VALKEYMODULE_CTX_FLAGS_LOADING | VALKEYMODULE_CTX_FLAGS_ASYNC_LOADING;
  if ((caller_flags & VALKEYMODULE_CTX_FLAGS_SLAVE) != 0) {
    if ((caller_flags & kAuthoritativeMutationFlags) == 0) {
      // Outside the replication/import/load stream on a replica (e.g. cron
      // backfill), POLICY_KEEP_EXPIRED hides expired hash fields even while the
      // hashtable entry still holds a shared stringRef. Retain and enqueue in
      // lookup_failed_deferred so replicated HDEL on a multi-field hash or
      // promotion still drains it after the index drops the key.
      auto &queue = pending_unshares_[db_num];
      if (!queue.ready.contains(search_key) &&
          !queue.deferred.contains(search_key) &&
          !queue.lookup_failed_active.contains(search_key)) {
        queue.lookup_failed_deferred.insert(search_key);
      }
      return true;
    }
    auto attr_str =
        vmsdk::MakeUniqueValkeyString(search_key.attribute_identifier->Str());
    if (GetHashFieldRefState(key_obj.get(), attr_str.get()) ==
        HashFieldRefState::kShared) {
      CancelPendingUnshare(db_num, search_key);
      return true;
    }
    return false;
  }
  // A hash on a primary whose field is not visible may hold the shared bytes
  // in a logically expired field read by RDB save / DUMP / import-source, so
  // keep the record until the key is deleted.
  CancelPendingUnshare(db_num, search_key);
  return true;
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
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return true;
  }
  auto tracked_it = db_it->second.find(search_key);
  if (tracked_it == db_it->second.end() || !tracked_it->second.is_shared) {
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
  const int key_type = ValkeyModule_KeyType(key_obj.get());
  if (key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
    return false;
  }
  if (key_type != VALKEYMODULE_KEYTYPE_HASH) {
    return true;
  }
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(search_key.attribute_identifier->Str());
  // A field that is not visible may be logically expired and still reference
  // the record.
  return GetHashFieldRefState(key_obj.get(), attribute_identifier_str.get()) ==
         HashFieldRefState::kUnshared;
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
  const auto ref_state =
      GetHashFieldRefState(key_obj.get(), attribute_identifier_str.get());
  if (ref_state == HashFieldRefState::kShared) {
    return true;
  }
  if (ref_state != HashFieldRefState::kUnshared) {
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
    lookup_failed_cron_ticks_ = 0;
    return;
  }
  tracked_keys_by_db_.erase(flush_info->dbnum);
  tracked_vectors_.erase(flush_info->dbnum);
  pending_unshares_.erase(flush_info->dbnum);
  if (pending_unshares_.empty()) {
    lookup_failed_cron_ticks_ = 0;
  }
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

VectorRegistry::UnshareResult VectorRegistry::UnshareWithValkey(
    int db_num, ValkeyModuleString *key_str,
    absl::string_view attribute_identifier,
    const indexes::VectorRecord &vector_record, size_t vector_size,
    bool already_verified_shared, bool already_counted_error) {
  vmsdk::VerifyMainThread();
  if (!hash_vector_sharing_ || !key_str) {
    return UnshareResult::kUnshared;
  }
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  const int ctx_flags = ValkeyModule_GetContextFlags(ctx_.get());
  const bool is_replica = (ctx_flags & VALKEYMODULE_CTX_FLAGS_SLAVE) != 0;
  auto attr_str = vmsdk::MakeUniqueValkeyString(attribute_identifier);
  if (!already_verified_shared) {
    auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
        ctx_.get(), key_str,
        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
    const int key_type = key_obj ? ValkeyModule_KeyType(key_obj.get())
                                 : VALKEYMODULE_KEYTYPE_EMPTY;
    if (key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
      return UnshareResult::kRetainUntilKeyDeleted;
    }
    if (key_type != VALKEYMODULE_KEYTYPE_HASH) {
      return UnshareResult::kUnshared;
    }
    switch (GetHashFieldRefState(key_obj.get(), attr_str.get())) {
      case HashFieldRefState::kUnshared:
        return UnshareResult::kUnshared;
      case HashFieldRefState::kMissing:
        if (is_replica && (ctx_flags & (VALKEYMODULE_CTX_FLAGS_REPLICATED |
                                        kSlotImportClientFlag)) == 0) {
          return UnshareResult::kDeferLookupFailed;
        }
        return UnshareResult::kRetainUntilKeyDeleted;
      case HashFieldRefState::kShared:
        break;
    }
  }
  // Pre-VALKEYMODULE_HASH_KEEPTTL (valkey-io/valkey#5353) fallback:
  // ValkeyModule_HashSet clears any per-field expiry, so snapshot and restore
  // the expiry via HPEXPIRETIME/HPEXPIREAT. Known limitations on cores without
  // VALKEYMODULE_HASH_KEEPTTL:
  // 1. In cluster mode, detached-context ValkeyModule_Call goes through
  //    getNodeByQuery, so HPEXPIRETIME fails on cluster replicas and
  //    cluster-down primaries; we fail closed (skip HashSet and keep the record
  //    tracked/deferred) rather than stripping the field's TTL.
  // 2. HPEXPIRETIME/HPEXPIREAT execute through normal command paths
  //    (lookupKeyRead side effects, WATCH invalidation, pub/sub hexpire
  //    notifications, and server.dirty increments).
  const auto expiry_ms =
      GetHashFieldExpiryMs(ctx_.get(), key_str, attr_str.get());
  if (!expiry_ms || *expiry_ms < -2) {
    if (!already_counted_error) {
      ++stats_.unshare_errors;
    }
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx_.get(), 5)
        << "Failed to query hash field TTL before unsharing vector (db="
        << db_num << ", field=" << attribute_identifier
        << "); keeping vector tracked to avoid clearing field TTL";
    return UnshareResult::kDeferLookupFailed;
  }
  // Note: when *expiry_ms == -2, fall through to re-open with NOEFFECTS and
  // re-check GetHashFieldRefState rather than returning early. On a primary
  // where the field expired right before or during HPEXPIRETIME (which is
  // read-only and leaves the shared stringRef in the dict until active expire
  // or DEL), GetHashFieldRefState returns kMissing, skips HashSet, and retains
  // the record until OnKeyDeleted. On a standalone
  // replica outside the replication client (POLICY_KEEP_EXPIRED), HPEXPIRETIME
  // returns -2 while the hashtable entry still holds a shared stringRef; defer
  // unsharing rather than calling HashSet with EXPIRY_NONE, which would
  // resurrect the expired field permanently on promotion. (Inside a replicated
  // command such as RENAME, server.current_client keeps POLICY_IGNORE_EXPIRE so
  // HPEXPIRETIME returns the past timestamp >= 0 and checkAlreadyExpired is 0
  // on replicas, allowing HashSet + HPEXPIREAT to unshare cleanly in-place.)
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str,
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
  const int post_key_type = key_obj ? ValkeyModule_KeyType(key_obj.get())
                                    : VALKEYMODULE_KEYTYPE_EMPTY;
  if (post_key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
    return UnshareResult::kRetainUntilKeyDeleted;
  }
  if (post_key_type != VALKEYMODULE_KEYTYPE_HASH) {
    return UnshareResult::kUnshared;
  }
  const auto post_ref_state =
      GetHashFieldRefState(key_obj.get(), attr_str.get());
  if (post_ref_state == HashFieldRefState::kUnshared) {
    return UnshareResult::kUnshared;
  }
  if (post_ref_state == HashFieldRefState::kMissing) {
    return is_replica ? UnshareResult::kDeferLookupFailed
                      : UnshareResult::kRetainUntilKeyDeleted;
  }
  if (*expiry_ms == -2) {
    return UnshareResult::kDeferLookupFailed;
  }
  if (!is_replica && *expiry_ms >= 0 &&
      *expiry_ms <= ValkeyModule_Milliseconds() + 5) {
    // On a primary, defer fields at or near expiry so Valkey's active-expire
    // reaps them natively and propagates HDEL/DEL rather than racing
    // HPEXPIREAT's checkAlreadyExpired boundary.
    return UnshareResult::kDeferLookupFailed;
  }
  auto raw_vec = vector_record.GetRawVector();
  auto val_str =
      vmsdk::MakeUniqueValkeyString(absl::string_view(raw_vec, vector_size));
  // HashSet returns the number of existing fields it updated, so replacing
  // the shared field returns 1; 0 means the copy did not happen.
  if (ValkeyModule_HashSet(key_obj.get(), VALKEYMODULE_HASH_NONE,
                           attr_str.get(), val_str.get(), nullptr) != 1) {
    ++stats_.unshare_errors;
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx_.get(), 5)
        << "ValkeyModule_HashSet failed while unsharing vector (db=" << db_num
        << ", field=" << attribute_identifier << ")";
    return UnshareResult::kRetainUntilKeyDeleted;
  }
  key_obj.reset();
  if (*expiry_ms >= 0 && !RestoreHashFieldExpiryMs(
                             ctx_.get(), key_str, attr_str.get(), *expiry_ms)) {
    ++stats_.unshare_errors;
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx_.get(), 5)
        << "Failed to restore hash field TTL after unsharing vector (db="
        << db_num << ", field=" << attribute_identifier
        << ", expiry_ms=" << *expiry_ms << ")";
    return UnshareResult::kRestoreFailed;
  }
  return UnshareResult::kUnshared;
}

void VectorRegistry::OnKeyDeleted(int db_num, absl::string_view key) {
  vmsdk::VerifyMainThread();
  EraseTrackedRecordsForKey(db_num, key);
}

void VectorRegistry::OnHashKeyMutated(ValkeyModuleCtx *ctx, int db_num,
                                      ValkeyModuleString *key_str,
                                      absl::string_view key,
                                      absl::string_view event) {
  vmsdk::VerifyMainThread();
  if (in_process_pending_unshares_ || !key_str || pending_unshares_.empty() ||
      tracked_keys_by_db_.empty()) {
    return;
  }
  auto pending_it = pending_unshares_.find(db_num);
  if (pending_it == pending_unshares_.end()) {
    return;
  }
  auto keys_db_it = tracked_keys_by_db_.find(db_num);
  if (keys_db_it == tracked_keys_by_db_.end()) {
    return;
  }
  auto key_it = keys_db_it->second.find(key);
  if (key_it == keys_db_it->second.end()) {
    return;
  }
  const int ctx_flags = ValkeyModule_GetContextFlags(ctx ? ctx : ctx_.get());
  const bool is_replica = (ctx_flags & VALKEYMODULE_CTX_FLAGS_SLAVE) != 0;
  if (!is_replica && !pending_it->second.HasLookupFailed()) {
    return;
  }
  if (is_replica && (ctx_flags & (VALKEYMODULE_CTX_FLAGS_REPLICATED |
                                  kSlotImportClientFlag)) == 0) {
    // Outside the replication/import stream on a replica, expired hash fields
    // are hidden by POLICY_KEEP_EXPIRED even while physically holding shared
    // string references.
    return;
  }
  struct CandidateAttr {
    InternedStringPtr attr;
    bool ready_or_deferred_only{false};
  };
  auto interned_key = key_it->first.Materialize();
  std::vector<CandidateAttr> candidates;
  for (const auto &borrowed_attr : key_it->second) {
    auto attr = borrowed_attr.Materialize();
    RegistryKey rk{.key = interned_key, .attribute_identifier = attr};
    if (pending_it->second.ContainsLookupFailed(rk)) {
      candidates.push_back(CandidateAttr{
          .attr = std::move(attr),
          .ready_or_deferred_only = false,
      });
    } else if (is_replica && (pending_it->second.ready.contains(rk) ||
                              pending_it->second.deferred.contains(rk))) {
      candidates.push_back(CandidateAttr{
          .attr = std::move(attr),
          .ready_or_deferred_only = true,
      });
    }
  }
  if (candidates.empty()) {
    return;
  }
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx_.get(), key_str, VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  if (!key_obj ||
      ValkeyModule_KeyType(key_obj.get()) != VALKEYMODULE_KEYTYPE_HASH) {
    return;
  }
  const bool ttl_mutated = (event == "hexpire" || event == "hpersist");
  const bool authoritative_missing = is_replica;
  for (const auto &candidate : candidates) {
    const auto &attr = candidate.attr;
    RegistryKey rk{.key = interned_key, .attribute_identifier = attr};
    auto tracked_db_it = tracked_vectors_.find(db_num);
    if (tracked_db_it == tracked_vectors_.end()) {
      break;
    }
    auto tracked_it = tracked_db_it->second.find(rk);
    if (tracked_it == tracked_db_it->second.end()) {
      CancelPendingUnshare(db_num, rk);
      continue;
    }
    auto attr_str = vmsdk::MakeUniqueValkeyString(attr->Str());
    const auto ref_state = GetHashFieldRefState(key_obj.get(), attr_str.get());
    if (candidate.ready_or_deferred_only) {
      if (ref_state != HashFieldRefState::kShared) {
        tracked_it->second.is_shared = false;
      }
      continue;
    }
    if (ref_state == HashFieldRefState::kUnshared ||
        (ref_state == HashFieldRefState::kMissing && authoritative_missing)) {
      tracked_it->second.is_shared = false;
      if (!HasMatchingVectorIndex(db_num, key, attr, key_obj.get(),
                                  tracked_it->second.record.size,
                                  /*require_tracked=*/false)) {
        EraseTrackedRecord(db_num, rk);
      } else if (HasMatchingVectorIndex(db_num, key, attr, key_obj.get(),
                                        tracked_it->second.record.size,
                                        /*require_tracked=*/true)) {
        CancelPendingUnshare(db_num, rk);
      } else if (tracked_it->second.record.vector_record.RefCount() > 1) {
        auto &queue = pending_unshares_[db_num];
        queue.EraseLookupFailed(rk);
        if (!queue.ready.contains(rk)) {
          queue.deferred.insert(rk);
        }
      } else {
        EraseTrackedRecord(db_num, rk);
      }
    } else if (ref_state == HashFieldRefState::kMissing) {
      // On a primary, any hash mutation (including "hexpired" triggered by
      // another field expiring on the same key) sees kMissing even while a
      // logically expired attribute still holds a shared stringRef. Cancel any
      // pending unshare and keep the record tracked until OnKeyDeleted.
      CancelPendingUnshare(db_num, rk);
    } else if (ttl_mutated &&
               (ctx_flags & VALKEYMODULE_CTX_FLAGS_CLUSTER) == 0) {
      auto p_it = pending_unshares_.find(db_num);
      if (p_it != pending_unshares_.end() &&
          p_it->second.ContainsLookupFailed(rk)) {
        auto &queue = p_it->second;
        queue.EraseLookupFailed(rk);
        queue.deferred.erase(rk);
        queue.ready.insert(std::move(rk));
      }
    }
  }
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
    bool src_was_lookup_failed = false;
    if (auto pending_it = pending_unshares_.find(src_db_num);
        pending_it != pending_unshares_.end()) {
      src_was_lookup_failed =
          pending_it->second.ContainsLookupFailed(src_search_key);
    }
    CancelPendingUnshare(src_db_num, src_search_key);
    auto entry = ExtractTrackedRecord(src_db_num, src_key, attr);
    if (!entry) {
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
      tracked_vectors_[dst_db_num].insert_or_assign(dst_search_key, *entry);
      AddToTrackedKeysIndex(dst_db_num, dst_search_key);
    } else if (hash_vector_sharing_ && entry->is_shared) {
      // The attribute is no longer indexed at destination; materialize the
      // vector payload back to Valkey before untracking it. Track under dst_rk
      // before calling UnshareWithValkey so re-entrant OnKeyDeleted cleans it
      // up if HPEXPIRETIME/HPEXPIREAT expires the destination key, while
      // missing/logically-expired fields or failed lookups keep the record
      // tracked at the destination name.
      if (!interned_dst_key) {
        interned_dst_key = StringInternStore::Intern(dst_key_view);
      }
      RegistryKey dst_rk{
          .key = interned_dst_key,
          .attribute_identifier = attr,
      };
      tracked_vectors_[dst_db_num].insert_or_assign(
          dst_rk,
          TrackedVectorEntry{.record = entry->record, .is_shared = true});
      AddToTrackedKeysIndex(dst_db_num, dst_rk);
      const auto result = UnshareWithValkey(
          dst_db_num, dst_key, attr->Str(), entry->record.vector_record,
          entry->record.size, /*already_verified_shared=*/false,
          /*already_counted_error=*/src_was_lookup_failed);
      if (result == UnshareResult::kUnshared ||
          result == UnshareResult::kRestoreFailed) {
        ExtractTrackedRecord(dst_db_num, dst_rk.key,
                             dst_rk.attribute_identifier);
      } else if (result == UnshareResult::kDeferLookupFailed) {
        auto dst_db_it = tracked_vectors_.find(dst_db_num);
        if (dst_db_it != tracked_vectors_.end() &&
            dst_db_it->second.contains(dst_rk)) {
          auto &queue = pending_unshares_[dst_db_num];
          if (!queue.ready.contains(dst_rk) &&
              !queue.lookup_failed_active.contains(dst_rk)) {
            queue.deferred.erase(dst_rk);
            queue.lookup_failed_deferred.insert(std::move(dst_rk));
          }
        }
      }
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
    // An empty key may be logically expired: its type is unknown, not wrong.
    if (key_obj &&
        ValkeyModule_KeyType(key_obj) != VALKEYMODULE_KEYTYPE_EMPTY &&
        !sub->GetAttributeDataType().IsProperType(key_obj)) {
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
    if (db_tracked.contains(rk) && !db_pending.ContainsLookupFailed(rk)) {
      if (!db_pending.deferred.empty()) {
        db_pending.deferred.erase(rk);
      }
      db_pending.ready.insert(std::move(rk));
    }
  }

  if (db_pending.empty()) {
    pending_unshares_.erase(db_num);
    return;
  }
  ProcessPendingUnshares(options::GetVectorUnshareBatchSize().GetValue(),
                         /*retry_lookup_failed=*/false);
}

void VectorRegistry::RemoveIndexKeys(
    int db_num, const InternedStringPtr &attribute_identifier,
    vmsdk::HugeFlatHashMap<uint64_t, InternedStringPtr> keys) {
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
    if (db_tracked.contains(rk) && !db_pending.ContainsLookupFailed(rk)) {
      if (!db_pending.deferred.empty()) {
        db_pending.deferred.erase(rk);
      }
      db_pending.ready.insert(std::move(rk));
    }
  }

  if (db_pending.empty()) {
    pending_unshares_.erase(db_num);
    return;
  }
  ProcessPendingUnshares(options::GetVectorUnshareBatchSize().GetValue(),
                         /*retry_lookup_failed=*/false);
}

size_t VectorRegistry::ProcessPendingUnshares(uint32_t batch_size,
                                              bool retry_lookup_failed) {
  vmsdk::VerifyMainThread();
  if (in_process_pending_unshares_ || pending_unshares_.empty() ||
      batch_size == 0) {
    return 0;
  }
  in_process_pending_unshares_ = true;
  auto reset_guard =
      absl::MakeCleanup([this]() { in_process_pending_unshares_ = false; });

  using PendingNode = vmsdk::HugeFlatHashSet<RegistryKey>::node_type;
  struct ExtractedPendingItem {
    int db_num;
    PendingNode node;
    bool from_lookup_failed{false};
  };
  std::vector<ExtractedPendingItem> batch;
  batch.reserve(std::min<size_t>(batch_size, GetPendingUnsharesCount()));

  auto extract_from_ready = [&](bool promote_deferred) {
    for (auto db_it = pending_unshares_.begin();
         db_it != pending_unshares_.end() && batch.size() < batch_size;) {
      int db_num = db_it->first;
      auto &queue = db_it->second;
      if (promote_deferred && queue.ready.empty() && !queue.deferred.empty()) {
        std::swap(queue.ready, queue.deferred);
      }
      while (!queue.ready.empty() && batch.size() < batch_size) {
        batch.push_back(ExtractedPendingItem{
            .db_num = db_num,
            .node = queue.ready.extract(queue.ready.begin()),
            .from_lookup_failed = false,
        });
      }
      if (queue.empty()) {
        pending_unshares_.erase(db_it++);
      } else {
        ++db_it;
      }
    }
  };

  auto extract_from_lookup_failed = [&](bool promote_deferred) {
    for (auto db_it = pending_unshares_.begin();
         db_it != pending_unshares_.end() && batch.size() < batch_size;) {
      int db_num = db_it->first;
      auto &queue = db_it->second;
      if (promote_deferred && queue.lookup_failed_active.empty() &&
          !queue.lookup_failed_deferred.empty()) {
        std::swap(queue.lookup_failed_active, queue.lookup_failed_deferred);
      }
      while (!queue.lookup_failed_active.empty() && batch.size() < batch_size) {
        batch.push_back(ExtractedPendingItem{
            .db_num = db_num,
            .node = queue.lookup_failed_active.extract(
                queue.lookup_failed_active.begin()),
            .from_lookup_failed = true,
        });
      }
      if (queue.empty()) {
        pending_unshares_.erase(db_it++);
      } else {
        ++db_it;
      }
    }
  };

  // Drain `ready` entries across all DBs first so previously deferred entries
  // cannot starve newly queued work. Only promote `deferred` once `ready` has
  // fewer than `batch_size` items remaining, and only retry `lookup_failed`
  // entries when permitted by the caller's retry policy.
  extract_from_ready(/*promote_deferred=*/false);
  if (batch.size() < batch_size) {
    extract_from_ready(/*promote_deferred=*/true);
  }
  if (retry_lookup_failed && batch.size() < batch_size) {
    extract_from_lookup_failed(/*promote_deferred=*/false);
    if (batch.size() < batch_size) {
      extract_from_lookup_failed(/*promote_deferred=*/true);
    }
  }

  size_t processed = 0;
  size_t busy_refcount_deferred = 0;
  std::vector<ExtractedPendingItem> deferred_items;
  std::vector<ExtractedPendingItem> lookup_failed_items;
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
                               key_obj.get(), tracked_it->second.record.size,
                               /*require_tracked=*/true)) {
      continue;
    }
    const bool was_shared = tracked_it->second.is_shared;
    int key_type = VALKEYMODULE_KEYTYPE_EMPTY;
    if (hash_vector_sharing_ && was_shared) {
      key_type = key_obj ? ValkeyModule_KeyType(key_obj.get())
                         : VALKEYMODULE_KEYTYPE_EMPTY;
      // A key that is not visible may be logically expired and still
      // reference the record; retain until OnKeyDeleted even if RefCount() > 1.
      if (key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
        continue;
      }
    }
    if (tracked_it->second.record.vector_record.RefCount() > 1) {
      ++busy_refcount_deferred;
      if (item.from_lookup_failed) {
        lookup_failed_items.push_back(std::move(item));
      } else {
        deferred_items.push_back(std::move(item));
      }
      continue;
    }
    if (hash_vector_sharing_ && was_shared &&
        key_type == VALKEYMODULE_KEYTYPE_HASH) {
      auto attr_str =
          vmsdk::MakeUniqueValkeyString(rk.attribute_identifier->Str());
      const auto ref_state =
          GetHashFieldRefState(key_obj.get(), attr_str.get());
      if (ref_state == HashFieldRefState::kShared) {
        // Copied so that the bytes stay alive through the unshare below, and
        // because UnshareWithValkey invokes HPEXPIRETIME, which may trigger
        // re-entrant OnKeyDeleted and invalidate tracked_db_it/tracked_it.
        auto record = tracked_it->second.record;
        key_obj.reset();
        const auto result = UnshareWithValkey(
            db_num, key_str.get(), rk.attribute_identifier->Str(),
            record.vector_record, record.size,
            /*already_verified_shared=*/true,
            /*already_counted_error=*/item.from_lookup_failed);
        if (result == UnshareResult::kDeferLookupFailed) {
          lookup_failed_items.push_back(std::move(item));
        } else if (result == UnshareResult::kUnshared ||
                   result == UnshareResult::kRestoreFailed) {
          ExtractTrackedRecord(db_num, rk.key, rk.attribute_identifier);
        }
        continue;
      }
      if (ref_state == HashFieldRefState::kMissing) {
        if ((ValkeyModule_GetContextFlags(ctx_.get()) &
             VALKEYMODULE_CTX_FLAGS_SLAVE) != 0) {
          // Outside the replication client on a replica (e.g. RunByMain index
          // teardown or server cron), POLICY_KEEP_EXPIRED hides expired hash
          // fields from HashGet/HashHasStringRef even while the hashtable entry
          // still holds a shared stringRef. Defer until a replicated mutation
          // reconciles under POLICY_IGNORE_EXPIRE or failover promotes the
          // node.
          lookup_failed_items.push_back(std::move(item));
          continue;
        }
        // On a primary (including lookup_failed retries and after failover
        // promotion), a field that is invisible on a live hash may be logically
        // expired and still hold the shared stringRef read by RDB save, DUMP,
        // or import-source HGET. Drain the pending unshare and keep the record
        // until OnKeyDeleted.
        continue;
      }
    }
    ExtractTrackedRecord(db_num, rk.key, rk.attribute_identifier);
  }

  // A batch in which every item was deferred because its vector was still
  // referenced made no progress: the busy items are retried every cron tick.
  if (!batch.empty() && busy_refcount_deferred == batch.size()) {
    ++stats_.unshare_stalled_batches;
    VMSDK_LOG_EVERY_N_SEC(WARNING, ctx_.get(), 60)
        << "Vector unshare made no progress: all " << batch.size()
        << " items in the batch are still referenced, "
        << GetPendingUnsharesCount() << " other unshares wait";
  }

  for (auto &deferred : deferred_items) {
    const auto &rk = deferred.node.value();
    auto tracked_db_it = tracked_vectors_.find(deferred.db_num);
    if (tracked_db_it == tracked_vectors_.end() ||
        !tracked_db_it->second.contains(rk)) {
      continue;
    }
    auto &queue = pending_unshares_[deferred.db_num];
    if (!queue.ready.contains(rk) && !queue.ContainsLookupFailed(rk)) {
      queue.deferred.insert_and_migrate(std::move(deferred.node));
    }
  }

  for (auto &failed : lookup_failed_items) {
    const auto &rk = failed.node.value();
    auto tracked_db_it = tracked_vectors_.find(failed.db_num);
    if (tracked_db_it == tracked_vectors_.end() ||
        !tracked_db_it->second.contains(rk)) {
      continue;
    }
    auto &queue = pending_unshares_[failed.db_num];
    if (!queue.ready.contains(rk) && !queue.deferred.contains(rk) &&
        !queue.lookup_failed_active.contains(rk)) {
      queue.lookup_failed_deferred.insert_and_migrate(std::move(failed.node));
    }
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
  // Drain every db's tables, so they finish even without traffic.
  if (vmsdk::HugeMapAdvisoryMigrationEnabled()) {
    size_t n = vmsdk::HugeMapCronMigrateBatch();
    for (auto &[_, tracked] : tracked_vectors_) {
      if (tracked.NeedsMigration()) {
        tracked.MigrateSome(n);
      }
    }
    for (auto &[_, keys] : tracked_keys_by_db_) {
      if (keys.NeedsMigration()) {
        keys.MigrateSome(n);
      }
    }
    for (auto &[_, pending] : pending_unshares_) {
      if (pending.ready.NeedsMigration()) {
        pending.ready.MigrateSome(n);
      }
      if (pending.deferred.NeedsMigration()) {
        pending.deferred.MigrateSome(n);
      }
      if (pending.lookup_failed_active.NeedsMigration()) {
        pending.lookup_failed_active.MigrateSome(n);
      }
      if (pending.lookup_failed_deferred.NeedsMigration()) {
        pending.lookup_failed_deferred.MigrateSome(n);
      }
    }
  }
  if (pending_unshares_.empty()) {
    lookup_failed_cron_ticks_ = 0;
    return;
  }
  bool has_lookup_failed = false;
  for (const auto &[_, pending] : pending_unshares_) {
    if (pending.HasLookupFailed()) {
      has_lookup_failed = true;
      break;
    }
  }
  bool retry_lookup_failed = false;
  if (!has_lookup_failed) {
    lookup_failed_cron_ticks_ = 0;
  } else {
    const int ctx_flags = ValkeyModule_GetContextFlags(ctx ? ctx : ctx_.get());
    if ((ctx_flags & VALKEYMODULE_CTX_FLAGS_SLAVE) != 0) {
      // On cluster replicas, detached-context HPEXPIRETIME always redirects; on
      // standalone replicas, lookup_failed holds unreaped expired fields that
      // appear kMissing outside the replication client (POLICY_KEEP_EXPIRED).
      // Keep failed items deferred until a replicated mutation cleans them up
      // or failover promotes this node.
      lookup_failed_cron_ticks_ = 0;
    } else if (++lookup_failed_cron_ticks_ >= kLookupFailedCronRetryTicks) {
      lookup_failed_cron_ticks_ = 0;
      retry_lookup_failed = true;
    }
  }
  ProcessPendingUnshares(options::GetVectorUnshareBatchSize().GetValue(),
                         retry_lookup_failed);
}

}  // namespace valkey_search
