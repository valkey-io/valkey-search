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
#include <vector>

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
  const auto &attr = vector_base->GetInternedAttributeIdentifier();

  if (!vector) {
    EraseTrackedRecord(db_num, key, attr);
    return {};
  }

  auto vector_str = vmsdk::ToStringView(vector);
  if (!vector_base->IsValidSizeVector(vector_str)) {
    if (IsEraseTrackedRecordSafe(db_num, key, vector_str, attr->Str(),
                                 attribute_data_type)) {
      EraseTrackedRecord(db_num, key, attr);
    }
    return ConstructVectorRecord(vector_str, vector_base);
  }

  indexes::VectorRecordWithSize result;
  auto &db = tracked_vectors_[db_num];
  auto &records = db.keys.try_emplace(key).first->second;
  auto it = std::find_if(records.begin(), records.end(),
                         [&](const auto &r) { return r.first == attr; });
  if (it != records.end() && it->second == vector_str) {
    ++stats_.dedup_cnt;
    result = it->second;
  } else {
    result = ConstructVectorRecord(vector_str, vector_base);
    if (it != records.end()) {
      it->second = result;
    } else {
      records.emplace_back(attr, result);
      ++db.entry_cnt;
    }
  }

  ShareWithValkey(db_num, key, attr->Str(), result.vector_record,
                  vector_base->GetVectorDataSize(), attribute_data_type);
  return result;
}

template <typename Pred>
void VectorRegistry::EraseAttrsIf(int db_num, PerDbRegistryMap::iterator it,
                                  Pred erase) {
  auto db_it = tracked_vectors_.find(db_num);
  auto &records = it->second;
  auto new_end = std::remove_if(records.begin(), records.end(), [&](auto &r) {
    return erase(r.first, r.second);
  });
  db_it->second.entry_cnt -= records.end() - new_end;
  records.erase(new_end, records.end());
  if (!records.empty()) {
    return;
  }
  db_it->second.keys.erase_and_migrate(it);
  if (db_it->second.keys.empty()) {
    tracked_vectors_.erase(db_it);
  }
}

void VectorRegistry::EraseTrackedRecord(
    int db_num, const InternedStringPtr &key,
    const InternedStringPtr &attribute_identifier) {
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return;
  }
  auto it = db_it->second.keys.find(key);
  if (it == db_it->second.keys.end()) {
    return;
  }
  EraseAttrsIf(db_num, it, [&](const auto &attr, const auto &) {
    return attr == attribute_identifier;
  });
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

}  // namespace

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
  ValkeyModule_SelectDb(ctx_.get(), db_num);
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
  for (const auto &[_, db] : tracked_vectors_) {
    total_entries += db.entry_cnt;
  }
  stats_.entry_cnt = total_entries;
  return stats_;
}

void VectorRegistry::OnFlushDB(const ValkeyModuleFlushInfo *flush_info) {
  vmsdk::VerifyMainThread();
  if (!flush_info || flush_info->dbnum == -1) {
    tracked_vectors_.clear();
    return;
  }
  tracked_vectors_.erase(flush_info->dbnum);
}

void VectorRegistry::OnSwapDB(const ValkeyModuleSwapDbInfo *swap_info) {
  vmsdk::VerifyMainThread();
  if (!swap_info || swap_info->dbnum_first == swap_info->dbnum_second) {
    return;
  }
  auto it1 = tracked_vectors_.find(swap_info->dbnum_first);
  auto it2 = tracked_vectors_.find(swap_info->dbnum_second);
  if (it1 == tracked_vectors_.end() && it2 == tracked_vectors_.end()) {
    return;
  }
  if (it1 != tracked_vectors_.end() && it2 != tracked_vectors_.end()) {
    std::swap(it1->second, it2->second);
    return;
  }
  auto from = it1 != tracked_vectors_.end() ? it1 : it2;
  int to_db = it1 != tracked_vectors_.end() ? swap_info->dbnum_second
                                            : swap_info->dbnum_first;
  auto db = std::move(from->second);
  tracked_vectors_.erase(from);
  tracked_vectors_.emplace(to_db, std::move(db));
}

bool VectorRegistry::UnshareWithValkey(
    ValkeyModuleKey *key_obj, ValkeyModuleString *attr_str,
    const indexes::VectorRecord &vector_record, size_t vector_size) {
  auto val_str = vmsdk::MakeUniqueValkeyString(
      absl::string_view(vector_record.GetRawVector(), vector_size));
  // Returns the number of existing fields updated: 1 on success.
  return ValkeyModule_HashSet(key_obj, VALKEYMODULE_HASH_NONE, attr_str,
                              val_str.get(), nullptr) == 1;
}

void VectorRegistry::MoveKey(int src_db_num, absl::string_view src_key,
                             int dst_db_num, absl::string_view dst_key) {
  vmsdk::VerifyMainThread();
  if (tracked_vectors_.empty() ||
      (src_db_num == dst_db_num && src_key == dst_key)) {
    return;
  }
  auto interned_dst_key = StringInternStore::Intern(dst_key);
  // The overwritten destination hash is gone, and its entries with it.
  if (auto dst_db_it = tracked_vectors_.find(dst_db_num);
      dst_db_it != tracked_vectors_.end()) {
    if (auto it = dst_db_it->second.keys.find(interned_dst_key);
        it != dst_db_it->second.keys.end()) {
      EraseAttrsIf(dst_db_num, it,
                   [](const auto &, const auto &) { return true; });
    }
  }
  auto src_db_it = tracked_vectors_.find(src_db_num);
  if (src_db_it == tracked_vectors_.end()) {
    return;
  }
  auto src_it = src_db_it->second.keys.find(StringInternStore::Intern(src_key));
  if (src_it == src_db_it->second.keys.end()) {
    return;
  }
  // A moved-from InlinedVector keeps its size; swap leaves the source empty.
  AttrRecords records;
  records.swap(src_it->second);
  src_db_it->second.entry_cnt -= records.size();
  EraseAttrsIf(src_db_num, src_it,
               [](const auto &, const auto &) { return true; });
  auto &dst_db = tracked_vectors_[dst_db_num];
  dst_db.entry_cnt += records.size();
  dst_db.keys.insert_or_assign(interned_dst_key, std::move(records));
}

void VectorRegistry::OnKeyspaceEvent(ValkeyModuleCtx *ctx, const char *event,
                                     ValkeyModuleString *key) {
  vmsdk::VerifyMainThread();
  if (tracked_vectors_.empty() || !event) {
    return;
  }
  int db_num = ValkeyModule_GetSelectedDb(ctx);
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return;
  }
  auto it = db_it->second.keys.find(
      StringInternStore::Intern(vmsdk::ToStringView(key)));
  if (it == db_it->second.keys.end()) {
    return;
  }
  absl::string_view ev(event);
  if (ev == "del" || ev == "expired" || ev == "evicted") {
    EraseAttrsIf(db_num, it, [](const auto &, const auto &) { return true; });
    return;
  }
  if (!hash_vector_sharing_) {
    return;
  }
  // Anything but a hash we can read is left to the sweep: an absent key may
  // be logically expired, still holding the shared bytes.
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      ctx, key, VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  if (!key_obj ||
      ValkeyModule_KeyType(key_obj.get()) != VALKEYMODULE_KEYTYPE_HASH) {
    return;
  }
  // An absent field may also be logically expired, unless this event just
  // removed it.
  bool field_removed = ev == "hdel" || ev == "hexpired";
  EraseAttrsIf(db_num, it, [&](const auto &attr, const auto &) {
    auto attr_str = vmsdk::MakeUniqueValkeyString(attr->Str());
    switch (GetHashFieldRefState(key_obj.get(), attr_str.get())) {
      case HashFieldRefState::kShared:
        return false;
      case HashFieldRefState::kUnshared:
        return true;
      case HashFieldRefState::kMissing:
        return field_removed;
    }
    return false;
  });
}

template <typename Pred>
void VectorRegistry::UnshareAttrsIf(int db_num, const InternedStringPtr &key,
                                    Pred select) {
  auto db_it = tracked_vectors_.find(db_num);
  if (db_it == tracked_vectors_.end()) {
    return;
  }
  auto it = db_it->second.keys.find(key);
  if (it == db_it->second.keys.end()) {
    return;
  }
  vmsdk::ValkeySelectDbGuard db_guard(ctx_.get(), db_num);
  vmsdk::UniqueValkeyOpenKey key_obj;
  bool is_hash = false;
  if (hash_vector_sharing_) {
    auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
    key_obj = vmsdk::MakeUniqueValkeyOpenKey(
        ctx_.get(), key_str.get(),
        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_WRITE);
    // An absent key may be logically expired while its hash still holds the
    // shared bytes. Its expired or del event erases the entries. A write open
    // returns an empty handle rather than null for an absent key.
    int key_type = key_obj ? ValkeyModule_KeyType(key_obj.get())
                           : VALKEYMODULE_KEYTYPE_EMPTY;
    if (key_type == VALKEYMODULE_KEYTYPE_EMPTY) {
      return;
    }
    is_hash = key_type == VALKEYMODULE_KEYTYPE_HASH;
  }
  EraseAttrsIf(db_num, it, [&](const auto &attr, const auto &record) {
    if (!select(attr, record)) {
      return false;
    }
    // Nothing can share these bytes: sharing is off, or the key was replaced
    // by another type.
    if (!is_hash) {
      return true;
    }
    auto attr_str = vmsdk::MakeUniqueValkeyString(attr->Str());
    switch (GetHashFieldRefState(key_obj.get(), attr_str.get())) {
      case HashFieldRefState::kMissing:
        // May be a logically expired field; its hexpired event erases it.
        return false;
      case HashFieldRefState::kUnshared:
        return true;
      case HashFieldRefState::kShared:
        if (!UnshareWithValkey(key_obj.get(), attr_str.get(),
                               record.vector_record, record.size)) {
          return false;
        }
        ++stats_.unshare_cnt;
        return true;
    }
    return false;
  });
}

void VectorRegistry::UnshareIfUnreferenced(int db_num,
                                           const InternedStringPtr &key) {
  UnshareAttrsIf(db_num, key, [](const auto &, const auto &record) {
    return record.vector_record.RefCount() == 1;
  });
}

void VectorRegistry::UnshareAttributes(
    int db_num, const InternedStringPtr &key,
    absl::Span<const InternedStringPtr> attributes) {
  vmsdk::VerifyMainThread();
  UnshareAttrsIf(db_num, key, [&](const auto &attr, const auto &) {
    return std::find(attributes.begin(), attributes.end(), attr) !=
           attributes.end();
  });
}

size_t VectorRegistry::SweepDb(int db_num, DbRegistry &db,
                               uint32_t batch_size) {
  auto &keys = db.keys;
  auto it = db.sweep_cursor ? keys.find(db.sweep_cursor) : keys.begin();
  if (it == keys.end()) {
    it = keys.begin();
  }
  std::vector<InternedStringPtr> candidates;
  size_t visited = 0;
  for (; it != keys.end() && visited < batch_size; ++it, ++visited) {
    for (const auto &[_, record] : it->second) {
      if (record.vector_record.RefCount() == 1) {
        candidates.push_back(it->first);
        break;
      }
    }
  }
  db.sweep_cursor = it == keys.end() ? InternedStringPtr() : it->first;
  // May erase db; it is not used past here.
  for (const auto &key : candidates) {
    UnshareIfUnreferenced(db_num, key);
  }
  return visited;
}

size_t VectorRegistry::SweepUnreferenced(uint32_t batch_size) {
  vmsdk::VerifyMainThread();
  std::vector<int> db_nums;
  for (const auto &[db_num, _] : tracked_vectors_) {
    db_nums.push_back(db_num);
  }
  size_t visited = 0;
  for (int db_num : db_nums) {
    auto db_it = tracked_vectors_.find(db_num);
    if (db_it != tracked_vectors_.end()) {
      visited += SweepDb(db_num, db_it->second, batch_size);
    }
  }
  return visited;
}

void VectorRegistry::OnServerCronCallback(ValkeyModuleCtx *ctx,
                                          ValkeyModuleEvent eid,
                                          uint64_t subevent, void *data) {
  vmsdk::VerifyMainThread();
  // Drain every db's tables, so they finish even without traffic.
  if (vmsdk::HugeMapAdvisoryMigrationEnabled()) {
    size_t n = vmsdk::HugeMapCronMigrateBatch();
    for (auto &[_, db] : tracked_vectors_) {
      if (db.keys.NeedsMigration()) {
        db.keys.MigrateSome(n);
      }
    }
  }
  SweepUnreferenced(options::GetVectorUnshareBatchSize().GetValue());
}

}  // namespace valkey_search
