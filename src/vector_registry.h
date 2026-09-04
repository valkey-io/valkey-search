/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/sharded_atomic.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class VectorRegistry {
 public:
  static VectorRegistry &Instance() {
    if (!InstancePtr()) {
      InstancePtr() = new VectorRegistry();
    }
    return *InstancePtr();
  }
  static void Construct(ValkeyModuleCtx *ctx) {
    Destruct();
    InstancePtr() = new VectorRegistry();
    InstancePtr()->Init(ctx);
  }
  static void Destruct() {
    if (InstancePtr()) {
      delete InstancePtr();
      InstancePtr() = nullptr;
    }
  }

  // Disallow copy and move.
  VectorRegistry(const VectorRegistry &) = delete;
  VectorRegistry &operator=(const VectorRegistry &) = delete;

  // Registers or deduplicates a vector record in the registry.
  // Returns VectorRecordWithSize.
  indexes::VectorRecordWithSize DedupOrConstruct(
      const InternedStringPtr &key, ValkeyModuleString *vector,
      const data_model::AttributeDataType &attribute_data_type, int db_num,
      const indexes::VectorBase *vector_base);

  // Checks whether any registered vector index in db_num matches the given
  // key and attribute_identifier. Also checks that the index vector data size
  // matches. If require_tracked is true, additionally checks that the index
  // currently tracks the key.
  bool HasMatchingVectorIndex(int db_num, absl::string_view key,
                              const InternedStringPtr &attribute_identifier,
                              ValkeyModuleKey *key_obj, size_t vector_size,
                              bool require_tracked = false) const;

  // Removes entries from VectorRegistry for a dropped vector index.
  void RemoveIndexKeys(int db_num,
                       const InternedStringPtr &attribute_identifier,
                       absl::Span<const InternedStringPtr> keys);
  void RemoveIndexKeys(int db_num,
                       const InternedStringPtr &attribute_identifier,
                       absl::flat_hash_map<uint64_t, InternedStringPtr> keys);

  // Processes up to batch_size pending unshare keys across all databases.
  // Returns the number of keys processed.
  size_t ProcessPendingUnshares(uint32_t batch_size);

  // Returns the total number of keys pending unshare.
  size_t GetPendingUnsharesCount() const;

  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);

  struct Stats {
    size_t entry_cnt;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_errors;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_hits;
    vmsdk::ShardedAtomic<uint64_t> dedup_cnt;
  };
  const Stats &GetStats() const;

  ValkeyModuleCtx *GetCtx() const { return ctx_.get(); }

  bool IsSharingActive() const { return hash_vector_sharing_; }

  void OnFlushDB(const ValkeyModuleFlushInfo *flush_info);
  void OnSwapDB(const ValkeyModuleSwapDbInfo *swap_info);
  enum class Action {
    kMove,
    kUnshare,
  };

  // Moves or unshares tracked vector records during RENAME / MOVE operations.
  void MoveKey(int src_db_num, const InternedStringPtr &src_key, int dst_db_num,
               ValkeyModuleString *dst_key,
               const absl::flat_hash_map<InternedStringPtr, Action> &actions);

 private:
  struct RegistryKey {
    InternedStringPtr key;
    InternedStringPtr attribute_identifier;

    bool operator==(const RegistryKey &o) const {
      return key == o.key && attribute_identifier == o.attribute_identifier;
    }

    template <typename H>
    friend H AbslHashValue(H h, const RegistryKey &k) {
      return H::combine(std::move(h), k.key, k.attribute_identifier);
    }
  };

  using PerDbRegistryMap =
      absl::flat_hash_map<RegistryKey, indexes::VectorRecordWithSize>;

  // Maps db_num -> inner map to track active vector records per database.
  absl::flat_hash_map<int, PerDbRegistryMap> tracked_vectors_;

  // Maps db_num -> pending unshare keys queued for rate-limited processing.
  absl::flat_hash_map<int, absl::flat_hash_set<RegistryKey>> pending_unshares_;

  void CancelPendingUnshare(int db_num, const RegistryKey &key);

  friend class VectorRegistryTest;
  bool hash_vector_sharing_{false};
  mutable Stats stats_;
  vmsdk::UniqueValkeyDetachedThreadSafeContext ctx_;

  VectorRegistry() = default;
  ~VectorRegistry() = default;
  static VectorRegistry *&InstancePtr() {
    static absl::NoDestructor<VectorRegistry *> instance{nullptr};
    return *instance;
  }
  void Init(ValkeyModuleCtx *ctx);

  // Shares a tracked vector string memory reference directly with the Valkey
  // Hash data model in the engine.
  bool ShareWithValkey(
      int db_num, const InternedStringPtr &key,
      absl::string_view attribute_identifier,
      const indexes::VectorRecord *vector_record, size_t vector_size,
      const data_model::AttributeDataType &attribute_data_type);
  std::optional<indexes::VectorRecordWithSize> ExtractTrackedRecord(
      int db_num, const InternedStringPtr &key,
      const InternedStringPtr &attribute_identifier);
  void EraseTrackedRecord(int db_num, const RegistryKey &key);
  // Replaces a shared string memory reference in the Valkey Hash data model
  // with an independent owned copy.
  bool UnshareWithValkey(ValkeyModuleKey *key_obj,
                         absl::string_view attribute_identifier,
                         const indexes::VectorRecord *vector_record,
                         size_t vector_size);
  bool IsEraseTrackedRecordSafe(
      int db_num, const InternedStringPtr &key, absl::string_view vector_str,
      absl::string_view attribute_identifier,
      const data_model::AttributeDataType &attribute_data_type);
};

}  // namespace valkey_search
