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
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/huge/flat_hash_map.h"
#include "vmsdk/src/managed_pointers.h"
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
      const indexes::VectorBase *vector_base, ValkeyModuleCtx *ctx = nullptr);

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
  void RemoveIndexKeys(
      int db_num, const InternedStringPtr &attribute_identifier,
      vmsdk::HugeFlatHashMap<uint64_t, InternedStringPtr> keys);

  // Processes up to batch_size pending unshare keys across all databases.
  // When `retry_lookup_failed` is false, records that previously failed
  // per-field TTL lookup are left queued without retrying them in this pass.
  // Returns the number of keys processed.
  size_t ProcessPendingUnshares(uint32_t batch_size,
                                bool retry_lookup_failed = true);

  // Returns the total number of keys pending unshare.
  size_t GetPendingUnsharesCount() const;

  // Also migrates every db's tables, so they drain without any traffic.
  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);

  struct Stats {
    size_t entry_cnt{0};
    uint64_t hash_sharing_errors{0};
    uint64_t hash_sharing_hits{0};
    uint64_t dedup_cnt{0};
    // Unshare batches in which every item was deferred because its vector was
    // still referenced, while other pending unshares waited.
    uint64_t unshare_stalled_batches{0};
    uint64_t unshare_errors{0};
  };
  const Stats &GetStats() const;

  ValkeyModuleCtx *GetCtx() const { return ctx_.get(); }

  bool IsSharingActive() const { return hash_vector_sharing_; }
  bool HasTrackedKeys() const { return !tracked_vectors_.empty(); }
  bool HasPendingUnshares() const { return !pending_unshares_.empty(); }

  void OnFlushDB(const ValkeyModuleFlushInfo *flush_info);
  void OnSwapDB(const ValkeyModuleSwapDbInfo *swap_info);
  // Purges tracked vector records and cancels pending unshares for a physically
  // deleted or overwritten key without interning untracked keys.
  void OnKeyDeleted(int db_num, absl::string_view key);
  // Reconciles deferred lookup-failed unshares when a hash field mutation
  // (e.g. replicated HDEL/HSET/HPERSIST on a multi-field hash) changes field
  // ownership or TTL state.
  void OnHashKeyMutated(ValkeyModuleCtx *ctx, int db_num,
                        ValkeyModuleString *key_str, absl::string_view key,
                        absl::string_view event);
  enum class Action {
    kMove,
    kUnshare,
  };

  // Moves or unshares tracked vector records during RENAME / MOVE operations.
  void MoveKey(int src_db_num, const InternedStringPtr &src_key, int dst_db_num,
               ValkeyModuleString *dst_key,
               const absl::flat_hash_map<InternedStringPtr, Action> &actions);

  // Returns all attributes and their vector sizes currently tracked in db_num
  // for the given key.
  absl::flat_hash_map<InternedStringPtr, size_t> GetTrackedAttributes(
      int db_num, absl::string_view key) const;

 private:
  static constexpr uint32_t kLookupFailedCronRetryTicks = 10;

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

  struct TrackedVectorEntry {
    indexes::VectorRecordWithSize record;
    bool is_shared{false};
  };

  using PerDbRegistryMap =
      vmsdk::HugeFlatHashMap<RegistryKey, TrackedVectorEntry>;

  // Maps db_num -> inner map to track active vector records per database.
  absl::flat_hash_map<int, PerDbRegistryMap> tracked_vectors_;

  // Transparent content hash/equality allowing absl::string_view probes into
  // tracked_keys_by_db_ without interning untracked keys.
  struct BorrowedKeyStringHash {
    using is_transparent = void;
    size_t operator()(const BorrowedInternedStringPtr &p) const {
      return absl::HashOf(p ? p.Str() : absl::string_view{});
    }
    size_t operator()(const InternedStringPtr &p) const {
      return absl::HashOf(p ? p->Str() : absl::string_view{});
    }
    size_t operator()(absl::string_view sv) const { return absl::HashOf(sv); }
  };

  struct BorrowedKeyStringEq {
    using is_transparent = void;
    bool operator()(const BorrowedInternedStringPtr &a,
                    const BorrowedInternedStringPtr &b) const {
      return RawInternedPtr(a) == RawInternedPtr(b);
    }
    bool operator()(const BorrowedInternedStringPtr &a,
                    const InternedStringPtr &b) const {
      return RawInternedPtr(a) == RawInternedPtr(b);
    }
    bool operator()(const InternedStringPtr &a,
                    const BorrowedInternedStringPtr &b) const {
      return RawInternedPtr(a) == RawInternedPtr(b);
    }
    bool operator()(const InternedStringPtr &a,
                    const InternedStringPtr &b) const {
      return RawInternedPtr(a) == RawInternedPtr(b);
    }
    bool operator()(const BorrowedInternedStringPtr &a,
                    absl::string_view b) const {
      return a && a.Str() == b;
    }
    bool operator()(absl::string_view a,
                    const BorrowedInternedStringPtr &b) const {
      return b && a == b.Str();
    }
    bool operator()(const InternedStringPtr &a, absl::string_view b) const {
      return a && a->Str() == b;
    }
    bool operator()(absl::string_view a, const InternedStringPtr &b) const {
      return b && a == b->Str();
    }
    bool operator()(absl::string_view a, absl::string_view b) const {
      return a == b;
    }
  };

  using BorrowedAttrSet =
      absl::flat_hash_set<BorrowedInternedStringPtr, InternedStringPtrHash,
                          InternedStringPtrEq>;
  // A HugeFlatHashMap, like tracked_vectors_, because it holds an entry per
  // tracked key and an absl table that size would rehash in one step.
  using PerDbTrackedKeysMap =
      vmsdk::HugeFlatHashMap<BorrowedInternedStringPtr, BorrowedAttrSet,
                             BorrowedKeyStringHash, BorrowedKeyStringEq>;

  // Maps db_num -> key -> tracked attribute_identifiers for O(1) per-key
  // lookup. Uses BorrowedInternedStringPtr because lifetime is tied 1-to-1
  // to the owning RegistryKey in tracked_vectors_. RemoveFromTrackedKeysIndex
  // must always be called before erasing the owning RegistryKey from
  // tracked_vectors_ because BorrowedKeyStringHash dereferences Str().
  absl::flat_hash_map<int, PerDbTrackedKeysMap> tracked_keys_by_db_;

  // Multi-tier pending unshare queue per DB:
  // - `ready` / `deferred`: normal two-tier queue so busy (RefCount() > 1)
  //   records do not starve newly queued work.
  // - `lookup_failed_active` / `lookup_failed_deferred`: two-tier cycle for
  //   records whose HPEXPIRETIME lookup failed (or replica hidden expired
  //   fields) so replicas skip futile retries, primaries back off retries
  //   across cron ticks, and each record's lookup failure is counted once in
  //   `unshare_errors`.
  // Invariant: all four sets are pairwise disjoint.
  struct PendingUnshareQueue {
    vmsdk::HugeFlatHashSet<RegistryKey> ready;
    vmsdk::HugeFlatHashSet<RegistryKey> deferred;
    vmsdk::HugeFlatHashSet<RegistryKey> lookup_failed_active;
    vmsdk::HugeFlatHashSet<RegistryKey> lookup_failed_deferred;

    bool HasLookupFailed() const {
      return !lookup_failed_active.empty() || !lookup_failed_deferred.empty();
    }
    bool ContainsLookupFailed(const RegistryKey &key) const {
      return lookup_failed_active.contains(key) ||
             lookup_failed_deferred.contains(key);
    }
    bool EraseLookupFailed(const RegistryKey &key) {
      const size_t erased =
          lookup_failed_active.erase(key) + lookup_failed_deferred.erase(key);
      return erased > 0;
    }
    bool empty() const {
      return ready.empty() && deferred.empty() && !HasLookupFailed();
    }
    size_t size() const {
      return ready.size() + deferred.size() + lookup_failed_active.size() +
             lookup_failed_deferred.size();
    }
  };

  // Maps db_num -> pending unshare keys queued for rate-limited processing.
  absl::flat_hash_map<int, PendingUnshareQueue> pending_unshares_;

  void CancelPendingUnshare(int db_num, const RegistryKey &key);
  void AddToTrackedKeysIndex(int db_num, const RegistryKey &key);
  void RemoveFromTrackedKeysIndex(
      int db_num, const InternedStringPtr &key,
      const InternedStringPtr &attribute_identifier);

  friend class VectorRegistryTest;
  bool hash_vector_sharing_{false};
  bool in_process_pending_unshares_{false};
  uint32_t lookup_failed_cron_ticks_{0};
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
      const indexes::VectorRecord &vector_record, size_t vector_size,
      const data_model::AttributeDataType &attribute_data_type);
  std::optional<TrackedVectorEntry> ExtractTrackedRecord(
      int db_num, const InternedStringPtr &key,
      const InternedStringPtr &attribute_identifier);
  void EraseTrackedRecord(int db_num, const RegistryKey &key);
  void EraseTrackedRecordsForKey(int db_num, absl::string_view key);
  enum class UnshareResult {
    kUnshared,
    kRestoreFailed,
    kRetainUntilKeyDeleted,
    kDeferLookupFailed,
  };

  // Replaces a shared string memory reference in the Valkey Hash data model
  // with an independent owned copy while preserving any per-field TTL.
  // When `already_verified_shared` is true, the caller has already confirmed
  // that `key_str` is a hash whose `attribute_identifier` holds a shared string
  // reference and closed its key handle.
  UnshareResult UnshareWithValkey(int db_num, ValkeyModuleString *key_str,
                                  absl::string_view attribute_identifier,
                                  const indexes::VectorRecord &vector_record,
                                  size_t vector_size,
                                  bool already_verified_shared = false,
                                  bool already_counted_error = false);
  bool ShouldRetainMissingVector(
      ValkeyModuleCtx *ctx, int db_num, const RegistryKey &search_key,
      const data_model::AttributeDataType &attribute_data_type);
  bool IsEraseTrackedRecordSafe(
      int db_num, const RegistryKey &search_key, absl::string_view vector_str,
      const data_model::AttributeDataType &attribute_data_type);
};

}  // namespace valkey_search
