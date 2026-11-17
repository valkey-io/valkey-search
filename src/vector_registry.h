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
#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/huge/flat_hash_map.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

// The ForceHashSharingError controlled variable, shared with
// LegacyVectorRegistry.
bool IsHashSharingErrorForced();

// True when the registry unshares lazily (VectorRegistry); false selects the
// pending-unshare LegacyVectorRegistry. Fixed at startup.
bool UseLazyVectorUnshare();

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

  // Visits up to batch_size entries per database, resuming where the
  // previous call stopped. An entry referenced only by the registry is no
  // longer used by any index: its bytes are copied back into the hash if the
  // hash still shares them, and the entry is erased. Returns the number of
  // entries visited.
  size_t SweepUnreferenced(uint32_t batch_size);

  // Called after subscribers for every keyspace event, whether or not an
  // index matches the key. Erases entries whose hash field no longer shares
  // the registry's bytes.
  void OnKeyspaceEvent(ValkeyModuleCtx *ctx, const char *event,
                       ValkeyModuleString *key);

  // Sweeps every db's entries and migrates their tables, so both finish
  // without any traffic.
  void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                            uint64_t subevent, void *data);

  struct Stats {
    size_t entry_cnt{0};
    uint64_t hash_sharing_errors{0};
    uint64_t hash_sharing_hits{0};
    uint64_t dedup_cnt{0};
    uint64_t unshare_cnt{0};
  };
  const Stats &GetStats() const;

  ValkeyModuleCtx *GetCtx() const { return ctx_.get(); }

  bool IsSharingActive() const { return hash_vector_sharing_; }

  void OnFlushDB(const ValkeyModuleFlushInfo *flush_info);
  void OnSwapDB(const ValkeyModuleSwapDbInfo *swap_info);

  // The key's value was replaced in place (SET, a *STORE, a module set-value
  // call) with no del event: erases every entry of the key. RENAME and MOVE
  // never overwrite this way.
  void OnKeyOverwritten(int db_num, absl::string_view key);

  // RENAME / MOVE: the hash object moves with its shared fields, so every
  // entry of src_key moves to dst_key. Entries of the overwritten dst_key are
  // dropped first; that hash is gone.
  void MoveKey(int src_db_num, absl::string_view src_key, int dst_db_num,
               absl::string_view dst_key);

  // An index's FILTER rejected the key, so the index no longer uses its
  // vectors. Unshares and erases the key's entries for those attributes
  // without waiting for the refcount: a deleted HNSW node may hold the record
  // until the index is dropped.
  void UnshareAttributes(int db_num, const InternedStringPtr &key,
                         absl::Span<const InternedStringPtr> attributes);

 private:
  // Vector attributes of one key. Almost always a single attribute.
  using AttrRecords = absl::InlinedVector<
      std::pair<InternedStringPtr, indexes::VectorRecordWithSize>, 1>;
  using PerDbRegistryMap =
      vmsdk::HugeFlatHashMap<InternedStringPtr, AttrRecords>;

  struct DbRegistry {
    PerDbRegistryMap keys;
    // Number of (key, attribute) entries in keys.
    size_t entry_cnt{0};
    // Next key SweepUnreferenced visits; null restarts from the beginning.
    InternedStringPtr sweep_cursor;
  };

  // Maps db_num -> tracked vector records of that database.
  absl::flat_hash_map<int, DbRegistry> tracked_vectors_;

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
      const indexes::VectorRecord &vector_record, size_t vector_size,
      const data_model::AttributeDataType &attribute_data_type);
  void EraseTrackedRecord(int db_num, const InternedStringPtr &key,
                          const InternedStringPtr &attribute_identifier);
  // Erases the entries of a key's attributes for which erase(attr, record)
  // returns true, and the key once it has none left.
  template <typename Pred>
  void EraseAttrsIf(int db_num, PerDbRegistryMap::iterator it, Pred erase);
  size_t SweepDb(int db_num, DbRegistry &db, uint32_t batch_size);
  // Replaces a shared string memory reference in the Valkey Hash data model
  // with an independent owned copy.
  // The field must currently be shared.
  bool UnshareWithValkey(ValkeyModuleKey *key_obj, ValkeyModuleString *attr_str,
                         const indexes::VectorRecord &vector_record,
                         size_t vector_size);
  // Unshares and erases the key's entries that only the registry references.
  void UnshareIfUnreferenced(int db_num, const InternedStringPtr &key);
  // Unshares and erases the key's entries for which select(attr, record)
  // returns true.
  template <typename Pred>
  void UnshareAttrsIf(int db_num, const InternedStringPtr &key, Pred select);
  bool IsEraseTrackedRecordSafe(
      int db_num, const InternedStringPtr &key, absl::string_view vector_str,
      absl::string_view attribute_identifier,
      const data_model::AttributeDataType &attribute_data_type);
};

}  // namespace valkey_search
