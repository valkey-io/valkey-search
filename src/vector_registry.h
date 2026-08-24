/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/allocator.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/sharded_atomic.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class VectorRegistry {
 public:
  static VectorRegistry &Instance() { return *InstancePtr(); }
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

  // Registers or updates a vector record in the registry for deduplication and
  // external sharing. If vector is nullptr, untracks and removes the record
  // from the registry for the given key and attribute. Returns the shared
  // VectorRecord pointer (reusing existing instance if payload matches).
  std::shared_ptr<indexes::VectorRecord> Track(
      const InternedStringPtr &key,
      const InternedStringPtr &attribute_identifier, ValkeyModuleString *vector,
      Allocator *allocator,
      const data_model::AttributeDataType &attribute_data_type, int db_num)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Retrieves the tracked VectorRecord and raw payload byte size for a given
  // key and attribute. Increments lookup_record_hits if found, or
  // lookup_record_misses if not present.
  std::pair<std::shared_ptr<indexes::VectorRecord>, size_t> LookupRecord(
      const InternedStringPtr &key,
      const InternedStringPtr &interned_attribute_identifier, int db_num) const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Batch untracks a map of keys if the registry holds the last remaining
  // reference to each vector record.
  void BatchUntrackIfUnused(const InternedStringPtr &attribute_identifier,
                            InternedStringHashMap<indexes::TrackedKeyMetadata>
                                &&tracked_metadata_by_key,
                            int db_num) ABSL_LOCKS_EXCLUDED(mutex_);

  struct Stats {
    size_t entry_cnt;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_errors;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_hits;
    vmsdk::ShardedAtomic<uint64_t> lookup_record_hits;
    vmsdk::ShardedAtomic<uint64_t> lookup_record_misses;
  };
  const Stats &GetStats() const;

  ValkeyModuleCtx *GetCtx() const { return ctx_.get(); }

  bool IsSharingActive() const { return hash_vector_sharing_; }

  // Untracks a vector record entry from the registry if the registry holds the
  // sole remaining reference (use_count == 1).
  void UntrackIfUnused(const InternedStringPtr &key,
                       const InternedStringPtr &interned_attribute_identifier,
                       int db_num) ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  struct RegistryKey {
    int db_num;
    InternedStringPtr key;
    InternedStringPtr attribute_identifier;

    bool operator==(const RegistryKey &o) const {
      return db_num == o.db_num && key == o.key &&
             attribute_identifier == o.attribute_identifier;
    }

    template <typename H>
    friend H AbslHashValue(H h, const RegistryKey &k) {
      return H::combine(std::move(h), k.db_num, k.key, k.attribute_identifier);
    }
  };

  struct RegistryValue {
    std::shared_ptr<indexes::VectorRecord> vector_record;
    size_t vector_record_size{0};
  };
  // Map to track active vector records.
  absl::flat_hash_map<RegistryKey, RegistryValue> tracked_vectors_
      ABSL_GUARDED_BY(mutex_);
  // Cache the last untracked vector to safely handle Valkey RENAME operations.
  // When Valkey executes RENAME, it fires two sequential events on the main
  // thread:
  // 1. A 'del' for the source key (which drops the VectorRecord).
  // 2. A 'rename_to' (treated as a 'set') for the destination key.
  // By caching the shared_ptr during the 'del' phase, we prevent the memory
  // from being freed prematurely (or being freed in a background worker thread
  // before the main thread can reclaim it). When the subsequent 'set' phase
  // arrives, we can reuse this cached pointer if the bytes match perfectly,
  // preventing ASAN heap-use-after-free and preserving the vector reference.
  struct LastUntracked {
    std::shared_ptr<indexes::VectorRecord> record;
    size_t size{0};
  };
  LastUntracked last_untracked_ ABSL_GUARDED_BY(mutex_);

  friend class VectorRegistryTest;
  bool hash_vector_sharing_{false};
  mutable Stats stats_;
  vmsdk::UniqueValkeyDetachedThreadSafeContext ctx_;
  mutable absl::Mutex mutex_;

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

  // Reverts external shared vector references in the Valkey engine back to
  // standard string values prior to untracking.
  void DetachFromValkey(const RegistryKey &search_key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Helper method that checks use_count and untracks an entry while mutex_ is
  // held.
  void LockFreeUntrackIfUnused(const RegistryKey &search_key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
};

}  // namespace valkey_search
