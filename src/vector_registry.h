/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <absl/base/no_destructor.h>

#include <memory>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
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
      const InternedStringPtr &attribute_identifier,
      ValkeyModuleString *vector, Allocator *allocator,
      const data_model::AttributeDataType &attribute_data_type)
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Retrieves the tracked VectorRecord and raw payload byte size for a given
  // key and attribute. Increments lookup_record_hits if found, or
  // lookup_record_misses if not present.
  std::pair<std::shared_ptr<indexes::VectorRecord>, size_t> LookupRecord(
      const InternedStringPtr &key,
      const InternedStringPtr &interned_attribute_identifier) const
      ABSL_LOCKS_EXCLUDED(mutex_);

  // Batch untracks a map of keys if the registry holds the last remaining
  // reference to each vector record.
  void BatchUntrackIfUnused(const InternedStringPtr &attribute_identifier,
                            InternedStringHashMap<indexes::TrackedKeyMetadata>
                                &&tracked_metadata_by_key)
      ABSL_LOCKS_EXCLUDED(mutex_);

  struct Stats {
    size_t entry_cnt ABSL_GUARDED_BY(mutex_);
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_errors;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_hits;
    vmsdk::ShardedAtomic<uint64_t> lookup_record_hits;
    vmsdk::ShardedAtomic<uint64_t> lookup_record_misses;
  };
  Stats GetStats() const ABSL_LOCKS_EXCLUDED(mutex_);

  ValkeyModuleCtx *GetCtx() const { return ctx_.get(); }

  // Untracks a vector record entry from the registry if the registry holds the
  // sole remaining reference (use_count == 1).
  void UntrackIfUnused(const InternedStringPtr &key,
                       const InternedStringPtr &interned_attribute_identifier)
      ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  using RegistryKey = std::pair<InternedStringPtr, InternedStringPtr>;

  struct RegistryValue {
    std::shared_ptr<indexes::VectorRecord> vector_record;
    size_t vector_record_size{0};
  };
  // Map to track active vector records.
  absl::flat_hash_map<RegistryKey, RegistryValue> tracked_vectors_
      ABSL_GUARDED_BY(mutex_);

  friend class VectorRegistryTest;
  bool hash_registration_supported_{false};
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
  bool ShareWithValkeyHash(
      const InternedStringPtr &key, absl::string_view attribute_identifier,
      const indexes::VectorRecord *vector_record, size_t vector_size,
      const data_model::AttributeDataType &attribute_data_type)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Reverts external shared vector references in the Valkey engine back to
  // standard string values prior to untracking.
  void DetachFromValkeyHash(const RegistryKey &search_key);

  // Helper method that checks use_count and untracks an entry while mutex_ is
  // held.
  void LockFreeUntrackIfUnused(const RegistryKey &search_key)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
};

}  // namespace valkey_search
