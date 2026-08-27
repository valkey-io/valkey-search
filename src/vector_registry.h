/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
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
  // Returns VectorRecordWithSize. If hash_vector_sharing_ is false,
  // constructs a VectorRecord directly.
  indexes::VectorRecordWithSize DedupOrConstruct(
      const InternedStringPtr &key, ValkeyModuleString *vector,
      const data_model::AttributeDataType &attribute_data_type, int db_num,
      const indexes::VectorBase *vector_base);

  struct Stats {
    size_t entry_cnt;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_errors;
    vmsdk::ShardedAtomic<uint64_t> hash_sharing_hits;
    vmsdk::ShardedAtomic<uint64_t> dedup_cnt;
  };
  const Stats &GetStats() const;

  ValkeyModuleCtx *GetCtx() const { return ctx_.get(); }

  bool IsSharingActive() const { return hash_vector_sharing_; }

 private:
  struct RegistryKey {
    int db_num{0};
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

  // Map to track active vector records.
  absl::flat_hash_map<RegistryKey, indexes::VectorRecordWithSize>
      tracked_vectors_;

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
  bool IsEraseTrackedRecordSafe(
      int db_num, const InternedStringPtr &key, absl::string_view vector_str,
      absl::string_view attribute_identifier,
      const data_model::AttributeDataType &attribute_data_type);
};

}  // namespace valkey_search
