/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "src/vector_registry.h"
#include "src/vector_registry_legacy.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Routes the calls both registries share to the one vector-lazy-unshare
// selects. Mode-specific calls name their registry directly.
namespace valkey_search::vector_registry {

inline void Construct(ValkeyModuleCtx *ctx) {
  if (UseLazyVectorUnshare()) {
    VectorRegistry::Construct(ctx);
  } else {
    LegacyVectorRegistry::Construct(ctx);
  }
}

inline void Destruct() {
  VectorRegistry::Destruct();
  LegacyVectorRegistry::Destruct();
}

inline indexes::VectorRecordWithSize DedupOrConstruct(
    const InternedStringPtr &key, ValkeyModuleString *vector,
    const data_model::AttributeDataType &attribute_data_type, int db_num,
    const indexes::VectorBase *vector_base) {
  if (UseLazyVectorUnshare()) {
    return VectorRegistry::Instance().DedupOrConstruct(
        key, vector, attribute_data_type, db_num, vector_base);
  }
  return LegacyVectorRegistry::Instance().DedupOrConstruct(
      key, vector, attribute_data_type, db_num, vector_base);
}

inline void OnFlushDB(const ValkeyModuleFlushInfo *flush_info) {
  if (UseLazyVectorUnshare()) {
    VectorRegistry::Instance().OnFlushDB(flush_info);
  } else {
    LegacyVectorRegistry::Instance().OnFlushDB(flush_info);
  }
}

inline void OnSwapDB(const ValkeyModuleSwapDbInfo *swap_info) {
  if (UseLazyVectorUnshare()) {
    VectorRegistry::Instance().OnSwapDB(swap_info);
  } else {
    LegacyVectorRegistry::Instance().OnSwapDB(swap_info);
  }
}

inline void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                                 uint64_t subevent, void *data) {
  if (UseLazyVectorUnshare()) {
    VectorRegistry::Instance().OnServerCronCallback(ctx, eid, subevent, data);
  } else {
    LegacyVectorRegistry::Instance().OnServerCronCallback(ctx, eid, subevent,
                                                          data);
  }
}

inline bool IsSharingActive() {
  return UseLazyVectorUnshare()
             ? VectorRegistry::Instance().IsSharingActive()
             : LegacyVectorRegistry::Instance().IsSharingActive();
}

struct Stats {
  size_t entry_cnt{0};
  uint64_t hash_sharing_errors{0};
  uint64_t hash_sharing_hits{0};
  uint64_t dedup_cnt{0};
  // Lazy mode only.
  uint64_t unshare_cnt{0};
  // Legacy mode only.
  size_t pending_unshare_cnt{0};
};

inline Stats GetStats() {
  if (UseLazyVectorUnshare()) {
    const auto &s = VectorRegistry::Instance().GetStats();
    return {.entry_cnt = s.entry_cnt,
            .hash_sharing_errors = s.hash_sharing_errors,
            .hash_sharing_hits = s.hash_sharing_hits,
            .dedup_cnt = s.dedup_cnt,
            .unshare_cnt = s.unshare_cnt};
  }
  auto &registry = LegacyVectorRegistry::Instance();
  const auto &s = registry.GetStats();
  return {.entry_cnt = s.entry_cnt,
          .hash_sharing_errors = s.hash_sharing_errors,
          .hash_sharing_hits = s.hash_sharing_hits,
          .dedup_cnt = s.dedup_cnt,
          .pending_unshare_cnt = registry.GetPendingUnsharesCount()};
}

}  // namespace valkey_search::vector_registry
