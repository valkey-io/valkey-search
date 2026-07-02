/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/index_schema.pb.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace indexes {
class VectorRecord;
}

using RegistryKey = std::pair<InternedStringPtr, std::string>;

struct RegistryValue {
  std::weak_ptr<indexes::VectorRecord> record;
  std::shared_ptr<indexes::VectorRecord> shared_externally;
  size_t record_size{0};
};

class VectorRegistry {
 public:
  static VectorRegistry &Instance() {
    static VectorRegistry *instance = new VectorRegistry();
    return *instance;
  }

  void Init(ValkeyModuleCtx *ctx);

  // Disallow copy and move.
  VectorRegistry(const VectorRegistry &) = delete;
  VectorRegistry &operator=(const VectorRegistry &) = delete;

  std::shared_ptr<indexes::VectorRecord> DedupConstruct(
      const InternedStringPtr &key, absl::string_view attribute_identifier,
      absl::string_view vector, float magnitude, Allocator *allocator)
      ABSL_LOCKS_EXCLUDED(share_externally_mutex_, mutex_);

  void Untrack(const InternedStringPtr &key,
               absl::string_view attribute_identifier,
               bool attribute_deleted = false)
      ABSL_LOCKS_EXCLUDED(share_externally_mutex_, mutex_);

  bool EngineShare(const InternedStringPtr &key,
                   absl::string_view attribute_identifier,
                   const indexes::VectorRecord *vector_record,
                   size_t vector_size,
                   const data_model::AttributeDataType &attribute_data_type)
      ABSL_LOCKS_EXCLUDED(share_externally_mutex_, mutex_);
  struct Stats {
    size_t hash_extern_errors{0};
    size_t entry_cnt{0};
    size_t shared_externally_cnt{0};
    size_t deduplication_hits{0};
    size_t deduplication_misses{0};
    size_t memory_saved_bytes{0};
  };
  Stats GetStats() const;

  ValkeyModuleCtx *GetCtx() const { return ctx_.Get().get(); }
  void Reset();

 private:
  // Map to track active vector records.
  absl::flat_hash_map<RegistryKey, RegistryValue> tracked_vectors_
      ABSL_GUARDED_BY(mutex_);

  absl::flat_hash_set<RegistryKey> pending_shared_externally_
      ABSL_GUARDED_BY(share_externally_mutex_);

  friend class VectorRegistryTest;
  bool hash_registration_supported_ = false;
  mutable Stats stats_ ABSL_GUARDED_BY(mutex_);
  vmsdk::MainThreadAccessGuard<vmsdk::UniqueValkeyDetachedThreadSafeContext>
      ctx_;
  bool scheduled_run_by_main_ = false ABSL_GUARDED_BY(share_externally_mutex_);
  mutable absl::Mutex mutex_;
  mutable absl::Mutex share_externally_mutex_;

  VectorRegistry() = default;
  ~VectorRegistry() = default;
  void ExecuteBatchEngineShare() ABSL_LOCKS_EXCLUDED(mutex_);
  bool PerformEngineShare(const RegistryKey &key) ABSL_LOCKS_EXCLUDED(mutex_);
  void ScheduleAsyncByMain();
};

}  // namespace valkey_search
