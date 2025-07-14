/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_VECTOR_EXTERNALIZER_H_
#define VALKEYSEARCH_SRC_VECTOR_EXTERNALIZER_H_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/utils/lru.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

constexpr size_t kLRUCapacity = 100;
char* ExternalizeCB(void* cb_data, size_t* len);
std::vector<char> DenormalizeVector(absl::string_view record, size_t type_size,
                                    float magnitude);

class VectorExternalizer {
 public:
  static VectorExternalizer& Instance() {
    static VectorExternalizer* instance = new VectorExternalizer();
    return *instance;
  }

  bool Externalize(const InternedStringPtr& key,
                   absl::string_view attribute_identifier,
                   data_model::AttributeDataType attribute_data_type,
                   const InternedStringPtr& vector,
                   std::optional<float> magnitude);
  void Remove(const InternedStringPtr& key,
              absl::string_view attribute_identifier,
              data_model::AttributeDataType attribute_data_type);
  void ProcessEngineUpdateQueue();

  struct Stats {
    size_t num_lru_entries{0};
    size_t hash_extern_errors{0};
    size_t lru_promote_cnt{0};
    size_t entry_cnt{0};
    size_t deferred_entry_cnt{0};
    size_t generated_value_cnt{0};
  };
  Stats GetStats() const;
  void ExternalizeCBCalled() { ++stats_.Get().generated_value_cnt; }

  struct VectorExternalizerEntry;
  struct LRUCacheEntry {
    LRUCacheEntry(std::vector<char>&& normalized_vector,
                  VectorExternalizerEntry* entry)
        : normalized_vector(std::move(normalized_vector)), entry(entry) {}
    ~LRUCacheEntry();
    std::vector<char> normalized_vector;
    VectorExternalizerEntry* entry{nullptr};
    LRUCacheEntry* next{nullptr};
    LRUCacheEntry* prev{nullptr};
  };
  struct VectorExternalizerEntry {
    InternedStringPtr vector;
    std::optional<float> magnitude;
    // We cache the normalized vector to ensure that the generated normalized
    // vector string remains alive until the engine deep copy it.
    std::unique_ptr<LRUCacheEntry> cache_normalized_;
  };
  void LRUPromote(LRUCacheEntry* entry);
  LRUCacheEntry* LRUAdd(LRUCacheEntry* entry);
  void LRURemove(LRUCacheEntry* entry);
  void Init(ValkeyModuleCtx* ctx);
  ValkeyModuleCtx* GetCtx() const {
    CHECK(ctx_.Get());
    return ctx_.Get().get();
  }
  vmsdk::UniqueValkeyString GetRecord(
      ValkeyModuleCtx* ctx, const AttributeDataType* attribute_data_type,
      ValkeyModuleKey* key_obj, absl::string_view key_cstr,
      absl::string_view attribute_identifier, bool& is_module_owned);

  // Used for testing.
  void Reset();

 private:
  VectorExternalizer();

  vmsdk::MainThreadAccessGuard<InternedStringMap<
      absl::flat_hash_map<std::string, VectorExternalizerEntry>>>
      shared_vectors_;
  vmsdk::MainThreadAccessGuard<InternedStringMap<
      absl::flat_hash_map<std::string, VectorExternalizerEntry>>>
      deferred_shared_vectors_;
  vmsdk::MainThreadAccessGuard<std::unique_ptr<LRU<LRUCacheEntry>>> lru_cache_;
  vmsdk::MainThreadAccessGuard<Stats> stats_;
  vmsdk::MainThreadAccessGuard<vmsdk::UniqueValkeyDetachedThreadSafeContext>
      ctx_;
  size_t EntriesCnt() const;
  size_t PendingEntriesCnt() const;
  bool hash_registration_supported_ = false;
};

template <typename T>
void CopyAndDenormalizeEmbedding(T* dst, T* src, size_t size, float magnitude) {
  for (size_t i = 0; i < size; i++) {
    dst[i] = src[i] * magnitude;
  }
}

};  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_VECTOR_EXTERNALIZER_H_
