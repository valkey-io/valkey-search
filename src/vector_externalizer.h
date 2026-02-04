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

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.pb.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

class VectorExternalizer {
 public:
  static VectorExternalizer& Instance() {
    static VectorExternalizer* instance = new VectorExternalizer();
    return *instance;
  }

  void Externalize(const InternedStringPtr& key,
                   absl::string_view attribute_identifier,
                   data_model::AttributeDataType attribute_data_type,
                   InternedStringPtr vector);

  void UnTrack(const InternedStringPtr& key,
               absl::string_view attribute_identifier,
               data_model::AttributeDataType attribute_data_type);

  struct Stats {
    size_t hash_extern_errors{0};
    size_t entry_cnt{0};
  };
  Stats GetStats() const;

  void Init(ValkeyModuleCtx* ctx);
  ValkeyModuleCtx* GetCtx() const {
    CHECK(ctx_.Get());
    return ctx_.Get().get();
  }

  // Used for testing.
  void Reset();

 private:
  VectorExternalizer() = default;

  vmsdk::MainThreadAccessGuard<vmsdk::UniqueValkeyDetachedThreadSafeContext>
      ctx_;
  vmsdk::MainThreadAccessGuard<InternedStringHashMap<
      absl::flat_hash_map<std::string, InternedStringPtr>>>
      tracked_vectors_;
  ABSL_GUARDED_BY(mutex_);
  void Track(const InternedStringPtr& key,
             absl::string_view attribute_identifier,
             InternedStringPtr interned_vector);
  bool hash_registration_supported_ = false;
  vmsdk::MainThreadAccessGuard<Stats> stats_;
};

template <typename T>
void CopyAndDenormalizeEmbedding(T* dst, T* src, size_t size, float magnitude) {
  for (size_t i = 0; i < size; i++) {
    dst[i] = src[i] * magnitude;
  }
}

};  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_VECTOR_EXTERNALIZER_H_
