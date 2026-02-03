/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/vector_externalizer.h"

#include <cstdlib>
#include <memory>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

void VectorExternalizer::Init(ValkeyModuleCtx* ctx) {
  hash_registration_supported_ =
      (ValkeyModule_GetApi("ValkeyModule_HashSetStringRef",
                           (void**)&ValkeyModule_HashSetStringRef) ==
       VALKEYMODULE_OK);
  ctx_ = vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx);
}

InternedStringPtr VectorExternalizer::Intern(
    const indexes::VectorBase* vec_index, absl::string_view vector_str) {
  absl::MutexLock lock(&mutex_);
  auto it = tracked_vectors_.find(vector_str);
  if (it != tracked_vectors_.end()) {
    vec_index->SetMagnitude(it->second->Str());
    return it->second;
  }
  InternedStringPtr interned_vector = vec_index->InternVector(vector_str);
  tracked_vectors_[vector_str] = interned_vector;
  return interned_vector;
}

InternedStringPtr VectorExternalizer::Externalize(
    const indexes::VectorBase* vec_index, const InternedStringPtr& key,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type,
    const ValkeyModuleString* vector) {
  vmsdk::VerifyMainThread();
  auto vector_str = vmsdk::ToStringView(vector);
  if (!vec_index->IsValidSizeVector(vector_str)) {
    return {};
  }
  InternedStringPtr interned_vector = Intern(vec_index, vector_str);
  if (!hash_registration_supported_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return {};
  }

  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(ctx_.Get().get(), key_str.get(),
                                                VALKEYMODULE_WRITE);
  CHECK(key_obj);
  if (ValkeyModule_HashHasStringRef(
          key_obj.get(),
          vmsdk::MakeUniqueValkeyString(attribute_identifier).get()) !=
      VALKEYMODULE_OK) {
    return {};
  }
  if (ValkeyModule_HashSetStringRef(
          key_obj.get(),
          vmsdk::MakeUniqueValkeyString(attribute_identifier).get(),
          interned_vector->Str().data(),
          interned_vector->Str().size()) != VALKEYMODULE_OK) {
    ++stats_.Get().hash_extern_errors;
    return {};
  }
  return interned_vector;
}

VectorExternalizer::Stats VectorExternalizer::GetStats() const {
  Stats ret = stats_.Get();
  absl::MutexLock lock(&mutex_);
  ret.entry_cnt = tracked_vectors_.size();
  return ret;
}

void VectorExternalizer::Reset() {
  ctx_.Get().reset();
  stats_.Get() = Stats();
}

}  // namespace valkey_search
