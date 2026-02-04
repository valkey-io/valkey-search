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

void VectorExternalizer::Track(const InternedStringPtr& key,
                               absl::string_view attribute_identifier,
                               InternedStringPtr interned_vector) {
  auto& tracked_vectors = tracked_vectors_.Get();
  tracked_vectors[key][attribute_identifier] = interned_vector;
}

void VectorExternalizer::UnTrack(
    const InternedStringPtr& key, absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type) {
  auto& tracked_vectors = tracked_vectors_.Get();
  auto it = tracked_vectors.find(key);
  if (it == tracked_vectors.end()) {
    return;
  }
  it->second.erase(attribute_identifier);
}

void VectorExternalizer::Externalize(
    const InternedStringPtr& key, absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type,
    InternedStringPtr interned_vector) {
  if (!hash_registration_supported_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return;
  }

  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(ctx_.Get().get(), key_str.get(),
                                                VALKEYMODULE_WRITE);
  CHECK(key_obj);
  if (ValkeyModule_HashHasStringRef(
          key_obj.get(),
          vmsdk::MakeUniqueValkeyString(attribute_identifier).get()) !=
      VALKEYMODULE_OK) {
    return;
  }
  Track(key, attribute_identifier, interned_vector);
  if (ValkeyModule_HashSetStringRef(
          key_obj.get(),
          vmsdk::MakeUniqueValkeyString(attribute_identifier).get(),
          interned_vector->Str().data(),
          interned_vector->Str().size()) != VALKEYMODULE_OK) {
    ++stats_.Get().hash_extern_errors;
    UnTrack(key, attribute_identifier, attribute_data_type);
  }
}

VectorExternalizer::Stats VectorExternalizer::GetStats() const {
  Stats ret = stats_.Get();
  ret.entry_cnt = tracked_vectors_.Get().size();
  return ret;
}

void VectorExternalizer::Reset() {
  ctx_.Get().reset();
  stats_.Get() = Stats();
}

}  // namespace valkey_search
