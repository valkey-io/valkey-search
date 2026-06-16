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
#include "src/valkey_search_options.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

void VectorExternalizer::Init(ValkeyModuleCtx* ctx) {
  hash_registration_supported_ =
      (ValkeyModule_GetApi("ValkeyModule_HashSetStringRef",
                           (void**)&ValkeyModule_HashSetStringRef) ==
       VALKEYMODULE_OK) &&
      (ValkeyModule_GetApi("ValkeyModule_HashExternalize",
                           (void**)&ValkeyModule_HashExternalize) ==
       VALKEYMODULE_OK) &&
      options::GetEnableVectorSharing().GetValue();
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
  if (it->second.empty()) {
    tracked_vectors.erase(it);
  }
}

bool VectorExternalizer::Externalize(
    const InternedStringPtr& key, absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type,
    InternedStringPtr interned_vector, size_t vector_size) {
  if (!hash_registration_supported_ ||
      attribute_data_type !=
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH) {
    return false;
  }

  auto key_str = vmsdk::MakeUniqueValkeyString(key->Str());
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(ctx_.Get().get(), key_str.get(),
                                                VALKEYMODULE_WRITE);
  CHECK(key_obj);
  auto attribute_identifier_str =
      vmsdk::MakeUniqueValkeyString(attribute_identifier);
  if (ValkeyModule_HashHasStringRef(
          key_obj.get(), attribute_identifier_str.get()) != VALKEYMODULE_OK) {
    return false;
  }

  if (ValkeyModule_HashSetStringRef(
          key_obj.get(), attribute_identifier_str.get(),
          interned_vector->Str().data(), vector_size) != VALKEYMODULE_OK) {
    ++stats_.Get().hash_extern_errors;
    return false;
  }
  Track(key, attribute_identifier, interned_vector);
  return true;
}

VectorExternalizer::Stats VectorExternalizer::GetStats() const {
  Stats ret = stats_.Get();
  ret.entry_cnt = tracked_vectors_.Get().size();
  return ret;
}

void VectorExternalizer::Reset() {
  ctx_.Get().reset();
  hash_registration_supported_ = false;
  tracked_vectors_.Get().clear();
  stats_.Get() = Stats();
}

}  // namespace valkey_search
