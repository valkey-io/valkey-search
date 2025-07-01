/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/module_type.h"

#include <string>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

void DoDeregister(ValkeyModuleCtx *ctx, ValkeyModuleKey *module_key,
                  absl::string_view key) {
  if (ValkeyModule_DeleteKey(module_key) != VALKEYMODULE_OK) {
    VMSDK_LOG(WARNING, ctx) << "failed to delete redis key " << key;
    DCHECK(false);
  }
}

ModuleType::ModuleType(ValkeyModuleCtx *ctx, absl::string_view key,
                       ValkeyModuleType *module_type)
    : module_type_(module_type),
      detached_ctx_(vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx)),
      key_(key) {
  DCHECK(module_type);
}

absl::Status ModuleType::Register(ValkeyModuleCtx *ctx, absl::string_view key,
                                  void *ptr, ValkeyModuleType *module_type) {
  auto valkey_str = MakeUniqueValkeyString(key);
  auto module_key =
      MakeUniqueValkeyOpenKey(ctx, valkey_str.get(), VALKEYMODULE_WRITE);
  if (!module_key) {
    return absl::InternalError(
        absl::StrCat("failed to open Valkey module key: ", key));
  }
  if (ValkeyModule_KeyType(module_key.get()) != VALKEYMODULE_KEYTYPE_EMPTY) {
    return absl::AlreadyExistsError(
        absl::StrCat("Valkey module key ", key, " already exists"));
  }
  if (ValkeyModule_ModuleTypeSetValue(module_key.get(), module_type, ptr) !=
      VALKEYMODULE_OK) {
    DoDeregister(ctx, module_key.get(), key);
    return absl::InternalError(
        absl::StrCat("failed to set module type value for key: ", key));
  }
  return absl::OkStatus();
}

absl::Status ModuleType::Deregister(ValkeyModuleCtx *ctx,
                                    absl::string_view key) {
  auto valkey_str = MakeUniqueValkeyString(key);

  if (!ValkeyModule_KeyExists(ctx, valkey_str.get())) return absl::OkStatus();

  auto module_key =
      MakeUniqueValkeyOpenKey(ctx, valkey_str.get(), VALKEYMODULE_WRITE);
  if (!module_key) {
    DCHECK(false);
    return absl::InternalError(
        absl::StrCat("failed to open redis key: ", key.data()));
  }
  DoDeregister(ctx, module_key.get(), key);
  return absl::OkStatus();
}

absl::Status ModuleType::Register(ValkeyModuleCtx *ctx) {
  return Register(ctx, key_, this, module_type_);
}

absl::Status ModuleType::Deregister(ValkeyModuleCtx *ctx) {
  return Deregister(ctx, key_);
}

}  // namespace vmsdk
