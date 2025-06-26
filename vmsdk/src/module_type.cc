/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
