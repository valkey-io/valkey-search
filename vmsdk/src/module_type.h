/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MODULETYPE_H_
#define VMSDK_SRC_MODULETYPE_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

class ModuleType {
 public:
  ModuleType(ValkeyModuleCtx *ctx, absl::string_view key,
             ValkeyModuleType *module_type);
  virtual ~ModuleType() = default;
  absl::Status Register(ValkeyModuleCtx *ctx);
  absl::Status Deregister(ValkeyModuleCtx *ctx);

  static absl::Status Register(ValkeyModuleCtx *ctx, absl::string_view key,
                               void *ptr, ValkeyModuleType *module_type);
  static absl::Status Deregister(ValkeyModuleCtx *ctx, absl::string_view key);

 protected:
  ValkeyModuleType *module_type_{nullptr};
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  std::string key_;
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_MODULETYPE_H_
