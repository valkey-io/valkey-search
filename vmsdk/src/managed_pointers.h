/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MANAGED_POINTERS_H_
#define VMSDK_SRC_MANAGED_POINTERS_H_

#include <cstddef>
#include <memory>

#include "absl/strings/string_view.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

struct ValkeyStringDeleter {
  void operator()(ValkeyModuleString *str) {
    ValkeyModule_FreeString(nullptr, str);
  }
};

struct ValkeyModuleUserDeleter {
  void operator()(ValkeyModuleUser *user) { ValkeyModule_FreeModuleUser(user); }
};

using UniqueValkeyString =
    std::unique_ptr<ValkeyModuleString, ValkeyStringDeleter>;

using UniqueValkeyModuleUser =
    std::unique_ptr<ValkeyModuleUser, ValkeyModuleUserDeleter>;

inline UniqueValkeyString UniquePtrValkeyString(
    ValkeyModuleString *valkey_str) {
  return std::unique_ptr<ValkeyModuleString, ValkeyStringDeleter>(valkey_str);
}

inline UniqueValkeyString MakeUniqueValkeyString(absl::string_view str) {
  auto valkey_str = ValkeyModule_CreateString(nullptr, str.data(), str.size());
  return UniquePtrValkeyString(valkey_str);
}

inline UniqueValkeyString MakeUniqueValkeyString(const char *str) {
  if (str) {
    return MakeUniqueValkeyString(absl::string_view(str));
  }
  return UniquePtrValkeyString(nullptr);
}

inline UniqueValkeyString RetainUniqueValkeyString(
    ValkeyModuleString *valkey_str) {
  ValkeyModule_RetainString(nullptr, valkey_str);
  return UniquePtrValkeyString(valkey_str);
}

struct ValkeyOpenKeyDeleter {
  void operator()(ValkeyModuleKey *module_key) {
    if (module_key) {
      ValkeyModule_CloseKey(module_key);
    }
  }
};

using UniqueValkeyOpenKey =
    std::unique_ptr<ValkeyModuleKey, ValkeyOpenKeyDeleter>;

inline UniqueValkeyOpenKey UniquePtrValkeyOpenKey(ValkeyModuleKey *valkey_key) {
  return std::unique_ptr<ValkeyModuleKey, ValkeyOpenKeyDeleter>(valkey_key);
}

inline UniqueValkeyOpenKey MakeUniqueValkeyOpenKey(ValkeyModuleCtx *ctx,
                                                   ValkeyModuleString *str,
                                                   int flags) {
  VerifyMainThread();
  auto module_key = ValkeyModule_OpenKey(ctx, str, flags);
  return std::unique_ptr<ValkeyModuleKey, ValkeyOpenKeyDeleter>(module_key);
}

struct ValkeyReplyDeleter {
  void operator()(ValkeyModuleCallReply *reply) {
    ValkeyModule_FreeCallReply(reply);
  }
};

using UniqueValkeyCallReply =
    std::unique_ptr<ValkeyModuleCallReply, ValkeyReplyDeleter>;

inline UniqueValkeyCallReply UniquePtrValkeyCallReply(
    ValkeyModuleCallReply *reply) {
  return std::unique_ptr<ValkeyModuleCallReply, ValkeyReplyDeleter>(reply);
}

struct ValkeyScanCursorDeleter {
  void operator()(ValkeyModuleScanCursor *cursor) {
    ValkeyModule_ScanCursorDestroy(cursor);
  }
};

using UniqueValkeyScanCursor =
    std::unique_ptr<ValkeyModuleScanCursor, ValkeyScanCursorDeleter>;

inline UniqueValkeyScanCursor UniquePtrValkeyScanCursor(
    ValkeyModuleScanCursor *cursor) {
  return std::unique_ptr<ValkeyModuleScanCursor, ValkeyScanCursorDeleter>(
      cursor);
}

inline UniqueValkeyScanCursor MakeUniqueValkeyScanCursor() {
  auto cursor = ValkeyModule_ScanCursorCreate();
  return std::unique_ptr<ValkeyModuleScanCursor, ValkeyScanCursorDeleter>(
      cursor);
}

struct ValkeyThreadSafeContextDeleter {
  void operator()(ValkeyModuleCtx *ctx) {
    // Contexts cannot be safely freed from a background thread since they
    // assert if IO threads are active.
    RunByMain([ctx]() { ValkeyModule_FreeThreadSafeContext(ctx); });
  }
};

using UniqueValkeyDetachedThreadSafeContext =
    std::unique_ptr<ValkeyModuleCtx, ValkeyThreadSafeContextDeleter>;

inline UniqueValkeyDetachedThreadSafeContext
MakeUniqueValkeyDetachedThreadSafeContext(ValkeyModuleCtx *base_ctx) {
  auto ctx = ValkeyModule_GetDetachedThreadSafeContext(base_ctx);
  return std::unique_ptr<ValkeyModuleCtx, ValkeyThreadSafeContextDeleter>(ctx);
}

using UniqueValkeyThreadSafeContext =
    std::unique_ptr<ValkeyModuleCtx, ValkeyThreadSafeContextDeleter>;

inline UniqueValkeyThreadSafeContext MakeUniqueValkeyThreadSafeContext(
    ValkeyModuleBlockedClient *bc) {
  auto ctx = ValkeyModule_GetThreadSafeContext(bc);
  return std::unique_ptr<ValkeyModuleCtx, ValkeyThreadSafeContextDeleter>(ctx);
}

struct ValkeyClusterNodesListDeleter {
  void operator()(char **nodes_list) {
    ValkeyModule_FreeClusterNodesList(nodes_list);
  }
};

using UniqueValkeyClusterNodesList =
    std::unique_ptr<char *, ValkeyClusterNodesListDeleter>;

inline UniqueValkeyClusterNodesList MakeUniqueValkeyClusterNodesList(
    ValkeyModuleCtx *ctx, size_t *num_nodes) {
  auto nodes_list = ValkeyModule_GetClusterNodesList(ctx, num_nodes);
  return std::unique_ptr<char *, ValkeyClusterNodesListDeleter>(nodes_list);
}

}  // namespace vmsdk
#endif  // VMSDK_SRC_MANAGED_POINTERS_H_
