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

using UniqueValkeyString =
    std::unique_ptr<ValkeyModuleString, ValkeyStringDeleter>;

inline UniqueValkeyString UniquePtrValkeyString(ValkeyModuleString *valkey_str) {
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
