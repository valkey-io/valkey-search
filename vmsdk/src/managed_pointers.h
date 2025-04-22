/*
 * Copyright (c) 2025, ValkeySearch contributors
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

struct RedisStringDeleter {
  void operator()(RedisModuleString *str) {
    RedisModule_FreeString(nullptr, str);
  }
};

using UniqueRedisString =
    std::unique_ptr<RedisModuleString, RedisStringDeleter>;

inline UniqueRedisString UniquePtrRedisString(RedisModuleString *redis_str) {
  return std::unique_ptr<RedisModuleString, RedisStringDeleter>(redis_str);
}

inline UniqueRedisString MakeUniqueRedisString(absl::string_view str) {
  auto redis_str = RedisModule_CreateString(nullptr, str.data(), str.size());
  return UniquePtrRedisString(redis_str);
}

inline UniqueRedisString MakeUniqueRedisString(const char *str) {
  if (str) {
    return MakeUniqueRedisString(absl::string_view(str));
  }
  return UniquePtrRedisString(nullptr);
}

inline UniqueRedisString RetainUniqueRedisString(RedisModuleString *redis_str) {
  RedisModule_RetainString(nullptr, redis_str);
  return UniquePtrRedisString(redis_str);
}

struct RedisOpenKeyDeleter {
  void operator()(RedisModuleKey *module_key) {
    if (module_key) {
      RedisModule_CloseKey(module_key);
    }
  }
};

using UniqueRedisOpenKey = std::unique_ptr<RedisModuleKey, RedisOpenKeyDeleter>;

inline UniqueRedisOpenKey UniquePtrRedisOpenKey(RedisModuleKey *redis_key) {
  return std::unique_ptr<RedisModuleKey, RedisOpenKeyDeleter>(redis_key);
}

inline UniqueRedisOpenKey MakeUniqueRedisOpenKey(RedisModuleCtx *ctx,
                                                 RedisModuleString *str,
                                                 int flags) {
  VerifyMainThread();
  auto module_key = RedisModule_OpenKey(ctx, str, flags);
  return std::unique_ptr<RedisModuleKey, RedisOpenKeyDeleter>(module_key);
}

struct RedisReplyDeleter {
  void operator()(RedisModuleCallReply *reply) {
    RedisModule_FreeCallReply(reply);
  }
};

using UniqueRedisCallReply =
    std::unique_ptr<RedisModuleCallReply, RedisReplyDeleter>;

inline UniqueRedisCallReply UniquePtrRedisCallReply(
    RedisModuleCallReply *reply) {
  return std::unique_ptr<RedisModuleCallReply, RedisReplyDeleter>(reply);
}

struct RedisScanCursorDeleter {
  void operator()(RedisModuleScanCursor *cursor) {
    RedisModule_ScanCursorDestroy(cursor);
  }
};

using UniqueRedisScanCursor =
    std::unique_ptr<RedisModuleScanCursor, RedisScanCursorDeleter>;

inline UniqueRedisScanCursor UniquePtrRedisScanCursor(
    RedisModuleScanCursor *cursor) {
  return std::unique_ptr<RedisModuleScanCursor, RedisScanCursorDeleter>(cursor);
}

inline UniqueRedisScanCursor MakeUniqueRedisScanCursor() {
  auto cursor = RedisModule_ScanCursorCreate();
  return std::unique_ptr<RedisModuleScanCursor, RedisScanCursorDeleter>(cursor);
}

struct RedisDetachedThreadSafeContextDeleter {
  void operator()(RedisModuleCtx *ctx) {
    // Contexts cannot be safely freed from a background thread since they
    // assert if IO threads are active.
    RunByMain([ctx]() { RedisModule_FreeThreadSafeContext(ctx); });
  }
};

using UniqueRedisDetachedThreadSafeContext =
    std::unique_ptr<RedisModuleCtx, RedisDetachedThreadSafeContextDeleter>;

inline UniqueRedisDetachedThreadSafeContext
MakeUniqueRedisDetachedThreadSafeContext(RedisModuleCtx *base_ctx) {
  auto ctx = RedisModule_GetDetachedThreadSafeContext(base_ctx);
  return std::unique_ptr<RedisModuleCtx, RedisDetachedThreadSafeContextDeleter>(
      ctx);
}

struct RedisClusterNodesListDeleter {
  void operator()(char **nodes_list) {
    RedisModule_FreeClusterNodesList(nodes_list);
  }
};

using UniqueRedisClusterNodesList =
    std::unique_ptr<char *, RedisClusterNodesListDeleter>;

inline UniqueRedisClusterNodesList MakeUniqueRedisClusterNodesList(
    RedisModuleCtx *ctx, size_t *num_nodes) {
  auto nodes_list = RedisModule_GetClusterNodesList(ctx, num_nodes);
  return std::unique_ptr<char *, RedisClusterNodesListDeleter>(nodes_list);
}

}  // namespace vmsdk
#endif  // VMSDK_SRC_MANAGED_POINTERS_H_
