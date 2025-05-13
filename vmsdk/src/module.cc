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

#include "vmsdk/src/module.h"

#include <list>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace module {

absl::Status RegisterInfo(RedisModuleCtx *ctx, RedisModuleInfoFunc info) {
  if (info == nullptr) {
    return absl::OkStatus();
  }
  if (RedisModule_RegisterInfoFunc(ctx, info) == REDISMODULE_ERR) {
    return absl::InternalError("Failed to register info");
  }
  return absl::OkStatus();
}

absl::Status RegisterCommands(RedisModuleCtx *ctx,
                              const std::list<CommandOptions> &commands) {
  for (auto &command : commands) {
    if (RedisModule_CreateCommand(ctx, command.cmd_name.data(),
                                  command.cmd_func, command.permissions.data(),
                                  command.first_key, command.last_key,
                                  command.key_step) == REDISMODULE_ERR) {
      return absl::InternalError(
          absl::StrCat("Failed to create command: ", command.cmd_name.data()));
    }
  }
  return absl::OkStatus();
}

int OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
           const Options &options) {
  if (RedisModule_Init(ctx, options.name.c_str(), options.version,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Failed to init module");
    return REDISMODULE_ERR;
  }
  auto status = vmsdk::InitLogging(ctx);
  if (!status.ok()) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING,
                    "Failed to init logging, %s", status.message().data());
    return REDISMODULE_ERR;
  }
  if (auto status = RegisterCommands(ctx, options.commands); !status.ok()) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "%s",
                    status.message().data());
    return REDISMODULE_ERR;
  }
  // Initialize counters for request count metric
  if (auto status = RegisterInfo(ctx, options.info); !status.ok()) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "%s",
                    status.message().data());
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int OnLoadDone(absl::Status status, RedisModuleCtx *ctx,
               const Options &options) {
  if (status.ok()) {
    VMSDK_LOG(NOTICE, ctx) << options.name
                           << " module was successfully loaded!";
    vmsdk::UseValkeyAlloc();
    return REDISMODULE_OK;
  }
  VMSDK_LOG(WARNING, ctx) << status.message().data();
  return REDISMODULE_ERR;
}
}  // namespace module

bool IsModuleLoaded(RedisModuleCtx *ctx, const std::string &name) {
  static absl::flat_hash_set<std::string> loaded_modules;
  if (loaded_modules.contains(name)) {
    return true;
  }

  auto reply =
      UniquePtrRedisCallReply(RedisModule_Call(ctx, "MODULE", "c", "LIST"));
  if (!reply ||
      RedisModule_CallReplyType(reply.get()) != REDISMODULE_REPLY_ARRAY) {
    return false;
  }

  size_t num_modules = RedisModule_CallReplyLength(reply.get());
  for (size_t i = 0; i < num_modules; ++i) {
    RedisModuleCallReply *mod_info =
        RedisModule_CallReplyArrayElement(reply.get(), i);
    if (!mod_info ||
        RedisModule_CallReplyType(mod_info) != REDISMODULE_REPLY_ARRAY) {
      continue;
    }

    size_t len = RedisModule_CallReplyLength(mod_info);

    for (size_t j = 0; j + 1 < len; j += 2) {
      RedisModuleCallReply *key =
          RedisModule_CallReplyArrayElement(mod_info, j);
      RedisModuleCallReply *val =
          RedisModule_CallReplyArrayElement(mod_info, j + 1);

      size_t key_len, val_len;
      const char *key_str = RedisModule_CallReplyStringPtr(key, &key_len);
      const char *val_str = RedisModule_CallReplyStringPtr(val, &val_len);
      absl::string_view module_key{key_str, key_len};
      absl::string_view module_value{val_str, val_len};

      if (module_key == "name" && module_value == name) {
        loaded_modules.insert(name);
        return true;
      }
    }
  }
  return false;
}
}  // namespace vmsdk
