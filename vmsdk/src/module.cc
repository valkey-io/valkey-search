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

#include "vmsdk/src/module.h"

#include <list>
#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace module {

int LogOnLoad(absl::Status status, RedisModuleCtx *ctx,
              const Options &options) {
  if (status.ok()) {
    RedisModule_Log(
        ctx, REDISMODULE_LOGLEVEL_NOTICE, "%s",
        absl::StrCat(options.name, " module was successfully loaded!").c_str());
    return REDISMODULE_OK;
  }
  RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "%s",
                  status.message().data());
  return REDISMODULE_ERR;
}

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
  if (RedisModule_Init(ctx, options.name.c_str(), 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Failed to init module");
    return REDISMODULE_ERR;
  }
  UseValkeyAlloc();
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
}  // namespace module
}  // namespace vmsdk
