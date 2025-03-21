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

#ifndef VMSDK_SRC_MODULE_H_
#define VMSDK_SRC_MODULE_H_

#include <list>
#include <optional>
#include <string>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/utils.h"  // IWYU pragma: keep
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#define VALKEY_MODULE(options)                                              \
  namespace {                                                               \
  extern "C" {                                                              \
  int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv,     \
                         int argc) {                                        \
    vmsdk::TrackCurrentAsMainThread();                                      \
    if (auto status = vmsdk::module::OnLoad(ctx, argv, argc, options);      \
        status != REDISMODULE_OK) {                                         \
      return status;                                                        \
    }                                                                       \
                                                                            \
    if (options.on_load.has_value()) {                                      \
      return vmsdk::module::OnLoadDone(                                     \
          options.on_load.value()(ctx, argv, argc, options), ctx, options); \
    }                                                                       \
    return vmsdk::module::OnLoadDone(absl::OkStatus(), ctx, options);       \
  }                                                                         \
  int RedisModule_OnUnload(RedisModuleCtx *ctx) {                           \
    if (options.on_unload.has_value()) {                                    \
      options.on_unload.value()(ctx, options);                              \
    }                                                                       \
    return REDISMODULE_OK;                                                  \
  }                                                                         \
  }                                                                         \
  }

namespace vmsdk {
namespace module {

struct CommandOptions {
  absl::string_view cmd_name;
  std::string permissions;
  RedisModuleCmdFunc cmd_func{nullptr};
  // By default - assume no keys.
  int first_key{0};
  int last_key{0};
  int key_step{0};
};

struct Options {
  std::string name;
  RedisModuleInfoFunc info{nullptr};
  std::list<CommandOptions> commands;
  using OnLoad = std::optional<absl::AnyInvocable<absl::Status(
      RedisModuleCtx *, RedisModuleString **, int, const Options &)>>;

  using OnUnload = std::optional<
      absl::AnyInvocable<void(RedisModuleCtx *, const Options &)>>;
  OnLoad on_load;
  OnUnload on_unload;
};

int OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
           const Options &options);
int OnLoadDone(absl::Status status, RedisModuleCtx *ctx,
               const Options &options);
absl::Status RegisterInfo(RedisModuleCtx *ctx, RedisModuleInfoFunc info);

}  // namespace module
using ModuleCommandFunc = absl::Status (*)(RedisModuleCtx *,
                                           RedisModuleString **, int);
template <ModuleCommandFunc func>
int CreateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  auto status = func(ctx, argv, argc);
  if (!status.ok()) {
    return RedisModule_ReplyWithError(ctx, status.message().data());
  }
  return REDISMODULE_OK;
}
}  // namespace vmsdk

#endif  // VMSDK_SRC_MODULE_H_
