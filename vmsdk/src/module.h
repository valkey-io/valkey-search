/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
  int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,  \
                          int argc) {                                       \
    vmsdk::TrackCurrentAsMainThread();                                      \
    if (auto status = vmsdk::module::OnLoad(ctx, argv, argc, options);      \
        status != VALKEYMODULE_OK) {                                        \
      return status;                                                        \
    }                                                                       \
                                                                            \
    if (options.on_load.has_value()) {                                      \
      return vmsdk::module::OnLoadDone(                                     \
          options.on_load.value()(ctx, argv, argc, options), ctx, options); \
    }                                                                       \
    return vmsdk::module::OnLoadDone(absl::OkStatus(), ctx, options);       \
  }                                                                         \
  int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {                         \
    if (options.on_unload.has_value()) {                                    \
      options.on_unload.value()(ctx, options);                              \
    }                                                                       \
    return VALKEYMODULE_OK;                                                 \
  }                                                                         \
  }                                                                         \
  }

namespace vmsdk {
namespace module {

constexpr absl::string_view kWriteFlag{"write"};
constexpr absl::string_view kReadOnlyFlag{"readonly"};
constexpr absl::string_view kFastFlag{"fast"};
constexpr absl::string_view kAdminFlag{"admin"};
constexpr absl::string_view kDenyOOMFlag{"deny-oom"};

struct CommandOptions {
  absl::string_view cmd_name;
  std::list<absl::string_view> permissions;
  std::list<absl::string_view> flags;
  ValkeyModuleCmdFunc cmd_func{nullptr};
  // By default - assume no keys.
  int first_key{0};
  int last_key{0};
  int key_step{0};
};

struct Options {
  std::string name;
  std::list<absl::string_view> acl_categories;
  int version;
  ValkeyModuleInfoFunc info{nullptr};
  std::list<CommandOptions> commands;
  using OnLoad = std::optional<absl::AnyInvocable<absl::Status(
      ValkeyModuleCtx *, ValkeyModuleString **, int, const Options &)>>;

  using OnUnload = std::optional<
      absl::AnyInvocable<void(ValkeyModuleCtx *, const Options &)>>;
  OnLoad on_load;
  OnUnload on_unload;
};

int OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
           const Options &options);
int OnLoadDone(absl::Status status, ValkeyModuleCtx *ctx,
               const Options &options);
absl::Status RegisterInfo(ValkeyModuleCtx *ctx, ValkeyModuleInfoFunc info);

}  // namespace module
using ModuleCommandFunc = absl::Status (*)(ValkeyModuleCtx *,
                                           ValkeyModuleString **, int);
template <ModuleCommandFunc func>
int CreateCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
  auto status = func(ctx, argv, argc);
  if (!status.ok()) {
    return ValkeyModule_ReplyWithError(ctx, status.message().data());
  }
  return VALKEYMODULE_OK;
}
bool IsModuleLoaded(ValkeyModuleCtx *ctx, const std::string &name);
}  // namespace vmsdk

#endif  // VMSDK_SRC_MODULE_H_
