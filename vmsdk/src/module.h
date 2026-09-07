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
#include "vmsdk/src/deferred_init.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/utils.h"  // IWYU pragma: keep
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Defines the module entry points.
//
// `module_name` and `module_version` must be constant-initialized (a constexpr
// name array and version), NOT members of `options`. They are read before the
// module's static initializers have run, at which point `options` -- which
// holds std::list and absl::AnyInvocable members and therefore requires dynamic
// initialization -- is still entirely zero. GCC happens to emit the
// constant-computable members of such an object statically, but Clang does not;
// reading options.name there yields nullptr. They must match the corresponding
// `options` fields; vmsdk::module::OnLoad checks this once initialization is
// complete.
//
// ValkeyModule_Init has to come first because it is what establishes
// ValkeyModule_Alloc/Free, and RunDeferredStaticInitializers must allocate
// through them. See vmsdk/src/deferred_init.cc.
#define VALKEY_MODULE(options, module_name, module_version)                 \
  namespace {                                                               \
  extern "C" {                                                              \
  int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,  \
                          int argc) {                                       \
    if (ValkeyModule_Init(ctx, module_name, module_version,                 \
                          VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {     \
      return VALKEYMODULE_ERR;                                              \
    }                                                                       \
    vmsdk::UseValkeyAlloc();                                                \
    vmsdk::RunDeferredStaticInitializers();                                 \
    /* Dynamically-initialized globals are usable from here on. */          \
    if (!vmsdk::verifyLoadedOnlyOnce()) {                                   \
      VMSDK_LOG(NOTICE, ctx) << "Module cannot be loaded more than once";   \
      return VALKEYMODULE_ERR;                                              \
    }                                                                       \
    vmsdk::TrackCurrentAsMainThread();                                      \
    if (auto status = vmsdk::module::OnLoad(ctx, argv, argc, options,       \
                                            module_name, module_version);   \
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
constexpr absl::string_view kDenyScriptFlag{"deny-script"};
constexpr absl::string_view kLoadingFlag{"allow-loading"};
constexpr absl::string_view kStaleFlag{"allow-stale"};

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
  // Points at the same constexpr string passed to VALKEY_MODULE.
  const char *name{nullptr};
  std::list<absl::string_view> acl_categories;
  vmsdk::ValkeyVersion version;
  vmsdk::ValkeyVersion minimum_valkey_server_version;
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
           const Options &options, const char *module_name,
           vmsdk::ValkeyVersion module_version);
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
// Used only for testing
void SetModuleLoaded(const std::string &name, bool remove = false);
}  // namespace vmsdk

#endif  // VMSDK_SRC_MODULE_H_
