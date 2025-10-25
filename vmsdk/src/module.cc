/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/module.h"

#include <list>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace module {

absl::Status RegisterInfo(ValkeyModuleCtx *ctx, ValkeyModuleInfoFunc info) {
  if (info == nullptr) {
    return absl::OkStatus();
  }
  if (ValkeyModule_RegisterInfoFunc(ctx, info) == VALKEYMODULE_ERR) {
    return absl::InternalError("Failed to register info");
  }
  return absl::OkStatus();
}

absl::Status AddACLCategories(
    ValkeyModuleCtx *ctx, const std::list<absl::string_view> &acl_categories) {
  for (auto &category : acl_categories) {
    if (ValkeyModule_AddACLCategory(ctx, category.data()) == VALKEYMODULE_ERR) {
      return absl::InternalError(absl::StrCat(
          "Failed to create a command ACL category: ", category.data()));
    }
  }
  return absl::OkStatus();
}

absl::Status RegisterCommands(ValkeyModuleCtx *ctx,
                              const std::list<CommandOptions> &commands) {
  for (auto &command : commands) {
    auto flags = absl::StrJoin(command.flags, " ");
    if (ValkeyModule_CreateCommand(ctx, command.cmd_name.data(),
                                   command.cmd_func, flags.c_str(),
                                   command.first_key, command.last_key,
                                   command.key_step) == VALKEYMODULE_ERR) {
      return absl::InternalError(
          absl::StrCat("Failed to create command: ", command.cmd_name.data()));
    }
    ValkeyModuleCommand *cmd =
        ValkeyModule_GetCommand(ctx, command.cmd_name.data());
    auto permissions = absl::StrJoin(command.permissions, " ");
    if (ValkeyModule_SetCommandACLCategories(cmd, permissions.c_str()) ==
        VALKEYMODULE_ERR) {
      return absl::InternalError(
          absl::StrCat("Failed to set ACL categories `", permissions,
                       "` for the command: ", command.cmd_name.data()));
    }
  }
  return absl::OkStatus();
}

int OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
           const Options &options) {
  if (ValkeyModule_Init(ctx, options.name.c_str(), options.version,
                        VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_WARNING,
                     "Failed to init module");
    return VALKEYMODULE_ERR;
  }
  auto status = vmsdk::InitLogging(ctx);
  if (!status.ok()) {
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_WARNING,
                     "Failed to init logging, %s", status.message().data());
    return VALKEYMODULE_ERR;
  }
  if (ValkeyModule_GetServerVersion == nullptr) {
    VMSDK_LOG(WARNING, ctx)
        << "ValkeyModule_GetServerVersion function is not available";
    return VALKEYMODULE_ERR;
  }
  auto server_version = ValkeyModule_GetServerVersion();
  if (server_version < options.minimum_valkey_version) {
    VMSDK_LOG(WARNING, ctx)
        << "Minimum required server version is "
        << vmsdk::DisplayValkeyVersion(options.minimum_valkey_version)
        << ", Current version is "
        << vmsdk::DisplayValkeyVersion(server_version);
    return VALKEYMODULE_ERR;
  }
  if (auto status = AddACLCategories(ctx, options.acl_categories);
      !status.ok()) {
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_WARNING, "%s",
                     status.message().data());
    return VALKEYMODULE_ERR;
  }
  if (auto status = RegisterCommands(ctx, options.commands); !status.ok()) {
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_WARNING, "%s",
                     status.message().data());
    return VALKEYMODULE_ERR;
  }
  // Initialize counters for request count metric
  if (auto status = RegisterInfo(ctx, options.info); !status.ok()) {
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_WARNING, "%s",
                     status.message().data());
    return VALKEYMODULE_ERR;
  }
  return VALKEYMODULE_OK;
}

int OnLoadDone(absl::Status status, ValkeyModuleCtx *ctx,
               const Options &options) {
  if (status.ok()) {
    VMSDK_LOG(NOTICE, ctx) << options.name
                           << " module was successfully loaded!";
    vmsdk::UseValkeyAlloc();
    return VALKEYMODULE_OK;
  }
  VMSDK_LOG(WARNING, ctx) << status.message().data();
  return VALKEYMODULE_ERR;
}
}  // namespace module
static absl::flat_hash_set<std::string> loaded_modules;
void SetModuleLoaded(const std::string &name, bool remove) {
  if (remove) {
    loaded_modules.erase(name);
    return;
  }
  loaded_modules.insert(name);
}

bool IsModuleLoaded(ValkeyModuleCtx *ctx, const std::string &name) {
  if (loaded_modules.contains(name)) {
    return true;
  }

  auto reply =
      UniquePtrValkeyCallReply(ValkeyModule_Call(ctx, "MODULE", "c", "LIST"));
  if (!reply ||
      ValkeyModule_CallReplyType(reply.get()) != VALKEYMODULE_REPLY_ARRAY) {
    return false;
  }

  size_t num_modules = ValkeyModule_CallReplyLength(reply.get());
  for (size_t i = 0; i < num_modules; ++i) {
    ValkeyModuleCallReply *mod_info =
        ValkeyModule_CallReplyArrayElement(reply.get(), i);
    if (!mod_info ||
        ValkeyModule_CallReplyType(mod_info) != VALKEYMODULE_REPLY_ARRAY) {
      continue;
    }

    size_t len = ValkeyModule_CallReplyLength(mod_info);
    for (size_t j = 0; j + 1 < len; j += 2) {
      ValkeyModuleCallReply *key =
          ValkeyModule_CallReplyArrayElement(mod_info, j);
      ValkeyModuleCallReply *val =
          ValkeyModule_CallReplyArrayElement(mod_info, j + 1);

      size_t key_len, val_len;
      const char *key_str = ValkeyModule_CallReplyStringPtr(key, &key_len);
      const char *val_str = ValkeyModule_CallReplyStringPtr(val, &val_len);
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
