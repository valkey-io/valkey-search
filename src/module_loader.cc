/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>

#include "src/commands/commands.h"
#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#define MODULE_VERSION 10000
/* The release stage is used in order to provide release status information.
 * In unstable branch the status is always "dev".
 * During release process the status will be set to rc1,rc2...rcN.
 * When the version is released the status will be "ga". */
#define MODULE_RELEASE_STAGE "rc1"

//
// Set the minimum acceptable server version
//
#define MINIMUM_VALKEY_VERSION vmsdk::MakeValkeyVersion(8, 1, 1)

namespace {

// Strip the '@' prefix from command categories (e.g., @read)
// to format them for Valkey Search's prefix ACL rules (e.g., read).
inline std::list<absl::string_view> ACLPermissionFormatter(
    const absl::flat_hash_set<absl::string_view> &cmd_permissions) {
  std::list<absl::string_view> permissions;
  for (auto permission : cmd_permissions) {
    CHECK(permission[0] == '@');
    permissions.push_back(permission.substr(1));
  }
  return permissions;
}
}  // namespace

vmsdk::module::Options options = {
    .name = "search",
    .acl_categories = ACLPermissionFormatter({
        valkey_search::kSearchCategory,
    }),
    .version = MODULE_VERSION,
    .minimum_valkey_version = MINIMUM_VALKEY_VERSION,
    .info = valkey_search::ModuleInfo,
    .commands =
        {
            {
                .cmd_name = valkey_search::kCreateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kCreateCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kFastFlag,
                          vmsdk::module::kDenyOOMFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTCreateCmd>,
                .command_info = &valkey_search::ftCreateInfo,
            },
            {
                .cmd_name = valkey_search::kDropIndexCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kDropIndexCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kFastFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTDropIndexCmd>,
            },
            {
                .cmd_name = valkey_search::kInfoCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kInfoCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kFastFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTInfoCmd>,
            },
            {
                .cmd_name = valkey_search::kListCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kListCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kAdminFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTListCmd>,
            },
            {
                .cmd_name = valkey_search::kSearchCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kDenyOOMFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTSearchCmd>,
            },
            {
                .cmd_name = valkey_search::kDebugCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kDebugCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kAdminFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTDebugCmd>,
            },
            {
                .cmd_name = valkey_search::kAggregateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kDenyOOMFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTAggregateCmd>,
            },
        },
    .on_load =
        [](ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::KeyspaceEventManager::InitInstance(
              std::make_unique<valkey_search::KeyspaceEventManager>());
          valkey_search::ValkeySearch::InitInstance(
              std::make_unique<valkey_search::ValkeySearch>());

          return valkey_search::ValkeySearch::Instance().OnLoad(ctx, argv,
                                                                argc);
        },
    .on_unload =
        [](ValkeyModuleCtx *ctx,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::ValkeySearch::Instance().OnUnload(ctx);
        },
};
VALKEY_MODULE(options);
