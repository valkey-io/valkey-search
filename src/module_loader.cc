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
#include "src/version.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

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

static const std::string kModuleVersion = std::format("{}.{}.{}.{}",
                            MODULE_VERSION / 10000, 
                            (MODULE_VERSION / 100) % 100, 
                            MODULE_VERSION % 100, 
                            MODULE_RELEASE_STAGE);

vmsdk::module::Options options = {
    .name = "search",
    .acl_categories = ACLPermissionFormatter({
        valkey_search::kSearchCategory,
    }),
    .version = kModuleVersion,
    .minimum_valkey_server_version = kMinimumServerVersion,
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
                .command_info = vmsdk::command_info::Info {
                    .version = kModuleVersion,
                    .summary = "Create an index",
                    .complexity = "O(1)",
                    .since = "1.0.0",
                    .arity = -2,
                    .args = {  
                        {
                            .name = "index",
                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                            .display_text = "index-name",
                        },
                        {
                            .name = "datatype",
                            .type = VALKEYMODULE_ARG_TYPE_ONEOF,
                            .token = "ON",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                {
                                    .name = "HashKeyword",
                                    .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                                    .token = "HASH",
                                },
                                {
                                    .name = "JsonKeyword",
                                    .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                                    .token = "JSON",
                                },
                            },
                        },
                        {
                            .name = "PrefixClause",
                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                            .summary = "Provides index prefixes",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                {
                                    .name = "count",
                                    .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                                    .token = "PREFIX",
                                    .display_text = "count",
                                },
                                {
                                    .name = "ReturnAttribute",
                                    .type = VALKEYMODULE_ARG_TYPE_STRING,
                                    .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
                                    .display_text = "name",
                                },
                            }
                        },
                        {
                            .name = "SchemaKeyword",
                            .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                            .token = "SCHEMA",
                        },
                        {
                            .name = "Schemas",
                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                            .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                {
                                    .name = "attribute",
                                    .type = VALKEYMODULE_ARG_TYPE_STRING,
                                    .display_text = "hash-member-or-JSON-path",
                                },
                                {
                                    .name = "alias",
                                    .type = VALKEYMODULE_ARG_TYPE_STRING,
                                    .token = "AS",
                                    .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                                    .display_text = "alias",
                                },
                                {
                                    .name = "attributetype",
                                    .type = VALKEYMODULE_ARG_TYPE_ONEOF,
                                    .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                        {
                                            .name = "numeric",
                                            .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                                            .token = "NUMERIC",
                                        },
                                        {
                                            .name = "tag",
                                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                                            .token = "TAG",
                                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                                {
                                                    .name = "separator",
                                                    .type = VALKEYMODULE_ARG_TYPE_STRING,
                                                    .token = "SEPARATOR",
                                                    .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                                                    .display_text = "separator",
                                                },
                                                {
                                                    .name = "casesensitive",
                                                    .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                                                    .token = "CASESENSITIVE",
                                                    .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                                                },
                                            },
                                        },
                                        {
                                            .name = "vector",
                                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                                            .token = "VECTOR",
                                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                                {
                                                    .name = "type",
                                                    .type = VALKEYMODULE_ARG_TYPE_ONEOF,
                                                    .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                                        {
                                                            .name = "flat",
                                                            .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                                                            .token = "FLAT",
                                                        },
                                                        {
                                                            .name = "hnsw",
                                                            .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                                                            .token = "HNSW",
                                                        },
                                                    },
                                                },
                                                {
                                                    .name = "count",
                                                    .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                                                    .display_text = "count",
                                                },
                                                {
                                                    .name = "arg",
                                                    .type = VALKEYMODULE_ARG_TYPE_STRING,
                                                    .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
                                                    .display_text = "arg",
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
            {
                .cmd_name = valkey_search::kDropIndexCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kDropIndexCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kFastFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTDropIndexCmd>,
                .command_info = vmsdk::command_info::Info {
                    .version = kModuleVersion,
                    .summary = "Delete an index",
                    .complexity = "O(1)",
                    .since = "1.0.0",
                    .arity = 2,
                    .args = {  
                        {
                            .name = "index",
                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                            .display_text = "index-name",
                        }
                    }
                },
            },
            {
                .cmd_name = valkey_search::kInfoCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kInfoCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kFastFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTInfoCmd>,
                .command_info = vmsdk::command_info::Info {
                    .version = kModuleVersion,
                    .summary = "Return information about current state of an index",
                    .complexity = "O(1)",
                    .since = "1.0.0",
                    .arity = -2,
                    .args = { 
                        {
                            .name = "index",
                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                            .display_text = "index-name",
                        }
                    }
                },
            },
            {
                .cmd_name = valkey_search::kListCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kListCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kAdminFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTListCmd>,
                .command_info = vmsdk::command_info::Info {
                    .version = kModuleVersion,
                    .summary = "Return current index names",
                    .complexity = "O(1)",
                    .since = "1.0.0",
                    .arity = 1,
                },
            },
            {
                .cmd_name = valkey_search::kSearchCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTSearchCmd>,
                .command_info = vmsdk::command_info::Info {
                    .version = kModuleVersion,
                    .summary = "Searches an index",
                    .complexity = "O(log N)",
                    .since = "1.0.0",
                    .arity = -2,
                    .args = {  
                        {
                            .name = "index",
                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                            .summary = "Name of the index",
                            .display_text = "index-name",
                        },
                        {
                            .name = "query",
                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                            .summary = "Specifies keys to be searching for",
                            .display_text = "query-string",
                        },
                        {
                            .name = "nocontent",
                            .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
                            .token = "NOCONTENT",
                            .summary = "When present, only the resulting key names are returned, no key values are included",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .display_text = "NOCONTENT",
                        },
                        {
                            .name = "timeout",
                            .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                            .token = "TIMEOUT",
                            .summary = "Overrides default command timeout",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                        },
                        {
                            .name = "params",
                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                            .summary = "Provides parameter substitution values",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                {
                                    .name = "count",
                                    .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                                    .token = "PARAMS",
                                    .display_text = "count"
                                },
                                {
                                    .name = "params",
                                    .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                                    .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
                                    .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                        {
                                            .name = "name",
                                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                                            .display_text = "name",
                                        },
                                        {
                                            .name = "value",
                                            .type = VALKEYMODULE_ARG_TYPE_STRING,
                                            .display_text = "value"
                                        },
                                    },
                                },
                            }
                        },
                        {
                            .name = "returns",
                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                            .token = "RETURNS",
                            .summary = "Provides parameter values",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .subargs = std::vector<vmsdk::command_info::ArgDescription> {
                                {
                                    .name = "count",
                                    .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                                    .display_text = "count",
                                },
                                {
                                    .name = "attribute",
                                    .type = VALKEYMODULE_ARG_TYPE_STRING,
                                    .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
                                    .display_text = "name",
                                },
                            }
                        },
                        {
                            .name = "limitclause",
                            .type = VALKEYMODULE_ARG_TYPE_BLOCK,
                            .token = "LIMIT",
                            .summary = "Limits number of results",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .subargs = {std::vector<vmsdk::command_info::ArgDescription>
                                {
                                    {
                                        .name = "offset",
                                        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                                        .display_text = "offset"
                                    },
                                    {
                                        .name = "count",
                                        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                                        .display_text = "count"
                                    },
                                }
                            },
                        },
                        {
                            .name = "dialect",
                            .type = VALKEYMODULE_ARG_TYPE_INTEGER,
                            .token = "DIALECT",
                            .summary = "Set language dialect",
                            .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
                            .display_text = "dialect",
                        },
                    }
                },
            },
            {
                .cmd_name = valkey_search::kDebugCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kDebugCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kAdminFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTDebugCmd>,
                .command_info = vmsdk::command_info::Info {
                    .version = kModuleVersion,
                    .summary = "non-production inquiry commands",
                    .complexity = "variable",
                    .since = "1.0.0",
                    .arity = -2,
                },
            },
            {
                .cmd_name = valkey_search::kInternalUpdateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kInternalUpdateCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kAdminFlag,
                          vmsdk::module::kFastFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTInternalUpdateCmd>,
            },
            {
                .cmd_name = valkey_search::kAggregateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag},
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
