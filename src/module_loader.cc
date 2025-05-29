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

#include <memory>

#include "src/commands/commands.h"
#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#define MODULE_VERSION 10000
/* The release stage is used in order to provide release status information.
 * In unstable branch the status is always "dev".
 * During release process the status will be set to rc1,rc2...rcN.
 * When the version is released the status will be "ga". */
#define MODULE_RELEASE_STAGE "rc1"

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

vmsdk::module::Options options = {
    .name = "search",
    .acl_categories = ACLPermissionFormatter({
        valkey_search::kSearchCategory,
    }),
    .version = MODULE_VERSION,
    .info = valkey_search::ModuleInfo,
    .commands =
        {
            {
                .cmd_name = valkey_search::kCreateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kCreateCmdPermissions),
                .flags = {vmsdk::module::kDenyOOMFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTCreateCmd>,
            },
            {
                .cmd_name = valkey_search::kDropIndexCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kDropIndexCmdPermissions),
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTDropIndexCmd>,
            },
            {
                .cmd_name = valkey_search::kInfoCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kInfoCmdPermissions),
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTInfoCmd>,
            },
            {
                .cmd_name = valkey_search::kListCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kListCmdPermissions),
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTListCmd>,
            },
            {
                .cmd_name = valkey_search::kSearchCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kDenyOOMFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTSearchCmd>,
            },
            {
                .cmd_name = valkey_search::kAggregateCommand,
                .permissions = "readonly",
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTAggregateCmd>,
            },
        }  // namespace
    ,
    .on_load =
        [](RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::KeyspaceEventManager::InitInstance(
              std::make_unique<valkey_search::KeyspaceEventManager>());
          valkey_search::ValkeySearch::InitInstance(
              std::make_unique<valkey_search::ValkeySearch>());

          return valkey_search::ValkeySearch::Instance().OnLoad(ctx, argv,
                                                                argc);
        },
    .on_unload =
        [](RedisModuleCtx *ctx,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::ValkeySearch::Instance().OnUnload(ctx);
        },
};
VALKEY_MODULE(options);
}  // namespace
