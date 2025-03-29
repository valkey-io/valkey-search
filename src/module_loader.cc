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

#include <memory>

#include "src/commands/commands.h"
#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

/* In development branch the module version is always "999999". */
#define MODULE_VERSION 999999
/* The release stage is used in order to provide release status information.
 * In development branch the status is always "dev".
 * During release process the status will be set to rc1,rc2...rcN.
 * When the version is released the status will be "ga". */
#define MODULE_RELEASE_STAGE "dev"

namespace {

vmsdk::module::Options options = {
    .name = "search",
    .version = MODULE_VERSION,
    .info = valkey_search::ModuleInfo,
    .commands =
        {
            {
                .cmd_name = valkey_search::kCreateCommand,
                .permissions = "write deny-oom",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTCreateCmd>,
            },
            {
                .cmd_name = valkey_search::kDropIndexCommand,
                .permissions = "write",
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTDropIndexCmd>,
            },
            {
                .cmd_name = valkey_search::kInfoCommand,
                .permissions = "readonly",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTInfoCmd>,
            },
            {
                .cmd_name = valkey_search::kListCommand,
                .permissions = "readonly",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTListCmd>,
            },
            {
                .cmd_name = valkey_search::kSearchCommand,
                .permissions = "readonly",
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTSearchCmd>,
            },
        },
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
