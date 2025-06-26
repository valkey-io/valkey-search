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

#include "src/server_events.h"

#include <cstdint>

#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::server_events {

void OnForkChildCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                         uint64_t subevent, void *data) {
  ValkeySearch::Instance().OnForkChildCallback(ctx, eid, subevent, data);
}

void OnFlushDBCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                       uint64_t subevent, void *data) {
  SchemaManager::Instance().OnFlushDBCallback(ctx, eid, subevent, data);
}

void OnLoadingCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                       uint64_t subevent, void *data) {
  SchemaManager::Instance().OnLoadingCallback(ctx, eid, subevent, data);
  if (coordinator::MetadataManager::IsInitialized()) {
    coordinator::MetadataManager::Instance().OnLoadingCallback(ctx, eid,
                                                               subevent, data);
  }
}

void OnSwapDBCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                      uint64_t subevent, void *data) {
  SchemaManager::Instance().OnSwapDB((ValkeyModuleSwapDbInfo *)data);
}

void OnServerCronCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                          uint64_t subevent, void *data) {
  ValkeySearch::Instance().OnServerCronCallback(ctx, eid, subevent, data);
  SchemaManager::Instance().OnServerCronCallback(ctx, eid, subevent, data);
  if (coordinator::MetadataManager::IsInitialized()) {
    coordinator::MetadataManager::Instance().OnServerCronCallback(
        ctx, eid, subevent, data);
  }
}

void AtForkPrepare() { ValkeySearch::Instance().AtForkPrepare(); }

void AfterForkParent() { ValkeySearch::Instance().AfterForkParent(); }

void SubscribeToServerEvents() {
  // Note: all the events are subscribed to here. The engine only supports
  // a single subscriber per each event, so we centralize it to this file to
  // prevent unexpected behavior. If multiple events are subscribed to in
  // different places, the engine will only call the callback for the first
  // subscriber and the other will silently be ignored.
  ValkeyModuleCtx *ctx = ValkeySearch::Instance().GetBackgroundCtx();
  ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_CronLoop,
                                      &OnServerCronCallback);
  pthread_atfork(AtForkPrepare, AfterForkParent, nullptr);
  ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_ForkChild,
                                      &OnForkChildCallback);
  ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_Loading,
                                      &OnLoadingCallback);
  ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_SwapDB,
                                      &OnSwapDBCallback);
  ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_FlushDB,
                                      &OnFlushDBCallback);
}

}  // namespace valkey_search::server_events
