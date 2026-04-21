/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/server_events.h"

#include <cstdint>

#include "src/coordinator/metadata_manager.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/utils.h"
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

void OnShutdownCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                        uint64_t subevent, void *data) {
  // Mark the module as shutting down so that RunByMain() stops
  // scheduling new tasks.
  vmsdk::MarkAsShuttingDown();
  // Clear all PausePoints so any waiting worker threads wake up and exit
  // their spin loops, then join all thread pools so every in-flight task
  // completes before we tear down index schemas.  This ordering matters:
  //   1. ClearAllPausePoints  – unblocks workers stuck in PausePoint().
  //   2. JoinAllThreadPools   – drains task queues and waits for every
  //      worker thread to exit.
  //   3. OnShutdownCallback   – removes all index schemas on the main
  //      thread.
  vmsdk::debug::ClearAllPausePoints();
  ValkeySearch::Instance().JoinAllThreadPools();
  SchemaManager::Instance().OnShutdownCallback(ctx, eid, subevent, data);
  ValkeySearch::Instance().OnShutdownCallback();
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
  ValkeyModule_SubscribeToServerEvent(ctx, ValkeyModuleEvent_Shutdown,
                                      &OnShutdownCallback);
}

}  // namespace valkey_search::server_events
