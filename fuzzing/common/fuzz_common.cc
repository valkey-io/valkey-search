/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "fuzzing/common/fuzz_common.h"

#include <cstdarg>
#include <cstdio>

#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/utils.h"

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------
ValkeyModuleCtx g_fake_ctx;
std::shared_ptr<FuzzIndexSchema> g_index_schema;

// ---------------------------------------------------------------------------
// Stub implementations for Valkey module API function pointers.
// These replace the real Valkey server functions that would normally be
// resolved at module load time.
// ---------------------------------------------------------------------------

static const char *StubStringPtrLen(const ValkeyModuleString *str,
                                    size_t *len) {
  if (len) {
    *len = str->data.size();
  }
  return str->data.c_str();
}

static ValkeyModuleString *StubCreateString(ValkeyModuleCtx *, const char *ptr,
                                            size_t len) {
  return new ValkeyModuleString{std::string(ptr, len)};
}

static ValkeyModuleString *StubCreateStringPrintf(ValkeyModuleCtx *,
                                                  const char *fmt, ...) {
  char out[1024];
  va_list args;
  va_start(args, fmt);
  vsnprintf(out, sizeof(out), fmt, args);
  va_end(args);
  return new ValkeyModuleString{std::string(out)};
}

static void StubFreeString(ValkeyModuleCtx *, ValkeyModuleString *str) {
  if (--str->cnt == 0) {
    delete str;
  }
}

static void StubRetainString(ValkeyModuleCtx *, ValkeyModuleString *str) {
  str->cnt++;
}

static ValkeyModuleCtx *StubGetDetachedThreadSafeContext(ValkeyModuleCtx *) {
  return &g_fake_ctx;
}

static ValkeyModuleCtx *StubGetThreadSafeContext(ValkeyModuleBlockedClient *) {
  return &g_fake_ctx;
}

static void StubFreeThreadSafeContext(ValkeyModuleCtx *) {}
static int StubSelectDb(ValkeyModuleCtx *, int) { return VALKEYMODULE_OK; }
static int StubGetSelectedDb(ValkeyModuleCtx *) { return 0; }

static unsigned long long StubDbSize(ValkeyModuleCtx *) {  // NOLINT
  return 0;
}

static ValkeyModuleScanCursor *StubScanCursorCreate() {
  return new ValkeyModuleScanCursor();
}

static void StubScanCursorDestroy(ValkeyModuleScanCursor *cursor) {
  delete cursor;
}

static int StubSubscribeToKeyspaceEvents(ValkeyModuleCtx *, int,
                                         ValkeyModuleNotificationFunc) {
  return VALKEYMODULE_OK;
}

static void StubLog(ValkeyModuleCtx *, const char *, const char *, ...) {}

static int StubSubscribeToServerEvent(ValkeyModuleCtx *, ValkeyModuleEvent,
                                      ValkeyModuleEventCallback) {
  return VALKEYMODULE_OK;
}

static int StubEventLoopAddOneShot(ValkeyModuleEventLoopOneShotFunc callback,
                                   void *data) {
  if (callback) {
    callback(data);
  }
  return VALKEYMODULE_OK;
}

static void StubSetModuleOptions(ValkeyModuleCtx *, int) {}

void InitValkeyModuleStubs() {
  ValkeyModule_StringPtrLen = StubStringPtrLen;
  ValkeyModule_CreateString = StubCreateString;
  ValkeyModule_CreateStringPrintf = StubCreateStringPrintf;
  ValkeyModule_FreeString = StubFreeString;
  ValkeyModule_RetainString = StubRetainString;
  ValkeyModule_GetDetachedThreadSafeContext = StubGetDetachedThreadSafeContext;
  ValkeyModule_GetThreadSafeContext = StubGetThreadSafeContext;
  ValkeyModule_FreeThreadSafeContext = StubFreeThreadSafeContext;
  ValkeyModule_SelectDb = StubSelectDb;
  ValkeyModule_GetSelectedDb = StubGetSelectedDb;
  ValkeyModule_DbSize = StubDbSize;
  ValkeyModule_ScanCursorCreate = StubScanCursorCreate;
  ValkeyModule_ScanCursorDestroy = StubScanCursorDestroy;
  ValkeyModule_SubscribeToKeyspaceEvents = StubSubscribeToKeyspaceEvents;
  ValkeyModule_Log = StubLog;
  ValkeyModule_SubscribeToServerEvent = StubSubscribeToServerEvent;
  ValkeyModule_EventLoopAddOneShot = StubEventLoopAddOneShot;
  ValkeyModule_SetModuleOptions = StubSetModuleOptions;
}

// ---------------------------------------------------------------------------
// FuzzIndexSchema implementation
// ---------------------------------------------------------------------------

std::shared_ptr<FuzzIndexSchema> FuzzIndexSchema::Create(
    ValkeyModuleCtx *ctx, absl::string_view key,
    const std::vector<absl::string_view> &subscribed_key_prefixes,
    std::unique_ptr<valkey_search::AttributeDataType> attribute_data_type) {
  valkey_search::data_model::IndexSchema proto;
  proto.set_name(std::string(key));
  proto.mutable_subscribed_key_prefixes()->Add(
      subscribed_key_prefixes.begin(), subscribed_key_prefixes.end());
  proto.set_language(valkey_search::data_model::LANGUAGE_ENGLISH);
  proto.set_punctuation(",.<>{}[]\"':;!@#$%^&*()-+=~/\\|");
  proto.set_with_offsets(true);

  auto schema = std::shared_ptr<FuzzIndexSchema>(
      new FuzzIndexSchema(ctx, proto, std::move(attribute_data_type)));

  auto status = schema->Init(ctx);
  if (!status.ok()) {
    return nullptr;
  }
  return schema;
}

FuzzIndexSchema::FuzzIndexSchema(
    ValkeyModuleCtx *ctx,
    const valkey_search::data_model::IndexSchema &index_schema_proto,
    std::unique_ptr<valkey_search::AttributeDataType> attribute_data_type)
    : IndexSchema(ctx, index_schema_proto, std::move(attribute_data_type),
                  /*mutations_thread_pool=*/nullptr, /*reload=*/false) {}

// ---------------------------------------------------------------------------
// Full environment initialization
// ---------------------------------------------------------------------------

void InitFuzzEnvironment() {
  static bool initialized = false;
  if (initialized) {
    return;
  }
  initialized = true;

  InitValkeyModuleStubs();
  vmsdk::TrackCurrentAsMainThread();

  // Initialize required singletons
  valkey_search::KeyspaceEventManager::InitInstance(
      std::make_unique<valkey_search::KeyspaceEventManager>());
  valkey_search::ValkeySearch::InitInstance(
      std::make_unique<valkey_search::ValkeySearch>());

  // Create a properly initialized FuzzIndexSchema
  std::vector<absl::string_view> key_prefixes = {"prefix:"};
  g_index_schema = FuzzIndexSchema::Create(
      &g_fake_ctx, "fuzz_index", key_prefixes,
      std::make_unique<valkey_search::HashAttributeDataType>());
}
