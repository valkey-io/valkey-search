/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef THIRD_PARTY_SRC_SERVER_EVENT_SUBSCRIPTION_H_
#define THIRD_PARTY_SRC_SERVER_EVENT_SUBSCRIPTION_H_

#include <cstdint>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::server_events {

void OnKeyCallback(ValkeyModuleCtx *ctx, ValkeyModuleEvent eid,
                   uint64_t subevent, void *data);
void SubscribeToServerEvents();

}  // namespace valkey_search::server_events

#endif  // THIRD_PARTY_SRC_SERVER_EVENT_SUBSCRIPTION_H_
