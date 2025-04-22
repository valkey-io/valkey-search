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

#include "vmsdk/src/blocked_client.h"

#include <array>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/strings/str_split.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
std::optional<bool> gCachedAllowBlockClientOnMutation;
void ResetCachedAllowBlockClientOnMutation() {
  gCachedAllowBlockClientOnMutation = std::nullopt;
}

bool EngineSupported(RedisModuleCtx *ctx) {
#ifdef BLOCK_CLIENT_ON_MUTATION
  return true;
#endif

  if (gCachedAllowBlockClientOnMutation.has_value()) {
    return gCachedAllowBlockClientOnMutation.value();
  }
  auto engine_version_str = EngineVersion(ctx);
  std::vector<std::string> engine_version_arr =
      absl::StrSplit(engine_version_str, '.');
  CHECK(engine_version_arr.size() == 3);
  std::array<uint32_t, 3> min_version{8, 1, 1};
  for (auto i = 0; i < engine_version_arr.size(); ++i) {
    auto part = std::atoi(engine_version_arr[i].c_str());
    if (part < min_version[i]) {
      VMSDK_LOG(NOTICE, ctx)
          << "Engine version, " << engine_version_str
          << " , does NOT support client blocking on keyspace notification.";
      gCachedAllowBlockClientOnMutation = false;
      return false;
    }
  }
  VMSDK_LOG(NOTICE, ctx)
      << "Engine version, " << engine_version_str
      << " , supports client blocking on keyspace notification.";
  gCachedAllowBlockClientOnMutation = true;
  return true;
}

absl::flat_hash_map<RedisModuleCtx *, BlockedClientEntry> gBlockedClients;
// Used for testing
absl::flat_hash_map<RedisModuleCtx *, BlockedClientEntry> &
TrackedBlockedClients() {
  return gBlockedClients;
}

BlockedClient::BlockedClient(RedisModuleCtx *ctx, bool keyspace_notification) {
  if (keyspace_notification && !EngineSupported(ctx)) {
    return;
  }
  tracked_ctx_ = ctx;
  auto it = gBlockedClients.find(ctx);
  if (it == gBlockedClients.end()) {
    blocked_client_ =
        RedisModule_BlockClient(ctx, nullptr, nullptr, nullptr, 0);
    if (!blocked_client_) {
      return;
    }
    gBlockedClients[ctx] = {1, blocked_client_};
    return;
  }
  blocked_client_ = it->second.blocked_client;
  auto &cnt = it->second.cnt;
  ++cnt;
}

BlockedClient::BlockedClient(RedisModuleCtx *ctx,
                             RedisModuleCmdFunc reply_callback,
                             RedisModuleCmdFunc timeout_callback,
                             void (*free_privdata)(RedisModuleCtx *, void *),
                             long long timeout_ms) {
  blocked_client_ = RedisModule_BlockClient(
      ctx, reply_callback, timeout_callback, free_privdata, timeout_ms);
}

BlockedClient &BlockedClient::operator=(BlockedClient &&other) noexcept {
  if (this != &other) {
    blocked_client_ = std::exchange(other.blocked_client_, nullptr);
    private_data_ = std::exchange(other.private_data_, nullptr);
    tracked_ctx_ = std::exchange(other.tracked_ctx_, nullptr);
    time_measurement_ongoing_ =
        std::exchange(other.time_measurement_ongoing_, false);
  }
  return *this;
}

void BlockedClient::SetReplyPrivateData(void *private_data) {
  private_data_ = private_data;
}

void BlockedClient::UnblockClient() {
  if (!blocked_client_) {
    return;
  }
  MeasureTimeEnd();
  auto blocked_client = std::exchange(blocked_client_, nullptr);
  auto private_data = std::exchange(private_data_, nullptr);
  auto tracked_ctx = std::exchange(tracked_ctx_, nullptr);
  if (tracked_ctx) {
    auto itr = gBlockedClients.find(tracked_ctx);
    CHECK(itr != gBlockedClients.end());
    auto &cnt = itr->second.cnt;
    CHECK_GT(cnt, 0);
    --cnt;
    if (cnt > 0) {
      return;
    }
    gBlockedClients.erase(tracked_ctx);
  }
  RedisModule_UnblockClient(blocked_client, private_data);
}

void BlockedClient::MeasureTimeStart() {
  if (time_measurement_ongoing_ || !blocked_client_) {
    return;
  }
  RedisModule_BlockedClientMeasureTimeStart(blocked_client_);
  time_measurement_ongoing_ = true;
}

void BlockedClient::MeasureTimeEnd() {
  if (!time_measurement_ongoing_ || !blocked_client_) {
    return;
  }
  RedisModule_BlockedClientMeasureTimeEnd(blocked_client_);
  time_measurement_ongoing_ = false;
}
}  // namespace vmsdk
