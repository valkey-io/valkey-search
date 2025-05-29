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

#include "vmsdk/src/blocked_client.h"

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
absl::Mutex blocked_clients_mutex;
static absl::NoDestructor<
    absl::flat_hash_map<unsigned long long, BlockedClientEntry>>
    tracked_blocked_clients;
// Used for testing
absl::flat_hash_map<unsigned long long, BlockedClientEntry> &
TrackedBlockedClients() {
  return *tracked_blocked_clients;
}
// The engine supports storing only one set of callback information (callback
// function, timeout, etc.) per client. If BlockedClient is called multiple
// times for the same client with non-empty callback information, it issues
// VM_BlockClient each time, causing the engine to overwrite the previous
// callback information with the latest one. If BlockedClient is called multiple
// times with empty callback information, only the first call triggers
// VM_BlockClient, while subsequent calls simply increment an internal reference
// count. VM_UnblockClient is only called once the reference count returns to
// zero.
BlockedClient::BlockedClient(RedisModuleCtx *ctx, bool handle_duplication) {
  if (handle_duplication) {
    unsigned long long tracked_client_id = RedisModule_GetClientId(ctx);
    if (tracked_client_id != 0) {
      absl::MutexLock lock(&blocked_clients_mutex);
      auto it = TrackedBlockedClients().find(tracked_client_id);
      if (it == TrackedBlockedClients().end()) {
        blocked_client_ =
            RedisModule_BlockClient(ctx, nullptr, nullptr, nullptr, 0);
        if (!blocked_client_) {
          return;
        }
        tracked_client_id_ = tracked_client_id;
        TrackedBlockedClients()[tracked_client_id_] = {1, blocked_client_};
        return;
      }
      tracked_client_id_ = tracked_client_id;
      blocked_client_ = it->second.blocked_client;
      auto &cnt = it->second.cnt;
      ++cnt;
      return;
    }
  }
  blocked_client_ = RedisModule_BlockClient(ctx, nullptr, nullptr, nullptr, 0);
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
    tracked_client_id_ = std::exchange(other.tracked_client_id_, 0);
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
  auto tracked_client_id = std::exchange(tracked_client_id_, 0);
  if (tracked_client_id) {
    absl::MutexLock lock(&blocked_clients_mutex);
    auto itr = TrackedBlockedClients().find(tracked_client_id);
    CHECK(itr != TrackedBlockedClients().end());
    auto &cnt = itr->second.cnt;
    CHECK_GT(cnt, 0);
    --cnt;
    if (cnt > 0) {
      return;
    }
    TrackedBlockedClients().erase(tracked_client_id);
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
