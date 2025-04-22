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

#ifndef VMSDK_SRC_BLOCKED_CLIENT_H_
#define VMSDK_SRC_BLOCKED_CLIENT_H_

#include <utility>

#include "absl/container/flat_hash_map.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

class BlockedClient {
 public:
  explicit BlockedClient(RedisModuleCtx *ctx,
                         bool keyspace_notification = false);
  BlockedClient(RedisModuleCtx *ctx, RedisModuleCmdFunc reply_callback,
                RedisModuleCmdFunc timeout_callback = nullptr,
                void (*free_privdata)(RedisModuleCtx *, void *) = nullptr,
                long long timeout_ms = 0);
  BlockedClient(BlockedClient &&other) noexcept
      : blocked_client_(std::exchange(other.blocked_client_, nullptr)),
        private_data_(std::exchange(other.private_data_, nullptr)),
        tracked_ctx_(std::exchange(other.tracked_ctx_, nullptr)),
        time_measurement_ongoing_(
            std::exchange(other.time_measurement_ongoing_, false)) {}

  BlockedClient &operator=(BlockedClient &&other) noexcept;

  BlockedClient(const BlockedClient &other) = delete;
  BlockedClient &operator=(const BlockedClient &other) = delete;

  operator RedisModuleBlockedClient *() const { return blocked_client_; }
  void SetReplyPrivateData(void *private_data);
  void UnblockClient();
  void MeasureTimeStart();
  void MeasureTimeEnd();
  ~BlockedClient() { UnblockClient(); }

 private:
  RedisModuleBlockedClient *blocked_client_{nullptr};
  void *private_data_{nullptr};
  bool time_measurement_ongoing_{false};
  RedisModuleCtx *tracked_ctx_{nullptr};
};

struct BlockedClientEntry {
  size_t cnt{0};
  RedisModuleBlockedClient *blocked_client{nullptr};
};
absl::flat_hash_map<RedisModuleCtx *, BlockedClientEntry> &
TrackedBlockedClients();
void ResetCachedAllowBlockClientOnMutation();
}  // namespace vmsdk
#endif  // VMSDK_SRC_BLOCKED_CLIENT_H_
