/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_BLOCKED_CLIENT_H_
#define VMSDK_SRC_BLOCKED_CLIENT_H_

#include <utility>

#include "absl/container/flat_hash_map.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

class BlockedClient {
 public:
  BlockedClient(ValkeyModuleCtx *ctx, bool handle_duplication);
  BlockedClient(ValkeyModuleCtx *ctx,
                ValkeyModuleCmdFunc reply_callback = nullptr,
                ValkeyModuleCmdFunc timeout_callback = nullptr,
                void (*free_privdata)(ValkeyModuleCtx *, void *) = nullptr,
                long long timeout_ms = 0);
  BlockedClient(BlockedClient &&other) noexcept
      : blocked_client_(std::exchange(other.blocked_client_, nullptr)),
        private_data_(std::exchange(other.private_data_, nullptr)),
        tracked_client_id_(std::exchange(other.tracked_client_id_, 0)),
        time_measurement_ongoing_(
            std::exchange(other.time_measurement_ongoing_, false)) {}

  BlockedClient &operator=(BlockedClient &&other) noexcept;

  BlockedClient(const BlockedClient &other) = delete;
  BlockedClient &operator=(const BlockedClient &other) = delete;

  operator ValkeyModuleBlockedClient *() const { return blocked_client_; }
  void SetReplyPrivateData(void *private_data);
  void UnblockClient();
  void MeasureTimeStart();
  void MeasureTimeEnd();
  ~BlockedClient() { UnblockClient(); }

 private:
  ValkeyModuleBlockedClient *blocked_client_{nullptr};
  void *private_data_{nullptr};
  bool time_measurement_ongoing_{false};
  unsigned long long tracked_client_id_{0};
};

struct BlockedClientEntry {
  size_t cnt{0};
  ValkeyModuleBlockedClient *blocked_client{nullptr};
};
absl::flat_hash_map<unsigned long long, BlockedClientEntry> &
TrackedBlockedClients();
}  // namespace vmsdk
#endif  // VMSDK_SRC_BLOCKED_CLIENT_H_
