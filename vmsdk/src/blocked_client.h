/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_BLOCKED_CLIENT_H_
#define VMSDK_SRC_BLOCKED_CLIENT_H_

#include <cstddef>
#include <utility>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
// Enum for different blocking categories
enum class BlockedClientCategory {
  kHash,
  kJson,
  kOther,
  kCategoryCount  // Used to size the array
};

class BlockedClient {
 public:
  BlockedClient(ValkeyModuleCtx *ctx, BlockedClientCategory category);
  BlockedClient(ValkeyModuleCtx *ctx,
                ValkeyModuleCmdFunc reply_callback = nullptr,
                ValkeyModuleCmdFunc timeout_callback = nullptr,
                void (*free_privdata)(ValkeyModuleCtx *, void *) = nullptr,
                long long timeout_ms = 0);
  BlockedClient(BlockedClient &&other) noexcept
      : blocked_client_(std::exchange(other.blocked_client_, nullptr)),
        private_data_(std::exchange(other.private_data_, nullptr)),
        time_measurement_ongoing_(
            std::exchange(other.time_measurement_ongoing_, false)),
        category_(other.category_),
        tracks_active_count_(std::exchange(other.tracks_active_count_, false)) {
  }

  BlockedClient &operator=(BlockedClient &&other) noexcept;

  BlockedClient(const BlockedClient &other) = delete;
  BlockedClient &operator=(const BlockedClient &other) = delete;

  static size_t GetActiveClientCount(BlockedClientCategory category);

  operator ValkeyModuleBlockedClient *() const { return blocked_client_; }
  void SetReplyPrivateData(void *private_data);
  void UnblockClient();
  void MeasureTimeStart();
  void MeasureTimeEnd(ValkeyModuleBlockedClient *blocked_client);
  ~BlockedClient() { UnblockClient(); }

 private:
  ValkeyModuleBlockedClient *blocked_client_{nullptr};
  void *private_data_{nullptr};
  bool time_measurement_ongoing_{false};
  BlockedClientCategory category_{BlockedClientCategory::kOther};
  bool tracks_active_count_{false};
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_BLOCKED_CLIENT_H_
