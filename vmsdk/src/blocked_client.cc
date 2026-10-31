/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/blocked_client.h"

#include <array>
#include <atomic>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace {
std::array<std::atomic<size_t>,
           static_cast<size_t>(BlockedClientCategory::kCategoryCount)>
    active_blocked_clients{};
}  // namespace

BlockedClient::BlockedClient(ValkeyModuleCtx *ctx,
                             BlockedClientCategory category)
    : category_(category) {
  blocked_client_ = ValkeyModule_BlockClient(ctx, nullptr, nullptr, nullptr, 0);
  if (blocked_client_) {
    active_blocked_clients[static_cast<size_t>(category_)].fetch_add(
        1, std::memory_order_relaxed);
    tracks_active_count_ = true;
  }
}

BlockedClient::BlockedClient(ValkeyModuleCtx *ctx,
                             ValkeyModuleCmdFunc reply_callback,
                             ValkeyModuleCmdFunc timeout_callback,
                             void (*free_privdata)(ValkeyModuleCtx *, void *),
                             long long timeout_ms) {
  blocked_client_ = ValkeyModule_BlockClient(
      ctx, reply_callback, timeout_callback, free_privdata, timeout_ms);
}

BlockedClient &BlockedClient::operator=(BlockedClient &&other) noexcept {
  if (this != &other) {
    UnblockClient();
    blocked_client_ = std::exchange(other.blocked_client_, nullptr);
    private_data_ = std::exchange(other.private_data_, nullptr);
    time_measurement_ongoing_ =
        std::exchange(other.time_measurement_ongoing_, false);
    category_ = other.category_;
    tracks_active_count_ = std::exchange(other.tracks_active_count_, false);
  }
  return *this;
}

size_t BlockedClient::GetActiveClientCount(BlockedClientCategory category) {
  return active_blocked_clients[static_cast<size_t>(category)].load(
      std::memory_order_relaxed);
}

void BlockedClient::SetReplyPrivateData(void *private_data) {
  private_data_ = private_data;
}

void BlockedClient::UnblockClient() {
  if (!blocked_client_) {
    return;
  }
  auto blocked_client = std::exchange(blocked_client_, nullptr);
  auto private_data = std::exchange(private_data_, nullptr);
  MeasureTimeEnd(blocked_client);
  if (std::exchange(tracks_active_count_, false)) {
    active_blocked_clients[static_cast<size_t>(category_)].fetch_sub(
        1, std::memory_order_relaxed);
  }
  ValkeyModule_UnblockClient(blocked_client, private_data);
}

void BlockedClient::MeasureTimeStart() {
  if (time_measurement_ongoing_ || !blocked_client_) {
    return;
  }
  ValkeyModule_BlockedClientMeasureTimeStart(blocked_client_);
  time_measurement_ongoing_ = true;
}

void BlockedClient::MeasureTimeEnd(ValkeyModuleBlockedClient *blocked_client) {
  if (!time_measurement_ongoing_ || !blocked_client) {
    return;
  }
  ValkeyModule_BlockedClientMeasureTimeEnd(blocked_client);
  time_measurement_ongoing_ = false;
}
}  // namespace vmsdk
