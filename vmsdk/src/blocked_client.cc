/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/blocked_client.h"

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
absl::Mutex blocked_clients_mutex;

// Implementation for BlockedClientTracker
size_t BlockedClientTracker::GetClientCount(BlockedClientCategory category) const {
  absl::MutexLock lock(&blocked_clients_mutex);
  return tracked_blocked_clients_[static_cast<size_t>(category)]->size();
}

absl::flat_hash_map<unsigned long long, BlockedClientEntry>& 
BlockedClientTracker::operator[](BlockedClientCategory category) {
  return *tracked_blocked_clients_[static_cast<size_t>(category)];
}

const absl::flat_hash_map<unsigned long long, BlockedClientEntry>& 
BlockedClientTracker::operator[](BlockedClientCategory category) const {
  return *tracked_blocked_clients_[static_cast<size_t>(category)];
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
BlockedClient::BlockedClient(ValkeyModuleCtx *ctx, bool handle_duplication, 
                            BlockedClientCategory category)
    : category_(category) {
  if (handle_duplication) {
    unsigned long long tracked_client_id = ValkeyModule_GetClientId(ctx);
    if (tracked_client_id != 0) {
      absl::MutexLock lock(&blocked_clients_mutex);
      auto& category_map = BlockedClientTracker::Instance()[category_];
      auto it = category_map.find(tracked_client_id);
      if (it == category_map.end()) {
        blocked_client_ =
            ValkeyModule_BlockClient(ctx, nullptr, nullptr, nullptr, 0);
        if (!blocked_client_) {
          return;
        }
        tracked_client_id_ = tracked_client_id;
        category_map[tracked_client_id_] = {1, blocked_client_};
        return;
      }
      tracked_client_id_ = tracked_client_id;
      blocked_client_ = it->second.blocked_client;
      auto &cnt = it->second.cnt;
      ++cnt;
      return;
    }
  }
  blocked_client_ = ValkeyModule_BlockClient(ctx, nullptr, nullptr, nullptr, 0);
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
    blocked_client_ = std::exchange(other.blocked_client_, nullptr);
    private_data_ = std::exchange(other.private_data_, nullptr);
    tracked_client_id_ = std::exchange(other.tracked_client_id_, 0);
    time_measurement_ongoing_ =
        std::exchange(other.time_measurement_ongoing_, false);
    category_ = std::exchange(other.category_, BlockedClientCategory::kOther);
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
  auto category = category_; // Save category before potential move
  
  if (tracked_client_id) {
    absl::MutexLock lock(&blocked_clients_mutex);
    auto& category_map = BlockedClientTracker::Instance()[category];
    auto itr = category_map.find(tracked_client_id);
    CHECK(itr != category_map.end());
    auto &cnt = itr->second.cnt;
    CHECK_GT(cnt, 0);
    --cnt;
    if (cnt > 0) {
      return;
    }
    category_map.erase(tracked_client_id);
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

void BlockedClient::MeasureTimeEnd() {
  if (!time_measurement_ongoing_ || !blocked_client_) {
    return;
  }
  ValkeyModule_BlockedClientMeasureTimeEnd(blocked_client_);
  time_measurement_ongoing_ = false;
}
}  // namespace vmsdk
