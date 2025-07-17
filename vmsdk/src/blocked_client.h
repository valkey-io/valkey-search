/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_BLOCKED_CLIENT_H_
#define VMSDK_SRC_BLOCKED_CLIENT_H_

#include <array>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
// External mutex for thread safety of BlockedClientTracker
extern absl::Mutex blocked_clients_mutex;

// Enum for different blocking categories
enum class BlockedClientCategory {
  kHash,
  kJson,
  kOther,
  kCategoryCount  // Used to size the array
};

// Forward declaration
struct BlockedClientEntry;

class BlockedClientTracker {
 public:
  static BlockedClientTracker& GetInstance();
  
  size_t GetClientCount(BlockedClientCategory category) const ABSL_LOCKS_EXCLUDED(blocked_clients_mutex);
  
  // Access to the map for a specific category
  absl::flat_hash_map<unsigned long long, BlockedClientEntry>& 
  operator[](BlockedClientCategory category) ABSL_EXCLUSIVE_LOCKS_REQUIRED(blocked_clients_mutex);
  
  const absl::flat_hash_map<unsigned long long, BlockedClientEntry>& 
  operator[](BlockedClientCategory category) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(blocked_clients_mutex);
  
 private:
  BlockedClientTracker() = default;
  
  std::array<absl::NoDestructor<
      absl::flat_hash_map<unsigned long long, BlockedClientEntry>>, 
      static_cast<size_t>(BlockedClientCategory::kCategoryCount)> tracked_blocked_clients_;
};

class BlockedClient {
 public:
  BlockedClient(ValkeyModuleCtx *ctx, bool handle_duplication,
                BlockedClientCategory category = BlockedClientCategory::kOther);
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
            std::exchange(other.time_measurement_ongoing_, false)),
            category_(std::exchange(other.category_, BlockedClientCategory::kOther)) {}

  BlockedClient &operator=(BlockedClient &&other) noexcept;

  BlockedClient(const BlockedClient &other) = delete;
  BlockedClient &operator=(const BlockedClient &other) = delete;

  operator ValkeyModuleBlockedClient *() const { return blocked_client_; }
  void SetReplyPrivateData(void *private_data);
  void UnblockClient();
  void MeasureTimeStart();
  void MeasureTimeEnd();
  ~BlockedClient() { UnblockClient(); }

  BlockedClientCategory GetCategory() const { return category_; }

 private:
  ValkeyModuleBlockedClient *blocked_client_{nullptr};
  void *private_data_{nullptr};
  bool time_measurement_ongoing_{false};
  unsigned long long tracked_client_id_{0};
  BlockedClientCategory category_{BlockedClientCategory::kOther};  // New member
};

struct BlockedClientEntry {
  size_t cnt{0};
  ValkeyModuleBlockedClient *blocked_client{nullptr};
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_BLOCKED_CLIENT_H_
