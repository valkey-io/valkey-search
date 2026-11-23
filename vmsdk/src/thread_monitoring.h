/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include <pthread.h>

#include <atomic>
#include <cstdint>
#include <optional>

#include "absl/status/statusor.h"

namespace vmsdk {

// Monitors the CPU usage of the thread that calls Init(). Until then, the
// getters return UnavailableError. Construction does not allocate.
class ThreadMonitor {
 public:
  ThreadMonitor() = default;
  ~ThreadMonitor() = default;

  // Must be called on the thread to be monitored. After it returns,
  // GetCPUTime may be called from any thread.
  void Init();

  // Main thread only: updates the last sample without synchronization.
  absl::StatusOr<double> GetThreadCPUPercentage();

  // Returns the thread spent time (user + system)
  absl::StatusOr<uint64_t> GetCPUTime() const;

 private:
  std::atomic_bool initialized_{false};
  pthread_t thread_id_{};
  std::optional<int64_t> last_cpu_time_;
  std::optional<int64_t> last_wall_time_micro_;
};
}  // namespace vmsdk
