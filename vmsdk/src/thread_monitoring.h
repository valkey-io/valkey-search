/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include <pthread.h>

#include "absl/status/statusor.h"

namespace vmsdk {

class ThreadMonitor {
 public:
  ThreadMonitor(pthread_t thread_id);
  ~ThreadMonitor() = default;

  absl::StatusOr<double> GetThreadCPUPercentage();

  // Returns the thread spent time (user + system)
  absl::StatusOr<uint64_t> GetCPUTime() const;

  std::optional<int64_t> last_cpu_time_;
  std::optional<int64_t> last_wall_time_micro_;
  pthread_t thread_id_;
};
}  // namespace vmsdk
