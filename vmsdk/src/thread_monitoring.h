/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include <pthread.h>

#include "absl/status/statusor.h"

#ifdef __APPLE__
#include <mach/mach.h>
#elif __linux__
#include <sys/resource.h>
#include <sys/time.h>
#endif

namespace vmsdk {

class ThreadMonitor {
 public:
  ThreadMonitor() = default;
  ~ThreadMonitor() = default;

  absl::StatusOr<double> GetThreadCPUPercentage();
  void Start(pthread_t thread_id);

 private:
#ifdef __APPLE__
  thread_inspect_t ConvertToMachThread();
#endif
  absl::StatusOr<uint64_t> GetCPUTime() const;

  uint64_t last_cpu_time_{0};
  int64_t last_wall_time_micro_;
  pthread_t thread_id_;
};
}  // namespace vmsdk
