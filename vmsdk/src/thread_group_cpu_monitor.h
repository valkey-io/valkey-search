/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"

namespace vmsdk {

class ThreadGroupCPUMonitor {
 public:
  ThreadGroupCPUMonitor(const std::string& thread_name_pattern);
  ~ThreadGroupCPUMonitor() = default;

  double GetTotalGrpcCPUTime() const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::ReaderMutexLock lock(&mutex_);
    return total_cpu_time_;
  }

  void UpdateTotalCPUTimeSec() ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  absl::StatusOr<double> CalcCurrentCPUTimeSec() const;

  std::string thread_name_pattern_;
  mutable absl::Mutex mutex_;
  double total_cpu_time_{0.0};
  double prev_cpu_time_{0.0};
};
}  // namespace vmsdk
