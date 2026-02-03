/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include "absl/status/statusor.h"

namespace vmsdk {

class ThreadGroupCPUMonitor {
 public:
  ThreadGroupCPUMonitor(const std::string& thread_name_pattern);
  ~ThreadGroupCPUMonitor() = default;

  absl::StatusOr<double> GetTotalGrpcCPUTime() const { return total_cpu_time_; }

  void UpdateTotalCPUTimeSec();

 private:
  absl::StatusOr<double> CalcCurrentCPUTimeSec() const;

  const std::string thread_name_pattern_;
  std::atomic<double> total_cpu_time_{0.0};
  double prev_cpu_time_{0.0};
};
}  // namespace vmsdk
