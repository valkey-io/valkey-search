/*
 * Copyright (c) 2026, valkey-search contributors
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

  double GetTotalGrpcCPUTime() const { return total_cpu_time_; }

  void UpdateTotalCPUTimeSec() const;

 private:
  absl::StatusOr<double> CalcCurrentCPUTimeSec() const;
#ifdef __APPLE__
  absl::StatusOr<std::vector<thread_act_t>> GetThreadsByNameMac() const;
#elif __linux__
  absl::StatusOr<std::vector<std::string>> GetThreadsByNameLinux() const;
#endif

  const std::string thread_name_pattern_;
  mutable double total_cpu_time_{0.0};
  mutable double prev_cpu_time_{0.0};
};
}  // namespace vmsdk
