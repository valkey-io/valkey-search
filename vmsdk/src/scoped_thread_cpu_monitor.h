/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include <atomic>
#include <cstdint>
#include <optional>

#include "vmsdk/src/thread_monitoring.h"

namespace vmsdk {

class ScopedThreadCPUMonitor {
 public:
  explicit ScopedThreadCPUMonitor(std::atomic<uint64_t> *accumulator);
  ~ScopedThreadCPUMonitor();

  ScopedThreadCPUMonitor(const ScopedThreadCPUMonitor &) = delete;
  ScopedThreadCPUMonitor &operator=(const ScopedThreadCPUMonitor &) = delete;

 private:
  std::atomic<uint64_t> *accumulator_;
  ThreadMonitor thread_monitor_;
  std::optional<uint64_t> start_cpu_time_usec_;
};

}  // namespace vmsdk
