/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/scoped_thread_cpu_monitor.h"

#include <pthread.h>

namespace vmsdk {

ScopedThreadCPUMonitor::ScopedThreadCPUMonitor(
    std::atomic<uint64_t> *accumulator)
    : accumulator_(accumulator), thread_monitor_(pthread_self()) {
  if (accumulator_ == nullptr) {
    return;
  }
  auto start_cpu_time = thread_monitor_.GetCPUTime();
  if (start_cpu_time.ok()) {
    start_cpu_time_usec_ = start_cpu_time.value();
  }
}

ScopedThreadCPUMonitor::~ScopedThreadCPUMonitor() {
  if (accumulator_ == nullptr || !start_cpu_time_usec_.has_value()) {
    return;
  }
  auto end_cpu_time = thread_monitor_.GetCPUTime();
  if (end_cpu_time.ok() && end_cpu_time.value() >= start_cpu_time_usec_) {
    accumulator_->fetch_add(end_cpu_time.value() - *start_cpu_time_usec_,
                            std::memory_order_relaxed);
  }
}

}  // namespace vmsdk
