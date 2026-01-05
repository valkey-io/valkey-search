/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/thread_monitoring.h"

#include <pthread.h>

#include "absl/time/clock.h"
#include "vmsdk/src/status/status_macros.h"

#ifdef __APPLE__
#include <mach/mach.h>
#elif __linux__
#include <sys/resource.h>
#include <sys/time.h>
#endif

namespace vmsdk {

static std::atomic_uint64_t thread_monitoring_cpu_error_cnt{0};

#ifdef __APPLE__
namespace {
thread_inspect_t ConvertToMachThread(pthread_t tid) {
  thread_inspect_t thread_id = pthread_mach_thread_np(tid);
  return thread_id;
}
}  // namespace

ThreadMonitor::ThreadInfoFunc ThreadMonitor::thread_info_func = thread_info;
#endif

ThreadMonitor::ThreadMonitor(pthread_t thread_id) { thread_id_ = thread_id; }

absl::StatusOr<double> ThreadMonitor::GetThreadCPUPercentage() {
  // First call, initializing values
  if (!last_cpu_time_.has_value() || !last_wall_time_micro_.has_value()) {
    last_wall_time_micro_ = absl::GetCurrentTimeNanos() / 1000;
    VMSDK_ASSIGN_OR_RETURN(last_cpu_time_, GetCPUTime());
    return 0.0;
  }
  // Get current CPU time
  VMSDK_ASSIGN_OR_RETURN(int64_t current_cpu_time, GetCPUTime());
  // Get current wall time
  int64_t current_wall_time_micro = absl::GetCurrentTimeNanos() / 1000;

  // Calculate elapsed times
  int64_t cpu_time_elapsed_us = current_cpu_time - last_cpu_time_.value();
  int64_t wall_time_elapsed =
      current_wall_time_micro - last_wall_time_micro_.value();

  // Update last measurements
  last_cpu_time_ = current_cpu_time;
  last_wall_time_micro_ = current_wall_time_micro;

  if (wall_time_elapsed == 0) {
    return 0.0;
  }

  if (cpu_time_elapsed_us < 0) {
    // Increment the monitoring counter for negative values
    return absl::FailedPreconditionError(absl::StrFormat(
        "Internal error in CPU calculation: negative cpu time for thread ID %u",
        static_cast<unsigned int>(thread_id_)));
  }

  if (wall_time_elapsed < 0) {
    return absl::InternalError(
        absl::StrFormat("Internal error in CPU calculation: negative wall time "
                        "for thread ID %u",
                        static_cast<unsigned int>(thread_id_)));
  }

  return (static_cast<double>(cpu_time_elapsed_us) / wall_time_elapsed) * 100.0;
}

absl::StatusOr<uint64_t> ThreadMonitor::GetCPUTime() const {
#ifdef __APPLE__
  thread_basic_info_data_t info;
  mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
  thread_inspect_t target = ConvertToMachThread(thread_id_);

  if (thread_info_func(target, THREAD_BASIC_INFO, (thread_info_t)&info,
                       &count) != KERN_SUCCESS) {
    return absl::InternalError(
        absl::StrFormat("Failed to get thread info for thread %p", thread_id_));
  }

  // Result in microseconds
  return (info.user_time.seconds * 1000000.0 + info.user_time.microseconds) +
         (info.system_time.seconds * 1000000.0 + info.system_time.microseconds);
#elif __linux__
  clockid_t cid;
  struct timespec ts;

  if (pthread_getcpuclockid(thread_id_, &cid) != 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to get CPU clock ID for thread %u",
                        static_cast<unsigned int>(thread_id_)));
  }

  if (clock_gettime(cid, &ts) != 0) {
    return absl::InternalError(
        absl::StrFormat("Failed to get value of clock for thread %u",
                        static_cast<unsigned int>(thread_id_)));
  }

  // Result in microseconds
  return (ts.tv_sec * 1000000) + (ts.tv_nsec / 1000);
#else
  return absl::UnimplementedError("Valkey supported for linux or macOs only");
#endif
}
}  // namespace vmsdk