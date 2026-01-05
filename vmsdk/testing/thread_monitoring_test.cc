/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/thread_monitoring.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

std::atomic_bool use_wrap_thread_functions = false;

#ifdef __APPLE__
#include <mach/mach.h>

kern_return_t mock_thread_info(thread_inspect_t target_thread,
                               thread_flavor_t flavor,
                               thread_info_t thread_info,
                               mach_msg_type_number_t *thread_info_count) {
  static int call_count = 10000;
  thread_basic_info_t info = (thread_basic_info_t)thread_info;
  info->user_time.seconds = --call_count;
  info->user_time.microseconds = 0;
  info->system_time.seconds = 0;
  info->system_time.microseconds = 0;
  return KERN_SUCCESS;
}
#endif

#ifdef __linux__
extern "C" {
int __real_clock_gettime(clockid_t clk_id, struct timespec *tp);

/**
 * Wrapper for clock_gettime() that provides mock decreasing timestamps for
 * testing.
 *
 * When use_wrap_thread_functions is false, delegates to the real
 * clock_gettime(). When enabled, returns decreasing second values starting from
 * 10000 to simulate negative time elapsed scenarios in thread monitoring tests.
 *
 * @param clk_id Clock identifier (ignored in mock mode)
 * @param tp Timespec structure to fill with mock or real time
 * @return 0 on success, or result from real clock_gettime()
 */
int __wrap_clock_gettime(clockid_t clk_id, struct timespec *tp) {
  if (!use_wrap_thread_functions.load(std::memory_order_relaxed)) {
    return __real_clock_gettime(clk_id, tp);
  }
  static int call_count = 10000;
  tp->tv_sec = --call_count;
  tp->tv_nsec = 0;
  return 0;
}
}
#endif

namespace vmsdk {

TEST(ThreadMonitorTest, MockedSystemCallsNegativeCPU) {
#ifdef __APPLE__
  ThreadMonitor::thread_info_func = mock_thread_info;
#endif
  use_wrap_thread_functions.store(true, std::memory_order_relaxed);
  ThreadMonitor monitor(pthread_self());

  // First call - high CPU time
  auto result1 = monitor.GetThreadCPUPercentage();
  ASSERT_TRUE(result1.ok());
  EXPECT_EQ(result1.value(), 0.0);

  // Second call - lower CPU time (negative elapsed)
  auto result2 = monitor.GetThreadCPUPercentage();
  ASSERT_FALSE(result2.ok());
  ASSERT_EQ(result2.status().code(), absl::StatusCode::kFailedPrecondition);
  use_wrap_thread_functions.store(false, std::memory_order_relaxed);
#ifdef __APPLE__
  ThreadMonitor::thread_info_func = thread_info;
#endif
}

}  // namespace vmsdk