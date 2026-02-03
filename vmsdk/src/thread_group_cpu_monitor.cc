/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/thread_group_cpu_monitor.h"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <string>
#include <vector>

#include "absl/strings/str_split.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

ThreadGroupCPUMonitor::ThreadGroupCPUMonitor(
    const std::string& thread_name_pattern)
    : thread_name_pattern_(thread_name_pattern) {}

void ThreadGroupCPUMonitor::UpdateTotalCPUTimeSec() {
  auto curr_cpu_time = CalcCurrentCPUTimeSec();
  if (!curr_cpu_time.ok()) {
    return;
  }

  if (total_cpu_time_.load(std::memory_order_relaxed) == 0.0) {
    // First calculation, add all of the current time
    total_cpu_time_.store(curr_cpu_time.value(), std::memory_order_relaxed);
    prev_cpu_time_ = curr_cpu_time.value();
    return;
  }
  uint64_t diff = curr_cpu_time.value() - prev_cpu_time_;
  total_cpu_time_.fetch_add(diff, std::memory_order_relaxed);
  prev_cpu_time_ = curr_cpu_time.value();
}

absl::StatusOr<double> ThreadGroupCPUMonitor::CalcCurrentCPUTimeSec() const {
#ifdef __APPLE__
  double total_cpu_time = 0.0;
  VMSDK_ASSIGN_OR_RETURN(auto result, GetThreadsByName(thread_name_pattern_));
  for (int i = 0; i < result.size(); i++) {
    thread_basic_info_data_t info;
    mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;

    if (thread_info(result[i], THREAD_BASIC_INFO, (thread_info_t)&info,
                    &count) == KERN_SUCCESS) {
      total_cpu_time +=
          (info.user_time.seconds + info.user_time.microseconds / 1000000.0) +
          (info.system_time.seconds +
           info.system_time.microseconds / 1000000.0);
    }
  }

  return total_cpu_time;
#elif __linux__

  double total_cpu_time = 0.0;
  long ticks_per_sec = sysconf(_SC_CLK_TCK);
  VMSDK_ASSIGN_OR_RETURN(auto result, GetThreadsByName(thread_name_pattern_));
  for (auto& path : result) {
    std::ifstream stat_file(path);
    if (!stat_file.is_open()) {
      VMSDK_LOG(NOTICE, nullptr) << "Failed to open file: " << path;
      continue;
    }
    std::string line{std::istreambuf_iterator<char>(stat_file),
                     std::istreambuf_iterator<char>()};

    std::vector<std::string> fields =
        absl::StrSplit(line, ' ', absl::SkipEmpty());

    if (fields.size() >= 15) {
      uint64_t utime = std::stoull(fields[13]);
      uint64_t stime = std::stoull(fields[14]);
      total_cpu_time += (utime + stime) / ticks_per_sec;
    }
  }

  return total_cpu_time;
#else
  return absl::UnimplementedError("Valkey supported for linux or macOs only");
#endif
}
}  // namespace vmsdk