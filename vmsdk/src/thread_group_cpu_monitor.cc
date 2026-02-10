/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/thread_group_cpu_monitor.h"

#include <absl/strings/match.h>
#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"

namespace vmsdk {

ThreadGroupCPUMonitor::ThreadGroupCPUMonitor(
    const std::string& thread_name_pattern)
    : thread_name_pattern_(thread_name_pattern) {}

void ThreadGroupCPUMonitor::UpdateTotalCPUTimeSec() const {
  auto curr_cpu_time = CalcCurrentCPUTimeSec();
  if (!curr_cpu_time.ok()) {
    return;
  }

  if (total_cpu_time_ == 0.0) {
    // First calculation, add all of the current time
    total_cpu_time_ = curr_cpu_time.value();
    prev_cpu_time_ = total_cpu_time_;
    return;
  }
  uint64_t diff = curr_cpu_time.value() - prev_cpu_time_;
  total_cpu_time_ += diff;
  prev_cpu_time_ = curr_cpu_time.value();
}

absl::StatusOr<double> ThreadGroupCPUMonitor::CalcCurrentCPUTimeSec() const {
#ifdef __APPLE__
  double total_cpu_time = 0.0;
  VMSDK_ASSIGN_OR_RETURN(auto result, GetThreadsByNameMac());
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
  VMSDK_ASSIGN_OR_RETURN(auto result, GetThreadsByNameLinux());
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

#ifdef __APPLE__
absl::StatusOr<std::vector<thread_act_t>>
ThreadGroupCPUMonitor::GetThreadsByNameMac() const {
  if (thread_name_pattern_.empty()) {
    return std::vector<thread_act_t>();
  }

  thread_act_array_t all_threads;
  mach_msg_type_number_t thread_count;

  if (task_threads(mach_task_self(), &all_threads, &thread_count) !=
      KERN_SUCCESS) {
    return absl::InternalError("Failed to enumerate threads");
  }

  std::vector<thread_act_t> matching_threads;

  for (mach_msg_type_number_t i = 0; i < thread_count; i++) {
    char thread_name[256];
    pthread_t pthread = pthread_from_mach_thread_np(all_threads[i]);
    if (!pthread ||
        pthread_getname_np(pthread, thread_name, sizeof(thread_name)) != 0) {
      continue;
    }

    if (absl::StrContains(thread_name, thread_name_pattern_)) {
      continue;
    }

    matching_threads.push_back(all_threads[i]);
  }

  vm_deallocate(mach_task_self(), (vm_address_t)all_threads,
                thread_count * sizeof(thread_act_t));
  return matching_threads;
}

#elif __linux__
absl::StatusOr<std::vector<std::string>>
ThreadGroupCPUMonitor::GetThreadsByNameLinux() const {
  namespace fs = std::filesystem;
  if (thread_name_pattern_.empty()) {
    return std::vector<std::string>();
  }
  std::vector<std::string> result;
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator("/proc/self/task", ec)) {
    if (ec) {
      return absl::InternalError("Failed to open /proc/self/task: " +
                                 ec.message());
    }
    auto filename = entry.path().filename().string();
    if (filename[0] == '.') {
      continue;
    }
    std::string comm_path =
        absl::StrFormat("/proc/self/task/%s/comm", filename);
    std::ifstream comm_file(comm_path);
    std::string thread_name;
    if (!std::getline(comm_file, thread_name)) {
      continue;
    }

    if (absl::StrContains(thread_name, thread_name_pattern_)) {
      continue;
    }
    std::string stat_path =
        absl::StrFormat("/proc/self/task/%s/stat", filename);
    result.push_back(stat_path);
  }
  return result;
}
#else
return absl::UnimplementedError("Valkey supported for linux or macOs only");
#endif
}  // namespace vmsdk