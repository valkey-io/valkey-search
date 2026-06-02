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

#include "vmsdk/src/status/status_macros.h"
#ifdef __APPLE__
#include <mach/mach.h>
#endif

namespace vmsdk {

namespace {
#ifdef __APPLE__
absl::StatusOr<std::vector<thread_act_t>> GetThreadsByNameMac(
    std::string thread_name_pattern) {
  if (thread_name_pattern.empty()) {
    return std::vector<thread_act_t>();
  }

  thread_act_array_t all_threads;
  mach_msg_type_number_t thread_count;

  auto status_code =
      task_threads(mach_task_self(), &all_threads, &thread_count);
  if (status_code != KERN_SUCCESS) {
    // Get the human-readable error string
    const char* error_reason = mach_error_string(status_code);
    return absl::InternalError(absl::StrFormat(
        "Failed to enumerate threads. Reason: %s", error_reason));
  }

  // Custom deleter for automatic cleanup of Mach-allocated thread array
  auto thread_array_deleter = [thread_count](thread_act_t* threads) {
    vm_deallocate(mach_task_self(), (vm_address_t)threads,
                  thread_count * sizeof(thread_act_t));
  };
  std::unique_ptr<thread_act_t, decltype(thread_array_deleter)> threads_guard{
      all_threads, thread_array_deleter};

  std::vector<thread_act_t> matching_threads;

  for (mach_msg_type_number_t i = 0; i < thread_count; i++) {
    char thread_name[256];
    pthread_t pthread = pthread_from_mach_thread_np(all_threads[i]);
    if (!pthread ||
        pthread_getname_np(pthread, thread_name, sizeof(thread_name)) != 0) {
      continue;
    }

    if (!absl::StrContains(thread_name, thread_name_pattern)) {
      continue;
    }

    matching_threads.push_back(all_threads[i]);
  }

  return matching_threads;
}

#elif __linux__
absl::StatusOr<std::vector<std::string>> GetThreadsByNameLinux(
    std::string thread_name_pattern) {
  namespace fs = std::filesystem;
  if (thread_name_pattern.empty()) {
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

    if (!absl::StrContains(thread_name, thread_name_pattern)) {
      continue;
    }
    std::string stat_path =
        absl::StrFormat("/proc/self/task/%s/stat", filename);
    result.push_back(stat_path);
  }
  return result;
}
#else
return absl::UnimplementedError(
    "Valkey-search supported for linux or macOs only");
#endif
}  // namespace

constexpr double kMicroToSec = 1000000.0;

ThreadGroupCPUMonitor::ThreadGroupCPUMonitor(
    const std::string& thread_name_pattern)
    : thread_name_pattern_(thread_name_pattern) {}

void ThreadGroupCPUMonitor::UpdateTotalCPUTimeSec() {
  absl::MutexLock lock(&mutex_);
  auto curr_cpu_time = CalcCurrentCPUTimeSec();
  if (!curr_cpu_time.ok()) {
    return;
  }

  if (!total_cpu_time_.has_value()) {
    total_cpu_time_ = curr_cpu_time.value();
    prev_cpu_time_ = total_cpu_time_.value();
    return;
  }
  uint64_t diff = curr_cpu_time.value() - prev_cpu_time_;
  *total_cpu_time_ += diff;
  prev_cpu_time_ = curr_cpu_time.value();
}

absl::StatusOr<double> ThreadGroupCPUMonitor::CalcCurrentCPUTimeSec() const {
#ifdef __APPLE__
  double total_cpu_time = 0.0;
  VMSDK_ASSIGN_OR_RETURN(auto result,
                         GetThreadsByNameMac(thread_name_pattern_));
  for (const auto& thread : result) {
    thread_basic_info_data_t info;
    mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;

    auto status_code =
        thread_info(thread, THREAD_BASIC_INFO, (thread_info_t)&info, &count);
    if (status_code != KERN_SUCCESS) {
      const char* error_reason = mach_error_string(status_code);
      return absl::InternalError(absl::StrFormat(
          "Failed to get thread info for thread: %u. Reason: %s", thread,
          error_reason));
    }
    total_cpu_time +=
        (info.user_time.seconds + info.user_time.microseconds / kMicroToSec) +
        (info.system_time.seconds +
         info.system_time.microseconds / kMicroToSec);
  }

  return total_cpu_time;
#elif __linux__

  double total_cpu_time = 0.0;
  long ticks_per_sec = sysconf(_SC_CLK_TCK);
  VMSDK_ASSIGN_OR_RETURN(auto result,
                         GetThreadsByNameLinux(thread_name_pattern_));
  for (auto& path : result) {
    std::ifstream stat_file(path);
    if (!stat_file.is_open()) {
      return absl::Status{absl::ErrnoToStatusCode(errno),
                          absl::StrFormat("Failed to open thread stats file in "
                                          "path: %s. Error: %s (errno: %d)",
                                          path, strerror(errno), errno)};
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
  return absl::UnimplementedError(
      "Valkey-search supported for linux or macOs only");
#endif
}
}  // namespace vmsdk