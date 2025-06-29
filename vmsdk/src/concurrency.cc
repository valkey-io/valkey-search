/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/concurrency.h"

#include <fstream>
#include <thread>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "vmsdk/src/log.h"

namespace vmsdk {

namespace helper {

// Helper function to extract and validate an integer from a line
int ExtractInteger(const std::string& line) {
  size_t pos = line.find(':');
  if (pos == std::string::npos) {
    return -1;  // Invalid format
  }

  std::string value = line.substr(pos + 1);

  // Trim leading/trailing spaces
  value = absl::StripAsciiWhitespace(value);

  // Ensure the value contains only digits
  if (value.empty() || !absl::c_all_of(value, ::isdigit)) {
    return -1;  // Invalid number
  }

  return std::stoi(value);
}

size_t ParseCPUInfo(std::istream& cpuinfo) {
  std::string line;
  int physical_id = -1;
  int cores_per_cpu = -1;
  absl::flat_hash_map<int, int> physical_cpu_cores;
  size_t total_physical_cores = 0;

  while (std::getline(cpuinfo, line)) {
    if (line.find("physical id") != std::string::npos) {
      physical_id = ExtractInteger(line);
    } else if (line.find("cpu cores") != std::string::npos) {
      cores_per_cpu = ExtractInteger(line);

      if (physical_id != -1 && cores_per_cpu > 0) {
        physical_cpu_cores[physical_id] = cores_per_cpu;
        physical_id = -1;  // Resetting here
      }
    }
  }

  for (const auto& [id, core_count] : physical_cpu_cores) {
    total_physical_cores += core_count;
  }

  return total_physical_cores;
}

/// Execute `command` and return its output
absl::StatusOr<std::string> ExecuteCommand(const std::string& command) {
  errno = 0;
  auto fp = ::popen(command.c_str(), "r");
  if (fp == nullptr) {
    return absl::ErrnoToStatus(
        errno, absl::StrCat("Could not execute command: ", command));
  }
  constexpr size_t LINE_SIZE = 1024;
  char line[LINE_SIZE];
  std::stringstream ss;
  while (std::fgets(line, LINE_SIZE, fp) != nullptr) {
    ss << line;
  }
  ::fclose(fp);
  return ss.str();
}

/// Return the number of physical cores by parsing the output of `lscpu` command
size_t ParseLscpuOutput(const std::string& lscpu_output) {
  constexpr absl::string_view CORES_PER_SOCKET = "Core(s) per socket";
  constexpr absl::string_view SOCKETS = "Socket(s)";

  std::vector<absl::string_view> lines = absl::StrSplit(lscpu_output, '\n');
  int cores_per_socket = -1;
  int sockets_count = -1;
  for (auto line : lines) {
    if (cores_per_socket == -1 && absl::StrContains(line, CORES_PER_SOCKET)) {
      cores_per_socket = ExtractInteger(std::string{line});
    } else if (sockets_count == -1 && absl::StrContains(line, SOCKETS)) {
      sockets_count = ExtractInteger(std::string{line});
    }

    if (sockets_count != -1 && cores_per_socket != -1) {
      break;
    }
  }

  if (sockets_count < 0 || cores_per_socket < 0) {
    VMSDK_LOG(NOTICE, nullptr) << "Error while parsing 'lscpu' output:\n"
                               << lscpu_output << "\n"
                               << ". Returning value from "
                                  "std::thread::hardware_concurrency()";
    return std::thread::hardware_concurrency();
  }
  return sockets_count * cores_per_socket;
}

}  // namespace helper

size_t GetPhysicalCPUCoresCount() {
  // Default
  size_t cpu_cores = std::thread::hardware_concurrency();
#if defined(__linux__) && defined(__aarch64__)
  auto res = helper::ExecuteCommand("/usr/bin/lscpu");
  if (!res.ok()) {
    VMSDK_LOG(WARNING, nullptr) << res.status().message()
                                << ". Returning value from "
                                   "std::thread::hardware_concurrency()";
  } else {
    const auto& lscpu_output = res.value();
    cpu_cores = helper::ParseLscpuOutput(lscpu_output);
  }
#elif defined(__linux__)
  std::ifstream cpuinfo("/proc/cpuinfo");
  if (!cpuinfo.is_open()) {
    VMSDK_LOG(NOTICE, nullptr)
        << "Could not read /proc/cpuinfo. Returning value from "
           "std::thread::hardware_concurrency()";
  } else {
    cpu_cores = helper::ParseCPUInfo(cpuinfo);
  }
#endif
  VMSDK_LOG(DEBUG, nullptr) << "Cores count is set to:" << cpu_cores;
  return cpu_cores;
}

}  // namespace vmsdk
