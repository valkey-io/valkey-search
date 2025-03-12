/*
 * Copyright (c) 2025, ValkeySearch contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "vmsdk/src/concurrency.h"

#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

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
  value.erase(0, value.find_first_not_of(" \t"));
  value.erase(value.find_last_not_of(" \t") + 1);

  // Ensure the value contains only digits
  if (value.empty() ||
      value.find_first_not_of("0123456789") != std::string::npos) {
    return -1;  // Invalid number
  }

  return std::stoi(value);
}

size_t ParseCPUInfo(std::istream& cpuinfo) {
  std::string line;
  int physical_id = -1;
  int cores_per_cpu = -1;
  std::unordered_map<int, int> physical_cpu_cores;
  size_t total_physical_cores = 0;

  while (std::getline(cpuinfo, line)) {
    if (line.find("physical id") != std::string::npos) {
      physical_id = ExtractInteger(line);
    } else if (line.find("cpu cores") != std::string::npos) {
      cores_per_cpu = ExtractInteger(line);

      if (physical_id != -1 && cores_per_cpu > 0) {
        physical_cpu_cores[physical_id] = cores_per_cpu;
      }
    }
  }

  for (const auto& [id, core_count] : physical_cpu_cores) {
    total_physical_cores += core_count;
  }

  return total_physical_cores;
}

}  // namespace helper

size_t GetPhysicalCPUCoresCount() {
#ifdef __linux__
  std::ifstream cpuinfo("/proc/cpuinfo");
  if (!cpuinfo.is_open()) {
    VMSDK_LOG(WARNING, nullptr)
        << "Could not read /proc/cpuinfo. Returning value from "
           "std::thread::hardware_concurrency()";
    return std::thread::hardware_concurrency();
  }
  return helper::ParseCPUInfo(cpuinfo);
#else
  return std::thread::hardware_concurrency();
#endif
}

}  // namespace vmsdk
