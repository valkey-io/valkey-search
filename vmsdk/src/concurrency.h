/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_CONCURRENCY_H_
#define VMSDK_SRC_CONCURRENCY_H_

#include <cstddef>
#include <istream>
#include <string>

namespace vmsdk {

// Returns the number of physical CPU cores.
size_t GetPhysicalCPUCoresCount();

namespace helper {

// Parses /proc/cpuinfo content and extracts the number of physical cores.
size_t ParseCPUInfo(std::istream& cpuinfo);

// Parses the output of the command `lscpu` and return the number of physical
// cores.
size_t ParseLscpuOutput(const std::string& lscpu_output);

// Extracts an integer value from a formatted key-value string.
int ExtractInteger(const std::string& line);

}  // namespace helper
}  // namespace vmsdk

#endif  // VMSDK_SRC_CONCURRENCY_H_
