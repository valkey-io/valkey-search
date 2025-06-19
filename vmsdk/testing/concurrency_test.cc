/*
 * Copyright (c) 2025, valkey-search contributors
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

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <thread>

namespace vmsdk {
namespace helper {

// Test fixture for ParseCPUInfo
class ParseCPUInfoTest : public ::testing::Test {
 protected:
  size_t CallParseCPUInfo(const std::string& cpuinfo_content) {
    std::istringstream stream(cpuinfo_content);
    return ParseCPUInfo(stream);
  }
};

// Test fixture for ParseLscpuOutput
class ParseLscpuTestTest : public ::testing::Test {
 protected:
  size_t CallParseLscpuOutput(const std::string& lscpu_output) {
    return ParseLscpuOutput(lscpu_output);
  }
};

// Test fixture for ExtractInteger
class ExtractIntegerTest : public ::testing::Test {
 protected:
  int CallExtractInteger(const std::string& line) {
    return ExtractInteger(line);
  }
};

// Valid integer extraction
TEST_F(ExtractIntegerTest, ValidInteger) {
  EXPECT_EQ(CallExtractInteger("cpu cores   : 4"), 4);
  EXPECT_EQ(CallExtractInteger("physical id : 12"), 12);
}

// Leading and trailing spaces
TEST_F(ExtractIntegerTest, TrimmingSpaces) {
  EXPECT_EQ(CallExtractInteger("cpu cores   :    16   "), 16);
}

// Non-numeric values should return -1
TEST_F(ExtractIntegerTest, NonNumericValues) {
  EXPECT_EQ(CallExtractInteger("cpu cores   : four"), -1);
  EXPECT_EQ(CallExtractInteger("cpu cores   : NaN"), -1);
}

// Missing colon should return -1
TEST_F(ExtractIntegerTest, MissingColon) {
  EXPECT_EQ(CallExtractInteger("cpu cores 4"), -1);
}

// Empty string should return -1
TEST_F(ExtractIntegerTest, EmptyString) {
  EXPECT_EQ(CallExtractInteger(""), -1);
}

// Test multiple CPUs with correct data
TEST_F(ParseCPUInfoTest, MultipleCPUs) {
  std::string cpuinfo =
      "processor   : 0\n"
      "physical id : 0\n"
      "cpu cores   : 4\n"
      "\n"
      "processor   : 1\n"
      "physical id : 1\n"
      "cpu cores   : 6\n";

  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 10);  // 4 + 6 = 10 cores
}

// Test a single CPU
TEST_F(ParseCPUInfoTest, SingleCPU) {
  std::string cpuinfo =
      "processor   : 0\n"
      "physical id : 0\n"
      "cpu cores   : 8\n";

  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 8);
}

// Test missing 'cpu cores' field (should return 0)
TEST_F(ParseCPUInfoTest, MissingCpuCores) {
  std::string cpuinfo =
      "processor   : 0\n"
      "physical id : 0\n";

  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 0);
}

// Test empty /proc/cpuinfo (should return 0)
TEST_F(ParseCPUInfoTest, EmptyCPUInfo) {
  std::string cpuinfo = "";
  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 0);
}

// Test malformed CPU info (should ignore invalid values)
TEST_F(ParseCPUInfoTest, MalformedCPUInfo) {
  std::string cpuinfo =
      "processor: 0\n"
      "physical id 0\n"      // Missing ':'
      "cpu cores : four\n";  // Non-numeric

  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 0);
}

// Test malformed CPU info (should ignore invalid values)
TEST_F(ParseCPUInfoTest, MalformedLineCPUInfo) {
  std::string cpuinfo =
      "processor: 0"
      "physical id 0"      // Missing ':'
      "cpu cores : four";  // Non-numeric

  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 0);
}

// Test case where there are valid and invalid entries mixed
TEST_F(ParseCPUInfoTest, MixedValidInvalidData) {
  std::string cpuinfo =
      "processor   : 0\n"
      "physical id : 0\n"
      "cpu cores   : 4\n"
      "\n"
      "processor   : 1\n"
      "physical id : 1\n"
      "cpu cores   : invalid\n"  // Malformed entry
      "\n"
      "processor   : 2\n"
      "physical id : 2\n"
      "cpu cores   : 6\n";

  EXPECT_EQ(CallParseCPUInfo(cpuinfo), 10);  // Only 4 + 6 should count
}

// Test parsing lscpu output with valid output
TEST_F(ParseLscpuTestTest, StandardLscpuOutput) {
  std::string output =
      R"#(
Core(s) per socket:  8
Socket(s):           1
NUMA node(s):        1
Vendor ID:           ARM
Model:               1
Stepping:            r1p1
BogoMIPS:            2100.00
)#";
  EXPECT_EQ(CallParseLscpuOutput(output), 8);  // 8 cores * 1 socket
}

// Test parsing lscpu output with missing "Cores" entry
TEST_F(ParseLscpuTestTest, MissingCoresEntry) {
  auto default_value = std::thread::hardware_concurrency();
  std::string output = R"#(
Socket(s):           1
NUMA node(s):        1
Vendor ID:           ARM
Model:               1
Stepping:            r1p1
BogoMIPS:            2100.00
)#";
  EXPECT_EQ(CallParseLscpuOutput(output), default_value);
}

// Test parsing lscpu output with missing "Socket" entry
TEST_F(ParseLscpuTestTest, MissingSocketEntry) {
  auto default_value = std::thread::hardware_concurrency();
  std::string output =
      R"#(
Core(s) per socket:  8
NUMA node(s):        1
Vendor ID:           ARM
Model:               1
Stepping:            r1p1
BogoMIPS:            2100.00
)#";
  EXPECT_EQ(CallParseLscpuOutput(output), default_value);
}

// Test parsing lscpu output with missing both socket & cores entries
TEST_F(ParseLscpuTestTest, MissingBothEntries) {
  auto default_value = std::thread::hardware_concurrency();
  std::string output =
      R"#(
NUMA node(s):        1
Vendor ID:           ARM
Model:               1
Stepping:            r1p1
BogoMIPS:            2100.00
)#";
  EXPECT_EQ(CallParseLscpuOutput(output), default_value);
}

}  // namespace helper
}  // namespace vmsdk
