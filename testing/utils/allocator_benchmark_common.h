/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_TESTING_UTILS_ALLOCATOR_BENCHMARK_COMMON_H_
#define VALKEYSEARCH_TESTING_UTILS_ALLOCATOR_BENCHMARK_COMMON_H_

#include <benchmark/benchmark.h>

#include <cstddef>
#include <memory>
#include <string>

namespace valkey_search::utils {
namespace {

constexpr int kNumInstances = 3;
constexpr int kInitialEntries = 1000000;
constexpr int kNumOperations = 100000;

enum class AllocatorType {
  kDefault,
};

// Helper to generate key strings
inline std::string GetKey(int id) { return "key_" + std::to_string(id); }

}  // namespace
}  // namespace valkey_search::utils

#endif  // VALKEYSEARCH_TESTING_UTILS_ALLOCATOR_BENCHMARK_COMMON_H_
