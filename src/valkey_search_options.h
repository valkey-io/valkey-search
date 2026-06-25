/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include "vmsdk/src/info.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/utils.h"

namespace valkey_search {
namespace options {

namespace config = vmsdk::config;

/// Return the value of the Query String Bytes configuration
uint32_t GetQueryStringBytes();

/// Return the value of the Query String Bytes configuration
uint32_t GetQueryStringBytes();

/// Return a mutable reference to the HNSW resize configuration parameter
config::Number& GetHNSWBlockSize();

/// Return the configuration entry that allows the caller to control the
/// number of reader threads
config::Number& GetReaderThreadCount();

/// Return the configuration entry that allows the caller to control the
/// number of writer threads
config::Number& GetWriterThreadCount();

/// Return the configuration entry that allows the caller to control the
/// number of utility threads
config::Number& GetUtilityThreadCount();

/// Return the configuration entry for search result background cleanup
const config::Boolean& GetSearchResultBackgroundCleanup();

/// Return the max time in seconds that the worker thread pool is
/// suspended after fork started
config::Number& GetMaxWorkerSuspensionSecs();

/// Return an immutable reference to the "use-coordinator" flag
const config::Boolean& GetUseCoordinator();

/// Return the configuration entry for skipping vector index RDB loading
const config::Boolean& GetSkipIndexLoad();

/// Return a mutable reference for testing
config::Boolean& GetSkipIndexLoadMutable();

/// Return the configuration entry for skipping corrupted AOF entries
const config::Boolean& GetSkipCorruptedInternalUpdateEntries();

/// Return the log level
config::Enum& GetLogLevel();

/// Return the configuration entry for HNSW allow_replace_deleted flag
const config::Boolean& GetHNSWAllowReplaceDeleted();

/// Return a mutable reference for testing
config::Boolean& GetHNSWAllowReplaceDeletedMutable();

/// Reset the state of the options (mainly needed for testing)
absl::Status Reset();

/// Default option of delivering partial results when timeout occurs
const config::Boolean& GetPreferPartialResults();

/// Default option of delivering consistent results when timeout occurs
const config::Boolean& GetPreferConsistentResults();

/// Return the configuration entry for high priority weight in thread pools
config::Number& GetHighPriorityWeight();

/// Return the timeout for ft.info fanout command
config::Number& GetFTInfoTimeoutMs();

/// Return the rpc timeout for ft.info fanout command
config::Number& GetFTInfoRpcTimeoutMs();

/// Return the queue wait threshold for preferring local node in fanout
/// (milliseconds)
config::Number& GetLocalFanoutQueueWaitThreshold();

/// Return the sample queue size for thread pool wait time tracking
config::Number& GetThreadPoolWaitTimeSamples();

/// Return the maximum number of words to search in text operations (prefix,
/// suffix, fuzzy)
config::Number& GetMaxTermExpansions();

/// Return the minimum TAG prefix length for wildcard queries (excluding '*')
config::Number& GetTagMinPrefixLength();

/// Return the search result buffer multiplier value
double GetSearchResultBufferMultiplier();

/// Return the prefiltering threshold ratio value
double GetPrefilteringThresholdRatio();

/// Return the configuration entry for draining mutation queue on save
const config::Boolean& GetDrainMutationQueueOnSave();

/// Return the configuration entry for draining mutation queue on load
const config::Boolean& GetDrainMutationQueueOnLoad();

/// Return the fanout data uniformity configuration (0-100)
config::Number& GetFanoutDataUniformity();

/// Return the minimum index size for applying fanout uniformity logic
config::Number& GetFanoutUniformityMinIndexSize();

/// Return the configuration entry for max mutation queue size during restore
config::Number& GetMaxMutationQueueSizeOnRestore();

/// Return the configuration entry for RDB write v2
const config::Boolean& GetRdbWriteV2();

/// Return the configuration entry for RDB read v2
const config::Boolean& GetRdbReadV2();

/// Return the threshold for async fanout operations
config::Number& GetAsyncFanoutThreshold();

/// Return the pool size for per-word Postings bucket mutexes
config::Number& GetRaxTargetMutexPoolSize();

/// Return the maximum number of keys to accumulate before content fetching
config::Number& GetMaxNonVectorSearchResultsFetched();

/// Return the mutation weight for vector index types
config::Number& GetMutationWeightVector();

/// Return the mutation weight for text index types
config::Number& GetMutationWeightText();

/// Return the mutation weight for numeric index types
config::Number& GetMutationWeightNumeric();

/// Return the mutation weight for tag index types
config::Number& GetMutationWeightTag();

/// Return the recursion depth of the query string from FT.SEARCH and
/// FT.AGGREGATE commands
config::Number& GetQueryStringDepth();

/// Return the configuration entry that controls compatibility-bug emulation.
/// See COMPATIBILITY.md for the semantics.
config::Version& GetEmulateRelease();

/// Return true if search.emulate-release >= version. Use this at sites that
/// gate the compatible vs legacy behavior of a fix described in
/// COMPATIBILITY.md.
bool EnabledInVersion(vmsdk::ValkeyVersion version);

/// Convenience overload — the constexpr ValkeyVersion ctor lets the compiler
/// fold the comparand at every call site.
inline bool EnabledInVersion(int major, int minor, int patch) {
  return EnabledInVersion(vmsdk::ValkeyVersion(major, minor, patch));
}

}  // namespace options
}  // namespace valkey_search

// Compatibility-defect fix gate. See COMPATIBILITY.md ("Compatibility
// Defects"). Selects between the corrected and legacy implementations of a
// compatibility-bug fix based on the runtime value of search.emulate-release:
//
//   * emulate-release >= major.minor.patch  -> run `fixed_fn()`
//   * otherwise                             -> run `old_fn()` and bump the
//                                              per-site INFO counter named
//                                              `label`.
//
// Both callables must be nullary and return the same type (void is allowed).
// `label` must be a string literal; the INFO field is registered under the
// "compatibility" section with the name `"compatibility-" + label`, so every
// metric emitted by this macro shares a common prefix. The counter is
// constructed lazily on the first macro invocation (regardless of path) and
// only incremented when the legacy branch runs.
//
// Usage:
//   auto result = VALKEY_SEARCH_COMPATIBILITY_FIX(
//       1, 2, 2, "ft_search_attr_order_pre_1_2_2",
//       [&] { return new_path(args); },
//       [&] { return old_path(args); });
//   // Surfaces as: compatibility:compatibility-ft_search_attr_order_pre_1_2_2
#define VALKEY_SEARCH_COMPATIBILITY_FIX(major, minor, patch, label, fixed_fn, \
                                        old_fn)                               \
  ([&]() {                                                                    \
    static constexpr ::vmsdk::ValkeyVersion kVsCompatFixVersion{              \
        (major), (minor), (patch)};                                           \
    static ::vmsdk::info_field::Integer kVsCompatFixCounter{                  \
        "compatibility", "compatibility-" label,                              \
        ::vmsdk::info_field::IntegerBuilder().App()};                         \
    if (::valkey_search::options::EnabledInVersion(kVsCompatFixVersion)) {    \
      return (fixed_fn)();                                                    \
    }                                                                         \
    kVsCompatFixCounter.Increment();                                          \
    return (old_fn)();                                                        \
  })()
