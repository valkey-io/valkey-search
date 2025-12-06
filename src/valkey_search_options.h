/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include "vmsdk/src/module_config.h"
#include "vmsdk/src/thread_pool.h"

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

/// Return the max time in seconds that the worker thread pool is
/// suspended after fork started
config::Number& GetMaxWorkerSuspensionSecs();

/// Return an immutable reference to the "use-coordinator" flag
const config::Boolean& GetUseCoordinator();

// Return the configuration entry for async client block threshold
config::Number& GetAsyncClientBlockThreshold();

// Return the configuration entry for async client ratio sync weight
config::Number& GetAsyncClientRatioSyncWeight();

// Return the configuration entry for async client ratio async weight
config::Number& GetAsyncClientRatioAsyncWeight();

// Return true if async client throttling is enabled
vmsdk::config::Boolean& IsAsyncClientThrottlingEnabled();

// Return the configuration entry for async client throttling hysteresis
// percentage
config::Number& GetAsyncClientThrottlingHysteresisPercentage();

// Return the configuration entry for async client priority
config::Enum& GetAsyncClientPriority();

/// Convert config enum value to Priority
vmsdk::ThreadPool::Priority GetAsyncClientPriorityValue();

/// Return the configuration entry for skipping vector index RDB loading
const config::Boolean& GetSkipIndexLoad();

/// Return a mutable reference for testing
config::Boolean& GetSkipIndexLoadMutable();

/// Return the log level
config::Enum& GetLogLevel();

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

}  // namespace options
}  // namespace valkey_search
