/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include "vmsdk/src/module_config.h"

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
