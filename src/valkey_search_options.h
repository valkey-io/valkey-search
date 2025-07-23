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

/// Return an immutable reference to the "use-coordinator" flag
const config::Boolean& GetUseCoordinator();

/// Return the log level
config::Enum& GetLogLevel();

/// Reset the state of the options (mainly needed for testing)
absl::Status Reset();

/// Allow delivery of partial results when timeout occurs
const config::Boolean& GetEnablePartialResults();

}  // namespace options
}  // namespace valkey_search
