/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "valkey_search_options.h"

#include "valkey_search.h"
#include "vmsdk/src/concurrency.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search {
namespace options {

constexpr uint32_t kHNSWDefaultBlockSize{10240};
constexpr uint32_t kHNSWMinimumBlockSize{0};

namespace {

/// Check that the new value for configuration item `hnsw-block-size` confirms
/// to the allowed values.
absl::Status ValidateHNSWBlockSize(long long new_value) {
  if (new_value < kHNSWMinimumBlockSize || new_value > UINT32_MAX) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Block size must be between %u and %u",
                        kHNSWMinimumBlockSize, UINT32_MAX));
  }
  return absl::OkStatus();
}

/// Resize `pool` to match its new value
void UpdateThreadPoolCount(vmsdk::ThreadPool* pool, long long new_value) {
  if (!pool) {
    return;
  }
  pool->Resize(new_value);
}

absl::Status ValidateLogLevel(const int value) {
  if (value >= static_cast<int>(LogLevel::kWarning) &&
      value <= static_cast<int>(LogLevel::kDebug)) {
    return absl::OkStatus();
  }
  return absl::OutOfRangeError(
      absl::StrFormat("Log level of: %d is out of range", value));
}

}  // namespace

// Configuration entries
namespace config = vmsdk::config;

/// Register the "--query-string-bytes" flag. Controls the length of the query
/// string of the FT.SEARCH cmd.
constexpr absl::string_view kQueryStringBytesConfig{"query-string-bytes"};
constexpr uint32_t kMaxQueryStringBytes{10240};
constexpr uint32_t kMinimumQueryStringBytes{1};
static auto query_string_bytes =
    config::NumberBuilder(kQueryStringBytesConfig,   // name
                          kMaxQueryStringBytes,      // default size
                          kMinimumQueryStringBytes,  // min size
                          kMaxQueryStringBytes)      // max size
        .WithValidationCallback(CHECK_RANGE(kMinimumQueryStringBytes,
                                            kMaxQueryStringBytes,
                                            kQueryStringBytesConfig))
        .Build();

constexpr absl::string_view kHNSWBlockSizeConfig{"hnsw-block-size"};
static auto hnsw_block_size =
    config::NumberBuilder(kHNSWBlockSizeConfig,   // name
                          kHNSWDefaultBlockSize,  // default size
                          kHNSWMinimumBlockSize,  // min size
                          UINT_MAX)               // max size
        .WithValidationCallback(ValidateHNSWBlockSize)
        .Build();

static const int64_t kDefaultThreadsCount = vmsdk::GetPhysicalCPUCoresCount();
constexpr uint32_t kMaxThreadsCount{1024};

/// Register the "--reader-threads" flag. Controls the readers thread pool
constexpr absl::string_view kReaderThreadsConfig{"reader-threads"};
static auto reader_threads_count =
    config::NumberBuilder(kReaderThreadsConfig,  // name
                          kDefaultThreadsCount,  // default size
                          1,                     // min size
                          kMaxThreadsCount)      // max size
        .WithModifyCallback(                     // set an "On-Modify" callback
            [](auto new_value) {
              UpdateThreadPoolCount(
                  ValkeySearch::Instance().GetReaderThreadPool(), new_value);
            })
        .Build();

/// Register the "--reader-threads" flag. Controls the writer thread pool
constexpr absl::string_view kWriterThreadsConfig{"writer-threads"};
static auto writer_threads_count =
    config::NumberBuilder(kWriterThreadsConfig,  // name
                          kDefaultThreadsCount,  // default size
                          1,                     // min size
                          kMaxThreadsCount)      // max size
        .WithModifyCallback(                     // set an "On-Modify" callback
            [](auto new_value) {
              UpdateThreadPoolCount(
                  ValkeySearch::Instance().GetWriterThreadPool(), new_value);
            })
        .Build();

/// Should this instance use coordinator?
constexpr absl::string_view kUseCoordinator{"use-coordinator"};
static auto use_coordinator =
    config::BooleanBuilder(kUseCoordinator, false)
        .WithFlags(VALKEYMODULE_CONFIG_HIDDEN)  // can only be set during
                                                // start-up
        .Build();

// Register an enumerator for the log level
static const std::vector<std::string_view> kLogLevelNames = {
    VALKEYMODULE_LOGLEVEL_WARNING,
    VALKEYMODULE_LOGLEVEL_NOTICE,
    VALKEYMODULE_LOGLEVEL_VERBOSE,
    VALKEYMODULE_LOGLEVEL_DEBUG,
};

static const std::vector<int> kLogLevelValues = {
    static_cast<int>(LogLevel::kWarning), static_cast<int>(LogLevel::kNotice),
    static_cast<int>(LogLevel::kVerbose), static_cast<int>(LogLevel::kDebug)};

/// Control the modules log level verbosity
constexpr absl::string_view kLogLevel{"log-level"};
static auto log_level =
    config::EnumBuilder(kLogLevel, static_cast<int>(LogLevel::kNotice),
                        kLogLevelNames, kLogLevelValues)
        .WithModifyCallback([](int value) {
          auto res = ValidateLogLevel(value);
          if (!res.ok()) {
            VMSDK_LOG(WARNING, nullptr)
                << "Invalid value: '" << value << "' provided to enum: '"
                << kLogLevel << "'. " << res.message();
            return;
          }
          // "value" is already validated using "ValidateLogLevel" callback
          // below
          auto log_level_str = kLogLevelNames[value];
          res = vmsdk::InitLogging(nullptr, log_level_str.data());
          if (!res.ok()) {
            VMSDK_LOG(WARNING, nullptr)
                << "Failed to initialize log with new value: " << log_level_str
                << ". " << res.message();
          }
        })
        .WithValidationCallback(ValidateLogLevel)
        .Build();

uint32_t GetQueryStringBytes() { return query_string_bytes->GetValue(); }

vmsdk::config::Number& GetHNSWBlockSize() {
  return dynamic_cast<vmsdk::config::Number&>(*hnsw_block_size);
}

vmsdk::config::Number& GetReaderThreadCount() {
  return dynamic_cast<vmsdk::config::Number&>(*reader_threads_count);
}

vmsdk::config::Number& GetWriterThreadCount() {
  return dynamic_cast<vmsdk::config::Number&>(*writer_threads_count);
}

const vmsdk::config::Boolean& GetUseCoordinator() {
  return dynamic_cast<const vmsdk::config::Boolean&>(*use_coordinator);
}

vmsdk::config::Enum& GetLogLevel() {
  return dynamic_cast<vmsdk::config::Enum&>(*log_level);
}

absl::Status Reset() {
  VMSDK_RETURN_IF_ERROR(use_coordinator->SetValue(false));
  return absl::OkStatus();
}

}  // namespace options
}  // namespace valkey_search
