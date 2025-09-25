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
constexpr uint32_t kDefaultFTInfoTimeoutMs{5000};
constexpr uint32_t kMinimumFTInfoTimeoutMs{100};
constexpr uint32_t kMaximumFTInfoTimeoutMs{300000};
constexpr uint32_t kDefaultFTInfoRpcTimeoutMs{2500};
constexpr uint32_t kMinimumFTInfoRpcTimeoutMs{100};
constexpr uint32_t kMaximumFTInfoRpcTimeoutMs{300000};

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
constexpr uint32_t kDefaultQueryStringBytes{10240};
constexpr uint32_t kMinimumQueryStringBytes{1};
static auto query_string_bytes =
    config::NumberBuilder(kQueryStringBytesConfig,   // name
                          kDefaultQueryStringBytes,  // default size
                          kMinimumQueryStringBytes,  // min size
                          UINT_MAX)                  // max size
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

/// Register the "--writer-threads" flag. Controls the writer thread pool
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

/// Register the "--max-worker-suspension-secs" flag.
/// Controls the resumption of the worker thread pool:
///   - If max-worker-suspension-secs > 0, resume the workers either when the
///     fork is died or after max-worker-suspension-secs seconds passed.
///   - If max-worker-suspension-secs <= 0, resume the workers when the fork
///     is born.
constexpr absl::string_view kMaxWorkerSuspensionSecs{
    "max-worker-suspension-secs"};
static auto max_worker_suspension_secs =
    config::Number(kMaxWorkerSuspensionSecs,  // name
                   60,                        // default value
                   0,                         // min value
                   3600);                     // max value

/// Should this instance use coordinator?
constexpr absl::string_view kUseCoordinator{"use-coordinator"};
static auto use_coordinator = config::BooleanBuilder(kUseCoordinator, false)
                                  .Hidden()  // can only be set during start-up
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

/// Should this instance skip loading index data from RDB?
constexpr absl::string_view kReIndexVectorRDBLoad{"skip-rdb-load"};
static auto rdb_load_skip_index =
    config::BooleanBuilder(kReIndexVectorRDBLoad, false).Build();

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

/// Should timeouts return partial results OR generate a TIMEOUT error?
constexpr absl::string_view kEnablePartialResults{"enable-partial-results"};
static config::Boolean enable_partial_results(kEnablePartialResults, true);

/// Configure the weight for high priority tasks in thread pools (0-100)
/// Low priority weight = 100 - high_priority_weight
constexpr absl::string_view kHighPriorityWeight{"high-priority-weight"};
static auto high_priority_weight =
    config::NumberBuilder(kHighPriorityWeight, 100, 0,
                          100)  // Default 100%, range 0-100
        .WithModifyCallback([](auto new_value) {
          // Update both reader and writer thread pools
          auto reader_pool = ValkeySearch::Instance().GetReaderThreadPool();
          auto writer_pool = ValkeySearch::Instance().GetWriterThreadPool();
          if (reader_pool) {
            reader_pool->SetHighPriorityWeight(new_value);
          }
          if (writer_pool) {
            writer_pool->SetHighPriorityWeight(new_value);
          }
        })
        .Build();

/// Register the "--ft-info-timeout-ms" flag. Controls the timeout for FT.INFO
/// operations
constexpr absl::string_view kFTInfoTimeoutMsConfig{"ft-info-timeout-ms"};
static auto ft_info_timeout_ms =
    vmsdk::config::NumberBuilder(
        kFTInfoTimeoutMsConfig,   // name
        kDefaultFTInfoTimeoutMs,  // default timeout (5 seconds)
        kMinimumFTInfoTimeoutMs,  // min timeout (100ms)
        kMaximumFTInfoTimeoutMs)  // max timeout (5 minutes)
        .Build();

/// Register the "--ft-info-rpc-timeout-ms" flag. Controls the timeout for
/// FT.INFO operations
constexpr absl::string_view kFTInfoRpcTimeoutMsConfig{"ft-info-rpc-timeout-ms"};
static auto ft_info_rpc_timeout_ms =
    vmsdk::config::NumberBuilder(
        kFTInfoRpcTimeoutMsConfig,   // name
        kDefaultFTInfoRpcTimeoutMs,  // default timeout (2.5 seconds)
        kMinimumFTInfoRpcTimeoutMs,  // min timeout (100ms)
        kMaximumFTInfoRpcTimeoutMs)  // max timeout (5 minutes)
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

vmsdk::config::Number& GetMaxWorkerSuspensionSecs() {
  return max_worker_suspension_secs;
}

const vmsdk::config::Boolean& GetUseCoordinator() {
  return dynamic_cast<const vmsdk::config::Boolean&>(*use_coordinator);
}

const vmsdk::config::Boolean& GetSkipIndexLoad() {
  return dynamic_cast<const vmsdk::config::Boolean&>(*rdb_load_skip_index);
}

vmsdk::config::Boolean& GetSkipIndexLoadMutable() {
  return dynamic_cast<vmsdk::config::Boolean&>(*rdb_load_skip_index);
}

vmsdk::config::Enum& GetLogLevel() {
  return dynamic_cast<vmsdk::config::Enum&>(*log_level);
}

absl::Status Reset() {
  VMSDK_RETURN_IF_ERROR(use_coordinator->SetValue(false));
  VMSDK_RETURN_IF_ERROR(rdb_load_skip_index->SetValue(false));
  return absl::OkStatus();
}

const vmsdk::config::Boolean& GetEnablePartialResults() {
  return static_cast<vmsdk::config::Boolean&>(enable_partial_results);
}

vmsdk::config::Number& GetHighPriorityWeight() {
  return dynamic_cast<vmsdk::config::Number&>(*high_priority_weight);
}

vmsdk::config::Number& GetFTInfoTimeoutMs() {
  return dynamic_cast<vmsdk::config::Number&>(*ft_info_timeout_ms);
}

vmsdk::config::Number& GetFTInfoRpcTimeoutMs() {
  return dynamic_cast<vmsdk::config::Number&>(*ft_info_rpc_timeout_ms);
}

}  // namespace options
}  // namespace valkey_search
