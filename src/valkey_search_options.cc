/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "valkey_search_options.h"

#include "absl/strings/numbers.h"
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

/// Register the "--utility-threads" flag. Controls the utility thread pool
constexpr absl::string_view kUtilityThreadsConfig{"utility-threads"};
static auto utility_threads_count =
    config::NumberBuilder(kUtilityThreadsConfig,  // name
                          1,                      // default size (1 thread)
                          1,                      // min size
                          kMaxThreadsCount)       // max size
        .WithModifyCallback(                      // set an "On-Modify" callback
            [](auto new_value) {
              UpdateThreadPoolCount(
                  ValkeySearch::Instance().GetUtilityThreadPool(), new_value);
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

/// Should this instance skip corrupted internal update?
constexpr absl::string_view kSkipCorruptedAOFEntries{
    "skip-corrupted-internal-update-entries"};
static auto skip_corrupted_internal_update_entries =
    config::BooleanBuilder(kSkipCorruptedAOFEntries, false).Build();

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

/// Prefer partial results by default of not
/// If set to true, search will use SOMESHARDS if user does not explicitly
/// provide an option in the command
constexpr absl::string_view kEnablePartialResults{"enable-partial-results"};
static config::Boolean prefer_partial_results(kEnablePartialResults, true);

/// Prefer consistenct results by default of not
/// If set to true, search will use CONSISTENT if user does not explicitly
/// provide an option in the command
constexpr absl::string_view kEnableConsistentResults{
    "enable-consistent-results"};
static config::Boolean prefer_consistent_results(kEnableConsistentResults,
                                                 false);

/// Enable search result background cleanup
/// If set to true, search result cleanup will be scheduled on background thread
constexpr absl::string_view kSearchResultBackgroundCleanup{
    "search-result-background-cleanup"};
static config::Boolean search_result_background_cleanup(
    kSearchResultBackgroundCleanup, true);

/// Configure the weight for high priority tasks in thread pools (0-100)
/// Low priority weight = 100 - high_priority_weight
constexpr absl::string_view kHighPriorityWeight{"high-priority-weight"};
static auto high_priority_weight =
    config::NumberBuilder(kHighPriorityWeight, 100, 0,
                          100)  // Default 100%, range 0-100
        .WithModifyCallback([](auto new_value) {
          // Update reader and writer thread pools only
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

/// Register the "--local-fanout-queue-wait-threshold" flag. Controls the queue
/// wait time threshold (in milliseconds) below which local node is preferred in
/// fanout operations
constexpr absl::string_view kLocalFanoutQueueWaitThresholdConfig{
    "local-fanout-queue-wait-threshold"};
constexpr uint32_t kDefaultLocalFanoutQueueWaitThreshold{
    50};  // 50ms queue wait time
constexpr uint32_t kMinimumLocalFanoutQueueWaitThreshold{
    0};  // 0ms queue wait time
constexpr uint32_t kMaximumLocalFanoutQueueWaitThreshold{
    10000};  // 10 seconds queue wait time
static auto local_fanout_queue_wait_threshold =
    vmsdk::config::NumberBuilder(
        kLocalFanoutQueueWaitThresholdConfig,   // name
        kDefaultLocalFanoutQueueWaitThreshold,  // default threshold (50ms)
        kMinimumLocalFanoutQueueWaitThreshold,  // min threshold (0ms)
        kMaximumLocalFanoutQueueWaitThreshold)  // max threshold (10s)
        .Build();

/// Register the "--thread-pool-wait-time-samples" flag. Controls the size of
/// the circular buffer for tracking queue wait times in thread pools
constexpr absl::string_view kThreadPoolWaitTimeSamplesConfig{
    "thread-pool-wait-time-samples"};
constexpr uint32_t kDefaultThreadPoolWaitTimeSamples{100};  // 100 samples
constexpr uint32_t kMinimumThreadPoolWaitTimeSamples{10};  // 10 samples minimum
constexpr uint32_t kMaximumThreadPoolWaitTimeSamples{
    10000};  // 10k samples maximum
static auto thread_pool_wait_time_samples =
    vmsdk::config::NumberBuilder(
        kThreadPoolWaitTimeSamplesConfig,   // name
        kDefaultThreadPoolWaitTimeSamples,  // default size (100)
        kMinimumThreadPoolWaitTimeSamples,  // min size (10)
        kMaximumThreadPoolWaitTimeSamples)  // max size (10k)
        .WithModifyCallback([](uint32_t new_size) {
          // Update thread pools when sample queue size changes
          auto& instance = ValkeySearch::Instance();
          if (auto reader_pool = instance.GetReaderThreadPool()) {
            reader_pool->ResizeSampleQueue(new_size);
          }
          if (auto writer_pool = instance.GetWriterThreadPool()) {
            writer_pool->ResizeSampleQueue(new_size);
          }
          if (auto utility_pool = instance.GetUtilityThreadPool()) {
            utility_pool->ResizeSampleQueue(new_size);
          }
        })
        .Build();

/// Register the "--max-term-expansions" flag. Controls the maximum number of
/// words to search in text operations (prefix, suffix, fuzzy) to limit memory
/// usage
constexpr absl::string_view kMaxTermExpansionsConfig{"max-term-expansions"};
constexpr uint32_t kDefaultMaxTermExpansions{200};     // Default 200 words
constexpr uint32_t kMinimumMaxTermExpansions{1};       // At least 1 word
constexpr uint32_t kMaximumMaxTermExpansions{100000};  // Max 100k words
static auto max_term_expansions =
    config::NumberBuilder(kMaxTermExpansionsConfig,   // name
                          kDefaultMaxTermExpansions,  // default limit (200)
                          kMinimumMaxTermExpansions,  // min limit (1)
                          kMaximumMaxTermExpansions)  // max limit (100k)
        .Build();

/// Register the "search-result-buffer-multiplier" flag
constexpr absl::string_view kSearchResultBufferMultiplierConfig{
    "search-result-buffer-multiplier"};
constexpr absl::string_view kDefaultSearchResultBufferMultiplier{"1.5"};
constexpr double kMinimumSearchResultBufferMultiplier{1.0};
constexpr double kMaximumSearchResultBufferMultiplier{1000.0};
static double search_result_buffer_multiplier{1.5};
static auto search_result_buffer_multiplier_config =
    config::StringBuilder(kSearchResultBufferMultiplierConfig,
                          kDefaultSearchResultBufferMultiplier)
        .WithValidationCallback([](const std::string& value) -> absl::Status {
          double parsed_value;
          if (!absl::SimpleAtod(value, &parsed_value)) {
            return absl::InvalidArgumentError(
                "Buffer multiplier must be a valid number");
          }
          if (parsed_value < kMinimumSearchResultBufferMultiplier ||
              parsed_value > kMaximumSearchResultBufferMultiplier) {
            return absl::InvalidArgumentError(absl::StrFormat(
                "Buffer multiplier must be between %.1f and %.1f",
                kMinimumSearchResultBufferMultiplier,
                kMaximumSearchResultBufferMultiplier));
          }
          return absl::OkStatus();
        })
        .WithModifyCallback([](const std::string& value) {
          double parsed_value;
          CHECK(absl::SimpleAtod(value, &parsed_value));
          search_result_buffer_multiplier = parsed_value;
        })
        .Build();

double GetSearchResultBufferMultiplier() {
  return search_result_buffer_multiplier;
}

/// Register the "drain-mutation-queue-on-load" flag
/// Drain the mutation queue after RDB load
constexpr absl::string_view kDrainMutationQueueOnLoadConfig{
    "drain-mutation-queue-on-load"};
static auto drain_mutation_queue_on_load =
    config::BooleanBuilder(kDrainMutationQueueOnLoadConfig, true)
        .Dev()  // can only be set in debug mode
        .Build();

/// Register the "drain-mutation-queue-on-save" flag
/// Drain the mutation queue before RDB save
constexpr absl::string_view kDrainMutationQueueOnSaveConfig{
    "drain-mutation-queue-on-save"};
static auto drain_mutation_queue_on_save =
    config::BooleanBuilder(kDrainMutationQueueOnSaveConfig, false).Build();

/// Register the "--async-fanout-threshold" flag. Controls the threshold
/// for async fanout operations (minimum number of targets to use async)
constexpr absl::string_view kAsyncFanoutThresholdConfig{
    "async-fanout-threshold"};
constexpr uint32_t kDefaultAsyncFanoutThreshold{30};     // 30 targets
constexpr uint32_t kMinimumAsyncFanoutThreshold{1};      // At least 1 target
constexpr uint32_t kMaximumAsyncFanoutThreshold{10000};  // Max 10k targets
static auto async_fanout_threshold =
    vmsdk::config::NumberBuilder(
        kAsyncFanoutThresholdConfig,   // name
        kDefaultAsyncFanoutThreshold,  // default threshold (30)
        kMinimumAsyncFanoutThreshold,  // min threshold (1)
        kMaximumAsyncFanoutThreshold)  // max threshold (10k)
        .Build();

/// Register the "--max-search-keys-accumulated" flag. Controls the maximum
/// number of keys to accumulate in background threads before content fetching.
/// This controls potential OOM by limiting the result set size before expensive
/// content fetching from the keyspace.
constexpr absl::string_view kMaxSearchKeysAccumulatedConfig{
    "max-search-keys-accumulated"};
constexpr uint32_t kDefaultMaxSearchKeysAccumulated{
    100000};  // 100K keys default
constexpr uint32_t kMinimumMaxSearchKeysAccumulated{1};
constexpr uint32_t kMaximumMaxSearchKeysAccumulated{10000000};  // Max 10M keys
static auto max_search_keys_accumulated =
    vmsdk::config::NumberBuilder(
        kMaxSearchKeysAccumulatedConfig,   // name
        kDefaultMaxSearchKeysAccumulated,  // default limit (100K)
        kMinimumMaxSearchKeysAccumulated,  // min limit (1)
        kMaximumMaxSearchKeysAccumulated)  // max limit (10M)
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

vmsdk::config::Number& GetUtilityThreadCount() {
  return dynamic_cast<vmsdk::config::Number&>(*utility_threads_count);
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

const vmsdk::config::Boolean& GetSkipCorruptedInternalUpdateEntries() {
  return dynamic_cast<const vmsdk::config::Boolean&>(
      *skip_corrupted_internal_update_entries);
}

vmsdk::config::Enum& GetLogLevel() {
  return dynamic_cast<vmsdk::config::Enum&>(*log_level);
}

absl::Status Reset() {
  VMSDK_RETURN_IF_ERROR(use_coordinator->SetValue(false));
  VMSDK_RETURN_IF_ERROR(rdb_load_skip_index->SetValue(false));
  return absl::OkStatus();
}

const vmsdk::config::Boolean& GetPreferPartialResults() {
  return static_cast<vmsdk::config::Boolean&>(prefer_partial_results);
}

const vmsdk::config::Boolean& GetPreferConsistentResults() {
  return static_cast<vmsdk::config::Boolean&>(prefer_consistent_results);
}

const vmsdk::config::Boolean& GetSearchResultBackgroundCleanup() {
  return static_cast<vmsdk::config::Boolean&>(search_result_background_cleanup);
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

vmsdk::config::Number& GetLocalFanoutQueueWaitThreshold() {
  return dynamic_cast<vmsdk::config::Number&>(
      *local_fanout_queue_wait_threshold);
}

vmsdk::config::Number& GetThreadPoolWaitTimeSamples() {
  return dynamic_cast<vmsdk::config::Number&>(*thread_pool_wait_time_samples);
}

vmsdk::config::Number& GetMaxTermExpansions() {
  return dynamic_cast<vmsdk::config::Number&>(*max_term_expansions);
}

const vmsdk::config::Boolean& GetDrainMutationQueueOnSave() {
  return dynamic_cast<const vmsdk::config::Boolean&>(
      *drain_mutation_queue_on_save);
}

const vmsdk::config::Boolean& GetDrainMutationQueueOnLoad() {
  return dynamic_cast<const vmsdk::config::Boolean&>(
      *drain_mutation_queue_on_load);
}

vmsdk::config::Number& GetAsyncFanoutThreshold() {
  return dynamic_cast<vmsdk::config::Number&>(*async_fanout_threshold);
}

vmsdk::config::Number& GetMaxSearchKeysBeforeContent() {
  return dynamic_cast<vmsdk::config::Number&>(*max_search_keys_accumulated);
}

}  // namespace options
}  // namespace valkey_search
