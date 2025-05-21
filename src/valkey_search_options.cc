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
#include "valkey_search_options.h"

#include "valkey_search.h"
#include "vmsdk/src/concurrency.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search {
namespace options {

constexpr uint32_t kHNSWDefaultBlockSize{10240};
constexpr uint32_t kHNSWMinimumBlockSize{0};
constexpr uint32_t kMaxThreadsCount{1024};

constexpr absl::string_view kHNSWBlockSizeConfig{"hnsw-block-size"};
constexpr absl::string_view kReaderThreadsConfig{"reader-threads"};
constexpr absl::string_view kWriterThreadsConfig{"writer-threads"};
constexpr absl::string_view kUseCoordinator{"use-coordinator"};
constexpr absl::string_view kLogLevel{"log-level"};
constexpr absl::string_view kThreads{"threads"};

static const int64_t kDefaultThreadsCount = vmsdk::GetPhysicalCPUCoresCount();

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

// Register an enumerator for the log level
static const std::vector<std::string_view> kLogLevelNames = {
    REDISMODULE_LOGLEVEL_WARNING,
    REDISMODULE_LOGLEVEL_NOTICE,
    REDISMODULE_LOGLEVEL_VERBOSE,
    REDISMODULE_LOGLEVEL_DEBUG,
};

static const std::vector<int> kLogLevelValues = {
    static_cast<int>(LogLevel::kWarning), static_cast<int>(LogLevel::kNotice),
    static_cast<int>(LogLevel::kVerbose), static_cast<int>(LogLevel::kDebug)};

void Options::DoInitialize() {
  hnsw_block_size_ =
      config::NumberBuilder(kHNSWBlockSizeConfig,   // name
                            kHNSWDefaultBlockSize,  // default size
                            kHNSWMinimumBlockSize,  // min size
                            UINT_MAX)               // max size
          .WithValidationCallback(ValidateHNSWBlockSize)
          .Build();

  threads_ = config::NumberBuilder(kThreads, -1, 0, kMaxThreadsCount)
                 .WithFlags(REDISMODULE_CONFIG_HIDDEN)
                 .WithModifyCallback([this](long long value) {
                   GetReaderThreadCount()
                       .SetValue(std::max(static_cast<int>(value), 1))
                       .IgnoreError();
                   GetWriterThreadCount()
                       .SetValue(std::max(static_cast<int>(value * 2.5), 1))
                       .IgnoreError();
                   threads_provided_.store(true, std::memory_order_relaxed);
                 })
                 .Build();

  reader_threads_count_ =
      config::NumberBuilder(kReaderThreadsConfig,  // name
                            kDefaultThreadsCount,  // default size
                            1,                     // min size
                            kMaxThreadsCount)      // max size
          .WithModifyCallback(  // set an "On-Modify" callback
              [](auto new_value) {
                UpdateThreadPoolCount(
                    ValkeySearch::Instance().GetReaderThreadPool(), new_value);
              })
          .WithValidationCallback(
              [this](long long) -> absl::Status { return CanUpdateThreads(); })
          .Build();

  writer_threads_count_ =
      config::NumberBuilder(kWriterThreadsConfig,  // name
                            kDefaultThreadsCount,  // default size
                            1,                     // min size
                            kMaxThreadsCount)      // max size
          .WithModifyCallback(  // set an "On-Modify" callback
              [](auto new_value) {
                UpdateThreadPoolCount(
                    ValkeySearch::Instance().GetWriterThreadPool(), new_value);
              })
          .WithValidationCallback(
              [this](long long) -> absl::Status { return CanUpdateThreads(); })
          .Build();

  use_coordinator_ =
      config::BooleanBuilder(kUseCoordinator, false)
          .WithFlags(REDISMODULE_CONFIG_IMMUTABLE)  // can only be set during
                                                    // start-up
          .Build();

  log_level_ =
      config::EnumBuilder(kLogLevel, static_cast<int>(LogLevel::kNotice),
                          kLogLevelNames, kLogLevelValues)
          .WithModifyCallback([](int value) {
            // value as validated in using the validation callback
            if (ValidateLogLevel(value).ok()) {
              vmsdk::InitLogging(nullptr, kLogLevelNames[value].data())
                  .IgnoreError();
            }
          })
          .WithValidationCallback(ValidateLogLevel)
          .Build();
}

/// If user passed "--threads" it triumphs any value provided by
/// "--reader-threads" / "--writer-threads"
absl::Status Options::CanUpdateThreads() const {
  if (IsThreadsProvided()) {
    return absl::AlreadyExistsError(
        "Can not modify thread pool count. Thread count was already set by "
        "--threads");
  }
  return absl::OkStatus();
}

static std::unique_ptr<Options> valkey_search_options;

void Options::InitInstance(std::unique_ptr<Options> instance) {
  vmsdk::config::ModuleConfigManager::InitInstance(
      std::make_unique<vmsdk::config::ModuleConfigManager>());
  valkey_search_options = std::move(instance);
  if (valkey_search_options) {
    // constructing
    valkey_search_options->DoInitialize();
  }
}

Options& Options::Instance() {
  CHECK(valkey_search_options)
      << "Did you forget to call  Options::InitInstance()?";
  return *valkey_search_options;
}

vmsdk::config::Number& Options::GetHNSWBlockSize() {
  return dynamic_cast<vmsdk::config::Number&>(*hnsw_block_size_);
}

vmsdk::config::Number& Options::GetReaderThreadCount() {
  return dynamic_cast<vmsdk::config::Number&>(*reader_threads_count_);
}

vmsdk::config::Number& Options::GetWriterThreadCount() {
  return dynamic_cast<vmsdk::config::Number&>(*writer_threads_count_);
}

const vmsdk::config::Boolean& Options::GetUseCoordinator() {
  return dynamic_cast<const vmsdk::config::Boolean&>(*use_coordinator_);
}

vmsdk::config::Enum& Options::GetLogLevel() {
  return dynamic_cast<vmsdk::config::Enum&>(*log_level_);
}

const vmsdk::config::Number& Options::GetThreads() {
  return dynamic_cast<const vmsdk::config::Number&>(*threads_);
}

}  // namespace options
}  // namespace valkey_search
