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
#pragma once

#include "vmsdk/src/module_config.h"

namespace valkey_search {
namespace options {

namespace config = vmsdk::config;

class Options {
 public:
  Options() = default;
  Options(const Options&) = delete;
  Options& operator=(const Options) = delete;

  static void InitInstance(std::unique_ptr<Options> instance);
  static Options& Instance();

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

  /// [DEPRECATED] Return an immutable reference to the deprecated flag:
  /// "threads"
  const config::Number& GetThreads();

 private:
  /// If user passed "--threads" it triumphs any value provided by
  /// "--reader-threads" / "--writer-threads"
  absl::Status CanUpdateThreads() const;

  bool IsThreadsProvided() const {
    return threads_provided_.load(std::memory_order_relaxed);
  }

  /// Do build the configuration entries
  void DoInitialize();

  /// Controls the HNSW resize increments
  std::unique_ptr<config::ConfigBase<long long>> hnsw_block_size_;
  /// **DEPRECATED** controls both the reader & writer pool size
  /// use --reader/--writer-threads instead
  std::unique_ptr<config::ConfigBase<long long>> threads_;
  /// Controls the reader threads count
  std::unique_ptr<config::ConfigBase<long long>> reader_threads_count_;
  /// Controls the writer threads count
  std::unique_ptr<config::ConfigBase<long long>> writer_threads_count_;
  /// Should this instance launch coordinator?
  std::unique_ptr<config::ConfigBase<bool>> use_coordinator_;
  /// Control the modules log level verbosity
  std::unique_ptr<config::ConfigBase<int>> log_level_;
  std::atomic_bool threads_provided_{false};
};

}  // namespace options
}  // namespace valkey_search
