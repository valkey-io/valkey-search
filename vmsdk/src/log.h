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

#ifndef VMSDK_SRC_LOG_H_
#define VMSDK_SRC_LOG_H_

#include <optional>
#include <string>

#include "absl/log/log.h"
#include "absl/log/log_entry.h"
#include "absl/log/log_sink.h"
#include "absl/status/status.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

enum class LogLevel : int {
  kWarning = 0,
  kNotice = 1,
  kVerbose = 2,
  kDebug = 3,
};
// NOLINTNEXTLINE
constexpr LogLevel WARNING = static_cast<LogLevel>(LogLevel::kWarning);
// NOLINTNEXTLINE
constexpr LogLevel NOTICE = static_cast<LogLevel>(LogLevel::kNotice);
// NOLINTNEXTLINE
constexpr LogLevel VERBOSE = static_cast<LogLevel>(LogLevel::kVerbose);
// NOLINTNEXTLINE
constexpr LogLevel DEBUG = static_cast<LogLevel>(LogLevel::kDebug);

namespace vmsdk {

const char* ToStrLogLevel(int log_level);

using LogFormatterFunc = std::string (*)(const absl::LogEntry&);

LogFormatterFunc GetSinkFormatter();
void SetSinkFormatter(LogFormatterFunc formatter);

class ValkeyLogSink : public absl::LogSink {
 public:
  void Send(const absl::LogEntry& entry) override;
  void SetContext(ValkeyModuleCtx* ctx) { ctx_ = ctx; }

 private:
  ValkeyModuleCtx* ctx_{nullptr};
};

class ValkeyIOLogSink : public absl::LogSink {
 public:
  void Send(const absl::LogEntry& entry) override;
  void SetModuleIO(ValkeyModuleIO* io) { io_ = io; }

 private:
  ValkeyModuleIO* io_{nullptr};
};

absl::Status InitLogging(
    ValkeyModuleCtx* ctx,
    std::optional<std::string> log_level_str = std::nullopt);

static thread_local ValkeyLogSink log_sink;
static thread_local ValkeyIOLogSink io_log_sink;

}  // namespace vmsdk

#define VMSDK_LOG(log_level, ctx)  \
  vmsdk::log_sink.SetContext(ctx); \
  VLOG(static_cast<int>(log_level)).ToSinkOnly(&vmsdk::log_sink)

#define VMSDK_LOG_EVERY_N(log_level, ctx, n) \
  vmsdk::log_sink.SetContext(ctx);           \
  VLOG_EVERY_N(static_cast<int>(log_level), n).ToSinkOnly(&vmsdk::log_sink)

#define VMSDK_LOG_EVERY_N_SEC(log_level, ctx, n) \
  vmsdk::log_sink.SetContext(ctx);               \
  VLOG_EVERY_N_SEC(static_cast<int>(log_level), n).ToSinkOnly(&vmsdk::log_sink)

#define VMSDK_IO_LOG(log_level, module_io)   \
  vmsdk::io_log_sink.SetModuleIO(module_io); \
  VLOG(static_cast<int>(log_level)).ToSinkOnly(&vmsdk::io_log_sink)

#define VMSDK_IO_LOG_EVERY_N(log_level, module_io, n) \
  vmsdk::io_log_sink.SetModuleIO(module_io);          \
  VLOG_EVERY_N(static_cast<int>(log_level), n).ToSinkOnly(&vmsdk::io_log_sink)

#define VMSDK_IO_LOG_EVERY_N_SEC(log_level, module_io, n) \
  vmsdk::io_log_sink.SetModuleIO(module_io);              \
  VLOG_EVERY_N_SEC(static_cast<int>(log_level), n)        \
      .ToSinkOnly(&vmsdk::io_log_sink)

#endif  // VMSDK_SRC_LOG_H_
