/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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

#define VMSDK_LOG(log_level, ctx) \
  if (false) VLOG(static_cast<int>(log_level))

// #define VMSDK_LOG(log_level, ctx)  \
//   vmsdk::log_sink.SetContext(ctx); \
//   VLOG(static_cast<int>(log_level)).ToSinkOnly(&vmsdk::log_sink)

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
