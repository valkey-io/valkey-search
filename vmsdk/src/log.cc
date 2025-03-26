/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#include "vmsdk/src/log.h"

#include <cerrno>
#include <cstddef>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/globals.h"
#include "absl/log/log_entry.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

const char* ToStrLogLevel(int log_level) {
  switch (log_level) {
    case 0:
      return REDISMODULE_LOGLEVEL_WARNING;
    case 1:
      return REDISMODULE_LOGLEVEL_NOTICE;
    case 2:
      return REDISMODULE_LOGLEVEL_VERBOSE;
    case 3:
      return REDISMODULE_LOGLEVEL_DEBUG;
  }
  CHECK(false);
}

static inline std::string DefaultSinkFormatter(const absl::LogEntry& entry) {
  pthread_t thread_id = pthread_self();
  return absl::StrFormat(
      "[%s], tid: %lu, %s:%d: %s", ToStrLogLevel(entry.verbosity()),
      reinterpret_cast<unsigned long>(thread_id), entry.source_filename(),
      entry.source_line(), entry.text_message());
}

struct SinkOptions {
  LogFormatterFunc formatter{DefaultSinkFormatter};
  bool log_level_specified{false};
};

static SinkOptions sink_options;

LogFormatterFunc GetSinkFormatter() { return sink_options.formatter; }
void SetSinkFormatter(LogFormatterFunc formatter) {
  if (formatter) {
    sink_options.formatter = formatter;
  } else {
    sink_options.formatter = DefaultSinkFormatter;
  }
}

const absl::flat_hash_map<std::string, LogLevel> kLogLevelMap = {
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_WARNING), LogLevel::kWarning},
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_NOTICE), LogLevel::kNotice},
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_VERBOSE), LogLevel::kVerbose},
    {absl::AsciiStrToLower(REDISMODULE_LOGLEVEL_DEBUG), LogLevel::kDebug},
};

absl::StatusOr<std::string> FetchEngineLogLevel(RedisModuleCtx* ctx) {
  auto reply = vmsdk::UniquePtrRedisCallReply(
      RedisModule_Call(ctx, "CONFIG", "cc", "GET", "loglevel"));
  if (reply == nullptr) {
    if (errno == EINVAL) {
      return absl::InvalidArgumentError(
          "Error fetch Valkey Engine log level: EINVAL (command "
          "name is invalid, the format specifier uses characters "
          "that are not recognized, or the command is called with "
          "the wrong number of arguments)");
    } else {
      return absl::InternalError(
          absl::StrCat("Error fetch Valkey Engine log level: errno=", errno));
    }
  }

  RedisModuleCallReply* loglevel_reply =
      RedisModule_CallReplyArrayElement(reply.get(), 1);

  if (loglevel_reply == nullptr ||
      RedisModule_CallReplyType(loglevel_reply) != REDISMODULE_REPLY_STRING) {
    return absl::NotFoundError(
        absl::StrCat("Log level value is missing or not a string."));
  }

  size_t len;
  const char* loglevel_str =
      RedisModule_CallReplyStringPtr(loglevel_reply, &len);
  return std::string(loglevel_str, len);
}

absl::Status InitLogging(RedisModuleCtx* ctx,
                         std::optional<std::string> log_level_str) {
  if (!log_level_str.has_value()) {
    auto engine_log_level = FetchEngineLogLevel(ctx);
    if (!engine_log_level.ok()) {
      // It is possible we can't get it, e.g. if the CONFIG command is renamed.
      // In such a case, we log a warning and default to NOTICE.
      VMSDK_LOG(WARNING, ctx)
          << "Failed to fetch Valkey Engine log level, "
          << engine_log_level.status() << ", using default log level: "
          << ToStrLogLevel(static_cast<int>(LogLevel::kNotice));
      log_level_str = ToStrLogLevel(static_cast<int>(LogLevel::kNotice));
    } else {
      log_level_str = engine_log_level.value();
    }
    sink_options.log_level_specified = false;
  } else {
    sink_options.log_level_specified = true;
  }
  auto itr = kLogLevelMap.find(absl::AsciiStrToLower(log_level_str.value()));
  if (itr == kLogLevelMap.end()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unknown severity `", log_level_str.value(), "`"));
  }
  absl::SetGlobalVLogLevel(static_cast<int>(itr->second));

  return absl::OkStatus();
}

const char* ReportedLogLevel(int log_level) {
  if (sink_options.log_level_specified) {
    return REDISMODULE_LOGLEVEL_WARNING;
  }
  return ToStrLogLevel(log_level);
}

void ValkeyLogSink::Send(const absl::LogEntry& entry) {
  RedisModule_Log(ctx_, ReportedLogLevel(entry.verbosity()), "%s",
                  GetSinkFormatter()(entry).c_str());
}

void ValkeyIOLogSink::Send(const absl::LogEntry& entry) {
  RedisModule_LogIOError(io_, ReportedLogLevel(entry.verbosity()), "%s",
                         GetSinkFormatter()(entry).c_str());
}

}  // namespace vmsdk
