/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/log.h"

#include <cerrno>
#include <cstddef>
#include <fstream>
#include <optional>
#include <string>
#include <unordered_map>

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
      return VALKEYMODULE_LOGLEVEL_WARNING;
    case 1:
      return VALKEYMODULE_LOGLEVEL_NOTICE;
    case 2:
      return VALKEYMODULE_LOGLEVEL_VERBOSE;
    case 3:
      return VALKEYMODULE_LOGLEVEL_DEBUG;
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
    {absl::AsciiStrToLower(VALKEYMODULE_LOGLEVEL_WARNING), LogLevel::kWarning},
    {absl::AsciiStrToLower(VALKEYMODULE_LOGLEVEL_NOTICE), LogLevel::kNotice},
    {absl::AsciiStrToLower(VALKEYMODULE_LOGLEVEL_VERBOSE), LogLevel::kVerbose},
    {absl::AsciiStrToLower(VALKEYMODULE_LOGLEVEL_DEBUG), LogLevel::kDebug},
};

absl::StatusOr<std::string> FetchEngineLogLevel(ValkeyModuleCtx* ctx) {
  auto reply = vmsdk::UniquePtrValkeyCallReply(
      ValkeyModule_Call(ctx, "CONFIG", "cc", "GET", "loglevel"));
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

  ValkeyModuleCallReply* loglevel_reply =
      ValkeyModule_CallReplyArrayElement(reply.get(), 1);

  if (loglevel_reply == nullptr ||
      ValkeyModule_CallReplyType(loglevel_reply) != VALKEYMODULE_REPLY_STRING) {
    return absl::NotFoundError(
        absl::StrCat("Log level value is missing or not a string."));
  }

  size_t len;
  const char* loglevel_str =
      ValkeyModule_CallReplyStringPtr(loglevel_reply, &len);
  return std::string(loglevel_str, len);
}

absl::Status InitLogging(ValkeyModuleCtx* ctx,
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
  //
  // Improve life by dumping the process mapping tables, this allows addr2line
  // on loaded modules to work.
  //
  std::ifstream maps("/proc/self/maps");
  std::string line;
  std::unordered_map<std::string, std::string> seen_bases;
  while (std::getline(maps, line)) {
    if (line.find(".so") != std::string::npos) {
      size_t dash = line.find('-');
      size_t path_start = line.find('/');
      if (dash != std::string::npos && path_start != std::string::npos) {
        std::string base_addr = line.substr(0, dash);
        std::string module = line.substr(path_start);
        if (module.find("/lib") != 0 && module.find("/usr") != 0) {
          auto it = seen_bases.find(base_addr);
          if (it == seen_bases.end() &&
              line.find(" r-x") != std::string::npos) {
            seen_bases[base_addr] = module;
            VMSDK_LOG(NOTICE, ctx)
                << ">>> Loaded Module: " << module << " Base: 0x" << base_addr;
          }
        }
      }
    }
  }

  return absl::OkStatus();
}

const char* ReportedLogLevel(int log_level) {
  if (sink_options.log_level_specified) {
    return VALKEYMODULE_LOGLEVEL_WARNING;
  }
  return ToStrLogLevel(log_level);
}

void ValkeyLogSink::Send(const absl::LogEntry& entry) {
  ValkeyModule_Log(ctx_, ReportedLogLevel(entry.verbosity()), "%s",
                   GetSinkFormatter()(entry).c_str());
}

void ValkeyIOLogSink::Send(const absl::LogEntry& entry) {
  ValkeyModule_LogIOError(io_, ReportedLogLevel(entry.verbosity()), "%s",
                          GetSinkFormatter()(entry).c_str());
}

}  // namespace vmsdk
