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

#include "vmsdk/src/status/status_builder.h"

#include <ios>
#include <ostream>
#include <streambuf>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

OStringStream::Streambuf::int_type OStringStream::Streambuf::overflow(int c) {
  CHECK(str_);
  if (!std::streambuf::traits_type::eq_int_type(
          c, std::streambuf::traits_type::eof())) {
    str_->push_back(static_cast<char>(c));
  }
  return 1;
}

std::streamsize OStringStream::Streambuf::xsputn(const char* s,
                                                 std::streamsize n) {
  CHECK(str_);
  str_->append(s, static_cast<size_t>(n));
  return n;
}

StatusBuilder::Rep::Rep(const absl::Status& s) : status(s) {}
StatusBuilder::Rep::Rep(absl::Status&& s) : status(std::move(s)) {}

StatusBuilder::Rep::Rep(const Rep& r)
    : status(r.status),
      logging_mode(r.logging_mode),
      log_severity(r.log_severity),
      verbose_level(r.verbose_level),
      n(r.n),
      period(r.period),
      stream_message(r.stream_message),
      should_log_stack_trace(r.should_log_stack_trace),
      message_join_style(r.message_join_style) {}

absl::Status StatusBuilder::JoinMessageToStatus(absl::Status s,
                                                absl::string_view msg,
                                                MessageJoinStyle style) {
  if (s.ok() || msg.empty()) {
    return s;
  }

  std::string new_msg;
  if (style == MessageJoinStyle::kAnnotate) {
    std::string formatted_msg{msg};
    if (!s.message().empty()) {
      new_msg = absl::StrFormat("%s; %s", s.message(), formatted_msg);
    } else {
      new_msg = formatted_msg;
    }
  } else if (style == MessageJoinStyle::kPrepend) {
    new_msg = absl::StrCat(msg, s.message());
  } else {
    new_msg = absl::StrCat(s.message(), msg);
  }
  return {s.code(), new_msg};
}

void StatusBuilder::ConditionallyLog(const absl::Status& status) const {
  if (rep_->logging_mode == Rep::LoggingMode::kDisabled) {
    return;
  }

  switch (rep_->logging_mode) {
    case Rep::LoggingMode::kVLog: {
      break;
    }
    case Rep::LoggingMode::kDisabled:
    case Rep::LoggingMode::kLog:
      break;
    case Rep::LoggingMode::kLogEveryN: {
      {
        struct LogSites {
          absl::Mutex mutex;
          absl::flat_hash_map<std::pair<const void*, uint>, uint>
              counts_by_file_and_line ABSL_GUARDED_BY(mutex);
        };
        static auto* log_every_n_sites = new LogSites();

        log_every_n_sites->mutex.Lock();
        const uint count =
            log_every_n_sites
                ->counts_by_file_and_line[{loc_.file_name(), loc_.line()}]++;
        log_every_n_sites->mutex.Unlock();

        if (count % rep_->n != 0) {
          return;
        }
        break;
      }
    }
  }
  RedisModule_Log(nullptr, REDISMODULE_LOGLEVEL_NOTICE, "%s:%d, %s",
                  loc_.file_name(), loc_.line(), status.message().data());
}

absl::Status StatusBuilder::CreateStatusAndConditionallyLog() && {
  absl::Status result = JoinMessageToStatus(
      std::move(rep_->status), rep_->stream_message, rep_->message_join_style);
  ConditionallyLog(result);
  // We consumed the status above, we set it to some error just to prevent
  // people relying on it become OK or something.
  rep_->status = absl::UnknownError("");
  rep_ = nullptr;
  return result;
}

std::ostream& operator<<(std::ostream& os, const StatusBuilder& builder) {
  return os << static_cast<absl::Status>(builder);
}

std::ostream& operator<<(std::ostream& os, StatusBuilder&& builder) {
  return os << static_cast<absl::Status>(std::move(builder));
}
}  // namespace vmsdk
