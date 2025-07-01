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

#include "vmsdk/src/utils.h"

#include <iomanip>
#include <string>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace {
static bool set_main_thread = false;
thread_local static bool is_main_thread = false;

void RunAnyInvocable(void *invocable) {
  absl::AnyInvocable<void()> *fn = (absl::AnyInvocable<void()> *)invocable;
  (*fn)();
  delete fn;
}
}  // namespace

int StartTimerFromBackgroundThread(RedisModuleCtx *ctx, mstime_t period,
                                   RedisModuleTimerProc callback, void *data) {
  return RunByMain([ctx, period, callback, data]() mutable {
    RedisModule_CreateTimer(ctx, period, callback, data);
  });
}

int StopTimerFromBackgroundThread(
    RedisModuleCtx *ctx, RedisModuleTimerID timer_id,
    absl::AnyInvocable<void(void *)> user_data_deleter) {
  return RunByMain([ctx, timer_id,
                    user_data_deleter =
                        std::move(user_data_deleter)]() mutable {
    void *timer_data;
    if (RedisModule_StopTimer(ctx, timer_id, &timer_data) == REDISMODULE_OK) {
      if (user_data_deleter) {
        user_data_deleter(timer_data);
      }
    }
  });
}

void TrackCurrentAsMainThread() {
  CHECK(!set_main_thread);
  is_main_thread = true;
  set_main_thread = true;
}

bool IsMainThread() { return is_main_thread; }

int RunByMain(absl::AnyInvocable<void()> fn, bool force_async) {
  if (IsMainThread() && !force_async) {
    fn();
    return REDISMODULE_OK;
  }
  auto call_by_main = new absl::AnyInvocable<void()>(std::move(fn));
  return RedisModule_EventLoopAddOneShot(RunAnyInvocable, call_by_main);
}

std::string WrongArity(absl::string_view cmd) {
  return absl::StrCat("ERR wrong number of arguments for ", cmd, " command");
}

bool IsRealUserClient(RedisModuleCtx *ctx) {
  auto client_id = RedisModule_GetClientId(ctx);
  if (client_id == 0) {
    return false;
  }
  if (RedisModule_IsAOFClient(client_id)) {
    return false;
  }
  if ((RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_REPLICATED)) {
    return false;
  }
  return true;
}

bool MultiOrLua(RedisModuleCtx *ctx) {
  return (RedisModule_GetContextFlags(ctx) &
          (REDISMODULE_CTX_FLAGS_MULTI | REDISMODULE_CTX_FLAGS_LUA)) != 0;
}

std::optional<absl::string_view> ParseHashTag(absl::string_view s) {
  auto start = s.find('{');
  // Does a left bracket exist and is NOT the last character
  if (start == absl::string_view::npos || (start + 1) == s.size()) {
    return std::nullopt;
  }
  auto end = s.find('}', start + 1);
  if (end == absl::string_view::npos) {
    return std::nullopt;
  }
  auto tag_size = end - (start + 1);
  if (tag_size == 0) {
    return std::nullopt;
  }
  return s.substr(start + 1, tag_size);
}

std::ostream &operator<<(std::ostream &os, const JsonQuotedStringView &s) {
  os << '"';
  for (auto itr = s.view_.begin(); itr != s.view_.end(); ++itr) {
    unsigned char c = *itr;
    switch (c) {
      case '"':
        os << "\\\"";
        break;
      case '\\':
        os << "\\\\";
        break;
      case '/':
        os << "/";
        break;
      case '\n':
        os << "\\n";
        break;
      case '\t':
        os << "\\t";
        break;
      case '\r':
        os << "\\r";
        break;
      case '\f':
        os << "\\f";
        break;
      case '\b':
        os << "\\b";
        break;
      default:
        if (c > 0x1f && c < 0x80) {
          os << c;
        } else {
          uint16_t codepoint;
          if (c <= 0x1f) {
            codepoint = int(c);
          } else if ((c & 0b11100000) == 0b11000000) {
            // Start of 2 byte sequence
            itr++;
            if (itr == s.view_.end() || ((*itr) & 0b11000000) != 0b10000000) {
              VMSDK_LOG(DEBUG, nullptr) << "Invalid Json Encode";
              codepoint = 0xFFFF;
            } else {
              codepoint = ((c & 0b11111) << 6) | ((*itr) & 0b00111111);
            }
          } else if ((c & 0b11110000) == 0b11100000) {
            // Start of 3 byte sequence
            ++itr;
            if (itr == s.view_.end() || ((*itr) & 0b11000000) != 0b10000000) {
              VMSDK_LOG(DEBUG, nullptr) << "Invalid Json Encode";
              codepoint = 0xffff;
            } else {
              unsigned char d = *itr;
              ++itr;
              if (itr == s.view_.end() || ((*itr) & 0b11000000) != 0b10000000) {
                VMSDK_LOG(DEBUG, nullptr) << "Invalid Json Encode";
                codepoint = 0xFFFF;
              } else {
                codepoint = ((c & 0b00001111) << 12) | ((d & 0b00111111) << 6) |
                            ((*itr) & 0b00111111);
              }
            }
          } else {
            VMSDK_LOG(DEBUG, nullptr) << "Invalid Json Encode";
            codepoint = 0xFFFF;
          }
          os << "\\u" << std::hex << std::setfill('0') << std::setw(4)
             << codepoint;
        }
        break;
    }
  }
  return os << '"';
}

std::optional<std::string> JsonUnquote(absl::string_view sv) {
  std::string result;
  result.reserve(sv.length());

  for (auto itr = sv.begin(); itr != sv.end(); ++itr) {
    if (*itr != '\\') {
      result += *itr;
    } else {
      ++itr;
      if (itr == sv.end()) {
        VMSDK_LOG(DEBUG, nullptr) << "Invalid JSON (\\ at end)";
        return std::nullopt;
      }
      switch (*itr) {
        case 'b':
          result += '\b';
          break;
        case 'n':
          result += '\n';
          break;
        case 'f':
          result += '\f';
          break;
        case '"':
          result += '"';
          break;
        case '\\':
          result += '\\';
          break;
        case 't':
          result += '\t';
          break;
        case 'r':
          result += '\r';
          break;
        case '/':
          result += '/';
          break;
        case 'u': {
          unsigned unicode = 0;
          for (auto unichar = 0; unichar < 4; ++unichar) {
            itr++;
            if (itr == sv.end()) {
              VMSDK_LOG(DEBUG, nullptr) << "Invalid JSON (Short unicode)";
              return std::nullopt;
            }
            char c = *itr;
            if (c >= '0' && c <= '9') {
              unicode = (unicode << 4) | (*itr - '0');
            } else if (c >= 'a' && c <= 'f') {
              unicode = (unicode << 4) | (*itr - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
              unicode = (unicode << 4) | (*itr - 'A' + 10);
            } else {
              VMSDK_LOG(DEBUG, nullptr)
                  << "Invalid JSON (invalid unicode char)";
              return std::nullopt;
            }
          }
          if (unicode < 0x100) {
            result += char(unicode);
          } else if (unicode < 0x1000) {
            result += char(0b11000000 | (unicode >> 6));
            result += char(0b10000000 | (unicode & 0b111111));
          } else {
            result += char(0b11100000) | (unicode >> 12);
            result += char(0b10000000) | ((unicode >> 6) & 0b111111);
            result += char(0b10000000) | (unicode & 0b111111);
          }
          break;
        }
      }
    }
  }
  return result;
}

}  // namespace vmsdk
