/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/utils.h"

#include <dirent.h>

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <string>
#include <utility>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/log.h"
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

int StartTimerFromBackgroundThread(ValkeyModuleCtx *ctx, mstime_t period,
                                   ValkeyModuleTimerProc callback, void *data) {
  return RunByMain([ctx, period, callback, data]() mutable {
    ValkeyModule_CreateTimer(ctx, period, callback, data);
  });
}

int StopTimerFromBackgroundThread(
    ValkeyModuleCtx *ctx, ValkeyModuleTimerID timer_id,
    absl::AnyInvocable<void(void *)> user_data_deleter) {
  return RunByMain([ctx, timer_id,
                    user_data_deleter =
                        std::move(user_data_deleter)]() mutable {
    void *timer_data;
    if (ValkeyModule_StopTimer(ctx, timer_id, &timer_data) == VALKEYMODULE_OK) {
      if (user_data_deleter) {
        user_data_deleter(timer_data);
      }
    }
  });
}

bool verifyLoadedOnlyOnce() {
  static bool prev_loaded = false;
  if (prev_loaded) {
    return false;
  }
  prev_loaded = true;
  return true;
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
    return VALKEYMODULE_OK;
  }
  auto call_by_main = new absl::AnyInvocable<void()>(std::move(fn));
  return ValkeyModule_EventLoopAddOneShot(RunAnyInvocable, call_by_main);
}

std::string WrongArity(absl::string_view cmd) {
  return absl::StrCat("ERR wrong number of arguments for '", cmd, "' command");
}

bool IsRealUserClient(ValkeyModuleCtx *ctx) {
  auto client_id = ValkeyModule_GetClientId(ctx);
  if (client_id == 0) {
    return false;
  }
  if (ValkeyModule_IsAOFClient(client_id)) {
    return false;
  }
  if ((ValkeyModule_GetContextFlags(ctx) & VALKEYMODULE_CTX_FLAGS_REPLICATED)) {
    return false;
  }
  return true;
}

bool MultiOrLua(ValkeyModuleCtx *ctx) {
  return (ValkeyModule_GetContextFlags(ctx) &
          (VALKEYMODULE_CTX_FLAGS_MULTI | VALKEYMODULE_CTX_FLAGS_LUA)) != 0;
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

//
// This is done "C" style to avoid memory allocations, so that it
// can be part of a crash dump.
//
size_t DisplayAsSIBytes(size_t bytes, char *buffer, size_t buffer_size) {
  double value = bytes;
  const size_t Ki = 1024;
  const size_t Mi = 1024 * Ki;
  const size_t Gi = 1024 * Mi;
  const size_t Ti = 1024 * Gi;
  const size_t Pi = 1024 * Ti;

  if (bytes >= Pi) {
    return snprintf(buffer, buffer_size, "%.2fPiB", value / Pi);
  } else if (bytes >= Ti) {
    return snprintf(buffer, buffer_size, "%.2fTiB", value / Ti);
  } else if (bytes >= Gi) {
    return snprintf(buffer, buffer_size, "%.2fGiB", value / Gi);
  } else if (bytes >= Mi) {
    return snprintf(buffer, buffer_size, "%.2fMiB", value / Mi);
  } else if (bytes >= Ki) {
    return snprintf(buffer, buffer_size, "%.2fKiB", value / Ki);
  } else {
    return snprintf(buffer, buffer_size, "%ld", bytes);
  }
}

absl::Status VerifyRange(long long num_value, std::optional<long long> min,
                         std::optional<long long> max) {
  if (min.has_value() && num_value < min.value()) {
    return absl::OutOfRangeError("Invalid range: Value below minimum");
  }
  if (max.has_value() && max.value() < num_value) {
    return absl::OutOfRangeError("Invalid range: Value above maximum");
  }
  return absl::OkStatus();
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

static const char *hex_chars = "0123456789abcdef";

std::string PrintableBytes(absl::string_view sv) {
  std::string result;
  for (const auto &c : sv) {
    if (std::isprint(c)) {
      if (c == '\\') {
        result += c;
      }
      result += c;
    } else {
      result += '\\';
      switch (c) {
        case '\n':
          result += 'n';
          break;
        case '\r':
          result += 'r';
          break;
        case '\t':
          result += 't';
          break;
        default:
          result += hex_chars[(c >> 4) & 0xf];
          result += hex_chars[c & 0xf];
          break;
      }
    }
  }
  return result;
}

std::string StringToHex(std::string_view s) {
  std::string result;
  for (auto &c : s) {
    if (&c != s.begin()) {
      result += ' ';
    }
    result += hex_chars[(c >> 4) & 0xF];
    result += hex_chars[c & 0xF];
  }
  return result;
}

#ifdef __APPLE__
absl::StatusOr<std::vector<thread_act_t>> GetThreadsByName(
    absl::string_view thread_name_pattern) {
  if (thread_name_pattern.empty()) {
    return std::vector<thread_act_t>();
  }

  thread_act_array_t all_threads;
  mach_msg_type_number_t thread_count;

  if (task_threads(mach_task_self(), &all_threads, &thread_count) !=
      KERN_SUCCESS) {
    return absl::InternalError("Failed to enumerate threads");
  }

  std::vector<thread_act_t> matching_threads;

  for (mach_msg_type_number_t i = 0; i < thread_count; i++) {
    char thread_name[256];
    pthread_t pthread = pthread_from_mach_thread_np(all_threads[i]);
    if (!pthread ||
        pthread_getname_np(pthread, thread_name, sizeof(thread_name)) != 0) {
      continue;
    }

    if (std::string_view(thread_name).find(thread_name_pattern) ==
        std::string_view::npos) {
      continue;
    }

    matching_threads.push_back(all_threads[i]);
  }

  vm_deallocate(mach_task_self(), (vm_address_t)all_threads,
                thread_count * sizeof(thread_act_t));
  return matching_threads;
}

#elif __linux__
absl::StatusOr<std::vector<std::string>> GetThreadsByName(
    absl::string_view thread_name_pattern) {
  namespace fs = std::filesystem;
  if (thread_name_pattern.empty()) {
    return std::vector<std::string>();
  }
  std::vector<std::string> result;
  DIR *dir = opendir("/proc/self/task");
  if (!dir) {
    return absl::InternalError("Failed to open /proc/self/task");
  }
  while (auto *entry = readdir(dir)) {
    if (entry->d_name[0] == '.') {
      continue;
    }
    std::string comm_path =
        absl::StrFormat("/proc/self/task/%s/comm", entry->d_name);
    std::ifstream comm_file(comm_path);
    std::string thread_name;
    if (!std::getline(comm_file, thread_name)) {
      continue;
    }

    VMSDK_LOG(NOTICE, nullptr) << "thread name " << thread_name;

    if (thread_name.find(thread_name_pattern) == std::string_view::npos) {
      continue;
    }
    std::string stat_path =
        absl::StrFormat("/proc/self/task/%s/stat", entry->d_name);
    result.push_back(stat_path);
  }
  closedir(dir);
  return result;
}
#endif

}  // namespace vmsdk
