
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

#ifndef VMSDK_SRC_UTILS_H_
#define VMSDK_SRC_UTILS_H_
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
namespace vmsdk {

class StopWatch {
 public:
  StopWatch() { Reset(); }
  ~StopWatch() = default;
  void Reset() { start_time_ = absl::Now(); }
  absl::Duration Duration() const { return absl::Now() - start_time_; }

 private:
  absl::Time start_time_;
};
// Timer creation from background threads is not safe. The event loop of Redis/
// Valkey releases the GIL, and during this period also checks the timer data
// structure for the next pending timer, meaning there is no way to safely
// create a timer from a background thread.
//
// This function creates a timer from a background thread by creating a task
// that is added to the event loop, which then creates the timer.
int StartTimerFromBackgroundThread(RedisModuleCtx *ctx, mstime_t period,
                                   RedisModuleTimerProc callback, void *data);
struct TimerDeletionTask {
  RedisModuleCtx *ctx;
  RedisModuleTimerID timer_id;
  absl::AnyInvocable<void(void *)> user_data_deleter;
};
int StopTimerFromBackgroundThread(
    RedisModuleCtx *ctx, RedisModuleTimerID timer_id,
    absl::AnyInvocable<void(void *)> user_data_deleter);

void TrackCurrentAsMainThread();
bool IsMainThread();
inline void VerifyMainThread() { CHECK(IsMainThread()); }

// MainThreadAccessGuard ensures that all access to the underlying data
// structure is done on the main thread.
template <typename T>
class MainThreadAccessGuard {
 public:
  MainThreadAccessGuard() = default;
  MainThreadAccessGuard(const T &var) : var_(var) {}
  MainThreadAccessGuard(T &&var) noexcept : var_(std::move(var)) {}
  MainThreadAccessGuard &operator=(MainThreadAccessGuard<T> const &other) {
    VerifyMainThread();
    var_ = other.var_;
    return *this;
  }
  MainThreadAccessGuard &operator=(MainThreadAccessGuard<T> &&other) noexcept {
    VerifyMainThread();
    var_ = std::move(other.var_);
    return *this;
  }
  T &Get() {
    VerifyMainThread();
    return var_;
  }
  const T &Get() const {
    VerifyMainThread();
    return var_;
  }

 private:
  T var_;
};

int RunByMain(absl::AnyInvocable<void()> fn, bool force_async = false);

std::string WrongArity(absl::string_view cmd);

inline std::ostream &operator<<(std::ostream &os, RedisModuleString *s) {
  return os << (*(std::string *)s);
}

//
// Parse out a hash tag from a string view
//
std::optional<absl::string_view> ParseHashTag(absl::string_view);

bool IsRealUserClient(RedisModuleCtx *ctx);
bool MultiOrLua(RedisModuleCtx *ctx);

std::optional<std::string> JsonUnquote(absl::string_view sv);

struct JsonQuotedStringView {
  absl::string_view view_;
  friend std::ostream &operator<<(std::ostream &os,
                                  const JsonQuotedStringView &js);
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_UTILS_H_
