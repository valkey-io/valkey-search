/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/utils.h"

#include <string>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
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
  return absl::StrCat("ERR wrong number of arguments for ", cmd, " command");
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

}  // namespace vmsdk
