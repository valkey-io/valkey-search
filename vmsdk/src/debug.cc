/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/debug.h"

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <pthread.h>

#include "module_config.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {
namespace debug {

absl::Mutex pause_point_lock;
struct Waiter {
  std::source_location location_;
  pthread_t threadid_;
  absl::Time start_time_;
};

absl::flat_hash_map<std::string, std::vector<Waiter>> pause_point_waiters;

static std::string ToString(const std::source_location& location) {
  std::string os;
  os = "Function: ";
  os += location.function_name();
  os += " File:";
  os += location.file_name();
  os += ":";
  os += std::to_string(location.line());
  return os;
}

void PausePoint(absl::string_view point, std::source_location location) {
  CHECK(!IsMainThread()) << "Pause point not allowed on main thread.";
  {
    absl::MutexLock lock(&pause_point_lock);
    auto it = pause_point_waiters.find(point);
    if (it == pause_point_waiters.end()) {
      return;
    }
    it->second.push_back(Waiter{
        .location_ = location,
        .threadid_ = pthread_self(),
        .start_time_ = absl::Now(),
    });  // Indicate that I'm waiting.
  }
  VMSDK_LOG(WARNING, nullptr)
      << "Waiting at pause point " << point << " @ " << ToString(location);
  auto message_time = absl::Now() + absl::Seconds(10);
  while (true) {
    absl::SleepFor(absl::Milliseconds(1));
    {
      absl::MutexLock lock(&pause_point_lock);
      if (!pause_point_waiters.contains(point)) {
        VMSDK_LOG(WARNING, nullptr)
            << "End of waiting at pause point " << point;
        return;
      }
    }
    if (absl::Now() > message_time) {
      VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 10)
          << "Waiting > 10 seconds at pause point " << point
          << " Location:" << ToString(location);
    }
  }
}

//
// This function is used by the control machinery (FT.DEBUG) to enable/disable
// and test PausePoints.
//
void PausePointControl(absl::string_view point, bool enable) {
  absl::MutexLock lock(&pause_point_lock);
  if (enable) {
    if (!pause_point_waiters.contains(point)) {
      pause_point_waiters[point];
    }
    CHECK(pause_point_waiters.contains(point));
  } else {
    pause_point_waiters.erase(point);
  }
}

//
// This function is used to determine how many threads are waiting at the
// PausePoint
//
absl::StatusOr<size_t> PausePointWaiters(absl::string_view point) {
  absl::MutexLock lock(&pause_point_lock);
  auto it = pause_point_waiters.find(point);
  if (it == pause_point_waiters.end()) {
    return absl::NotFoundError("Pause Point not found");
  } else {
    VMSDK_LOG(WARNING, nullptr)
        << "PAUSEPOINT: " << it->second.size() << " Waiters";
    return it->second.size();
  }
}

//
// General display of state
//
void PausePointList(ValkeyModuleCtx* ctx) {
  absl::MutexLock lock(&pause_point_lock);
  ValkeyModule_ReplyWithArray(ctx, 2 * pause_point_waiters.size());
  for (auto& [pausepoint, waiters] : pause_point_waiters) {
    ValkeyModule_ReplyWithSimpleString(ctx, pausepoint.data());
    ValkeyModule_ReplyWithArray(ctx, 6 * waiters.size());
    for (auto& w : waiters) {
      // ValkeyModule_ReplyWithArray(ctx, 6);
      ValkeyModule_ReplyWithSimpleString(ctx, "Location");
      ValkeyModule_ReplyWithSimpleString(ctx, ToString(w.location_).data());
      ValkeyModule_ReplyWithSimpleString(ctx, "Threadid");
      ValkeyModule_ReplyWithLongLong(ctx, (long long)(uintptr_t)w.threadid_);
      ValkeyModule_ReplyWithSimpleString(ctx, "WaitSeconds");
      ValkeyModule_ReplyWithDouble(
          ctx, absl::ToDoubleSeconds(absl::Now() - w.start_time_));
    }
  }
}

}  // namespace debug
}  // namespace vmsdk
