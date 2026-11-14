/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/cancel.h"

#include <atomic>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"

namespace valkey_search {
namespace cancel {

CONTROLLED_SIZE_T(TimeoutPollFrequency, 100);
CONTROLLED_BOOLEAN(ForceTimeout, false);

static vmsdk::info_field::Integer Timeouts(
    "timeouts", "cancel-timeouts", vmsdk::info_field::IntegerBuilder().Dev());

TEST_COUNTER(gRPCCancels);
TEST_COUNTER(ForceCancels);

// Tokens are made and polled on worker and gRPC threads, where
// ValkeyModule_Milliseconds is unsafe: Valkey's ustime() caches the time of day
// in unsynchronized statics.
static long long NowMs() { return absl::ToUnixMillis(absl::Now()); }

//
// A Concrete implementation of Token that can be used to cancel
// operations based on a timeout and optionally a gRPC server handle
//
struct TokenImpl : public Base {
  TokenImpl(long long deadline_ms, grpc::CallbackServerContext *context)
      : deadline_ms_(deadline_ms), context_(context) {}

  void Cancel() override {
    is_cancelled_.store(true, std::memory_order_relaxed);
  }

  // A token is shared by all threads working on one query, so the state is
  // atomic, and only the thread that first sets is_cancelled_ counts and logs
  // the cancellation.
  bool IsCancelled() override {
    // count_ only throttles polling, so a lost increment between threads is
    // harmless; load/store avoids a locked read-modify-write.
    int count = count_.load(std::memory_order_relaxed) + 1;
    count_.store(count, std::memory_order_relaxed);
    if (count > TimeoutPollFrequency.GetValue()) {
      count_.store(0, std::memory_order_relaxed);
      if (!is_cancelled_.load(std::memory_order_relaxed)) {
        if (NowMs() >= deadline_ms_) {
          if (SetCancelled()) {
            Timeouts.Increment(1);
            VMSDK_LOG(DEBUG, nullptr)
                << "CANCEL: Timeout reached, cancelling operation";
          }
        } else if (context_ && context_->IsCancelled()) {
          if (SetCancelled()) {
            gRPCCancels.Increment(1);
            VMSDK_LOG(DEBUG, nullptr) << "CANCEL: gRPC context cancelled";
          }
        } else if (ForceTimeout.GetValue()) {
          if (SetCancelled()) {
            ForceCancels.Increment(1);
            VMSDK_LOG(WARNING, nullptr) << "CANCEL: Timeout forced";
          }
        } else if (!vmsdk::IsMainThread()) {
          PAUSEPOINT("Cancel");
        }
      }
    }
    return is_cancelled_.load(std::memory_order_relaxed);
  }

  // Returns true only for the call that changed the state.
  bool SetCancelled() {
    return !is_cancelled_.exchange(true, std::memory_order_relaxed);
  }

  std::atomic<bool> is_cancelled_{false};  // Once cancelled, stay cancelled

  long long deadline_ms_;
  grpc::CallbackServerContext *context_;
  std::atomic<int> count_{0};
};

Token Make(long long timeout_ms, grpc::CallbackServerContext *context) {
  long long deadline_ms = timeout_ms + NowMs();
  return std::make_shared<TokenImpl>(deadline_ms, context);
}

}  // namespace cancel
}  // namespace valkey_search
