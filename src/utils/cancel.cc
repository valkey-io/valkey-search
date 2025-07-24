/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/cancel.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"
#include "vmsdk/src/info.h"

namespace valkey_search {
namespace cancel {

static vmsdk::config::Number kPollFrequency("timeout-poll-frequency", 100, 1, std::numeric_limits<long long>::max());
static vmsdk::config::Boolean kTestForceTimeout("test-force-timeout", false);

static vmsdk::info_field::Integer kTimeouts("timeouts", "cancel-timeouts", vmsdk::info_field::IntegerBuilder().Dev());
static vmsdk::info_field::Integer kgRPCCancels("timeouts", "cancel-grpc", vmsdk::info_field::IntegerBuilder().Dev());
static vmsdk::info_field::Integer kForceCancels("timeouts", "cancel-forced", vmsdk::info_field::IntegerBuilder().Dev());

//
// A Concrete implementation of Token that can be used to cancel  
// operations based on a timeout and optionally a gRPC server handle
//
struct TokenImpl : public Base {
  TokenImpl(long long deadline_ms, grpc::CallbackServerContext *context) : deadline_ms_(deadline_ms), context_(context) {}

  void Cancel() override {
    is_cancelled_ = true; // Once cancelled, stay cancelled
  }

  bool IsCancelled() override {
    if (++count_ > kPollFrequency.GetValue()) {
      count_ = 0;
      if (!is_cancelled_) {
        if (ValkeyModule_Milliseconds() >= deadline_ms_) {
          is_cancelled_ = true; // Operation should be cancelled
          kTimeouts.Increment(1);
          VMSDK_LOG(DEBUG, nullptr) << "CANCEL: Timeout reached, cancelling operation";
        } else if (context_ && context_->IsCancelled()) {
          is_cancelled_ = true; // Operation should be cancelled
          kgRPCCancels.Increment(1);
          VMSDK_LOG(DEBUG, nullptr) << "CANCEL: gRPC context cancelled";
        } else if (kTestForceTimeout.GetValue()) {
          is_cancelled_ = true; // Operation should be cancelled
          kForceCancels.Increment(1);
          VMSDK_LOG(WARNING, nullptr) << "CANCEL: Timeout forced";
        }
      }
    }
    return is_cancelled_;
  }

  bool is_cancelled_{false}; // Once cancelled, stay cancelled

  long long deadline_ms_;
  grpc::CallbackServerContext *context_;
  int count_{0};
};

Token Make(long long timeout_ms, grpc::CallbackServerContext *context) {
  long long deadline_ms = timeout_ms + ValkeyModule_Milliseconds();
  VMSDK_LOG(WARNING, nullptr) << " Creating timeout " << timeout_ms;
  return std::make_shared<TokenImpl>(deadline_ms, context);
}

} // namespace cancel
} // namespace valkey_search
