/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/cancel.h"

#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace cancel {

CONTROLLED_SIZE_T(TimeoutPollFrequency, 100);
CONTROLLED_BOOLEAN(ForceTimeout, false);

static vmsdk::info_field::Integer Timeouts(
    "timeouts", "cancel-timeouts", vmsdk::info_field::IntegerBuilder().Dev());

TEST_COUNTER(gRPCCancels);
TEST_COUNTER(ForceCancels);

//
// A Concrete implementation of Token that can be used to cancel
// operations based on a timeout and optionally a gRPC server handle
//
struct TokenImpl : public Base {
  TokenImpl(long long deadline_ms, grpc::CallbackServerContext *context)
      : deadline_ms_(deadline_ms), context_(context) {}

  void Cancel() override {
    is_cancelled_ = true;  // Once cancelled, stay cancelled
  }

  bool IsCancelled() override {
    if (++count_ > TimeoutPollFrequency.GetValue()) {
      count_ = 0;
      if (!is_cancelled_) {
        if (ValkeyModule_Milliseconds() >= deadline_ms_) {
          is_cancelled_ = true;  // Operation should be cancelled
          Timeouts.Increment(1);
          VMSDK_LOG(DEBUG, nullptr)
              << "CANCEL: Timeout reached, cancelling operation";
        } else if (context_ && context_->IsCancelled()) {
          is_cancelled_ = true;  // Operation should be cancelled
          gRPCCancels.Increment(1);
          VMSDK_LOG(DEBUG, nullptr) << "CANCEL: gRPC context cancelled";
        } else if (ForceTimeout.GetValue()) {
          is_cancelled_ = true;  // Operation should be cancelled
          ForceCancels.Increment(1);
          VMSDK_LOG(WARNING, nullptr) << "CANCEL: Timeout forced";
        } else if (!vmsdk::IsMainThread()) {
          PAUSEPOINT("Cancel");
        }
      }
    }
    return is_cancelled_;
  }

  bool is_cancelled_{false};  // Once cancelled, stay cancelled

  long long deadline_ms_;
  grpc::CallbackServerContext *context_;
  int count_{0};
};

Token Make(long long timeout_ms, grpc::CallbackServerContext *context) {
  long long deadline_ms = timeout_ms + ValkeyModule_Milliseconds();
  return std::make_shared<TokenImpl>(deadline_ms, context);
}

}  // namespace cancel
}  // namespace valkey_search
