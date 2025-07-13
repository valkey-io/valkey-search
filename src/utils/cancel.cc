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

vmsdk::config::Number kPollFrequency("timeout-poll-frequency", 100, 1, std::numeric_limits<long long>::max());
vmsdk::config::Boolean kForceTimeout("debug-force-timeout", false);
vmsdk::info_field::Integer kTimeouts("timeouts", "cancel-timeouts", vmsdk::info_field::IntegerBuilder().Dev());

bool OnTime::IsCancelled() {
  if (++count_ > kPollFrequency.GetValue()) {
    count_ = 0;
    long long now_us = ValkeyModule_Milliseconds();
    if (now_us >= deadline_ms_ || kForceTimeout.GetValue()) {
      is_cancelled_ = true; // Operation should be cancelled
      kTimeouts.Increment(1);
    }
  }
  return is_cancelled_;
}

void OnTime::Cancel() {
  is_cancelled_ = true; // Once cancelled, stay cancelled
}

OnTime::OnTime(long long timeout_ms) : deadline_ms_(timeout_ms + ValkeyModule_Milliseconds()) {}

Token OnTime::Make(long long timeout_ms) {
  return std::shared_ptr<OnTime>(new OnTime(timeout_ms));
}

} // namespace cancel
} // namespace valkey_search
