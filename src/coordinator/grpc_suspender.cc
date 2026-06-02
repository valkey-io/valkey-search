/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/grpc_suspender.h"

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

namespace valkey_search::coordinator {

absl::Status GRPCSuspender::Suspend() {
  absl::MutexLock lock(&mutex_);
  if (suspended_) {
    return absl::FailedPreconditionError("Already suspended");
  }
  suspended_ = true;
  while (count_ > 0) {
    in_flight_tasks_completed_.Wait(&mutex_);
  }
  return absl::OkStatus();
}

absl::Status GRPCSuspender::Resume() {
  absl::MutexLock lock(&mutex_);
  if (!suspended_) {
    return absl::FailedPreconditionError("Not suspended");
  }
  CHECK_EQ(count_, 0);
  suspended_ = false;
  resume_.SignalAll();
  return absl::OkStatus();
}

void GRPCSuspender::Increment() {
  absl::MutexLock lock(&mutex_);
  while (suspended_) {
    resume_.Wait(&mutex_);
  }
  ++count_;
}

void GRPCSuspender::Decrement() {
  absl::MutexLock lock(&mutex_);
  --count_;
  if (count_ == 0) {
    in_flight_tasks_completed_.SignalAll();
  }
}

}  // namespace valkey_search::coordinator
