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
