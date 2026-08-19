/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_GRPC_SUSPENDER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_GRPC_SUSPENDER_H_

#include <cstdint>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"

namespace valkey_search::coordinator {

// GRPCSuspender is used to suspend and resume gRPC callbacks in combination
// with GRPCSuspensionGuard. This is used to ensure that gRPC callbacks do not
// access shared mutexes used by the child process during fork.
class GRPCSuspender {
 public:
  static GRPCSuspender& Instance() {
    // Use NoDestructor to avoid atexit destruction racing with gRPC
    // event_engine threads that may still be executing GRPCSuspensionGuard
    // destructors (which call Decrement() -> MutexLock). The GRPCSuspender
    // singleton outlives all threads; the OS reclaims memory at process exit.
    static absl::NoDestructor<GRPCSuspender> instance;
    return *instance;
  }
  absl::Status Suspend();
  absl::Status Resume();
  void Increment();
  void Decrement();

 private:
  GRPCSuspender() = default;
  friend class absl::NoDestructor<GRPCSuspender>;

  absl::Mutex mutex_;
  int64_t count_ ABSL_GUARDED_BY(mutex_) = 0;
  bool suspended_ ABSL_GUARDED_BY(mutex_) = false;
  absl::CondVar in_flight_tasks_completed_ ABSL_GUARDED_BY(mutex_);
  absl::CondVar resume_ ABSL_GUARDED_BY(mutex_);
};

// gRPC runs server callbacks and client-provided callbacks on a background
// thread. This guard ensures that these threads do not access any shared
// mutexes used by the child process during fork. It should be acquired by each
// gRPC callback so that new callbacks can be suspended prior to forking.
class GRPCSuspensionGuard {
 public:
  explicit GRPCSuspensionGuard(GRPCSuspender& grpc_suspender)
      : grpc_suspender_(grpc_suspender) {
    grpc_suspender_.Increment();
  }
  ~GRPCSuspensionGuard() { grpc_suspender_.Decrement(); }

 private:
  GRPCSuspender& grpc_suspender_;
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_GRPC_SUSPENDER_H_
