/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_MRMW_MUTEX_H_
#define VMSDK_SRC_MRMW_MUTEX_H_

#include <cstdint>
#include <optional>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

struct TimeSlicedMRMWStats {
  std::atomic<uint64_t> read_periods{0};
  std::atomic<uint64_t> read_time_microseconds{0};  // cumulative
  std::atomic<uint64_t> write_periods{0};
  std::atomic<uint64_t> write_time_microseconds{0};  // cumulative
};

// Forward declaration for global statistics
extern TimeSlicedMRMWStats global_stats;

// Global statistics accessor function
inline const TimeSlicedMRMWStats& GetGlobalTimeSlicedMRMWStats() {
  return global_stats;
}

struct MRMWMutexOptions {
  absl::Duration read_quota_duration;
  absl::Duration read_switch_grace_period;
  absl::Duration write_quota_duration;
  absl::Duration write_switch_grace_period;
};
// Time Sliced Multi-Reader Multi-Writer Mutex
// MRMWMutex supports concurrent reads or concurrent writes but not a mix of
// concurrent reads and writes at the same time. It allows fine-tuning of
// behavior with the following parameters:
// 1. read/write_switch_grace_period: The inactivity interval of the mode
// to initiate a switch.
// 2. read/write_quota_duration: Defines the maximum duration to remain in the
// active mode while the inverse mode is requested to be acquired.
//
// These parameters can be used to prioritize reads over writes or vice versa.
class ABSL_LOCKABLE TimeSlicedMRMWMutex {
 public:
  TimeSlicedMRMWMutex(const MRMWMutexOptions& options);
  TimeSlicedMRMWMutex(const TimeSlicedMRMWMutex&) = delete;
  TimeSlicedMRMWMutex& operator=(const TimeSlicedMRMWMutex&) = delete;
  ~TimeSlicedMRMWMutex() = default;
  enum class Mode {
    kLockRead,
    kLockWrite,
  };
  void ReaderLock(bool& may_prolong, bool ignore_time_quota)
      ABSL_SHARED_LOCK_FUNCTION() ABSL_LOCKS_EXCLUDED(mutex_);
  void WriterLock(bool& may_prolong, bool ignore_time_quota)
      ABSL_SHARED_LOCK_FUNCTION() ABSL_LOCKS_EXCLUDED(mutex_);

  void Unlock(bool may_prolong, bool ignore_time_quota) ABSL_UNLOCK_FUNCTION();
  void IncMayProlongCount() ABSL_LOCKS_EXCLUDED(mutex_);

 private:
  void Lock(Mode target_mode, bool& may_prolong, bool ignore_time_quota)
      ABSL_LOCKS_EXCLUDED(mutex_);
  inline uint32_t& GetWaiters(Mode target_mode)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? reader_waiters_ : writer_waiters_;
  };
  inline uint32_t GetWaitersConst(Mode target_mode) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? reader_waiters_ : writer_waiters_;
  };
  inline absl::CondVar& GetCondVar(Mode target_mode)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? read_cond_var_ : write_cond_var_;
  };
  inline const absl::Duration& GetTimeQuota(Mode target_mode) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? read_quota_duration_
                                          : write_quota_duration_;
  };
  inline bool& WaitWithTimer(Mode target_mode)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return target_mode == Mode::kLockRead ? read_wait_with_timer_
                                          : write_wait_with_timer_;
  };
  const absl::Duration& GetSwitchGracePeriod() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return current_mode_ == Mode::kLockRead ? read_switch_grace_period_
                                            : write_switch_grace_period_;
  }
  absl::Duration CalcWaitTime() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  absl::Duration CalcEffectiveGraceTime() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool MinimizeWaitTime() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool ShouldSwitch() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void InitSwitch() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  inline bool HasTimeQuotaExceeded() const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return ignore_time_quota_count_ == 0 && !switch_wait_mode_.has_value() &&
           stop_watch_.Duration() > GetTimeQuota(current_mode_);
  }
  void SwitchWithWait(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void WaitSwitch(Mode target_mode) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static inline Mode GetInverseMode(Mode target_mode) {
    return target_mode == Mode::kLockRead ? Mode::kLockWrite : Mode::kLockRead;
  }

  mutable absl::Mutex mutex_;
  Mode current_mode_ ABSL_GUARDED_BY(mutex_){Mode::kLockRead};
  int active_lock_count_ ABSL_GUARDED_BY(mutex_){0};
  vmsdk::StopWatch last_lock_acquired_ ABSL_GUARDED_BY(mutex_);
  absl::CondVar write_cond_var_ ABSL_GUARDED_BY(mutex_);
  absl::CondVar read_cond_var_ ABSL_GUARDED_BY(mutex_);
  uint32_t reader_waiters_ ABSL_GUARDED_BY(mutex_){0};
  uint32_t writer_waiters_ ABSL_GUARDED_BY(mutex_){0};
  bool read_wait_with_timer_ ABSL_GUARDED_BY(mutex_){false};
  bool write_wait_with_timer_ ABSL_GUARDED_BY(mutex_){false};
  const absl::Duration read_quota_duration_;
  const absl::Duration read_switch_grace_period_;
  const absl::Duration write_quota_duration_;
  const absl::Duration write_switch_grace_period_;
  std::optional<Mode> switch_wait_mode_ ABSL_GUARDED_BY(mutex_);
  vmsdk::StopWatch stop_watch_ ABSL_GUARDED_BY(mutex_);
  int switches_ ABSL_GUARDED_BY(mutex_){0};
  uint32_t may_prolong_count_ ABSL_GUARDED_BY(mutex_){0};
  uint32_t ignore_time_quota_count_ ABSL_GUARDED_BY(mutex_){0};
};

class ABSL_SCOPED_LOCKABLE ReaderMutexLock {
 public:
  explicit ReaderMutexLock(TimeSlicedMRMWMutex* mutex, bool may_prolong = false,
                           bool ignore_time_quota = false)
      ABSL_SHARED_LOCK_FUNCTION(mutex)
      : mutex_(mutex),
        may_prolong_(may_prolong),
        ignore_time_quota_(ignore_time_quota) {
    ++global_stats.read_periods;
    timer_.Reset();
    mutex->ReaderLock(may_prolong_, ignore_time_quota);
  }

  ReaderMutexLock(const ReaderMutexLock&) = delete;
  ReaderMutexLock(ReaderMutexLock&&) = delete;
  ReaderMutexLock& operator=(const ReaderMutexLock&) = delete;
  ReaderMutexLock& operator=(ReaderMutexLock&&) = delete;
  void SetMayProlong();
  ~ReaderMutexLock() ABSL_UNLOCK_FUNCTION() {
    mutex_->Unlock(may_prolong_, ignore_time_quota_);
    global_stats.read_time_microseconds +=
        absl::ToInt64Microseconds(timer_.Duration());
  }

 private:
  TimeSlicedMRMWMutex* const mutex_;
  bool may_prolong_;
  bool ignore_time_quota_;
  vmsdk::StopWatch timer_;
};

class ABSL_SCOPED_LOCKABLE WriterMutexLock {
 public:
  explicit WriterMutexLock(TimeSlicedMRMWMutex* mutex, bool may_prolong = false,
                           bool ignore_time_quota = false)
      ABSL_SHARED_LOCK_FUNCTION(mutex)
      : mutex_(mutex),
        may_prolong_(may_prolong),
        ignore_time_quota_(ignore_time_quota) {
    ++global_stats.write_periods;
    timer_.Reset();
    mutex->WriterLock(may_prolong_, ignore_time_quota);
  }

  WriterMutexLock(const WriterMutexLock&) = delete;
  WriterMutexLock(WriterMutexLock&&) = delete;
  WriterMutexLock& operator=(const WriterMutexLock&) = delete;
  WriterMutexLock& operator=(WriterMutexLock&&) = delete;
  void SetMayProlong();
  ~WriterMutexLock() ABSL_UNLOCK_FUNCTION() {
    mutex_->Unlock(may_prolong_, ignore_time_quota_);
    global_stats.write_time_microseconds +=
        absl::ToInt64Microseconds(timer_.Duration());
  }

 private:
  TimeSlicedMRMWMutex* const mutex_;
  bool may_prolong_;
  bool ignore_time_quota_;
  vmsdk::StopWatch timer_;
};

}  // namespace vmsdk
#endif  // VMSDK_SRC_MRMW_MUTEX_H_
