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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>

#include "absl/random/random.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/time_sliced_mrmw_mutex.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

namespace {
// Mock class for the task function
class MockTask {
 public:
  MOCK_METHOD(void, Execute, ());
};

class MRMWMutexTest : public ::testing::Test {};
enum class Mode { kRead, kWrite, kMix };
void ModeTest(Mode mode, ThreadPool& readers_pool, ThreadPool& writers_pool,
              bool rand_delay, uint32_t may_prolong_frequency) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);
  auto max = std::max(readers_pool.Size(), writers_pool.Size());
  auto pool_size_factor = 5000;
  auto cnt = 0;
  if (mode == Mode::kRead || mode == Mode::kMix) {
    cnt += readers_pool.Size() * pool_size_factor;
  }
  if (mode == Mode::kWrite || mode == Mode::kMix) {
    cnt += writers_pool.Size() * pool_size_factor;
  }
  absl::BlockingCounter blocking_refcount(cnt);
  std::atomic<int> count = 0;
  absl::BitGen gen;
  MockTask mock_task_read;
  std::atomic<uint64_t> total_delay{0};
  StopWatch stop_watch;
  EXPECT_CALL(mock_task_read, Execute())
      .WillRepeatedly([&blocking_refcount, &count, &mrmw_mutex, &gen,
                       rand_delay, &total_delay, may_prolong_frequency]() {
        bool may_prolong =
            may_prolong_frequency > 0 && (count % may_prolong_frequency) == 0;
        ReaderMutexLock lock(&mrmw_mutex, may_prolong && (count % 4 == 0));
        if (may_prolong && (count % 2 == 0)) {
          auto delay = absl::uniform_int_distribution<int>(0, 50)(gen);
          lock.SetMayProlong();
          total_delay += delay;
          absl::SleepFor(absl::Microseconds(delay));
        }
        if (rand_delay) {
          auto delay = absl::uniform_int_distribution<int>(0, 100)(gen);
          if (may_prolong) {
            delay *= 10;
          }
          total_delay += delay;
          absl::SleepFor(absl::Microseconds(delay));
        }
        ++count;
        blocking_refcount.DecrementCount();
      });
  MockTask mock_task_write;
  EXPECT_CALL(mock_task_write, Execute())
      .WillRepeatedly([&blocking_refcount, &count, &mrmw_mutex, &gen,
                       rand_delay, &total_delay, may_prolong_frequency]() {
        bool may_prolong =
            may_prolong_frequency > 0 && (count % may_prolong_frequency) == 0;
        WriterMutexLock lock(&mrmw_mutex, may_prolong && (count % 4 == 0));
        if (may_prolong && (count % 2 == 0)) {
          auto delay = absl::uniform_int_distribution<int>(0, 50)(gen);
          lock.SetMayProlong();
          total_delay += delay;
          absl::SleepFor(absl::Microseconds(delay));
        }
        if (rand_delay) {
          auto delay = absl::uniform_int_distribution<int>(0, 100)(gen);
          if (may_prolong) {
            delay *= 10;
          }
          total_delay += delay;
          absl::SleepFor(absl::Microseconds(delay));
        }
        ++count;
        blocking_refcount.DecrementCount();
      });

  for (size_t i = 0; i < max * pool_size_factor; ++i) {
    if (i < readers_pool.Size() * pool_size_factor &&
        (mode == Mode::kRead || mode == Mode::kMix)) {
      readers_pool.Schedule([&mock_task_read]() { mock_task_read.Execute(); },
                            ThreadPool::Priority::kHigh);
    }
    if (i < writers_pool.Size() * pool_size_factor &&
        (mode == Mode::kWrite || mode == Mode::kMix)) {
      writers_pool.Schedule([&mock_task_write]() { mock_task_write.Execute(); },
                            ThreadPool::Priority::kHigh);
    }
  }
  blocking_refcount.Wait();
  if (rand_delay) {
    auto duration = stop_watch.Duration();
    std::cerr << "total_delay: " << total_delay << ", duration: " << duration
              << "\n";
  }
  if (mode == Mode::kMix) {
    EXPECT_EQ(count, readers_pool.Size() * pool_size_factor +
                         writers_pool.Size() * pool_size_factor);
  } else if (mode == Mode::kWrite) {
    EXPECT_EQ(count, writers_pool.Size() * pool_size_factor);
  } else if (mode == Mode::kRead) {
    EXPECT_EQ(count, readers_pool.Size() * pool_size_factor);
  }
}

TEST_F(MRMWMutexTest, OnePoolModeRead) {
  for (const uint32_t may_prolong_frequency : {0, 10}) {
    ThreadPool thread_pool("test-pool-", 8);
    thread_pool.StartWorkers();
    ModeTest(Mode::kRead, thread_pool, thread_pool, false,
             may_prolong_frequency);
    ModeTest(Mode::kRead, thread_pool, thread_pool, true,
             may_prolong_frequency);
  }
}

TEST_F(MRMWMutexTest, TwoPoolsModeRead) {
  for (const uint32_t may_prolong_frequency : {0, 10}) {
    ThreadPool reader_pool("reader-pool-", 5);
    reader_pool.StartWorkers();
    ThreadPool writer_pool("writer-pool-", 3);
    writer_pool.StartWorkers();
    ModeTest(Mode::kRead, reader_pool, writer_pool, false,
             may_prolong_frequency);
    ModeTest(Mode::kRead, reader_pool, writer_pool, true,
             may_prolong_frequency);
  }
}

TEST_F(MRMWMutexTest, OnePoolModeWrite) {
  for (const uint32_t may_prolong_frequency : {0, 5}) {
    ThreadPool thread_pool("test-pool-", 8);
    thread_pool.StartWorkers();
    ModeTest(Mode::kWrite, thread_pool, thread_pool, false,
             may_prolong_frequency);
    ModeTest(Mode::kWrite, thread_pool, thread_pool, true,
             may_prolong_frequency);
  }
}

TEST_F(MRMWMutexTest, TwoPoolsModeWrite) {
  for (const uint32_t may_prolong_frequency : {0, 8}) {
    ThreadPool reader_pool("reader-pool-", 3);
    reader_pool.StartWorkers();
    ThreadPool writer_pool("writer-pool-", 5);
    writer_pool.StartWorkers();
    ModeTest(Mode::kWrite, reader_pool, writer_pool, false,
             may_prolong_frequency);
    ModeTest(Mode::kWrite, reader_pool, writer_pool, true,
             may_prolong_frequency);
  }
}

TEST_F(MRMWMutexTest, OnePoolModeMix) {
  for (const uint32_t may_prolong_frequency : {0, 20}) {
    ThreadPool thread_pool("test-pool-", 8);
    thread_pool.StartWorkers();
    ModeTest(Mode::kMix, thread_pool, thread_pool, false,
             may_prolong_frequency);
    ModeTest(Mode::kMix, thread_pool, thread_pool, true, may_prolong_frequency);
  }
}

TEST_F(MRMWMutexTest, TwoPoolsModeMix) {
  for (const uint32_t may_prolong_frequency : {0, 25}) {
    ThreadPool reader_pool("reader-pool-", 4);
    reader_pool.StartWorkers();
    ThreadPool writer_pool("writer-pool-", 4);
    writer_pool.StartWorkers();
    ModeTest(Mode::kMix, reader_pool, writer_pool, false,
             may_prolong_frequency);
    ModeTest(Mode::kMix, reader_pool, writer_pool, true, may_prolong_frequency);
  }
}

TEST_F(MRMWMutexTest, VerifyMayProlong) {
  ThreadPool thread_pool("test-pool-", 8);
  thread_pool.StartWorkers();
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Milliseconds(1);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);
  absl::Notification may_prolong_notification;
  absl::Notification read_tasks_completed_notification;
  absl::Notification may_prolong_release_notification;
  const size_t read_tasks = 40;
  absl::BlockingCounter blocking_refcount(read_tasks);
  std::atomic<bool> run_write;
  std::atomic<bool> in_prolong_read;
  thread_pool.Schedule(
      [&mrmw_mutex, &may_prolong_notification,
       &read_tasks_completed_notification, &in_prolong_read,
       &may_prolong_release_notification, &options]() {
        {
          ReaderMutexLock lock(&mrmw_mutex, true);
          absl::SleepFor(options.read_quota_duration * 2);

          in_prolong_read = true;
          may_prolong_notification.Notify();
          read_tasks_completed_notification.WaitForNotification();
          in_prolong_read = false;
        }
        may_prolong_release_notification.Notify();
      },
      ThreadPool::Priority::kHigh);

  absl::Notification write_tasks_completed_notification;
  thread_pool.Schedule(
      [&mrmw_mutex, &run_write, &write_tasks_completed_notification]() {
        WriterMutexLock lock(&mrmw_mutex, false);
        run_write = true;
        write_tasks_completed_notification.Notify();
      },
      ThreadPool::Priority::kHigh);
  may_prolong_notification.WaitForNotification();
  EXPECT_TRUE(in_prolong_read);
  std::atomic<int> count = 0;
  thread_pool.Schedule(
      [&mrmw_mutex, &in_prolong_read, &may_prolong_release_notification, &count,
       read_tasks]() {
        ReaderMutexLock lock(&mrmw_mutex, true);
        EXPECT_EQ(count, read_tasks);
        may_prolong_release_notification.WaitForNotification();
        EXPECT_FALSE(in_prolong_read);
      },
      ThreadPool::Priority::kHigh);

  for (size_t i = 0; i < read_tasks; ++i) {
    thread_pool.Schedule(
        [&mrmw_mutex, &count, &blocking_refcount]() {
          ++count;
          ReaderMutexLock lock(&mrmw_mutex, false);
          blocking_refcount.DecrementCount();
        },
        ThreadPool::Priority::kHigh);
  }
  blocking_refcount.Wait();
  EXPECT_FALSE(run_write);
  read_tasks_completed_notification.Notify();
  write_tasks_completed_notification.WaitForNotification();
}

TEST_F(MRMWMutexTest, SkipWait) {
  ThreadPool thread_pool("test-pool-", 8);
  thread_pool.StartWorkers();
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Minutes(100);
  options.read_switch_grace_period = absl::Minutes(10);
  options.write_quota_duration = absl::Minutes(500);
  options.write_switch_grace_period = absl::Minutes(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);
  absl::BlockingCounter blocking_refcount(2);
  thread_pool.Schedule(
      [&mrmw_mutex, &blocking_refcount]() {
        ReaderMutexLock lock(&mrmw_mutex, true);
        absl::SleepFor(absl::Seconds(1));
        blocking_refcount.DecrementCount();
      },
      ThreadPool::Priority::kHigh);
  thread_pool.Schedule(
      [&mrmw_mutex, &blocking_refcount]() {
        WriterMutexLock lock(&mrmw_mutex, true);
        absl::SleepFor(absl::Seconds(1));
        blocking_refcount.DecrementCount();
      },
      ThreadPool::Priority::kHigh);
  blocking_refcount.Wait();
}

}  // namespace

}  // namespace vmsdk
