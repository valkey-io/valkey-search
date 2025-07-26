/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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

  MockTask mock_task_read;
  std::atomic<uint64_t> total_delay{0};
  StopWatch stop_watch;
  EXPECT_CALL(mock_task_read, Execute())
      .WillRepeatedly([&blocking_refcount, &count, &mrmw_mutex, rand_delay,
                       &total_delay, may_prolong_frequency]() {
        absl::BitGen gen;
        bool may_prolong =
            may_prolong_frequency > 0 && (count % may_prolong_frequency) == 0;
        {
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
        }
        blocking_refcount.DecrementCount();
      });
  MockTask mock_task_write;
  EXPECT_CALL(mock_task_write, Execute())
      .WillRepeatedly([&blocking_refcount, &count, &mrmw_mutex, rand_delay,
                       &total_delay, may_prolong_frequency]() {
        absl::BitGen gen;
        bool may_prolong =
            may_prolong_frequency > 0 && (count % may_prolong_frequency) == 0;
        {
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
        }
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
          in_prolong_read = true;
          may_prolong_notification.Notify();
          absl::SleepFor(options.read_quota_duration * 2);
        }
        read_tasks_completed_notification.WaitForNotification();
        in_prolong_read = false;
        may_prolong_release_notification.Notify();
      },
      ThreadPool::Priority::kHigh);

  absl::Notification write_tasks_completed_notification;
  thread_pool.Schedule(
      [&mrmw_mutex, &run_write, &write_tasks_completed_notification]() {
        {
          WriterMutexLock lock(&mrmw_mutex, false);
          run_write = true;
        }
        write_tasks_completed_notification.Notify();
      },
      ThreadPool::Priority::kHigh);
  may_prolong_notification.WaitForNotification();  
  EXPECT_TRUE(in_prolong_read);
  std::atomic<int> count = 0;

  for (size_t i = 0; i < read_tasks; ++i) {
    thread_pool.Schedule(
        [&mrmw_mutex, &run_write, &count, &blocking_refcount]() {
          ++count;
          {
            ReaderMutexLock lock(&mrmw_mutex, false);
            for (auto i = 0; i < 10; ++i) {
            }
            EXPECT_FALSE(run_write);
          }
          blocking_refcount.DecrementCount();
        },
        ThreadPool::Priority::kHigh);
  }
  blocking_refcount.Wait();
  read_tasks_completed_notification.Notify();
  write_tasks_completed_notification.WaitForNotification();
  EXPECT_TRUE(thread_pool.MarkForStop(ThreadPool::StopMode::kGraceful).ok());
  thread_pool.JoinWorkers();
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
        {
          ReaderMutexLock lock(&mrmw_mutex, true);
          absl::SleepFor(absl::Seconds(1));
        }
        blocking_refcount.DecrementCount();
      },
      ThreadPool::Priority::kHigh);
  thread_pool.Schedule(
      [&mrmw_mutex, &blocking_refcount]() {
        {
          WriterMutexLock lock(&mrmw_mutex, true);
          absl::SleepFor(absl::Seconds(1));
        }
        blocking_refcount.DecrementCount();
      },
      ThreadPool::Priority::kHigh);
  blocking_refcount.Wait();
}

TEST_F(MRMWMutexTest, ReaderLockMetrics) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);

  auto& stats = GetGlobalTimeSlicedMRMWStats();
  uint64_t initial_read_periods = stats.read_periods;
  uint64_t initial_read_time = stats.read_time_microseconds;

  {
    ReaderMutexLock lock(&mrmw_mutex);
    // Add small delay to ensure time accumulation
    absl::SleepFor(absl::Microseconds(100));
  }

  EXPECT_EQ(stats.read_periods, initial_read_periods + 1);
  EXPECT_GT(stats.read_time_microseconds, initial_read_time);
  // Verify timing is reasonable (should be at least 100 microseconds)
  EXPECT_GE(stats.read_time_microseconds - initial_read_time, 90);
}

TEST_F(MRMWMutexTest, WriterLockMetrics) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);

  auto& stats = GetGlobalTimeSlicedMRMWStats();
  uint64_t initial_write_periods = stats.write_periods;
  uint64_t initial_write_time = stats.write_time_microseconds;

  {
    WriterMutexLock lock(&mrmw_mutex);
    // Add small delay to ensure time accumulation
    absl::SleepFor(absl::Microseconds(150));
  }

  EXPECT_EQ(stats.write_periods, initial_write_periods + 1);
  EXPECT_GT(stats.write_time_microseconds, initial_write_time);
  // Verify timing is reasonable (should be at least 150 microseconds)
  EXPECT_GE(stats.write_time_microseconds - initial_write_time, 140);
}

TEST_F(MRMWMutexTest, MultipleLockMetrics) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);

  auto& stats = GetGlobalTimeSlicedMRMWStats();
  uint64_t initial_read_periods = stats.read_periods;
  uint64_t initial_write_periods = stats.write_periods;
  uint64_t initial_read_time = stats.read_time_microseconds;
  uint64_t initial_write_time = stats.write_time_microseconds;

  // Multiple read locks
  for (int i = 0; i < 3; ++i) {
    ReaderMutexLock lock(&mrmw_mutex);
    absl::SleepFor(absl::Microseconds(50));
  }

  // Multiple write locks
  for (int i = 0; i < 2; ++i) {
    WriterMutexLock lock(&mrmw_mutex);
    absl::SleepFor(absl::Microseconds(75));
  }

  EXPECT_EQ(stats.read_periods, initial_read_periods + 3);
  EXPECT_EQ(stats.write_periods, initial_write_periods + 2);
  EXPECT_GT(stats.read_time_microseconds, initial_read_time);
  EXPECT_GT(stats.write_time_microseconds, initial_write_time);
  
  // Verify accumulated timing
  EXPECT_GE(stats.read_time_microseconds - initial_read_time, 140);  // ~3 * 50
  EXPECT_GE(stats.write_time_microseconds - initial_write_time, 140); // ~2 * 75
}

TEST_F(MRMWMutexTest, ConcurrentMetrics) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);

  ThreadPool thread_pool("test-pool-", 4);
  thread_pool.StartWorkers();

  auto& stats = GetGlobalTimeSlicedMRMWStats();
  uint64_t initial_read_periods = stats.read_periods;
  uint64_t initial_write_periods = stats.write_periods;

  const int num_read_tasks = 10;
  const int num_write_tasks = 5;
  absl::BlockingCounter counter(num_read_tasks + num_write_tasks);

  // Schedule read tasks
  for (int i = 0; i < num_read_tasks; ++i) {
    thread_pool.Schedule([&mrmw_mutex, &counter]() {
      ReaderMutexLock lock(&mrmw_mutex);
      absl::SleepFor(absl::Microseconds(10));
      counter.DecrementCount();
    }, ThreadPool::Priority::kHigh);
  }

  // Schedule write tasks
  for (int i = 0; i < num_write_tasks; ++i) {
    thread_pool.Schedule([&mrmw_mutex, &counter]() {
      WriterMutexLock lock(&mrmw_mutex);
      absl::SleepFor(absl::Microseconds(20));
      counter.DecrementCount();
    }, ThreadPool::Priority::kHigh);
  }

  counter.Wait();

  EXPECT_EQ(stats.read_periods, initial_read_periods + num_read_tasks);
  EXPECT_EQ(stats.write_periods, initial_write_periods + num_write_tasks);
}

TEST_F(MRMWMutexTest, GlobalStatisticsVerification) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  
  TimeSlicedMRMWMutex mutex1(options);
  TimeSlicedMRMWMutex mutex2(options);

  auto& stats = GetGlobalTimeSlicedMRMWStats();
  uint64_t initial_read_periods = stats.read_periods;
  uint64_t initial_write_periods = stats.write_periods;

  // Test that multiple mutex instances contribute to same global stats
  {
    ReaderMutexLock lock1(&mutex1);
    absl::SleepFor(absl::Microseconds(50));
  }
  
  {
    WriterMutexLock lock2(&mutex2);
    absl::SleepFor(absl::Microseconds(50));
  }

  EXPECT_EQ(stats.read_periods, initial_read_periods + 1);
  EXPECT_EQ(stats.write_periods, initial_write_periods + 1);

  // Verify GetGlobalTimeSlicedMRMWStats returns the same instance
  auto& stats2 = GetGlobalTimeSlicedMRMWStats();
  EXPECT_EQ(&stats, &stats2);
}

TEST_F(MRMWMutexTest, TimingAccuracy) {
  MRMWMutexOptions options;
  options.read_quota_duration = absl::Seconds(1000);
  options.read_switch_grace_period = absl::Microseconds(100);
  options.write_quota_duration = absl::Microseconds(500);
  options.write_switch_grace_period = absl::Microseconds(50);
  TimeSlicedMRMWMutex mrmw_mutex(options);

  auto& stats = GetGlobalTimeSlicedMRMWStats();
  
  // Test different sleep durations
  const std::vector<int> sleep_durations = {100, 200, 500};
  
  for (int sleep_micros : sleep_durations) {
    uint64_t initial_read_time = stats.read_time_microseconds;
    
    StopWatch timer;
    {
      ReaderMutexLock lock(&mrmw_mutex);
      absl::SleepFor(absl::Microseconds(sleep_micros));
    }
    auto actual_duration = absl::ToInt64Microseconds(timer.Duration());
    
    uint64_t measured_time = stats.read_time_microseconds - initial_read_time;
    
    // Allow for some timing variance (±50 microseconds)
    EXPECT_GE(measured_time, sleep_micros - 50);
    EXPECT_LE(measured_time, actual_duration + 50);
  }
}

}  // namespace

}  // namespace vmsdk
