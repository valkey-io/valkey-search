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

#include "vmsdk/src/thread_pool.h"

#include <atomic>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstddef>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <vector>

#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

namespace {
// Mock class for the task function
class MockTask {
 public:
  MOCK_METHOD(void, Execute, ());
};

class ThreadPoolTest : public ::testing::TestWithParam<ThreadPool::Priority> {};
INSTANTIATE_TEST_SUITE_P(
    ThreadPoolTests, ThreadPoolTest,
    testing::ValuesIn<ThreadPool::Priority>({ThreadPool::Priority::kHigh,
                                             ThreadPool::Priority::kLow}),
    [](const testing::TestParamInfo<ThreadPool::Priority> &info) {
      return info.param == ThreadPool::Priority::kHigh ? "task_priority_high"
                                                       : "task_priority_low";
    });

TEST_F(ThreadPoolTest, StartAndJoin) {
  ThreadPool thread_pool("test-pool", 10);
  EXPECT_FALSE(thread_pool.SuspendWorkers().ok());
  StopWatch stop_watch;
  thread_pool.StartWorkers();
  thread_pool.JoinWorkers();
  EXPECT_LT(stop_watch.Duration(), absl::Seconds(1));
  EXPECT_FALSE(thread_pool.SuspendWorkers().ok());
  thread_pool.JoinWorkers();
}

TEST_P(ThreadPoolTest, StartSuspendAndJoin) {
  auto priority = GetParam();
  ThreadPool thread_pool("test-pool", 10);
  StopWatch stop_watch;
  thread_pool.StartWorkers();
  VMSDK_EXPECT_OK(thread_pool.SuspendWorkers());
  thread_pool.JoinWorkers();
  EXPECT_LT(stop_watch.Duration(), absl::Seconds(1));
  EXPECT_FALSE(thread_pool.Schedule([]() { absl::SleepFor(absl::Seconds(1)); },
                                    priority));
}

TEST_P(ThreadPoolTest, SuspendAndResume) {
  auto priority = GetParam();
  ThreadPool thread_pool("test-pool", 3);
  StopWatch stop_watch;
  thread_pool.StartWorkers();
  absl::Notification notification;
  VMSDK_EXPECT_OK(thread_pool.SuspendWorkers());
  EXPECT_TRUE(thread_pool.Schedule([&notification]() { notification.Notify(); },
                                   priority));
  EXPECT_FALSE(notification.WaitForNotificationWithTimeout(absl::Seconds(3)));
  VMSDK_EXPECT_OK(thread_pool.ResumeWorkers());
  EXPECT_TRUE(notification.WaitForNotificationWithTimeout(absl::Seconds(1)));
}

TEST_P(ThreadPoolTest, AbruptMarkForStop) {
  auto priority = GetParam();
  const size_t thread_count = 3;
  ThreadPool thread_pool("test-pool", thread_count);

  thread_pool.StartWorkers();
  absl::BlockingCounter blocking_refcount(thread_count);

  for (size_t i = 0; i < thread_count * 10; ++i) {
    if (i < thread_count) {
      EXPECT_TRUE(thread_pool.Schedule(
          [&blocking_refcount]() {
            absl::SleepFor(absl::Seconds(1));
            blocking_refcount.DecrementCount();
          },
          priority));
    } else {
      EXPECT_TRUE(thread_pool.Schedule(
          []() { absl::SleepFor(absl::Seconds(2)); }, priority));
    }
  }
  absl::SleepFor(absl::Milliseconds(100));
  VMSDK_EXPECT_OK(thread_pool.MarkForStop(ThreadPool::StopMode::kAbrupt));
  blocking_refcount.Wait();
  StopWatch stop_watch;
  thread_pool.JoinWorkers();
  EXPECT_LT(stop_watch.Duration(), absl::Seconds(1));
  EXPECT_EQ(thread_pool.QueueSize(), 9 * thread_count);
  VMSDK_EXPECT_OK(thread_pool.MarkForStop(ThreadPool::StopMode::kAbrupt));
  EXPECT_FALSE(thread_pool.MarkForStop(ThreadPool::StopMode::kGraceful).ok());
}

TEST_P(ThreadPoolTest, GracefulMarkForStop) {
  auto priority = GetParam();
  const size_t thread_count = 3;
  ThreadPool thread_pool("test-pool", thread_count);

  thread_pool.StartWorkers();
  absl::BlockingCounter blocking_refcount(thread_count * 3);

  for (size_t i = 0; i < thread_count * 3; ++i) {
    EXPECT_TRUE(thread_pool.Schedule(
        [&blocking_refcount]() {
          absl::SleepFor(absl::Milliseconds(100));
          blocking_refcount.DecrementCount();
        },
        priority));
  }
  absl::SleepFor(absl::Milliseconds(100));
  VMSDK_EXPECT_OK(thread_pool.MarkForStop(ThreadPool::StopMode::kGraceful));
  blocking_refcount.Wait();
  StopWatch stop_watch;
  thread_pool.JoinWorkers();
  EXPECT_LT(stop_watch.Duration(), absl::Seconds(1));
  EXPECT_EQ(thread_pool.QueueSize(), 0);
  VMSDK_EXPECT_OK(thread_pool.MarkForStop(ThreadPool::StopMode::kGraceful));
  VMSDK_EXPECT_OK(thread_pool.MarkForStop(ThreadPool::StopMode::kAbrupt));
  EXPECT_FALSE(thread_pool.MarkForStop(ThreadPool::StopMode::kGraceful).ok());
}

TEST_P(ThreadPoolTest, SuspendAndResumeLongTask) {
  auto test = [](bool with_delay, ThreadPool::Priority priority) {
    ThreadPool thread_pool("test-pool", 3);
    absl::BlockingCounter blocking_refcount(3);
    thread_pool.StartWorkers();
    StopWatch stop_watch;
    EXPECT_TRUE(thread_pool.Schedule(
        [&blocking_refcount]() {
          absl::SleepFor(absl::Seconds(1));
          blocking_refcount.DecrementCount();
        },
        priority));
    EXPECT_TRUE(thread_pool.Schedule(
        [&blocking_refcount]() {
          absl::SleepFor(absl::Seconds(1));
          blocking_refcount.DecrementCount();
        },
        priority));
    if (with_delay) {
      absl::SleepFor(absl::Milliseconds(100));
    }
    VMSDK_EXPECT_OK(thread_pool.SuspendWorkers());
    EXPECT_TRUE(thread_pool.Schedule(
        [&blocking_refcount]() { blocking_refcount.DecrementCount(); },
        priority));
    stop_watch.Reset();
    VMSDK_EXPECT_OK(thread_pool.ResumeWorkers());
    EXPECT_LT(stop_watch.Duration(), absl::Seconds(2));
    blocking_refcount.Wait();
  };
  auto priority = GetParam();
  for (auto with_delay : {true, false}) {
    test(with_delay, priority);
  }
}

TEST_P(ThreadPoolTest, EnqueueAndExecuteTasks) {
  auto priority = GetParam();
  ThreadPool thread_pool("test-pool", 10);
  thread_pool.StartWorkers();
  auto blocking_refcount =
      std::make_shared<absl::BlockingCounter>(thread_pool.Size());
  for (size_t i = 0; i < thread_pool.Size(); ++i) {
    auto mock_task = std::make_shared<MockTask>();
    EXPECT_CALL(*mock_task, Execute());
    EXPECT_TRUE(thread_pool.Schedule(
        [mock_task = mock_task, blocking_refcount = blocking_refcount] {
          mock_task->Execute();
          blocking_refcount->DecrementCount();
        },
        priority));
  }
  blocking_refcount->Wait();
}

TEST_P(ThreadPoolTest, VerifyFifo) {
  auto priority = GetParam();
  ThreadPool thread_pool("test-pool", 1);
  thread_pool.StartWorkers();
  std::vector<MockTask> mock_tasks(1000);
  absl::BlockingCounter blocking_refcount(mock_tasks.size());
  size_t task_id = 0;
  for (size_t i = 0; i < mock_tasks.size(); ++i) {
    EXPECT_CALL(mock_tasks[i], Execute());
    EXPECT_TRUE(thread_pool.Schedule(
        [&mock_tasks, i, &task_id, &blocking_refcount] {
          mock_tasks[i].Execute();
          EXPECT_EQ(i, task_id);
          ++task_id;
          blocking_refcount.DecrementCount();
        },
        priority));
  }
  blocking_refcount.Wait();
}

TEST_P(ThreadPoolTest, ConcurrentWorkers) {
  auto priority = GetParam();
  ThreadPool thread_pool("test-pool", 5);
  thread_pool.StartWorkers();
  std::vector<MockTask> mock_tasks(thread_pool.Size());
  std::mutex mutex;
  std::condition_variable condition;
  std::atomic<size_t> last_task = mock_tasks.size();
  for (size_t i = 0; i < mock_tasks.size(); ++i) {
    EXPECT_CALL(mock_tasks[i], Execute());
    EXPECT_TRUE(thread_pool.Schedule(
        [i, &mock_tasks, &last_task, &mutex, &condition] {
          mock_tasks[i].Execute();
          std::unique_lock<std::mutex> lock(mutex);
          condition.wait(lock, [&] { return i + 1 == last_task; });
          --last_task;
          condition.notify_all();
        },
        priority));
  }
  std::unique_lock<std::mutex> lock(mutex);
  condition.wait(lock, [&] { return last_task == 0; });
}
TEST_F(ThreadPoolTest, priority) {
  // Test that high priority tasks are executed before low priority tasks
  ThreadPool thread_pool("test-pool", 5);
  thread_pool.StartWorkers();
  const size_t tasks = thread_pool.Size() * 2;
  std::atomic<int> pending_run_low_priority = tasks;
  std::atomic<int> pending_run_high_priority = tasks;
  absl::BlockingCounter pending_tasks(tasks * 2);
  absl::Mutex mutex;
  {
    absl::MutexLock lock(&mutex);
    for (size_t i = 0; i < thread_pool.Size(); ++i) {
      EXPECT_TRUE(
          thread_pool.Schedule([&mutex] { absl::MutexLock lock(&mutex); },
                               ThreadPool::Priority::kHigh));
    }

    for (size_t i = 0; i < tasks; ++i) {
      EXPECT_TRUE(thread_pool.Schedule(
          [&pending_run_low_priority, &pending_run_high_priority,
           &pending_tasks, &mutex] {
            absl::MutexLock lock(&mutex);
            // Making sure that all high priority tasks were executed before any
            // low priority
            EXPECT_EQ(pending_run_high_priority, 0);
            --pending_run_low_priority;
            pending_tasks.DecrementCount();
          },
          ThreadPool::Priority::kLow));
    }
    for (size_t i = 0; i < tasks; ++i) {
      EXPECT_TRUE(thread_pool.Schedule(
          [&pending_run_low_priority, &pending_run_high_priority, &tasks,
           &pending_tasks, &mutex] {
            absl::MutexLock lock(&mutex);
            // Making sure that no low priority tasks were executed before
            // high priority tasks
            EXPECT_EQ(pending_run_low_priority, tasks);
            --pending_run_high_priority;
            pending_tasks.DecrementCount();
          },
          ThreadPool::Priority::kHigh));
    }
    EXPECT_GE(thread_pool.QueueSize(), tasks * 2);
  }
  // wait for all tasks to finish
  pending_tasks.Wait();
  // EXPECT_EQ(thread_pool.QueueSize(), 0);
}

}  // namespace

}  // namespace vmsdk
