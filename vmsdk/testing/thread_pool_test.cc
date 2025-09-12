/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/thread_pool.h"

#include <atomic>
#include <condition_variable>  // NOLINT(build/c++11)
#include <cstddef>
#include <memory>
#include <mutex>   // NOLINT(build/c++11)
#include <thread>  // NOLINT(build/c++11)
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

}  // namespace

class ThreadPoolTest : public ::testing::TestWithParam<ThreadPool::Priority> {};
INSTANTIATE_TEST_SUITE_P(
    ThreadPoolTests, ThreadPoolTest,
    testing::ValuesIn<ThreadPool::Priority>({ThreadPool::Priority::kHigh,
                                             ThreadPool::Priority::kLow}),
    [](const testing::TestParamInfo<ThreadPool::Priority>& info) {
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
#ifdef BROKEN_UNIT_TEST
TEST_F(ThreadPoolTest, priority) {
  // Test that high priority tasks are executed before low priority tasks
  const size_t thread_count = 5;
  ThreadPool thread_pool("test-pool", thread_count);
  const size_t tasks = thread_count * 2;
  std::atomic<int> pending_run_low_priority = tasks;
  std::atomic<int> pending_run_high_priority = tasks;
  absl::BlockingCounter pending_tasks(tasks * 2);
  absl::Mutex mutex;
  {
    absl::MutexLock lock(&mutex);
    for (size_t i = 0; i < thread_count; ++i) {
      EXPECT_TRUE(
          thread_pool.Schedule([&mutex] { absl::MutexLock lock(&mutex); },
                               ThreadPool::Priority::kHigh));
    }
    //// The logic below fails, because it assumes that the scheduling of tasks
    /// and their execution is the same. Which it is / not. It's possible for a
    /// low priority task to be started while a high priority task is still
    /// running. This isn't / true. As the high prio threads decrement the
    /// blocking counter and terminate, it's possible for a low priority /
    /// thread to get started (since there's an idle thread in the pool) and
    /// then to beat the remaining high priority / threads to win access to the
    /// mutex. In other words, the mutex access doesn't honor the thread
    /// priorities and that / causes this test to fail intermittently.
    for (size_t i = 0; i < tasks; ++i) {
      EXPECT_TRUE(thread_pool.Schedule(
          [&pending_run_low_priority, &pending_run_high_priority,
           &pending_tasks, &mutex]() {
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
           &pending_tasks, &mutex]() {
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
  // Now that tasks have been loaded to the thread pool, start the workers
  thread_pool.StartWorkers();
  // wait for all tasks to finish
  pending_tasks.Wait();
  // EXPECT_EQ(thread_pool.QueueSize(), 0);
}
#endif
TEST_F(ThreadPoolTest, DynamicSizing) {
  const size_t thread_count = 10;
  ThreadPool thread_pool("test-pool", thread_count);
  thread_pool.StartWorkers();
  EXPECT_EQ(thread_pool.Size(), thread_count);

  thread_pool.Resize(5, true);
  EXPECT_EQ(thread_pool.Size(), 5);

  thread_pool.JoinTerminatedWorkers();
  EXPECT_EQ(thread_pool.pending_join_threads_.Size(), 0);

  EXPECT_EQ(thread_pool.Size(), 5);
  thread_pool.Resize(15, true);

  EXPECT_EQ(thread_pool.Size(), 15);
  thread_pool.JoinWorkers();

  EXPECT_EQ(thread_pool.threads_.Size(), 0);
  EXPECT_EQ(thread_pool.pending_join_threads_.Size(), 0);
}

namespace {
constexpr size_t kThreadCount = 2;
std::shared_ptr<absl::BlockingCounter> ScheduleTasks(
    ThreadPool& thread_pool, std::atomic_bool& atomic_flag, size_t modulo) {
  std::shared_ptr<absl::BlockingCounter> blocking_counter =
      std::make_shared<absl::BlockingCounter>(kThreadCount);

  for (size_t i = 0; i < kThreadCount; i++) {
    EXPECT_TRUE(thread_pool.Schedule(
        [&atomic_flag, modulo, blocking_counter]() {
          uint64_t counter = 0;
          while (!atomic_flag) {
            counter++;
            if (counter % modulo == 0) {
              absl::SleepFor(absl::Microseconds(1));
            }
          }
          blocking_counter->DecrementCount();
        },
        vmsdk::ThreadPool::Priority::kHigh));
  }
  return blocking_counter;
}
}  // namespace

TEST_F(ThreadPoolTest, TestCPUUsage) {
  const size_t thread_count{2};
  ThreadPool thread_pool("test-pool", thread_count);
  std::atomic_bool atomic_flag{true};
  int modulo{0};
  // Initializing the threads with first simple tasks
  auto blocking_counter = ScheduleTasks(thread_pool, atomic_flag, modulo);
  thread_pool.StartWorkers();
  blocking_counter->Wait();
  absl::SleepFor(absl::Milliseconds(100));
  // Expect current CPU avg to be around 0
  EXPECT_LT(thread_pool.GetAvgCPUPercentage().value(), 1.0);

  atomic_flag.store(false);
  // Increasing modulo means we will be sleeping less in the task, so CPU
  // expected to rise
  modulo = 1000;
  blocking_counter = ScheduleTasks(thread_pool, atomic_flag, modulo);
  absl::SleepFor(absl::Milliseconds(100));
  double first_sample = thread_pool.GetAvgCPUPercentage().value();
  // Expect the avg CPU does not pass 100%
  EXPECT_LT(first_sample, 100.0);
  atomic_flag.store(true);
  blocking_counter->Wait();

  // Occupy again the threads with new tasks
  atomic_flag.store(false);
  // Decrease the amount of time the threads will sleep by increasing modulo
  modulo = 10000;
  blocking_counter = ScheduleTasks(thread_pool, atomic_flag, modulo);
  absl::SleepFor(absl::Milliseconds(100));
  // Threads are expected now to work harder, so current CPU will be higher
  // than previous sample
  double second_sample = thread_pool.GetAvgCPUPercentage().value();
  EXPECT_GT(second_sample, first_sample);
  atomic_flag.store(true);
  blocking_counter->Wait();
  thread_pool.JoinWorkers();
}

// ============================================================================
// THREAD POOL FAIRNESS TESTS
// ============================================================================

class ThreadPoolFairnessTest : public ::testing::Test {
 protected:
  ThreadPoolFairnessTest() : thread_pool_("fairness-test-pool", 4) {}

  void SetUp() override { thread_pool_.StartWorkers(); }

  void TearDown() override { thread_pool_.JoinWorkers(); }

  ThreadPool thread_pool_;
};

// Test weight setter and getter functionality
TEST_F(ThreadPoolFairnessTest, WeightSetterGetter) {
  // Test default weight
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 100);

  // Test setting valid weights
  thread_pool_.SetHighPriorityWeight(0);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 0);

  thread_pool_.SetHighPriorityWeight(50);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 50);

  thread_pool_.SetHighPriorityWeight(100);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 100);
}

// Test weight clamping for invalid values
TEST_F(ThreadPoolFairnessTest, WeightClamping) {
  // Test negative values get clamped to 0
  thread_pool_.SetHighPriorityWeight(-1);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 0);

  thread_pool_.SetHighPriorityWeight(-100);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 0);

  // Test values > 100 get clamped to 100
  thread_pool_.SetHighPriorityWeight(101);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 100);

  thread_pool_.SetHighPriorityWeight(999);
  EXPECT_EQ(thread_pool_.GetHighPriorityWeight(), 100);
}

// Test fairness under concurrent weight changes
TEST_F(ThreadPoolFairnessTest, ConcurrentWeightChanges) {
  const int total_tasks = 500;
  std::atomic<bool> stop_weight_changes{false};
  std::atomic<int> tasks_executed{0};

  // Thread that continuously changes weights
  std::thread weight_changer([this, &stop_weight_changes]() {
    const std::vector<int> weights = {0, 25, 50, 75, 100};
    size_t weight_index = 0;

    while (!stop_weight_changes) {
      thread_pool_.SetHighPriorityWeight(weights[weight_index]);
      weight_index = (weight_index + 1) % weights.size();
      absl::SleepFor(absl::Microseconds(100));
    }
  });

  absl::BlockingCounter counter(total_tasks);

  // Schedule tasks while weights are changing
  for (int i = 0; i < total_tasks; ++i) {
    ThreadPool::Priority priority =
        (i % 2 == 0) ? ThreadPool::Priority::kHigh : ThreadPool::Priority::kLow;

    EXPECT_TRUE(thread_pool_.Schedule(
        [&tasks_executed, &counter]() {
          tasks_executed++;
          counter.DecrementCount();
        },
        priority));
  }

  counter.Wait();
  stop_weight_changes = true;
  weight_changer.join();

  // All tasks should have executed despite weight changes
  EXPECT_EQ(tasks_executed.load(), total_tasks);
}

// Test starvation prevention
TEST_F(ThreadPoolFairnessTest, StarvationPrevention) {
  // Set weight to heavily favor high priority but allow some low priority
  thread_pool_.SetHighPriorityWeight(90);  // 90% high, 10% low

  std::atomic<int> low_executed{0};
  std::atomic<bool> continue_scheduling{true};

  // Continuously schedule high priority tasks
  std::thread high_priority_scheduler([this, &continue_scheduling]() {
    while (continue_scheduling) {
      thread_pool_.Schedule([]() { absl::SleepFor(absl::Microseconds(100)); },
                            ThreadPool::Priority::kHigh);
      absl::SleepFor(absl::Microseconds(10));
    }
  });

  // Schedule low priority tasks and wait for some to execute
  const int low_tasks = 10;
  absl::BlockingCounter low_counter(low_tasks);

  for (int i = 0; i < low_tasks; ++i) {
    EXPECT_TRUE(thread_pool_.Schedule(
        [&low_executed, &low_counter]() {
          low_executed++;
          low_counter.DecrementCount();
        },
        ThreadPool::Priority::kLow));
  }

  // Wait for low priority tasks to complete (with timeout)
  auto start_time = absl::Now();
  low_counter.Wait();
  auto duration = absl::Now() - start_time;

  continue_scheduling = false;
  high_priority_scheduler.join();

  // Verify low priority tasks eventually executed
  EXPECT_EQ(low_executed.load(), low_tasks);
  // Verify it didn't take too long (starvation would cause timeout)
  EXPECT_LT(duration, absl::Seconds(5));
}

// Test performance regression - fairness shouldn't significantly impact
// performance
TEST_F(ThreadPoolFairnessTest, PerformanceRegression) {
  const int total_tasks = 10000;

  // Test with default weight (100% high priority - should be fast)
  thread_pool_.SetHighPriorityWeight(100);

  auto start_time = absl::Now();
  absl::BlockingCounter counter_default(total_tasks);

  for (int i = 0; i < total_tasks; ++i) {
    EXPECT_TRUE(thread_pool_.Schedule(
        [&counter_default]() { counter_default.DecrementCount(); },
        ThreadPool::Priority::kHigh));
  }

  counter_default.Wait();
  auto default_duration = absl::Now() - start_time;

  // Test with fairness weight (50/50 distribution)
  thread_pool_.SetHighPriorityWeight(50);

  start_time = absl::Now();
  absl::BlockingCounter counter_fairness(total_tasks);

  for (int i = 0; i < total_tasks; ++i) {
    ThreadPool::Priority priority =
        (i % 2 == 0) ? ThreadPool::Priority::kHigh : ThreadPool::Priority::kLow;

    EXPECT_TRUE(thread_pool_.Schedule(
        [&counter_fairness]() { counter_fairness.DecrementCount(); },
        priority));
  }

  counter_fairness.Wait();
  auto fairness_duration = absl::Now() - start_time;

  // Counter-based fairness may have higher overhead than random-based
  double overhead_ratio = absl::ToDoubleSeconds(fairness_duration) /
                          absl::ToDoubleSeconds(default_duration);
  EXPECT_LT(overhead_ratio, 3.0);
}

// Test edge cases
TEST_F(ThreadPoolFairnessTest, EdgeCases) {
  // Test with empty queues
  thread_pool_.SetHighPriorityWeight(50);

  // No tasks scheduled, should handle gracefully
  absl::SleepFor(absl::Milliseconds(100));

  // Test rapid weight changes with few tasks
  std::atomic<int> executed{0};
  absl::BlockingCounter counter(5);

  for (int i = 0; i < 5; ++i) {
    thread_pool_.SetHighPriorityWeight(i * 25);

    EXPECT_TRUE(thread_pool_.Schedule(
        [&executed, &counter]() {
          executed++;
          counter.DecrementCount();
        },
        ThreadPool::Priority::kHigh));
  }

  counter.Wait();
  EXPECT_EQ(executed.load(), 5);
}

// Test fairness distribution with different weights
class ThreadPoolFairnessDistributionTest
    : public ::testing::TestWithParam<int> {};

INSTANTIATE_TEST_SUITE_P(WeightTests, ThreadPoolFairnessDistributionTest,
                         testing::Values(0, 25, 50, 75, 100),
                         [](const testing::TestParamInfo<int>& info) {
                           return "HighPriority" + std::to_string(info.param) +
                                  "Percent";
                         });

TEST_P(ThreadPoolFairnessDistributionTest, StatisticalDistribution) {
  ThreadPool thread_pool("fairness-single-worker", 1);
  const int weight = GetParam();
  const int total_tasks = 1000;
  const int half_tasks = total_tasks / 2;

  thread_pool.SetHighPriorityWeight(weight);

  std::atomic<int> high_executed{0};
  std::atomic<int> low_executed{0};

  // Counter for exactly half the tasks
  absl::BlockingCounter counter(half_tasks);
  absl::BlockingCounter worker_counter(1);

  // 1. Schedule all tasks (they will be queued but not executed)
  // Tasks will suspend the pool themselves when counter reaches zero
  for (int i = 0; i < total_tasks / 2; ++i) {
    EXPECT_TRUE(thread_pool.Schedule(
        [&high_executed, &counter, &worker_counter]() {
          high_executed++;
          if (counter.DecrementCount()) {
            // This was the final task - suspend the thread immediately
            worker_counter.Wait();
          }
        },
        ThreadPool::Priority::kHigh));

    EXPECT_TRUE(thread_pool.Schedule(
        [&low_executed, &counter, &worker_counter]() {
          low_executed++;
          if (counter.DecrementCount()) {
            // This was the final task - suspend the thread immediately
            worker_counter.Wait();
          }
        },
        ThreadPool::Priority::kLow));
  }

  // 2. Verify tasks are queued but not executed yet
  EXPECT_EQ(high_executed.load(), 0);
  EXPECT_EQ(low_executed.load(), 0);
  EXPECT_EQ(thread_pool.QueueSize(), total_tasks);

  // 3. Start the worker and let it execute exactly half the tasks
  thread_pool.StartWorkers();

  // 4. Wait for exactly half the tasks to complete (pool will auto-suspend)
  counter.Wait();

  // 5. Verify exactly half the tasks were executed
  const int executed_tasks = high_executed.load() + low_executed.load();
  EXPECT_EQ(executed_tasks, half_tasks);

  // 6. Check the distribution of executed tasks
  // Test distribution based on weight
  if (weight == 0) {
    // 0% weight should mean no high priority tasks executed
    EXPECT_EQ(high_executed.load(), 0);
    EXPECT_EQ(low_executed.load(), half_tasks);
  } else if (weight == 100) {
    // 100% weight should mean no low priority tasks executed
    EXPECT_EQ(high_executed.load(), half_tasks);
    EXPECT_EQ(low_executed.load(), 0);
  } else {
    // For other weights, check exact distribution (counter-based fairness is
    // deterministic)
    const int expected_high_tasks = (half_tasks * weight) / 100;
    const int expected_low_tasks = half_tasks - expected_high_tasks;

    EXPECT_EQ(high_executed.load(), expected_high_tasks);
    EXPECT_EQ(low_executed.load(), expected_low_tasks);
  }

  VMSDK_EXPECT_OK(thread_pool.MarkForStop(ThreadPool::StopMode::kAbrupt));
  worker_counter.DecrementCount();
  thread_pool.JoinWorkers();
}

// Test kMax priority always takes precedence
TEST_P(ThreadPoolFairnessDistributionTest, MaxPriorityPreservation) {
  ThreadPool thread_pool("fairness-single-worker", 1);
  const int weight = GetParam();
  const int tasks_per_priority = 3;

  std::atomic<int> high_executed{0};
  std::atomic<int> low_executed{0};
  std::atomic<int> max_executed{0};

  absl::BlockingCounter counter(tasks_per_priority);  // 3 of kMax priority
  absl::BlockingCounter worker_counter(1);

  // Set weight to favor low priority (should not affect kMax)
  thread_pool.SetHighPriorityWeight(weight);  // 10% high, 90% low

  // Schedule tasks in reverse priority order to test precedence
  for (int i = 0; i < tasks_per_priority; ++i) {
    EXPECT_TRUE(thread_pool.Schedule([&low_executed]() { low_executed++; },
                                     ThreadPool::Priority::kLow));

    EXPECT_TRUE(thread_pool.Schedule([&high_executed]() { high_executed++; },
                                     ThreadPool::Priority::kHigh));

    EXPECT_TRUE(thread_pool.Schedule(
        [&max_executed, &counter, &worker_counter]() {
          max_executed++;
          if (counter.DecrementCount()) {
            worker_counter.Wait();
          }
        },
        ThreadPool::Priority::kMax));
  }

  thread_pool.StartWorkers();
  counter.Wait();

  // Validate only  kMax tasks should have executed
  EXPECT_EQ(high_executed.load(), 0);
  EXPECT_EQ(low_executed.load(), 0);
  EXPECT_EQ(max_executed.load(), tasks_per_priority);

  worker_counter.DecrementCount();
  // Wait for all tasks to be completed
  thread_pool.JoinWorkers();
  // All tasks should have executed
  EXPECT_EQ(high_executed.load(), tasks_per_priority);
  EXPECT_EQ(low_executed.load(), tasks_per_priority);
  EXPECT_EQ(max_executed.load(), tasks_per_priority);
}

}  // namespace vmsdk
