/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/thread_pool.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <numeric>
#include <ranges>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
#include "vmsdk/src/module_config.h"

namespace {

class ThreadRunContext {
 public:
  ThreadRunContext(vmsdk::ThreadPool *pool,
                   std::shared_ptr<vmsdk::ThreadPool::Thread> thread)
      : pool_{pool}, thread_{thread} {}

  vmsdk::ThreadPool *GetPool() { return pool_; }
  std::shared_ptr<vmsdk::ThreadPool::Thread> GetThread() { return thread_; }

 private:
  vmsdk::ThreadPool *pool_ = nullptr;
  std::shared_ptr<vmsdk::ThreadPool::Thread> thread_ = nullptr;
};

void *RunWorkerThread(void *arg) {
  ThreadRunContext *ctx = static_cast<ThreadRunContext *>(arg);
  ctx->GetPool()->WorkerThread(ctx->GetThread());
  delete ctx;  // shallow delete
  return nullptr;
}
}  // namespace

namespace vmsdk {

ThreadPool::ThreadPool(const std::string &name_prefix, size_t num_threads)
    : initial_thread_count_(num_threads),
      priority_tasks_(static_cast<int>(ThreadPool::Priority::kMax) + 1),
      name_prefix_(name_prefix) {}

void ThreadPool::StartWorkers() {
  CHECK(!started_);
  started_ = true;
  IncrThreadCountBy(initial_thread_count_);
}

bool ThreadPool::Schedule(absl::AnyInvocable<void()> task, Priority priority) {
  absl::MutexLock lock(&queue_mutex_);
  if (stop_mode_.has_value()) {
    return false;
  }
  auto &tasks_queue = GetPriorityTasksQueue(priority);
  tasks_queue.emplace(std::move(task));
  condition_.Signal();
  return true;
}

absl::Status ThreadPool::MarkForStop(StopMode stop_mode) {
  absl::MutexLock lock(&queue_mutex_);
  if (stop_mode_ == stop_mode) {
    return absl::OkStatus();
  }
  if (stop_mode_ == StopMode::kAbrupt && stop_mode == StopMode::kGraceful) {
    return absl::InvalidArgumentError(
        "Cannot set stop mode to kGraceful after kAbrupt mode was set");
  }
  stop_mode_ = stop_mode;
  suspend_workers_ = false;
  condition_.SignalAll();
  return absl::OkStatus();
}

ThreadPool::~ThreadPool() { JoinWorkers(); }

void ThreadPool::JoinWorkers() {
  {
    absl::MutexLock lock(&queue_mutex_);
    if (!stop_mode_.has_value()) {
      stop_mode_ = StopMode::kGraceful;
      condition_.SignalAll();
    }
    suspend_workers_ = false;
  }

  threads_.ClearWithCallback(
      [](auto thread) { pthread_join(thread->thread_id, nullptr); });
  started_ = false;

  JoinTerminatedWorkers();
}

void ThreadPool::JoinTerminatedWorkers() {
  pending_join_threads_.ClearWithCallback(
      [](auto thread) { pthread_join(thread->thread_id, nullptr); });
}

absl::Status ThreadPool::SuspendWorkers() {
  absl::MutexLock lock(&suspend_resume_mutex_);
  DCHECK(!blocking_refcount_);
  {
    absl::MutexLock lock(&queue_mutex_);
    if (!started_) {
      return absl::InvalidArgumentError("Thread pool is not started");
    }
    if (stop_mode_.has_value()) {
      return absl::InvalidArgumentError(
          "Cannot suspend workers after the thread pool is marked for stop");
    }
    if (suspend_workers_) {
      return absl::InvalidArgumentError("Thread pool is already suspended");
    }
    suspend_workers_ = true;
    blocking_refcount_ =
        std::make_unique<absl::BlockingCounter>(threads_.Size());
    condition_.SignalAll();
  }
  blocking_refcount_->Wait();
  blocking_refcount_ = nullptr;
  return absl::OkStatus();
}

absl::Status ThreadPool::ResumeWorkers() {
  absl::MutexLock lock(&suspend_resume_mutex_);
  DCHECK(!blocking_refcount_);
  {
    absl::MutexLock lock(&queue_mutex_);
    if (stop_mode_.has_value()) {
      return absl::InvalidArgumentError(
          "Cannot resume workers after the thread pool is marked for stop");
    }
    if (!suspend_workers_) {
      return absl::InvalidArgumentError("Thread pool is not suspended");
    }
    suspend_workers_ = false;
    blocking_refcount_ =
        std::make_unique<absl::BlockingCounter>(threads_.Size());
  }

  blocking_refcount_->Wait();
  blocking_refcount_ = nullptr;
  return absl::OkStatus();
}

bool SuspendResumeReady(bool *suspend_workers) { return !(*suspend_workers); }

void ThreadPool::AwaitSuspensionCleared()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(queue_mutex_) {
  if (!suspend_workers_) {
    return;
  }
  blocking_refcount_->DecrementCount();
  queue_mutex_.Await(absl::Condition(SuspendResumeReady, &suspend_workers_));
  if (blocking_refcount_) {
    blocking_refcount_->DecrementCount();
  }
}

void ThreadPool::WorkerThread(std::shared_ptr<Thread> thread) {
  while (true) {
    absl::AnyInvocable<void()> task;
    {
      absl::MutexLock lock(&queue_mutex_);
      AwaitSuspensionCleared();
      auto condition = absl::Condition(this, &ThreadPool::QueueReady);
      while (!condition.Eval()) {
        condition_.WaitWithTimeout(&queue_mutex_, absl::Seconds(1));
        if (thread->IsShutdown()) {
          thread->InvokeShutdownCallback();
          // remove this thread from the threads list and place it in the
          // pending join list
          threads_.PopIf([thread](std::shared_ptr<Thread> t) {
            return t->thread_id == thread->thread_id;
          });
          pending_join_threads_.Add(thread);
          return;
        }
      }
      if (stop_mode_.has_value() &&
          (stop_mode_.value() == StopMode::kAbrupt ||
           std::all_of(priority_tasks_.begin(), priority_tasks_.end(),
                       [](const auto &tasks) { return tasks.empty(); }))) {
        return;
      }
      if (suspend_workers_) {
        continue;
      }

      for (auto &tasks : priority_tasks_ | std::views::reverse) {
        if (!tasks.empty()) {
          task = std::move(tasks.front());
          tasks.pop();
          break;
        }
      }
    }
    task();
  }
}

size_t ThreadPool::QueueSize() const {
  absl::MutexLock lock(&queue_mutex_);
  return std::accumulate(
      priority_tasks_.begin(), priority_tasks_.end(), 0,
      [](size_t sum, const auto &tasks) { return sum + tasks.size(); });
}

void ThreadPool::IncrThreadCountBy(size_t count) {
  for (size_t i = 0; i < count; ++i) {
    std::shared_ptr<Thread> thread_ptr = std::make_shared<Thread>();
    ThreadRunContext *context = new ThreadRunContext{this, thread_ptr};
    pthread_create(&thread_ptr->thread_id, nullptr, RunWorkerThread, context);
#ifndef __APPLE__
    size_t thread_num = threads_.Size();
    pthread_setname_np(thread_ptr->thread_id,
                       (name_prefix_ + std::to_string(thread_num)).c_str());
#endif
    threads_.Add(thread_ptr);
  }
}

void ThreadPool::DecrThreadCountBy(size_t count, bool sync) {
  auto threads = threads_.PopBackMulti(count);
  absl::BlockingCounter counter{static_cast<int>(threads.size())};
  for (const auto &thread : threads) {
    if (sync) {
      thread->Shutdown([&counter]() {
        counter.DecrementCount();
      });  // signal the thread to exit
    } else {
      thread->Shutdown();
    }
  }

  if (sync) {
    counter.Wait();
  }
}

void ThreadPool::Resize(size_t count, bool wait_for_resize) {
  size_t current_size = Size();
  if (count == current_size) {
    return;
  } else if (count > current_size) {
    // We need to add more threads
    IncrThreadCountBy(count - current_size);
  } else {
    // Shutdown "current_size - count" threads
    DecrThreadCountBy(current_size - count, wait_for_resize);
  }
}
}  // namespace vmsdk
