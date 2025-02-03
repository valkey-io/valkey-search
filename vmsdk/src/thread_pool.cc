/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
namespace {

void *RunWorkerThread(void *arg) {
  vmsdk::ThreadPool *pool = static_cast<vmsdk::ThreadPool *>(arg);
  pool->WorkerThread();
  return nullptr;
}
}  // namespace

namespace vmsdk {

ThreadPool::ThreadPool(const std::string &name_prefix, size_t num_threads)
    : threads_(num_threads),
      priority_tasks_(static_cast<int>(ThreadPool::Priority::kMax) + 1),
      name_prefix_(name_prefix) {}

void ThreadPool::StartWorkers() {
  CHECK(!started_);
  started_ = true;
  for (size_t i = 0; i < threads_.size(); ++i) {
    pthread_create(&threads_[i], nullptr, RunWorkerThread, this);
    pthread_setname_np(threads_[i], (name_prefix_ + std::to_string(i)).c_str());
  }
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
  if (!started_) {
    return;
  }
  for (size_t i = 0; i < threads_.size(); ++i) {
    pthread_join(threads_[i], nullptr);
  }
  started_ = false;
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
        std::make_unique<absl::BlockingCounter>(threads_.size());
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
        std::make_unique<absl::BlockingCounter>(threads_.size());
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

void ThreadPool::WorkerThread() {
  while (true) {
    absl::AnyInvocable<void()> task;
    {
      absl::MutexLock lock(&queue_mutex_);
      AwaitSuspensionCleared();
      auto condition = absl::Condition(this, &ThreadPool::QueueReady);
      while (!condition.Eval()) {
        condition_.Wait(&queue_mutex_);
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
      for (auto it = priority_tasks_.rbegin(); it != priority_tasks_.rend();
           ++it) {
        if (!it->empty()) {
          task = std::move(it->front());
          it->pop();
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

}  // namespace vmsdk
