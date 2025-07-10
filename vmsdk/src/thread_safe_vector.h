#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <optional>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/synchronization/mutex.h"

/// A thread safe vector protected by a mutex
namespace vmsdk {
template <typename T>
class ThreadSafeVector {
 public:
  // This type is neither copyable nor assignable.
  ThreadSafeVector(const ThreadSafeVector&) = delete;
  ThreadSafeVector& operator=(const ThreadSafeVector&) = delete;
  ThreadSafeVector() = default;

  size_t Size() const {
    absl::ReaderMutexLock lock{&mutex_};
    return vec_.size();
  }
  bool IsEmpty() const { return Size() == 0; }

  /// Pop the first item that matches the predicate (starting from the top of
  /// the list)
  std::optional<T> PopIf(std::function<bool(T)> predicate) {
    absl::MutexLock lock{&mutex_};
    if (vec_.empty()) {
      return std::nullopt;
    }
    auto where = absl::c_find_if(vec_, predicate);
    if (where == vec_.end()) {
      return std::nullopt;
    }
    auto item = std::move(*where);
    vec_.erase(where);
    return item;
  }

  /// Remove up to `count` items from the end of the list and return them.
  std::vector<T> PopBackMulti(size_t count) {
    absl::MutexLock lock{&mutex_};
    count = std::min(count, vec_.size());
    if (count == 0) {
      return {};
    }

    // Move the last "count" items to our result vector and resize the original
    // vector
    std::vector<T> items;
    items.reserve(count);
    std::move(vec_.end() - count, vec_.end(), std::back_inserter(items));
    vec_.resize(vec_.size() - count);
    return items;
  }

  /// Append `item` at the end of the vector
  void Add(T item) {
    absl::MutexLock lock{&mutex_};
    vec_.push_back(std::move(item));
  }

  /// Clear the list. Before items are removed from the list, apply `callback`
  /// for each item.
  ///
  /// `callback` can be `nullptr`.
  void ClearWithCallback(std::function<void(T)> callback) {
    absl::MutexLock lock{&mutex_};
    ForEachInternal(callback);
    vec_.clear();
  }

  void ForEach(std::function<void(T)> callback) {
    absl::MutexLock lock{&mutex_};
    ForEachInternal(callback);
  }

  /// This is the same as calling `ClearWithCallback()` with `nullptr` as the
  /// callback
  void Clear() { ClearWithCallback(nullptr); }

 private:
  void ForEachInternal(std::function<void(T)> callback) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    if (callback != nullptr) {
      for (auto& item : vec_) {
        callback(item);
      }
    }
  }
  mutable absl::Mutex mutex_;
  std::vector<T> vec_ ABSL_GUARDED_BY(mutex_);
};
}  // namespace vmsdk
