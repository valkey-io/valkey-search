#pragma once

#include <functional>
#include <memory>
#include <utility>

#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"

namespace valkey_search::query::fanout {

template <typename ResponseProto, typename LocalResult>
class PartitionResultsTrackerBase {
 public:
  PartitionResultsTrackerBase() = default;
  PartitionResultsTrackerBase(const PartitionResultsTrackerBase&) = delete;
  PartitionResultsTrackerBase& operator=(const PartitionResultsTrackerBase&) =
      delete;
  virtual ~PartitionResultsTrackerBase() = default;

  // Called when a remote response arrives
  void AddResults(const ResponseProto& resp) {
    absl::MutexLock lock(&mutex_);
    OnResponse(resp);
  }

  // Called when a local partition result arrives
  void AddResults(const LocalResult& local) {
    absl::MutexLock lock(&mutex_);
    OnLocal(local);
  }

  // Called on RPC-level error
  void HandleError(const std::string& err) {
    absl::MutexLock lock(&mutex_);
    OnError(err);
  }

 protected:
  // Called under lock by the derived destructor to finish up.
  void FinishUnderLock() {
    absl::MutexLock l(&mutex_);
    OnFinished();
  }
  // Derived classes implement these to aggregate and finalize
  virtual void OnResponse(const ResponseProto& resp) = 0;
  virtual void OnLocal(const LocalResult& local) = 0;
  virtual void OnError(const std::string& err) = 0;
  virtual void OnFinished() {};

 private:
  absl::Mutex mutex_;
};

}  // namespace valkey_search::query::fanout
