#pragma once

#include "absl/synchronization/mutex.h"
#include "absl/status/statusor.h"
#include "absl/functional/any_invocable.h"
#include <functional>
#include <memory>
#include <utility>

namespace valkey_search::query::fanout {

// -----------------------------------------------------------------------------
// A thread-safe base for aggregating fan-out results (remote & local)
// and invoking a callback once all captured references go out of scope.
//
// Template parameters:
//   ResultType      - the aggregated result type (e.g., PrimaryInfoResult)
//   ResponseProto   - the gRPC response type (e.g., InfoIndexPartitionResponse)
//   LocalResult     - the local result type (may be same as ResultType)
//   Parameters      - user-defined parameters type, passed through to callback
// -----------------------------------------------------------------------------
template<
    typename ResultType,
    typename ResponseProto,
    typename LocalResult,
    typename Parameters>
class PartitionResultsTrackerBase {
 public:
  using Callback = absl::AnyInvocable<void(absl::StatusOr<ResultType>, std::unique_ptr<Parameters>)>;

  PartitionResultsTrackerBase(
      Callback callback,
      std::unique_ptr<Parameters> parameters)
      : callback_(std::move(callback)),
        parameters_(std::move(parameters)) {}

  // Add a remote gRPC response
  void AddResults(const ResponseProto& response) {
    absl::MutexLock lock(&mutex_);
    if (HasError(response)) {
      AggregateError(GetError(response), aggregated_result_);
    } else {
      AggregateFromResponse(response, aggregated_result_);
    }
  }

  // Add a local partition result
  void AddResults(const LocalResult& local) {
    absl::MutexLock lock(&mutex_);
    if (HasError(local)) {
      AggregateError(GetError(local), aggregated_result_);
    } else {
      AggregateFromLocal(local, aggregated_result_);
    }
  }

  // Record an explicit error message
  void HandleError(const std::string& error_message) {
    absl::MutexLock lock(&mutex_);
    AggregateError(error_message, aggregated_result_);
  }

  ~PartitionResultsTrackerBase() {
    absl::MutexLock lock(&mutex_);
    callback_(aggregated_result_, std::move(parameters_));
  }

  absl::Mutex& GetMutex() { return mutex_; }
  const absl::Mutex& GetMutex() const { return mutex_; }
  ResultType& GetAggregatedResult() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) { return aggregated_result_; }
  const ResultType& GetAggregatedResult() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_) { return aggregated_result_; }
  const Callback& GetCallback() const { return callback_; }
  const std::unique_ptr<Parameters>& GetParameters() const { return parameters_; }

 protected:
  // Subclasses must implement these to merge each partition's data
  virtual void AggregateFromResponse(const ResponseProto& response,
                                     ResultType& result) = 0;
  virtual void AggregateFromLocal(const LocalResult& local,
                                  ResultType& result) = 0;
  virtual void AggregateError(const std::string& error,
                              ResultType& result) = 0;

  // Subclasses can override these if error-testing differs
  virtual bool HasError(const ResponseProto& response) { return !response.error().empty(); }
  virtual std::string GetError(const ResponseProto& response) { return response.error(); }
  virtual bool HasError(const LocalResult& local) { return !local.error.empty(); }
  virtual std::string GetError(const LocalResult& local) { return local.error; }

private:
  absl::Mutex mutex_;
  ResultType aggregated_result_ ABSL_GUARDED_BY(mutex_);
  Callback callback_;
  std::unique_ptr<Parameters> parameters_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace valkey_search::query::fanout
