/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/fanout.h"

#include <atomic>
#include <memory>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "gmock/gmock.h"
#include "grpcpp/support/status.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/util.h"
#include "src/query/search.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/cluster_map.h"
#include "vmsdk/src/thread_pool.h"

namespace valkey_search::query::fanout {
namespace {

using ::testing::_;

class MockSearchParameters : public query::SearchParameters {
 public:
  // Save the final status after the tracker is destroyed.
  std::shared_ptr<absl::Status> final_status = std::make_shared<absl::Status>();

  void QueryCompleteBackground(
      std::unique_ptr<query::SearchParameters> self) override {
    *final_status = self->search_result.status;
  }
  void QueryCompleteMainThread(
      std::unique_ptr<query::SearchParameters> self) override {
    *final_status = self->search_result.status;
  }
};

vmsdk::cluster_map::NodeInfo MakeRemoteNode(int port) {
  vmsdk::cluster_map::NodeInfo node;
  node.socket_address.primary_endpoint = "127.0.0.1";
  node.socket_address.port = port;
  return node;
}

vmsdk::cluster_map::NodeInfo MakeLocalNode() {
  vmsdk::cluster_map::NodeInfo node;
  node.is_local = true;
  return node;
}

std::string CoordinatorAddress(int port) {
  return absl::StrCat("127.0.0.1:", coordinator::GetCoordinatorPort(port));
}

class FanoutCancellationTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    auto index_schema = CreateIndexSchema("fanout_test_index", &fake_ctx_);
    ASSERT_TRUE(index_schema.ok()) << index_schema.status();
    index_schema_ = std::move(index_schema.value());

    thread_pool_ = std::make_unique<vmsdk::ThreadPool>("fanout-test-pool", 1);
    thread_pool_->StartWorkers();
    saved_async_fanout_threshold_ =
        options::GetAsyncFanoutThreshold().GetValue();
  }

  void TearDown() override {
    VMSDK_EXPECT_OK(options::GetAsyncFanoutThreshold().SetValue(
        saved_async_fanout_threshold_));
    thread_pool_->JoinWorkers();
    index_schema_.reset();
    ValkeySearchTest::TearDown();
  }

  void CreatePendingRemote(
      int port, std::stop_token* token_out,
      coordinator::SearchIndexPartitionCallback* done_out = nullptr) {
    auto client = std::make_shared<coordinator::MockClient>();
    EXPECT_CALL(client_pool_, GetClient(CoordinatorAddress(port)))
        .WillRepeatedly(testing::Return(client));
    EXPECT_CALL(*client, SearchIndexPartition(_, _, _))
        .WillOnce([token_out, done_out](
                      std::unique_ptr<coordinator::SearchIndexPartitionRequest>,
                      std::stop_token token,
                      coordinator::SearchIndexPartitionCallback done) {
          *token_out = token;
          if (done_out != nullptr) {
            *done_out = std::move(done);
          }
        });
  }

  void CreateRemoteCompletingWithStatus(int port, grpc::Status status) {
    auto client = std::make_shared<coordinator::MockClient>();
    EXPECT_CALL(client_pool_, GetClient(CoordinatorAddress(port)))
        .WillRepeatedly(testing::Return(client));
    EXPECT_CALL(*client, SearchIndexPartition(_, _, _))
        .WillOnce(
            [status](std::unique_ptr<coordinator::SearchIndexPartitionRequest>,
                     std::stop_token,
                     coordinator::SearchIndexPartitionCallback done) {
              coordinator::SearchIndexPartitionResponse response;
              done(status, response);
            });
  }

  // Record whether stop was requested before the RPC call.
  // Return CANCELLED, as TryCancel would.
  void CreateRemoteObservingStop(int port, std::atomic_bool* stopped_at_call) {
    auto client = std::make_shared<coordinator::MockClient>();
    EXPECT_CALL(client_pool_, GetClient(CoordinatorAddress(port)))
        .WillRepeatedly(testing::Return(client));
    EXPECT_CALL(*client, SearchIndexPartition(_, _, _))
        .WillOnce([stopped_at_call](
                      std::unique_ptr<coordinator::SearchIndexPartitionRequest>,
                      std::stop_token token,
                      coordinator::SearchIndexPartitionCallback done) {
          stopped_at_call->store(token.stop_requested());
          coordinator::SearchIndexPartitionResponse response;
          done(grpc::Status(grpc::StatusCode::CANCELLED, "cancelled"),
               response);
        });
  }

  std::unique_ptr<MockSearchParameters> CreateMockParameter(
      bool enable_partial_results = false) {
    auto parameters = std::make_unique<MockSearchParameters>();
    parameters->index_schema = index_schema_;
    parameters->index_schema_name = index_schema_->GetName();
    parameters->db_num = 0;
    parameters->no_content = true;
    parameters->enable_partial_results = enable_partial_results;
    parameters->cancellation_token = cancel::Make(10000, nullptr);
    return parameters;
  }

  ValkeyModuleCtx fake_ctx_;
  std::shared_ptr<MockIndexSchema> index_schema_;
  coordinator::MockClientPool client_pool_;
  std::unique_ptr<vmsdk::ThreadPool> thread_pool_;
  long long saved_async_fanout_threshold_;
};

// Set up two pending remotes, one failing remote, and one local search.
// Expect the failure to cancel all other work.
TEST_F(FanoutCancellationTest, RemoteFailureCancelsOtherRemotesAndLocal) {
  auto pending_remote_a = MakeRemoteNode(1);
  auto pending_remote_b = MakeRemoteNode(2);
  auto failing_remote = MakeRemoteNode(3);
  auto local = MakeLocalNode();
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {
      pending_remote_a, pending_remote_b, failing_remote, local};

  std::stop_token remote_a_token;
  std::stop_token remote_b_token;
  CreatePendingRemote(1, &remote_a_token);
  CreatePendingRemote(2, &remote_b_token);
  CreateRemoteCompletingWithStatus(
      3, grpc::Status(grpc::StatusCode::INTERNAL, "remote failure"));

  auto parameters = CreateMockParameter();
  auto local_token = parameters->cancellation_token;

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();

  EXPECT_TRUE(remote_a_token.stop_requested());
  EXPECT_TRUE(remote_b_token.stop_requested());
  EXPECT_TRUE(local_token->IsCancelled());
}

// Set up a failing local search and one pending remote.
// Expect the local failure to cancel the remote.
TEST_F(FanoutCancellationTest, LocalFailureCancelsRemoteSibling) {
  auto pending_remote = MakeRemoteNode(1);
  auto local = MakeLocalNode();
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {pending_remote, local};

  std::stop_token remote_token;
  CreatePendingRemote(1, &remote_token);

  auto parameters = CreateMockParameter();
  parameters->attribute_alias = "nonexistent_attribute";
  parameters->k = 1;

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();

  EXPECT_TRUE(remote_token.stop_requested());
}

// Set up one failing and one pending remote with partial results enabled.
// Expect the sibling and fanout token to remain active.
TEST_F(FanoutCancellationTest,
       RemoteFailureDoesNotCancelSiblingsWithPartialResults) {
  auto pending_remote = MakeRemoteNode(1);
  auto failing_remote = MakeRemoteNode(2);
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {pending_remote,
                                                       failing_remote};

  std::stop_token remote_token;
  coordinator::SearchIndexPartitionCallback pending_done;
  CreatePendingRemote(1, &remote_token, &pending_done);
  CreateRemoteCompletingWithStatus(
      2, grpc::Status(grpc::StatusCode::INTERNAL, "remote failure"));

  auto parameters = CreateMockParameter(true);
  auto fanout_token = parameters->cancellation_token;

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();

  EXPECT_FALSE(remote_token.stop_requested());
  EXPECT_FALSE(fanout_token->IsCancelled());

  coordinator::SearchIndexPartitionResponse response;
  pending_done(grpc::Status::OK, response);
  pending_done = nullptr;
}

// Set up a consistency failure with partial results enabled.
// Expect the consistency failure to cancel the sibling.
TEST_F(FanoutCancellationTest,
       ConsistencyFailureCancelsSiblingsDespitePartialResults) {
  auto pending_remote = MakeRemoteNode(1);
  auto failing_remote = MakeRemoteNode(2);
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {pending_remote,
                                                       failing_remote};

  std::stop_token remote_token;
  CreatePendingRemote(1, &remote_token);
  CreateRemoteCompletingWithStatus(
      2, grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                      "stale slot fingerprint"));

  // Partial results do not allow consistency failures.
  auto parameters = CreateMockParameter(/*enable_partial_results=*/true);
  parameters->enable_consistency = true;
  auto fanout_token = parameters->cancellation_token;

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();

  EXPECT_TRUE(remote_token.stop_requested());
  EXPECT_TRUE(fanout_token->IsCancelled());
}

// Set up two remote targets with one failure and no local target.
// Expect the failure to cancel the remote sibling.
TEST_F(FanoutCancellationTest, RemoteOnlyFanoutFailureCancelsSiblings) {
  // Test cancellation without a local target.
  auto pending_remote = MakeRemoteNode(1);
  auto failing_remote = MakeRemoteNode(2);
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {pending_remote,
                                                       failing_remote};

  std::stop_token remote_token;
  CreatePendingRemote(1, &remote_token);
  CreateRemoteCompletingWithStatus(
      2, grpc::Status(grpc::StatusCode::INTERNAL, "remote failure"));

  auto parameters = CreateMockParameter();

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();

  EXPECT_TRUE(remote_token.stop_requested());
}

// Set up one failing and one cancelled remote.
// Expect the original failure to remain the final error.
TEST_F(FanoutCancellationTest, FirstErrorWinsOverPropagatedCancellations) {
  auto pending_remote = MakeRemoteNode(1);
  auto failing_remote = MakeRemoteNode(2);
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {pending_remote,
                                                       failing_remote};

  std::stop_token remote_token;
  coordinator::SearchIndexPartitionCallback pending_done;
  CreatePendingRemote(1, &remote_token, &pending_done);
  CreateRemoteCompletingWithStatus(
      2, grpc::Status(grpc::StatusCode::INTERNAL, "remote failure"));

  auto parameters = CreateMockParameter();
  auto final_status = parameters->final_status;

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();
  ASSERT_TRUE(remote_token.stop_requested());

  // A cancelled sibling must not replace the root error.
  coordinator::SearchIndexPartitionResponse response;
  pending_done(grpc::Status(grpc::StatusCode::CANCELLED, "cancelled"),
               response);
  // Release the callback to destroy the tracker.
  pending_done = nullptr;

  EXPECT_EQ(final_status->code(), absl::StatusCode::kInternal);
  EXPECT_EQ(final_status->message(), "remote failure");
}

// Queue one failing and two pending remotes behind a blocked worker.
// Expect queued calls to observe cancellation before they start.
TEST_F(FanoutCancellationTest, QueuedAsyncRemotesObserveStopBeforeDispatch) {
  // Use two workers and three tasks. Block one worker.
  // The failure task runs first. Queued tasks must see the stop.
  VMSDK_EXPECT_OK(options::GetAsyncFanoutThreshold().SetValue(2));
  absl::Notification release_blocker;
  vmsdk::ThreadPool multi_worker_pool("fanout-async-pool", 2);
  multi_worker_pool.StartWorkers();
  multi_worker_pool.Schedule(
      [&release_blocker] { release_blocker.WaitForNotification(); },
      vmsdk::ThreadPool::Priority::kHigh);

  auto failing_remote = MakeRemoteNode(1);
  auto queued_remote_a = MakeRemoteNode(2);
  auto queued_remote_b = MakeRemoteNode(3);
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {
      failing_remote, queued_remote_a, queued_remote_b};

  // Notify after the failure handler requests stop.
  absl::Notification failure_handled;
  auto failing_client = std::make_shared<coordinator::MockClient>();
  EXPECT_CALL(client_pool_, GetClient(CoordinatorAddress(1)))
      .WillRepeatedly(testing::Return(failing_client));
  EXPECT_CALL(*failing_client, SearchIndexPartition(_, _, _))
      .WillOnce([&failure_handled](
                    std::unique_ptr<coordinator::SearchIndexPartitionRequest>,
                    std::stop_token,
                    coordinator::SearchIndexPartitionCallback done) {
        coordinator::SearchIndexPartitionResponse response;
        done(grpc::Status(grpc::StatusCode::INTERNAL, "remote failure"),
             response);
        failure_handled.Notify();
      });
  std::atomic_bool a_stopped_at_call{false};
  std::atomic_bool b_stopped_at_call{false};
  CreateRemoteObservingStop(2, &a_stopped_at_call);
  CreateRemoteObservingStop(3, &b_stopped_at_call);

  auto parameters = CreateMockParameter();
  auto final_status = parameters->final_status;

  // Always release the blocked worker and drain the pool.
  // JoinWorkers() is idempotent.
  absl::Cleanup drain_pool = [&release_blocker, &multi_worker_pool] {
    if (!release_blocker.HasBeenNotified()) {
      release_blocker.Notify();
    }
    multi_worker_pool.JoinWorkers();
  };

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       &multi_worker_pool)
                  .ok());
  // Release the worker after cancellation.
  ASSERT_TRUE(
      failure_handled.WaitForNotificationWithTimeout(absl::Seconds(30)));
  release_blocker.Notify();
  multi_worker_pool.JoinWorkers();

  EXPECT_TRUE(a_stopped_at_call.load());
  EXPECT_TRUE(b_stopped_at_call.load());
  // Propagated CANCELLED must not replace the root error.
  EXPECT_EQ(final_status->code(), absl::StatusCode::kInternal);
}

// Set up a failing local search and one pending remote with partial results.
// Expect the remote and fanout token to remain active.
TEST_F(FanoutCancellationTest,
       LocalFailureDoesNotCancelSiblingsWithPartialResults) {
  auto pending_remote = MakeRemoteNode(1);
  auto local = MakeLocalNode();
  std::vector<vmsdk::cluster_map::NodeInfo> targets = {pending_remote, local};

  std::stop_token remote_token;
  coordinator::SearchIndexPartitionCallback pending_done;
  CreatePendingRemote(1, &remote_token, &pending_done);

  // Local failure with partial results must not cancel the remote.
  auto parameters = CreateMockParameter(/*enable_partial_results=*/true);
  parameters->attribute_alias = "nonexistent_attribute";
  parameters->k = 1;
  auto fanout_token = parameters->cancellation_token;

  ASSERT_TRUE(PerformSearchFanoutAsync(&fake_ctx_, targets, &client_pool_,
                                       std::move(parameters),
                                       thread_pool_.get())
                  .ok());
  thread_pool_->JoinWorkers();

  EXPECT_FALSE(remote_token.stop_requested());
  EXPECT_FALSE(fanout_token->IsCancelled());

  coordinator::SearchIndexPartitionResponse response;
  pending_done(grpc::Status::OK, response);
  pending_done = nullptr;
}

}  // namespace
}  // namespace valkey_search::query::fanout
