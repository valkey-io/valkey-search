/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gmock/gmock.h"
#include "grpcpp/grpcpp.h"
#include "gtest/gtest.h"
#include "src/coordinator/client.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/server.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

class MultiSearchClientTest : public ::testing::Test {
 protected:
  void SetUp() override { mock_client_ = std::make_shared<MockClient>(); }
  std::shared_ptr<MockClient> mock_client_;
};

// Verify that MockClient::MultiSearchIndexPartition is invocable and that the
// callback receives the response object. Establishes the basic gMock wiring
// for the new RPC method.
TEST_F(MultiSearchClientTest, MockInvokesUserCallback) {
  // Build a 2-arm request whose sub-requests share envelope-relevant fields.
  auto request = std::make_unique<MultiSearchIndexPartitionRequest>();
  for (int i = 0; i < 2; ++i) {
    auto* sub = request->add_sub_requests();
    sub->set_db_num(0);
    sub->set_index_schema_name("idx");
    sub->set_timeout_ms(1000);
    sub->set_dialect(2);
    sub->mutable_index_fingerprint_version()->set_fingerprint(0xABCD);
    sub->mutable_index_fingerprint_version()->set_version(7);
    sub->set_slot_fingerprint(0x1234);
  }
  // Make the two arms semantically distinct (vector vs non-vector).
  request->mutable_sub_requests(1)->set_attribute_alias("vec");
  request->mutable_sub_requests(1)->set_k(5);

  EXPECT_CALL(*mock_client_, MultiSearchIndexPartition(testing::_, testing::_))
      .WillOnce([](std::unique_ptr<MultiSearchIndexPartitionRequest> req,
                   MultiSearchIndexPartitionCallback done) {
        ASSERT_EQ(req->sub_requests_size(), 2);
        EXPECT_EQ(req->sub_requests(0).index_schema_name(), "idx");
        EXPECT_EQ(req->sub_requests(1).attribute_alias(), "vec");

        MultiSearchIndexPartitionResponse response;
        // arm[0]: success with one neighbor.
        auto* sub0 = response.add_sub_responses();
        sub0->set_grpc_code(0);
        auto* n0 = sub0->mutable_response()->add_neighbors();
        n0->set_key("doc:1");
        n0->set_score(0.0);
        sub0->mutable_response()->set_total_count(1);
        // arm[1]: success with two neighbors.
        auto* sub1 = response.add_sub_responses();
        sub1->set_grpc_code(0);
        auto* n1a = sub1->mutable_response()->add_neighbors();
        n1a->set_key("doc:2");
        n1a->set_score(0.5);
        auto* n1b = sub1->mutable_response()->add_neighbors();
        n1b->set_key("doc:3");
        n1b->set_score(0.7);
        sub1->mutable_response()->set_total_count(2);

        done(grpc::Status::OK, response);
      });

  bool callback_called = false;
  mock_client_->MultiSearchIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status,
                         MultiSearchIndexPartitionResponse& resp) {
        EXPECT_TRUE(status.ok());
        ASSERT_EQ(resp.sub_responses_size(), 2);
        EXPECT_EQ(resp.sub_responses(0).grpc_code(), 0u);
        EXPECT_EQ(resp.sub_responses(0).response().neighbors_size(), 1);
        EXPECT_EQ(resp.sub_responses(1).response().neighbors_size(), 2);
        callback_called = true;
      });
  EXPECT_TRUE(callback_called);
}

// Round-trips the per-arm status fields of MultiSearchSubResponse and checks
// the client's demux of them.
//
// This drives a MockClient, so it exercises a response shape the real server
// does not emit: MultiSearchIndexPartition finishes the reactor with the first
// non-OK arm status, and a unary RPC that finishes non-OK never delivers its
// message, so a delivered sub-response always carries grpc_code == 0. A shard
// answers every arm or none -- see the comment on MultiSearchSubResponse in
// coordinator.proto for why. This covers the client's defensive demux path,
// not a failure policy.
TEST_F(MultiSearchClientTest, PerArmErrorEncodedInline) {
  auto request = std::make_unique<MultiSearchIndexPartitionRequest>();
  request->add_sub_requests();
  request->add_sub_requests();

  EXPECT_CALL(*mock_client_, MultiSearchIndexPartition(testing::_, testing::_))
      .WillOnce([](std::unique_ptr<MultiSearchIndexPartitionRequest> req,
                   MultiSearchIndexPartitionCallback done) {
        MultiSearchIndexPartitionResponse response;
        // arm[0]: success.
        response.add_sub_responses()->set_grpc_code(0);
        // arm[1]: per-arm cancellation.
        auto* sub1 = response.add_sub_responses();
        sub1->set_grpc_code(
            static_cast<uint32_t>(grpc::StatusCode::DEADLINE_EXCEEDED));
        sub1->set_error_message("arm 1 cancelled");
        done(grpc::Status::OK, response);
      });

  mock_client_->MultiSearchIndexPartition(
      std::move(request),
      [](grpc::Status status, MultiSearchIndexPartitionResponse& resp) {
        EXPECT_TRUE(status.ok());  // RPC itself succeeded
        ASSERT_EQ(resp.sub_responses_size(), 2);
        EXPECT_EQ(resp.sub_responses(0).grpc_code(), 0u);
        EXPECT_EQ(resp.sub_responses(1).grpc_code(),
                  static_cast<uint32_t>(grpc::StatusCode::DEADLINE_EXCEEDED));
        EXPECT_EQ(resp.sub_responses(1).error_message(), "arm 1 cancelled");
      });
}

// Round-trip the MultiSearch request/response through SerializeAsString /
// ParseFromString to confirm the proto wire format is well-formed.
TEST(MultiSearchProtoTest, RequestRoundTrip) {
  MultiSearchIndexPartitionRequest req;
  for (int i = 0; i < 3; ++i) {
    auto* sub = req.add_sub_requests();
    sub->set_db_num(2);
    sub->set_index_schema_name("idx");
    sub->set_timeout_ms(500);
    sub->set_dialect(2);
    sub->mutable_index_fingerprint_version()->set_fingerprint(i);
    sub->mutable_index_fingerprint_version()->set_version(1);
    sub->set_slot_fingerprint(0x99);
  }
  std::string serialized;
  ASSERT_TRUE(req.SerializeToString(&serialized));

  MultiSearchIndexPartitionRequest decoded;
  ASSERT_TRUE(decoded.ParseFromString(serialized));
  EXPECT_EQ(decoded.sub_requests_size(), 3);
  EXPECT_EQ(decoded.sub_requests(2).index_fingerprint_version().fingerprint(),
            2u);
}

TEST(MultiSearchProtoTest, ResponseRoundTrip) {
  MultiSearchIndexPartitionResponse resp;
  for (int i = 0; i < 2; ++i) {
    auto* sub = resp.add_sub_responses();
    sub->set_grpc_code(i);
    if (i == 1) {
      sub->set_error_message("oops");
    }
    auto* n = sub->mutable_response()->add_neighbors();
    n->set_key("k" + std::to_string(i));
    n->set_score(0.1f * static_cast<float>(i));
    sub->mutable_response()->set_total_count(1);
  }
  std::string serialized;
  ASSERT_TRUE(resp.SerializeToString(&serialized));

  MultiSearchIndexPartitionResponse decoded;
  ASSERT_TRUE(decoded.ParseFromString(serialized));
  EXPECT_EQ(decoded.sub_responses_size(), 2);
  EXPECT_EQ(decoded.sub_responses(1).error_message(), "oops");
  EXPECT_EQ(decoded.sub_responses(0).response().neighbors(0).key(), "k0");
}

// ---------------------------------------------------------------------------
// Enqueue refusal: the reader thread pool rejects the search.
//
// ThreadPool::Schedule returns false once the pool is in stop mode (module
// shutdown joins the pools while the coordinator gRPC server is still serving,
// so this is racy by construction). The scheduled task -- and with it the
// search parameters and the arm completion callback -- is then destroyed
// without ever running, so nothing finishes the reactor unless the handler
// notices the refusal. These tests drive a real in-process gRPC server whose
// reader pool refuses every task, and assert the RPC completes with
// UNAVAILABLE instead of hanging until the client deadline.
// ---------------------------------------------------------------------------
class EnqueueRefusedTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    index_schema_ =
        CreateIndexSchema(std::string(kIndexName), &fake_ctx_).value();
    EXPECT_CALL(*index_schema_, GetIdentifier(::testing::_))
        .Times(::testing::AnyNumber());

    // A pool that refuses everything, exactly as a pool in stop mode does.
    reader_pool_ = std::make_unique<MockThreadPool>("test-readers", 1);
    EXPECT_CALL(*reader_pool_, Schedule(::testing::_, ::testing::_))
        .WillRepeatedly(::testing::Return(false));

    service_ = std::make_unique<Service>(
        vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(&fake_ctx_),
        reader_pool_.get());
    grpc::ServerBuilder builder;
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_NE(server_, nullptr);
    stub_ = Coordinator::NewStub(
        server_->InProcessChannel(grpc::ChannelArguments()));
  }

  void TearDown() override {
    stub_.reset();
    // An RPC handler that leaves its reactor unfinished -- the regression these
    // tests guard -- blocks Shutdown()/Wait() forever, even with a shutdown
    // deadline. Bound it: shut the server down on its own thread, and if that
    // does not finish, report it and deliberately leak the server, the service
    // and the pool rather than hanging the suite. Only a failing run leaks.
    auto shutdown_done = std::make_shared<std::atomic<bool>>(false);
    grpc::Server* server = server_.get();
    std::thread([server, shutdown_done] {
      server->Shutdown(std::chrono::system_clock::now() +
                       std::chrono::seconds(2));
      server->Wait();
      shutdown_done->store(true);
    }).detach();
    for (int i = 0; i < 100 && !shutdown_done->load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!shutdown_done->load()) {
      ADD_FAILURE() << "coordinator server did not shut down within 10s: an "
                       "RPC handler left its reactor unfinished";
      (void)server_.release();
      (void)service_.release();
      (void)reader_pool_.release();
      index_schema_.reset();
      ValkeySearchTest::TearDown();
      return;
    }
    server_.reset();
    service_.reset();
    reader_pool_.reset();
    index_schema_.reset();
    ValkeySearchTest::TearDown();
  }

  // A request that passes every check ahead of the enqueue, so the refusal is
  // the only thing that can fail it.
  SearchIndexPartitionRequest MakeRequest() const {
    SearchIndexPartitionRequest request;
    request.set_db_num(0);
    request.set_index_schema_name(std::string(kIndexName));
    request.set_timeout_ms(60000);
    request.set_dialect(2);
    request.set_score_as("__score");
    request.set_no_content(true);
    request.set_enable_consistency(false);
    request.mutable_index_fingerprint_version()->set_fingerprint(
        index_schema_->GetFingerprint());
    request.mutable_index_fingerprint_version()->set_version(
        index_schema_->GetVersion());
    request.set_slot_fingerprint(0);
    return request;
  }

  // Every RPC below carries a deadline: a regression that leaves the reactor
  // unfinished must fail the test, not hang the suite.
  static std::unique_ptr<grpc::ClientContext> MakeContext() {
    auto context = std::make_unique<grpc::ClientContext>();
    context->set_deadline(std::chrono::system_clock::now() +
                          std::chrono::seconds(10));
    return context;
  }

  static constexpr absl::string_view kIndexName{"enqueue_refused_index"};
  std::shared_ptr<MockIndexSchema> index_schema_;
  std::unique_ptr<MockThreadPool> reader_pool_;
  std::unique_ptr<Service> service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<Coordinator::Stub> stub_;
};

// Single-arm reactor: SearchIndexPartition must finish the reactor itself when
// the enqueue is refused.
TEST_F(EnqueueRefusedTest, SingleArmFinishesWithUnavailable) {
  auto context = MakeContext();
  SearchIndexPartitionResponse response;
  grpc::Status status =
      stub_->SearchIndexPartition(context.get(), MakeRequest(), &response);

  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE)
      << "error_message: " << status.error_message();
  EXPECT_EQ(response.neighbors_size(), 0);
}

// Multi-arm reactor: every arm must report, so Completion::remaining reaches
// zero and the enclosing reactor finishes with the first arm error.
TEST_F(EnqueueRefusedTest, MultiArmFinishesWithUnavailable) {
  auto context = MakeContext();
  MultiSearchIndexPartitionRequest request;
  *request.add_sub_requests() = MakeRequest();
  *request.add_sub_requests() = MakeRequest();
  MultiSearchIndexPartitionResponse response;

  grpc::Status status =
      stub_->MultiSearchIndexPartition(context.get(), request, &response);

  // The reactor only finishes once Completion::remaining reaches zero, so a
  // prompt UNAVAILABLE is proof that both arms reported; an arm that silently
  // dropped its callback would surface here as DEADLINE_EXCEEDED.
  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE)
      << "error_message: " << status.error_message();
  // gRPC drops the response message of a unary call that finishes non-OK, so
  // the per-arm sub_responses are not observable from the client here.
}

}  // namespace valkey_search::coordinator
