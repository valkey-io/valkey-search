/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/coordinator/client.h"
#include "src/coordinator/coordinator.pb.h"
#include "testing/coordinator/common.h"

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

// Per-arm errors are encoded inline in MultiSearchSubResponse so a single arm
// failure does not kill the whole RPC.
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

}  // namespace valkey_search::coordinator
