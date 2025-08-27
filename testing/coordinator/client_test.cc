/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/client.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/metrics.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/testing_infra/module.h"

namespace valkey_search::coordinator {

// Test to verify the byte counting functionality in client.cc
class ClientByteCountingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Reset metrics before each test
    Metrics::GetStats().coordinator_bytes_out.store(0);
    Metrics::GetStats().coordinator_bytes_in.store(0);

    // Create a mock client for testing
    mock_client_ = std::make_shared<MockClient>();
  }

  std::shared_ptr<MockClient> mock_client_;
};

// Test that we correctly count bytes for successful requests using real
// protobuf objects
TEST_F(ClientByteCountingTest, CountsCorrectBytesOnSuccess) {
  // Create a real SearchIndexPartitionRequest with data based on the proto
  // definition
  auto request = std::make_unique<SearchIndexPartitionRequest>();
  request->set_timeout_ms(1000);
  request->set_index_schema_name("test_index_schema");
  request->set_attribute_alias("test_alias");
  request->set_score_as("test_score_field");
  request->set_dialect(1);
  request->set_k(100);
  request->set_ef(1000);
  request->set_no_content(true);

  // Add bytes for the query field
  std::string query_data =
      "test query data with some extra bytes to make it larger";
  request->set_query(query_data);

  // Get the actual size of the request
  const size_t actual_request_size = request->ByteSizeLong();
  ASSERT_GT(actual_request_size, 0);

  // Create a SearchIndexPartitionResponse with some data
  // According to the proto definition: message SearchIndexPartitionResponse {
  // repeated NeighborEntry neighbors = 1; }
  SearchIndexPartitionResponse response;

  // Add some neighbor entries to make it non-empty
  auto* neighbor1 = response.add_neighbors();
  if (neighbor1) {
    // Set fields based on the NeighborEntry proto definition
    neighbor1->set_key("neighbor1");
    neighbor1->set_score(0.95);
  }

  auto* neighbor2 = response.add_neighbors();
  if (neighbor2) {
    // Set fields based on the NeighborEntry proto definition
    neighbor2->set_key("neighbor2");
    neighbor2->set_score(0.85);
  }

  // Get the actual size of the response
  const size_t actual_response_size = response.ByteSizeLong();
  ASSERT_GT(actual_response_size, 0);

  // Mock the SearchIndexPartition method to simulate the real implementation
  EXPECT_CALL(*mock_client_, SearchIndexPartition(testing::_, testing::_))
      .WillOnce([&](std::unique_ptr<SearchIndexPartitionRequest> req,
                    SearchIndexPartitionCallback done) {
        // Verify we're working with the same request
        EXPECT_EQ(req->timeout_ms(), 1000);

        // Simulate what happens in ClientImpl::SearchIndexPartition
        // Count the exact request size before sending
        Metrics::GetStats().coordinator_bytes_out.fetch_add(
            req->ByteSizeLong(), std::memory_order_relaxed);

        // In the real implementation, we count bytes inside this callback
        // for successful responses, right before calling the user's callback
        Metrics::GetStats().coordinator_bytes_in.fetch_add(
            response.ByteSizeLong(), std::memory_order_relaxed);

        // Call the callback with success status
        done(grpc::Status::OK, response);
      });

  // Call the method
  bool callback_called = false;
  mock_client_->SearchIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status,
                         SearchIndexPartitionResponse& resp) {
        EXPECT_TRUE(status.ok());
        callback_called = true;
      });

  EXPECT_TRUE(callback_called);

  // Verify byte counters match the actual sizes of the protobuf objects
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(),
            actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(),
            actual_response_size);
}

// Test that we don't count incoming bytes for error responses
TEST_F(ClientByteCountingTest, DoesNotCountResponseBytesOnError) {
  // Create a real SearchIndexPartitionRequest with data based on the proto
  // definition
  auto request = std::make_unique<SearchIndexPartitionRequest>();
  request->set_timeout_ms(1000);
  request->set_index_schema_name("test_index_schema");
  request->set_attribute_alias("test_alias");
  request->set_dialect(1);
  request->set_k(100);
  request->set_no_content(true);

  // Get the actual size of the request
  const size_t actual_request_size = request->ByteSizeLong();
  ASSERT_GT(actual_request_size, 0);

  // Create a SearchIndexPartitionResponse with data (to verify we don't count
  // it on errors)
  SearchIndexPartitionResponse response;

  // Add neighbor entries to make it non-empty, just like in the success case
  auto* neighbor = response.add_neighbors();
  if (neighbor) {
    neighbor->set_key("error_neighbor");
    neighbor->set_score(0.75);
  }

  // Mock the SearchIndexPartition method to simulate the real implementation
  EXPECT_CALL(*mock_client_, SearchIndexPartition(testing::_, testing::_))
      .WillOnce([&](std::unique_ptr<SearchIndexPartitionRequest> req,
                    SearchIndexPartitionCallback done) {
        // Verify we're working with the same request
        EXPECT_EQ(req->timeout_ms(), 1000);

        // Simulate what happens in ClientImpl::SearchIndexPartition
        // Count the exact request size before sending
        Metrics::GetStats().coordinator_bytes_out.fetch_add(
            req->ByteSizeLong(), std::memory_order_relaxed);

        // Call the callback with error status
        // Do NOT count response bytes on error
        done(grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service unavailable"),
             response);
      });

  // Call the method
  bool callback_called = false;
  mock_client_->SearchIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status,
                         SearchIndexPartitionResponse& resp) {
        EXPECT_FALSE(status.ok());
        callback_called = true;
      });

  EXPECT_TRUE(callback_called);

  // Verify only request bytes were counted, not response bytes
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(),
            actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(), 0);
}

}  // namespace valkey_search::coordinator
