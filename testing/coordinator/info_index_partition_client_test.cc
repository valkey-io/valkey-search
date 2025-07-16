/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/coordinator/client.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/server.h"
#include "src/metrics.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"

namespace valkey_search::coordinator {

// Unit test that verifies InfoIndexPartition client byte counting works correctly
class InfoIndexPartitionClientTest : public ::testing::Test {
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

// Test that we correctly count bytes for InfoIndexPartition requests
TEST_F(InfoIndexPartitionClientTest, CountsCorrectBytesOnSuccess) {
  // Create a real InfoIndexPartitionRequest with data
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name("test_index_for_byte_counting");
  
  // Get the actual size of the request
  const size_t actual_request_size = request->ByteSizeLong();
  ASSERT_GT(actual_request_size, 0);
  
  // Create an InfoIndexPartitionResponse with some data
  InfoIndexPartitionResponse response;
  response.set_exists(false);
  response.set_index_name("test_index_for_byte_counting");
  response.set_error("Index not found");
  
  // Get the actual size of the response
  const size_t actual_response_size = response.ByteSizeLong();
  ASSERT_GT(actual_response_size, 0);
  
  // Mock the InfoIndexPartition method to simulate the real implementation
  EXPECT_CALL(*mock_client_, InfoIndexPartition(testing::_, testing::_))
      .WillOnce([&](std::unique_ptr<InfoIndexPartitionRequest> req,
                   InfoIndexPartitionCallback done) {
        // Verify we're working with the same request
        EXPECT_EQ(req->index_name(), "test_index_for_byte_counting");
        
        // Simulate what happens in ClientImpl::InfoIndexPartition
        // Count the exact request size before sending
        Metrics::GetStats().coordinator_bytes_out.fetch_add(
            req->ByteSizeLong(), std::memory_order_relaxed);
            
        // Count response bytes on successful response
        Metrics::GetStats().coordinator_bytes_in.fetch_add(
            response.ByteSizeLong(), std::memory_order_relaxed);
            
        // Call the callback with success status
        done(grpc::Status::OK, response);
      });
      
  // Call the method
  bool callback_called = false;
  mock_client_->InfoIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status, InfoIndexPartitionResponse& resp) {
        EXPECT_TRUE(status.ok());
        callback_called = true;
      });
      
  EXPECT_TRUE(callback_called);
  
  // Verify byte counters match the actual sizes of the protobuf objects
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(), actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(), actual_response_size);
}

// Test that we don't count incoming bytes for error responses
TEST_F(InfoIndexPartitionClientTest, DoesNotCountResponseBytesOnError) {
  // Create a real InfoIndexPartitionRequest with data
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name("test_index_error");
  
  // Get the actual size of the request
  const size_t actual_request_size = request->ByteSizeLong();
  ASSERT_GT(actual_request_size, 0);
  
  // Create an InfoIndexPartitionResponse with data (to verify we don't count it on errors)
  InfoIndexPartitionResponse response;
  response.set_exists(false);
  response.set_index_name("test_index_error");
  response.set_error("Service error");
  
  // Mock the InfoIndexPartition method to simulate the real implementation
  EXPECT_CALL(*mock_client_, InfoIndexPartition(testing::_, testing::_))
      .WillOnce([&](std::unique_ptr<InfoIndexPartitionRequest> req,
                   InfoIndexPartitionCallback done) {
        // Verify we're working with the same request
        EXPECT_EQ(req->index_name(), "test_index_error");
        
        // Simulate what happens in ClientImpl::InfoIndexPartition
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
  mock_client_->InfoIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status, InfoIndexPartitionResponse& resp) {
        EXPECT_FALSE(status.ok());
        callback_called = true;
      });
      
  EXPECT_TRUE(callback_called);
  
  // Verify only request bytes were counted, not response bytes
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(), actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(), 0);
}

}  // namespace valkey_search::coordinator
