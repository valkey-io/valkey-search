/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "gmock/gmock.h"
#include "grpc/grpc.h"
#include "grpcpp/channel.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/status.h"
#include "gtest/gtest.h"
#include "src/coordinator/client.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/metrics.h"
#include "testing/common.h"

namespace valkey_search::coordinator {

// Fake service implementation for testing
class FakeCoordinatorService final : public Coordinator::Service {
 public:
  grpc::Status InfoIndexPartition(
      grpc::ServerContext* context,
      const InfoIndexPartitionRequest* request,
      InfoIndexPartitionResponse* response) override {
    
    // Simulate different responses based on index name
    std::string index_name = request->index_name();
    
    if (index_name == "existing_index") {
      // Simulate an existing index with data
      response->set_exists(true);
      response->set_index_name(index_name);
      response->set_num_docs(42);
      response->set_num_records(100);
      response->set_hash_indexing_failures(0);
      response->set_backfill_scanned_count(50);
      response->set_backfill_db_size(1024);
      response->set_backfill_inqueue_tasks(5);
      response->set_backfill_complete_percent(85.5);
      response->set_backfill_in_progress(true);
      response->set_mutation_queue_size(3);
      response->set_recent_mutations_queue_delay(10);
      response->set_state("ACTIVE");
      return grpc::Status::OK;
    } else if (index_name == "error_index") {
      // Simulate a server error
      return grpc::Status(grpc::StatusCode::INTERNAL, "Internal server error");
    } else if (index_name == "empty_index") {
      // Simulate an empty but existing index
      response->set_exists(true);
      response->set_index_name(index_name);
      response->set_num_docs(0);
      response->set_num_records(0);
      response->set_hash_indexing_failures(0);
      response->set_backfill_scanned_count(0);
      response->set_backfill_db_size(0);
      response->set_backfill_inqueue_tasks(0);
      response->set_backfill_complete_percent(100.0);
      response->set_backfill_in_progress(false);
      response->set_mutation_queue_size(0);
      response->set_recent_mutations_queue_delay(0);
      response->set_state("READY");
      return grpc::Status::OK;
    } else {
      // Simulate non-existent index
      response->set_exists(false);
      response->set_index_name(index_name);
      response->set_error("Index not found");
      return grpc::Status::OK;
    }
  }
  
  // We need to implement other methods even if we don't use them
  grpc::Status GetGlobalMetadata(
      grpc::ServerContext* context,
      const GetGlobalMetadataRequest* request,
      GetGlobalMetadataResponse* response) override {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not implemented in fake service");
  }
  
  grpc::Status SearchIndexPartition(
      grpc::ServerContext* context,
      const SearchIndexPartitionRequest* request,
      SearchIndexPartitionResponse* response) override {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not implemented in fake service");
  }
};

// Integration test that uses a real gRPC server with fake service
class InfoIndexPartitionIntegrationTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    
    // Reset metrics before each test
    Metrics::GetStats().coordinator_bytes_out.store(0);
    Metrics::GetStats().coordinator_bytes_in.store(0);
    
    // Start fake gRPC server
    server_address_ = "localhost:0"; // Let system choose port
    fake_service_ = std::make_unique<FakeCoordinatorService>();
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials(), &selected_port_);
    builder.RegisterService(fake_service_.get());
    server_ = builder.BuildAndStart();
    
    ASSERT_NE(server_, nullptr) << "Failed to start fake gRPC server";
    ASSERT_GT(selected_port_, 0) << "Failed to get server port";
    
    // Update server address with actual port
    server_address_ = "localhost:" + std::to_string(selected_port_);
    
    // Create client using MakeInsecureClient with proper context
    client_ = ClientImpl::MakeInsecureClient(
        vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(&fake_ctx_),
        server_address_);
    ASSERT_NE(client_, nullptr) << "Failed to create client";
    
    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  
  void TearDown() override {
    client_.reset();
    if (server_) {
      server_->Shutdown();
      server_->Wait();
    }
    fake_service_.reset();
    ValkeySearchTest::TearDown();
  }
  
  std::string server_address_;
  int selected_port_ = 0;
  std::unique_ptr<FakeCoordinatorService> fake_service_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<Client> client_;
};

// Test with existing index
TEST_F(InfoIndexPartitionIntegrationTest, ExistingIndex) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name("existing_index");
  
  std::promise<InfoIndexPartitionResponse> response_promise;
  std::promise<grpc::Status> status_promise;
  
  client_->InfoIndexPartition(
      std::move(request),
      [&](grpc::Status status, InfoIndexPartitionResponse& response) {
        status_promise.set_value(status);
        response_promise.set_value(response);
      });
  
  // Wait for callback with timeout
  auto status_future = status_promise.get_future();
  auto response_future = response_promise.get_future();
  
  ASSERT_EQ(status_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(response_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  
  grpc::Status status = status_future.get();
  InfoIndexPartitionResponse response = response_future.get();
  
  // Verify status and response
  EXPECT_TRUE(status.ok()) << "Status error: " << status.error_message();
  EXPECT_TRUE(response.exists());
  EXPECT_EQ(response.index_name(), "existing_index");
  EXPECT_EQ(response.num_docs(), 42);
  EXPECT_EQ(response.num_records(), 100);
  EXPECT_EQ(response.hash_indexing_failures(), 0);
  EXPECT_EQ(response.backfill_scanned_count(), 50);
  EXPECT_EQ(response.backfill_db_size(), 1024);
  EXPECT_EQ(response.backfill_inqueue_tasks(), 5);
  EXPECT_FLOAT_EQ(response.backfill_complete_percent(), 85.5);
  EXPECT_TRUE(response.backfill_in_progress());
  EXPECT_EQ(response.mutation_queue_size(), 3);
  EXPECT_EQ(response.recent_mutations_queue_delay(), 10);
  EXPECT_EQ(response.state(), "ACTIVE");
  EXPECT_TRUE(response.error().empty());
  
  // Verify bytes were counted
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_out.load(), 0);
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_in.load(), 0);
}

// Test with non-existent index
TEST_F(InfoIndexPartitionIntegrationTest, NonExistentIndex) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name("non_existent_index");
  
  std::promise<InfoIndexPartitionResponse> response_promise;
  std::promise<grpc::Status> status_promise;
  
  client_->InfoIndexPartition(
      std::move(request),
      [&](grpc::Status status, InfoIndexPartitionResponse& response) {
        status_promise.set_value(status);
        response_promise.set_value(response);
      });
  
  // Wait for callback with timeout
  auto status_future = status_promise.get_future();
  auto response_future = response_promise.get_future();
  
  ASSERT_EQ(status_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(response_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  
  grpc::Status status = status_future.get();
  InfoIndexPartitionResponse response = response_future.get();
  
  // Verify status and response
  EXPECT_TRUE(status.ok()) << "Status error: " << status.error_message();
  EXPECT_FALSE(response.exists());
  EXPECT_EQ(response.index_name(), "non_existent_index");
  EXPECT_EQ(response.error(), "Index not found");
  
  // Verify bytes were counted
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_out.load(), 0);
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_in.load(), 0);
}

// Test with empty index
TEST_F(InfoIndexPartitionIntegrationTest, EmptyIndex) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name("empty_index");
  
  std::promise<InfoIndexPartitionResponse> response_promise;
  std::promise<grpc::Status> status_promise;
  
  client_->InfoIndexPartition(
      std::move(request),
      [&](grpc::Status status, InfoIndexPartitionResponse& response) {
        status_promise.set_value(status);
        response_promise.set_value(response);
      });
  
  // Wait for callback with timeout
  auto status_future = status_promise.get_future();
  auto response_future = response_promise.get_future();
  
  ASSERT_EQ(status_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(response_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  
  grpc::Status status = status_future.get();
  InfoIndexPartitionResponse response = response_future.get();
  
  // Verify status and response
  EXPECT_TRUE(status.ok()) << "Status error: " << status.error_message();
  EXPECT_TRUE(response.exists());
  EXPECT_EQ(response.index_name(), "empty_index");
  EXPECT_EQ(response.num_docs(), 0);
  EXPECT_EQ(response.num_records(), 0);
  EXPECT_EQ(response.state(), "READY");
  EXPECT_FALSE(response.backfill_in_progress());
  EXPECT_FLOAT_EQ(response.backfill_complete_percent(), 100.0);
  EXPECT_TRUE(response.error().empty());
}

// Test server error handling
TEST_F(InfoIndexPartitionIntegrationTest, ServerError) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_index_name("error_index");
  
  std::promise<InfoIndexPartitionResponse> response_promise;
  std::promise<grpc::Status> status_promise;
  
  client_->InfoIndexPartition(
      std::move(request),
      [&](grpc::Status status, InfoIndexPartitionResponse& response) {
        status_promise.set_value(status);
        response_promise.set_value(response);
      });
  
  // Wait for callback with timeout
  auto status_future = status_promise.get_future();
  auto response_future = response_promise.get_future();
  
  ASSERT_EQ(status_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_EQ(response_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  
  grpc::Status status = status_future.get();
  InfoIndexPartitionResponse response = response_future.get();
  
  // Verify error status
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
  EXPECT_EQ(status.error_message(), "Internal server error");
  
  // Verify request bytes were counted but not response bytes (due to error)
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_out.load(), 0);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(), 0);
}

// Test multiple concurrent requests
TEST_F(InfoIndexPartitionIntegrationTest, ConcurrentRequests) {
  const int num_requests = 5;
  std::vector<std::promise<InfoIndexPartitionResponse>> response_promises(num_requests);
  std::vector<std::promise<grpc::Status>> status_promises(num_requests);
  
  // Send multiple concurrent requests
  for (int i = 0; i < num_requests; ++i) {
    auto request = std::make_unique<InfoIndexPartitionRequest>();
    request->set_index_name("existing_index");
    
    client_->InfoIndexPartition(
        std::move(request),
        [&, i](grpc::Status status, InfoIndexPartitionResponse& response) {
          status_promises[i].set_value(status);
          response_promises[i].set_value(response);
        });
  }
  
  // Wait for all callbacks
  for (int i = 0; i < num_requests; ++i) {
    auto status_future = status_promises[i].get_future();
    auto response_future = response_promises[i].get_future();
    
    ASSERT_EQ(status_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(response_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    
    grpc::Status status = status_future.get();
    InfoIndexPartitionResponse response = response_future.get();
    
    EXPECT_TRUE(status.ok()) << "Request " << i << " failed: " << status.error_message();
    EXPECT_TRUE(response.exists());
    EXPECT_EQ(response.index_name(), "existing_index");
    EXPECT_EQ(response.num_docs(), 42);
    EXPECT_EQ(response.num_records(), 100);
  }
  
  // Verify bytes were counted for all requests
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_out.load(), num_requests * 10);
  EXPECT_GT(Metrics::GetStats().coordinator_bytes_in.load(), num_requests * 10);
}

}  // namespace valkey_search::coordinator
