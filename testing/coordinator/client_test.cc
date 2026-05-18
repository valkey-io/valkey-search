/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/client.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/metrics.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::coordinator {

namespace {

class FakeCoordinatorAsync final
    : public Coordinator::StubInterface::async_interface {
 public:
  void GetGlobalMetadata(::grpc::ClientContext *,
                         const GetGlobalMetadataRequest *,
                         GetGlobalMetadataResponse *,
                         std::function<void(::grpc::Status)> done) override {
    ADD_FAILURE() << "Unexpected GetGlobalMetadata async call";
    done(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, ""));
  }

  void GetGlobalMetadata(::grpc::ClientContext *,
                         const GetGlobalMetadataRequest *,
                         GetGlobalMetadataResponse *,
                         ::grpc::ClientUnaryReactor *) override {
    ADD_FAILURE() << "Unexpected GetGlobalMetadata reactor call";
  }

  void SearchIndexPartition(::grpc::ClientContext *,
                            const SearchIndexPartitionRequest *,
                            SearchIndexPartitionResponse *response,
                            std::function<void(::grpc::Status)> done) override {
    *response = search_response;
    done(search_status);
  }

  void SearchIndexPartition(::grpc::ClientContext *,
                            const SearchIndexPartitionRequest *,
                            SearchIndexPartitionResponse *,
                            ::grpc::ClientUnaryReactor *) override {
    ADD_FAILURE() << "Unexpected SearchIndexPartition reactor call";
  }

  void InfoIndexPartition(::grpc::ClientContext *,
                          const InfoIndexPartitionRequest *,
                          InfoIndexPartitionResponse *response,
                          std::function<void(::grpc::Status)> done) override {
    *response = info_response;
    done(info_status);
  }

  void InfoIndexPartition(::grpc::ClientContext *,
                          const InfoIndexPartitionRequest *,
                          InfoIndexPartitionResponse *,
                          ::grpc::ClientUnaryReactor *) override {
    ADD_FAILURE() << "Unexpected InfoIndexPartition reactor call";
  }

  SearchIndexPartitionResponse search_response;
  grpc::Status search_status = grpc::Status::OK;
  InfoIndexPartitionResponse info_response;
  grpc::Status info_status = grpc::Status::OK;
};

class FakeCoordinatorStub final : public Coordinator::StubInterface {
 public:
  FakeCoordinatorAsync *async_stub() { return &async_; }

  grpc::Status GetGlobalMetadata(::grpc::ClientContext *,
                                 const GetGlobalMetadataRequest &,
                                 GetGlobalMetadataResponse *) override {
    ADD_FAILURE() << "Unexpected GetGlobalMetadata sync call";
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "");
  }

  grpc::Status SearchIndexPartition(::grpc::ClientContext *,
                                    const SearchIndexPartitionRequest &,
                                    SearchIndexPartitionResponse *) override {
    ADD_FAILURE() << "Unexpected SearchIndexPartition sync call";
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "");
  }

  grpc::Status InfoIndexPartition(::grpc::ClientContext *,
                                  const InfoIndexPartitionRequest &,
                                  InfoIndexPartitionResponse *) override {
    ADD_FAILURE() << "Unexpected InfoIndexPartition sync call";
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "");
  }

  Coordinator::StubInterface::async_interface *async() override {
    return &async_;
  }

 private:
  ::grpc::ClientAsyncResponseReaderInterface<GetGlobalMetadataResponse> *
  AsyncGetGlobalMetadataRaw(::grpc::ClientContext *,
                            const GetGlobalMetadataRequest &,
                            ::grpc::CompletionQueue *) override {
    ADD_FAILURE() << "Unexpected AsyncGetGlobalMetadataRaw call";
    return nullptr;
  }

  ::grpc::ClientAsyncResponseReaderInterface<GetGlobalMetadataResponse> *
  PrepareAsyncGetGlobalMetadataRaw(::grpc::ClientContext *,
                                   const GetGlobalMetadataRequest &,
                                   ::grpc::CompletionQueue *) override {
    ADD_FAILURE() << "Unexpected PrepareAsyncGetGlobalMetadataRaw call";
    return nullptr;
  }

  ::grpc::ClientAsyncResponseReaderInterface<SearchIndexPartitionResponse> *
  AsyncSearchIndexPartitionRaw(::grpc::ClientContext *,
                               const SearchIndexPartitionRequest &,
                               ::grpc::CompletionQueue *) override {
    ADD_FAILURE() << "Unexpected AsyncSearchIndexPartitionRaw call";
    return nullptr;
  }

  ::grpc::ClientAsyncResponseReaderInterface<SearchIndexPartitionResponse> *
  PrepareAsyncSearchIndexPartitionRaw(::grpc::ClientContext *,
                                      const SearchIndexPartitionRequest &,
                                      ::grpc::CompletionQueue *) override {
    ADD_FAILURE() << "Unexpected PrepareAsyncSearchIndexPartitionRaw call";
    return nullptr;
  }

  ::grpc::ClientAsyncResponseReaderInterface<InfoIndexPartitionResponse> *
  AsyncInfoIndexPartitionRaw(::grpc::ClientContext *,
                             const InfoIndexPartitionRequest &,
                             ::grpc::CompletionQueue *) override {
    ADD_FAILURE() << "Unexpected AsyncInfoIndexPartitionRaw call";
    return nullptr;
  }

  ::grpc::ClientAsyncResponseReaderInterface<InfoIndexPartitionResponse> *
  PrepareAsyncInfoIndexPartitionRaw(::grpc::ClientContext *,
                                    const InfoIndexPartitionRequest &,
                                    ::grpc::CompletionQueue *) override {
    ADD_FAILURE() << "Unexpected PrepareAsyncInfoIndexPartitionRaw call";
    return nullptr;
  }

  FakeCoordinatorAsync async_;
};

}  // namespace

// Test to verify the byte counting functionality in client.cc
class ClientByteCountingTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    Metrics::GetStats().coordinator_bytes_out.store(0);
    Metrics::GetStats().coordinator_bytes_in.store(0);
  }
};

TEST_F(ClientByteCountingTest, CountsResponseBytesBeforeCallbackMutation) {
  auto request = std::make_unique<SearchIndexPartitionRequest>();
  request->set_timeout_ms(1000);
  request->set_index_schema_name("test_index_schema");
  request->set_attribute_alias("test_alias");
  request->set_query("test query data");
  request->set_k(2);

  const size_t actual_request_size = request->ByteSizeLong();
  ASSERT_GT(actual_request_size, 0);

  SearchIndexPartitionResponse response;
  response.set_total_count(2);

  auto *neighbor1 = response.add_neighbors();
  neighbor1->set_key("neighbor1");
  neighbor1->set_score(0.95);
  auto *attribute1 = neighbor1->add_attribute_contents();
  attribute1->set_identifier("title");
  attribute1->set_content("first neighbor payload");

  auto *neighbor2 = response.add_neighbors();
  neighbor2->set_key("neighbor2");
  neighbor2->set_score(0.85);
  auto *attribute2 = neighbor2->add_attribute_contents();
  attribute2->set_identifier("body");
  attribute2->set_content("second neighbor payload");

  const size_t actual_response_size = response.ByteSizeLong();
  ASSERT_GT(actual_response_size, 0);

  auto stub = std::make_unique<FakeCoordinatorStub>();
  stub->async_stub()->search_response = response;
  ClientImpl client(nullptr, "test_address", std::move(stub));

  bool callback_called = false;
  client.SearchIndexPartition(
      std::move(request),
      [&](grpc::Status status, SearchIndexPartitionResponse &resp) {
        EXPECT_TRUE(status.ok());
        callback_called = true;
        resp.clear_neighbors();
        EXPECT_LT(resp.ByteSizeLong(), actual_response_size);
      });

  EXPECT_TRUE(callback_called);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(),
            actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(),
            actual_response_size);
}

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
  auto *neighbor1 = response.add_neighbors();
  if (neighbor1) {
    // Set fields based on the NeighborEntry proto definition
    neighbor1->set_key("neighbor1");
    neighbor1->set_score(0.95);
  }

  auto *neighbor2 = response.add_neighbors();
  if (neighbor2) {
    // Set fields based on the NeighborEntry proto definition
    neighbor2->set_key("neighbor2");
    neighbor2->set_score(0.85);
  }

  // Get the actual size of the response
  const size_t actual_response_size = response.ByteSizeLong();
  ASSERT_GT(actual_response_size, 0);

  auto stub = std::make_unique<FakeCoordinatorStub>();
  stub->async_stub()->search_response = response;
  ClientImpl client(nullptr, "test_address", std::move(stub));

  bool callback_called = false;
  client.SearchIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status,
                         SearchIndexPartitionResponse &resp) {
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
  auto *neighbor = response.add_neighbors();
  if (neighbor) {
    neighbor->set_key("error_neighbor");
    neighbor->set_score(0.75);
  }

  auto stub = std::make_unique<FakeCoordinatorStub>();
  stub->async_stub()->search_response = response;
  stub->async_stub()->search_status =
      grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service unavailable");
  ClientImpl client(nullptr, "test_address", std::move(stub));

  bool callback_called = false;
  client.SearchIndexPartition(
      std::move(request),
      [&callback_called](grpc::Status status,
                         SearchIndexPartitionResponse &resp) {
        EXPECT_FALSE(status.ok());
        callback_called = true;
      });

  EXPECT_TRUE(callback_called);

  // Verify only request bytes were counted, not response bytes
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(),
            actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(), 0);
}

TEST_F(ClientByteCountingTest, CountsInfoResponseBytesBeforeCallbackMutation) {
  auto request = std::make_unique<InfoIndexPartitionRequest>();
  request->set_db_num(0);
  request->set_index_name("test_index_schema");
  request->set_require_consistency(true);

  const size_t actual_request_size = request->ByteSizeLong();
  ASSERT_GT(actual_request_size, 0);

  InfoIndexPartitionResponse response;
  response.set_exists(true);
  response.set_index_name("test_index_schema");
  response.set_num_docs(10);
  response.set_num_records(20);
  auto *attribute = response.add_attributes();
  attribute->set_identifier("title");
  attribute->set_alias("title_alias");
  attribute->set_user_indexed_memory(1024);
  attribute->set_num_records(20);

  const size_t actual_response_size = response.ByteSizeLong();
  ASSERT_GT(actual_response_size, 0);

  auto stub = std::make_unique<FakeCoordinatorStub>();
  stub->async_stub()->info_response = response;
  ClientImpl client(nullptr, "test_address", std::move(stub));

  bool callback_called = false;
  client.InfoIndexPartition(
      std::move(request),
      [&](grpc::Status status, InfoIndexPartitionResponse &resp) {
        EXPECT_TRUE(status.ok());
        callback_called = true;
        resp.clear_attributes();
        EXPECT_LT(resp.ByteSizeLong(), actual_response_size);
      });

  EXPECT_TRUE(callback_called);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_out.load(),
            actual_request_size);
  EXPECT_EQ(Metrics::GetStats().coordinator_bytes_in.load(),
            actual_response_size);
}

}  // namespace valkey_search::coordinator
