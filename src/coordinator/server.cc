/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/server.h"

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "grpc/grpc.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/health_check_service_interface.h"
#include "grpcpp/security/server_credentials.h"
#include "grpcpp/server_builder.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/server_callback.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/grpc_suspender.h"
#include "src/coordinator/metadata_manager.h"
#include "src/coordinator/search_converter.h"
#include "src/coordinator/util.h"
#include "src/index_schema.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/info.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::coordinator {

CONTROLLED_SIZE_T(ForceRemoteFailCount, 0);
CONTROLLED_SIZE_T(ForceIndexNotFoundError, 0);

grpc::ServerUnaryReactor* Service::GetGlobalMetadata(
    grpc::CallbackServerContext* context,
    const GetGlobalMetadataRequest* request,
    GetGlobalMetadataResponse* response) {
  GRPCSuspensionGuard guard(GRPCSuspender::Instance());
  auto latency_sample = SAMPLE_EVERY_N(100);
  grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
  if (!MetadataManager::IsInitialized()) {
    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL,
                                 "MetadataManager is not initialized"));
    Metrics::GetStats().coordinator_server_get_global_metadata_failure_cnt++;
    Metrics::GetStats()
        .coordinator_server_get_global_metadata_failure_latency.SubmitSample(
            std::move(latency_sample));
    return reactor;
  }
  vmsdk::RunByMain([reactor, response,
                    latency_sample = std::move(latency_sample)]() mutable {
    response->set_allocated_metadata(const_cast<GlobalMetadata*>(
        MetadataManager::Instance().GetGlobalMetadata().release()));
    reactor->Finish(grpc::Status::OK);
    Metrics::GetStats().coordinator_server_get_global_metadata_success_cnt++;
    Metrics::GetStats()
        .coordinator_server_get_global_metadata_success_latency.SubmitSample(
            std::move(latency_sample));
  });
  return reactor;
}

void RecordSearchMetrics(bool failure,
                         std::unique_ptr<vmsdk::StopWatch> sample) {
  if (failure) {
    Metrics::GetStats().coordinator_server_search_index_partition_failure_cnt++;
    Metrics::GetStats()
        .coordinator_server_search_index_partition_failure_latency.SubmitSample(
            std::move(sample));
  } else {
    Metrics::GetStats().coordinator_server_search_index_partition_success_cnt++;
    Metrics::GetStats()
        .coordinator_server_search_index_partition_success_latency.SubmitSample(
            std::move(sample));
  }
}

void SerializeNeighbors(SearchIndexPartitionResponse* response,
                        const std::vector<indexes::Neighbor>& neighbors) {
  for (const auto& neighbor : neighbors) {
    auto* neighbor_proto = response->add_neighbors();
    neighbor_proto->set_key(std::move(*neighbor.external_id));
    neighbor_proto->set_score(neighbor.distance);
    if (neighbor.attribute_contents) {
      const auto& attribute_contents = neighbor.attribute_contents.value();
      for (const auto& [identifier, record] : attribute_contents) {
        auto contents = neighbor_proto->add_attribute_contents();
        contents->set_identifier(identifier);
        contents->set_content(vmsdk::ToStringView(record.value.get()));
      }
    }
  }
}

// SearchParameters subclass for remote responder (remote shard in fanout).
// Handles in-flight retry completion by processing neighbors and sending gRPC
// response.
class RemoteResponderSearch : public query::SearchParameters {
 public:
  SearchIndexPartitionResponse* response;
  grpc::ServerUnaryReactor* reactor;
  std::unique_ptr<vmsdk::StopWatch> latency_sample;
  size_t total_count;
  void QueryCompleteBackground(
      std::unique_ptr<SearchParameters> self) override {
    CHECK(no_content);
    SerializeNeighbors(response, search_result.neighbors);
    response->set_total_count(search_result.total_count);
    reactor->Finish(grpc::Status::OK);
    RecordSearchMetrics(false, std::move(latency_sample));
  }

  void QueryCompleteMainThread(
      std::unique_ptr<SearchParameters> self) override {
    if (!search_result.status.ok() && !enable_partial_results ||
        cancellation_token->IsCancelled()) {
      reactor->Finish({grpc::StatusCode::DEADLINE_EXCEEDED,
                       "Search operation cancelled due to timeout"});
      RecordSearchMetrics(true, std::move(latency_sample));
      return;
    }
    CHECK(!no_content);  // Shouldn't be here!
    // Content already resolved by ResolveContent — just serialize and send.
    SerializeNeighbors(response, search_result.neighbors);
    response->set_total_count(search_result.total_count);
    reactor->Finish(grpc::Status::OK);
    RecordSearchMetrics(false, std::move(latency_sample));
  }

};

grpc::Status Service::PerformSlotConsistencyCheck(
    uint64_t expected_slot_fingerprint) {
  // compare slot fingerprint
  // use the cached cluster map for now, refreshing the cluster map would need
  // the client to execute commands at that node, will use new core api in the
  // future
  auto cluster_map = ValkeySearch::Instance().GetClusterMap();
  CHECK(cluster_map);
  auto current_node_shard = cluster_map->GetCurrentNodeShard();
  CHECK(current_node_shard);
  uint64_t my_shard_slot_fingerprint = current_node_shard->slots_fingerprint;
  if (my_shard_slot_fingerprint != expected_slot_fingerprint) {
    return {grpc::StatusCode::FAILED_PRECONDITION, "Slot fingerprint mismatch"};
  }
  return grpc::Status::OK;
}

grpc::Status Service::PerformIndexConsistencyCheck(
    const IndexFingerprintVersion& expected_fingerprint_version,
    const std::shared_ptr<IndexSchema>& schema) {
  if (schema->GetFingerprint() != expected_fingerprint_version.fingerprint() ||
      schema->GetVersion() != expected_fingerprint_version.version()) {
    return {grpc::StatusCode::FAILED_PRECONDITION, "Slot fingerprint mismatch"};
  }
  return grpc::Status::OK;
}

void Service::EnqueueSearchRequest(
    std::unique_ptr<RemoteResponderSearch> search_operation,
    vmsdk::ThreadPool* reader_thread_pool, ValkeyModuleCtx* detached_ctx,
    SearchIndexPartitionResponse* response, grpc::ServerUnaryReactor* reactor,
    std::unique_ptr<vmsdk::StopWatch> latency_sample) {
  search_operation->response = response;
  search_operation->latency_sample = std::move(latency_sample);
  search_operation->reactor = reactor;

  auto status =
      query::SearchAsync(std::move(search_operation), reader_thread_pool,
                         query::SearchMode::kRemote);

  if (!status.ok()) {
    VMSDK_LOG(WARNING, detached_ctx)
        << "Failed to enqueue search request: " << status.message();
    RecordSearchMetrics(true, nullptr);
    reactor->Finish(ToGrpcStatus(status));
  }
}

DEV_INTEGER_COUNTER(grpc, search_index_rpc_requests);

grpc::ServerUnaryReactor* Service::SearchIndexPartition(
    grpc::CallbackServerContext* context,
    const SearchIndexPartitionRequest* request,
    SearchIndexPartitionResponse* response) {
  search_index_rpc_requests.Increment();
  GRPCSuspensionGuard guard(GRPCSuspender::Instance());
  auto latency_sample = SAMPLE_EVERY_N(100);
  grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
  auto StatusWrapper = [&]() -> absl::Status {
    auto search_operation = std::make_unique<RemoteResponderSearch>();
    VMSDK_RETURN_IF_ERROR(GRPCSearchRequestToParameters(
        *request, context, search_operation.get()));

    // perform index consistency check (index fingerprint/version), required
    auto schema = SchemaManager::Instance()
                      .GetIndexSchema(search_operation->db_num,
                                      search_operation->index_schema_name)
                      .value();
    VMSDK_RETURN_IF_ERROR(ToAbslStatus(PerformIndexConsistencyCheck(
        request->index_fingerprint_version(), schema)));

    if (request->enable_consistency()) {
      // Perform consistency checks on main thread, then enqueue search
      VMSDK_RETURN_IF_ERROR(ToAbslStatus(
          PerformSlotConsistencyCheck(request->slot_fingerprint())));
    }
    // Consistency checks passed, now enqueue the search
    EnqueueSearchRequest(std::move(search_operation), reader_thread_pool_,
                         detached_ctx_.get(), response, reactor,
                         std::move(latency_sample));
    return absl::OkStatus();
  };
  auto status = StatusWrapper();
  if (!status.ok()) {
    reactor->Finish(ToGrpcStatus(status));
    RecordSearchMetrics(true, std::move(latency_sample));
  }
  return reactor;
}

std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse>
Service::GenerateInfoResponse(
    const coordinator::InfoIndexPartitionRequest& request) {
  vmsdk::VerifyMainThread();
  uint32_t db_num = request.db_num();
  std::string index_name = request.index_name();
  coordinator::InfoIndexPartitionResponse response;
  // test path: simulate index not found error
  if (ForceIndexNotFoundError.GetValue() > 0) {
    ForceIndexNotFoundError.Decrement();
    std::string test_error_str =
        "Test Error: Index " + index_name + " not found";
    response.set_exists(false);
    response.set_index_name(index_name);
    response.set_error(test_error_str);
    response.set_error_type(coordinator::FanoutErrorType::INDEX_NAME_ERROR);
    grpc::Status error_status(grpc::StatusCode::NOT_FOUND, test_error_str);
    return std::make_pair(error_status, response);
  }
  auto status_or_schema =
      SchemaManager::Instance().GetIndexSchema(db_num, index_name);
  if (!status_or_schema.ok()) {
    response.set_exists(false);
    response.set_index_name(index_name);
    response.set_error(status_or_schema.status().ToString());
    response.set_error_type(coordinator::FanoutErrorType::INDEX_NAME_ERROR);
    grpc::Status error_status(grpc::StatusCode::NOT_FOUND,
                              status_or_schema.status().ToString());
    return std::make_pair(error_status, response);
  }
  auto schema = std::move(status_or_schema.value());

  auto set_inconsistent_error = [&]() {
    response.set_exists(true);
    response.set_index_name(index_name);
    response.set_error(
        "Index fingerprint/version or slot fingerprint mismatch");
    response.set_error_type(
        coordinator::FanoutErrorType::INCONSISTENT_STATE_ERROR);
    VMSDK_LOG(NOTICE, nullptr) << "DEBUG: Fingerprint, version or slot "
                                  "fingerprint mismatch at server.cc";
    grpc::Status error_status(
        grpc::StatusCode::FAILED_PRECONDITION,
        "Cluster not in a consistent state, please retry.");
    return std::make_pair(error_status, response);
  };

  // perform index consistency check (index fingerprint/version), required
  if (!request.has_index_fingerprint_version()) {
    return set_inconsistent_error();
  } else {
    auto consistency_status = PerformIndexConsistencyCheck(
        request.index_fingerprint_version(), schema);
    if (!consistency_status.ok()) {
      return set_inconsistent_error();
    }
  }

  // perform slot consistency check if in CONSISTENT mode only
  if (request.require_consistency()) {
    auto slot_consistency_status =
        PerformSlotConsistencyCheck(request.slot_fingerprint());
    if (!slot_consistency_status.ok()) {
      return set_inconsistent_error();
    }
  }

  IndexSchema::InfoIndexPartitionData data =
      schema->GetInfoIndexPartitionData();

  response.set_exists(true);
  response.set_index_name(index_name);
  response.set_db_num(db_num);
  response.set_num_docs(data.num_docs);
  response.set_num_records(data.num_records);
  response.set_hash_indexing_failures(data.hash_indexing_failures);
  response.set_backfill_scanned_count(data.backfill_scanned_count);
  response.set_backfill_db_size(data.backfill_db_size);
  response.set_backfill_inqueue_tasks(data.backfill_inqueue_tasks);
  response.set_backfill_complete_percent(data.backfill_complete_percent);
  response.set_backfill_in_progress(data.backfill_in_progress);
  response.set_mutation_queue_size(data.mutation_queue_size);
  response.set_recent_mutations_queue_delay(data.recent_mutations_queue_delay);
  response.set_state(data.state);
  return std::make_pair(grpc::Status::OK, response);
}

grpc::ServerUnaryReactor* Service::InfoIndexPartition(
    grpc::CallbackServerContext* context,
    const InfoIndexPartitionRequest* request,
    InfoIndexPartitionResponse* response) {
  GRPCSuspensionGuard guard(GRPCSuspender::Instance());
  auto latency_sample = SAMPLE_EVERY_N(100);
  grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
  // simulate grpc timeout for testing only
  if (ForceRemoteFailCount.GetValue() > 0) {
    ForceRemoteFailCount.Decrement();
    return reactor;
  }
  vmsdk::RunByMain([reactor, response, request,
                    latency_sample = std::move(latency_sample)]() mutable {
    auto [status, info_response] = Service::GenerateInfoResponse(*request);
    *response = std::move(info_response);
    reactor->Finish(status);
  });
  return reactor;
}

ServerImpl::ServerImpl(std::unique_ptr<Service> coordinator_service,
                       std::unique_ptr<grpc::Server> server, uint16_t port)
    : coordinator_service_(std::move(coordinator_service)),
      server_(std::move(server)),
      port_(port) {}

std::unique_ptr<Server> ServerImpl::Create(
    ValkeyModuleCtx* ctx, vmsdk::ThreadPool* reader_thread_pool,
    uint16_t port) {
  std::string server_address = absl::StrCat("[::]:", port);
  grpc::EnableDefaultHealthCheckService(true);
  std::shared_ptr<grpc::ServerCredentials> creds =
      grpc::InsecureServerCredentials();
  auto coordinator_service = std::make_unique<Service>(
      vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(ctx),
      reader_thread_pool);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, creds);
  // Set the SO_REUSEADDR option
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 1);
  builder.RegisterService(coordinator_service.get());
  builder.AddChannelArgument(GRPC_ARG_MINIMAL_STACK, 1);
  builder.AddChannelArgument(GRPC_ARG_OPTIMIZATION_TARGET, "latency");
  builder.AddChannelArgument(GRPC_ARG_TCP_TX_ZEROCOPY_ENABLED, 1);
  auto server = builder.BuildAndStart();
  if (server == nullptr) {
    VMSDK_LOG(WARNING, ctx)
        << "Failed to start Coordinator Server on port " << port;
    for (size_t attempt = 2; attempt <= 10; ++attempt) {
      std::string lsof_cmd =
          "lsof -i :" + std::to_string(port) + " 2>/dev/null";
      FILE* pipe = popen(lsof_cmd.c_str(), "r");
      if (pipe) {
        char buffer[256];
        VMSDK_LOG(WARNING, ctx)
            << "Diagnosing other usage with this shell command:";
        VMSDK_LOG(WARNING, ctx) << ">> lsof -i: " << port;
        while (fgets(buffer, sizeof(buffer), pipe)) {
          std::string line(buffer);
          if (!line.empty() && line.back() == '\n') {
            line.pop_back();
          }
          VMSDK_LOG(WARNING, ctx) << ">> " << line;
        }
        VMSDK_LOG(WARNING, ctx) << ">> <end of lsof output>";
        pclose(pipe);
      } else {
        VMSDK_LOG(WARNING, ctx) << "Could not check port " << port << " usage";
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(100 * attempt));  // backoff
      VMSDK_LOG(WARNING, ctx)
          << "Retrying to start Coordinator Server (attempt " << attempt << ")";
      server = builder.BuildAndStart();
      if (server != nullptr) {
        VMSDK_LOG(NOTICE, ctx)
            << "Successfully started Coordinator Server on " << server_address
            << " after " << attempt << " attempts";
        break;
      }
    }
    VMSDK_LOG(WARNING, ctx)
        << "Failed to start Coordinator Server on " << server_address;
    return nullptr;
  }
  VMSDK_LOG(NOTICE, ctx) << "Coordinator Server listening on "
                         << server_address;
  return std::unique_ptr<Server>(
      new ServerImpl(std::move(coordinator_service), std::move(server), port));
}

}  // namespace valkey_search::coordinator
