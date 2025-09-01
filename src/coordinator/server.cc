/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/server.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <utility>

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
#include "module_config.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/grpc_suspender.h"
#include "src/coordinator/metadata_manager.h"
#include "src/coordinator/search_converter.h"
#include "src/coordinator/util.h"
#include "src/index_schema.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::coordinator {

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
                        const std::deque<indexes::Neighbor>& neighbors) {
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

grpc::ServerUnaryReactor* Service::SearchIndexPartition(
    grpc::CallbackServerContext* context,
    const SearchIndexPartitionRequest* request,
    SearchIndexPartitionResponse* response) {
  PAUSEPOINT("Search.gRPC");
  GRPCSuspensionGuard guard(GRPCSuspender::Instance());
  auto latency_sample = SAMPLE_EVERY_N(100);
  grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
  auto vector_search_parameters =
      GRPCSearchRequestToParameters(*request, context);
  if (!vector_search_parameters.ok()) {
    reactor->Finish(ToGrpcStatus(vector_search_parameters.status()));
    RecordSearchMetrics(true, std::move(latency_sample));
    return reactor;
  }

  // Enqueue into the thread pool
  auto status = query::SearchAsync(
      std::move(*vector_search_parameters), reader_thread_pool_,
      [response, reactor, latency_sample = std::move(latency_sample)](
          auto& neighbors,
          std::unique_ptr<query::VectorSearchParameters> parameters) mutable {
        if (!neighbors.ok()) {
          reactor->Finish(ToGrpcStatus(neighbors.status()));
          RecordSearchMetrics(true, std::move(latency_sample));
          return;
        }
        if (parameters->cancellation_token->IsCancelled() &&
            !valkey_search::options::GetEnablePartialResults().GetValue()) {
          reactor->Finish({grpc::StatusCode::DEADLINE_EXCEEDED,
                           "Search operation cancelled due to timeout"});
          RecordSearchMetrics(true, std::move(latency_sample));
          return;
        }
        if (parameters->no_content) {
          SerializeNeighbors(response, neighbors.value());
          reactor->Finish(grpc::Status::OK);
          RecordSearchMetrics(false, std::move(latency_sample));
        } else {
          vmsdk::RunByMain([parameters = std::move(parameters), response,
                            reactor, latency_sample = std::move(latency_sample),
                            neighbors =
                                std::move(neighbors.value())]() mutable {
            const auto& attribute_data_type =
                parameters->index_schema->GetAttributeDataType();
            auto ctx = vmsdk::MakeUniqueValkeyThreadSafeContext(nullptr);
            if (parameters->attribute_alias.empty()) {
              query::ProcessNonVectorNeighborsForReply(
                  ctx.get(), attribute_data_type, neighbors, *parameters);
            } else {
              auto vector_identifier =
                  parameters->index_schema
                      ->GetIdentifier(parameters->attribute_alias)
                      .value();
              query::ProcessNeighborsForReply(ctx.get(), attribute_data_type,
                                              neighbors, *parameters,
                                              vector_identifier);
            }
            SerializeNeighbors(response, neighbors);
            reactor->Finish(grpc::Status::OK);
            RecordSearchMetrics(false, std::move(latency_sample));
          });
        }
      },
      query::SearchMode::kRemote);
  if (!status.ok()) {
    VMSDK_LOG(WARNING, detached_ctx_.get())
        << "Failed to enqueue search request: " << status.message();
    // We lost our latency sample since it was owned by the callback.
    RecordSearchMetrics(true, nullptr);
    reactor->Finish(ToGrpcStatus(status));
  }
  return reactor;
}

grpc::ServerUnaryReactor* Service::InfoIndexPartition(
    grpc::CallbackServerContext* context,
    const InfoIndexPartitionRequest* request,
    InfoIndexPartitionResponse* response) {
  GRPCSuspensionGuard guard(GRPCSuspender::Instance());
  auto latency_sample = SAMPLE_EVERY_N(100);
  grpc::ServerUnaryReactor* reactor = context->DefaultReactor();

  vmsdk::RunByMain([reactor, response, idx = request->index_name(),
                    latency_sample = std::move(latency_sample)]() mutable {
    auto status_or_schema =
        SchemaManager::Instance().GetIndexSchema(/*db=*/0, idx);
    if (!status_or_schema.ok()) {
      response->set_exists(false);
      response->set_index_name(idx);
      response->set_error(status_or_schema.status().ToString());
      response->set_error_type(coordinator::FanoutErrorType::INDEX_NAME_ERROR);
      reactor->Finish(ToGrpcStatus(status_or_schema.status()));
      return;
    }
    auto schema = std::move(status_or_schema.value());
    IndexSchema::InfoIndexPartitionData data =
        schema->GetInfoIndexPartitionData();

    std::optional<uint64_t> fingerprint;
    std::optional<uint32_t> version;

    // Get the full metadata to access both fingerprint and version
    auto global_metadata =
        coordinator::MetadataManager::Instance().GetGlobalMetadata();
    if (global_metadata->type_namespace_map().contains(
            kSchemaManagerMetadataTypeName)) {
      const auto& entry_map = global_metadata->type_namespace_map().at(
          kSchemaManagerMetadataTypeName);
      if (entry_map.entries().contains(idx)) {
        const auto& entry = entry_map.entries().at(idx);
        fingerprint = entry.fingerprint();
        version = entry.version();
      }
    }

    response->set_exists(true);
    response->set_index_name(idx);
    response->set_num_docs(data.num_docs);
    response->set_num_records(data.num_records);
    response->set_hash_indexing_failures(data.hash_indexing_failures);
    response->set_backfill_scanned_count(data.backfill_scanned_count);
    response->set_backfill_db_size(data.backfill_db_size);
    response->set_backfill_inqueue_tasks(data.backfill_inqueue_tasks);
    response->set_backfill_complete_percent(data.backfill_complete_percent);
    response->set_backfill_in_progress(data.backfill_in_progress);
    response->set_mutation_queue_size(data.mutation_queue_size);
    response->set_recent_mutations_queue_delay(
        data.recent_mutations_queue_delay);
    response->set_state(data.state);
    if (fingerprint.has_value()) {
      response->set_schema_fingerprint(fingerprint.value());
    }
    if (version.has_value()) {
      response->set_version(version.value());
    }
    reactor->Finish(grpc::Status::OK);
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
  builder.RegisterService(coordinator_service.get());
  builder.AddChannelArgument(GRPC_ARG_MINIMAL_STACK, 1);
  builder.AddChannelArgument(GRPC_ARG_OPTIMIZATION_TARGET, "latency");
  builder.AddChannelArgument(GRPC_ARG_TCP_TX_ZEROCOPY_ENABLED, 1);
  auto server = builder.BuildAndStart();
  if (server == nullptr) {
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
