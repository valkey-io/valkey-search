/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/grpc_suspender.h"
#include "src/coordinator/metadata_manager.h"
#include "src/coordinator/search_converter.h"
#include "src/coordinator/util.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/response_generator.h"
#include "src/query/search.h"
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
  GRPCSuspensionGuard guard(GRPCSuspender::Instance());
  auto latency_sample = SAMPLE_EVERY_N(100);
  grpc::ServerUnaryReactor* reactor = context->DefaultReactor();
  auto vector_search_parameters = GRPCSearchRequestToParameters(*request);
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
        if (parameters->no_content) {
          SerializeNeighbors(response, neighbors.value());
          reactor->Finish(grpc::Status::OK);
          RecordSearchMetrics(false, std::move(latency_sample));
        } else {
          vmsdk::RunByMain(
              [parameters = std::move(parameters), response, reactor,
               latency_sample = std::move(latency_sample),
               neighbors = std::move(neighbors.value())]() mutable {
                const auto& attribute_data_type =
                    parameters->index_schema->GetAttributeDataType();
                auto ctx = vmsdk::MakeUniqueRedisThreadSafeContext(nullptr);
                if (parameters->attribute_alias.empty()) {
                    query::ProcessNonVectorNeighborsForReply(ctx.get(), attribute_data_type,
                                            neighbors, *parameters);
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
      false);
  if (!status.ok()) {
    VMSDK_LOG(WARNING, detached_ctx_.get())
        << "Failed to enqueue search request: " << status.message();
    // We lost our latency sample since it was owned by the callback.
    RecordSearchMetrics(true, nullptr);
    reactor->Finish(ToGrpcStatus(status));
  }
  return reactor;
}

ServerImpl::ServerImpl(std::unique_ptr<Service> coordinator_service,
                       std::unique_ptr<grpc::Server> server, uint16_t port)
    : coordinator_service_(std::move(coordinator_service)),
      server_(std::move(server)),
      port_(port) {}

std::unique_ptr<Server> ServerImpl::Create(
    RedisModuleCtx* ctx, vmsdk::ThreadPool* reader_thread_pool, uint16_t port) {
  std::string server_address = absl::StrCat("[::]:", port);
  grpc::EnableDefaultHealthCheckService(true);
  std::shared_ptr<grpc::ServerCredentials> creds =
      grpc::InsecureServerCredentials();
  auto coordinator_service = std::make_unique<Service>(
      vmsdk::MakeUniqueRedisDetachedThreadSafeContext(ctx), reader_thread_pool);
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
