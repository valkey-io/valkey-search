/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/client.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/base/call_once.h"
#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "grpc/grpc.h"
#include "grpcpp/channel.h"
#include "grpcpp/client_context.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/support/channel_arguments.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/grpc_suspender.h"
#include "src/metrics.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

// clang-format off
constexpr absl::string_view kRetryPolicy =
    "{\"methodConfig\" : [{"
    "   \"name\" : [{\"service\": \"valkey_search.coordinator.Coordinator\"}],"
    "   \"waitForReady\": false,"
    "   \"retryPolicy\": {"
    "     \"maxAttempts\": 5,"
    "     \"initialBackoff\": \"0.100s\","
    "     \"maxBackoff\": \"1s\","
    "     \"backoffMultiplier\": 1.0,"
    "     \"retryableStatusCodes\": ["
    "       \"UNAVAILABLE\","
    "       \"UNKNOWN\","
    "       \"RESOURCE_EXHAUSTED\","
    "       \"INTERNAL\","
    "       \"DATA_LOSS\""
    "     ]"
    "    }"
    "}]}";
// clang-format on

grpc::ChannelArguments& GetChannelArgs() {
  static absl::once_flag once;
  static grpc::ChannelArguments channel_args;
  absl::call_once(once, []() {
    channel_args.SetServiceConfigJSON(std::string(kRetryPolicy));
  });
  channel_args.SetInt(GRPC_ARG_MINIMAL_STACK, 1);
  channel_args.SetString(GRPC_ARG_OPTIMIZATION_TARGET, "latency");
  channel_args.SetInt(GRPC_ARG_TCP_TX_ZEROCOPY_ENABLED, 1);
  return channel_args;
}

std::shared_ptr<Client> ClientImpl::MakeInsecureClient(
    vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
    absl::string_view address) {
  std::shared_ptr<grpc::ChannelCredentials> creds =
      grpc::InsecureChannelCredentials();
  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateCustomChannel(std::string(address), creds, GetChannelArgs());
  return std::make_unique<ClientImpl>(std::move(detached_ctx), address,
                                      Coordinator::NewStub(channel));
}

ClientImpl::ClientImpl(vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
                       absl::string_view address,
                       std::unique_ptr<Coordinator::Stub> stub)
    : detached_ctx_(std::move(detached_ctx)),
      address_(address),
      stub_(std::move(stub)) {}

void ClientImpl::GetGlobalMetadata(GetGlobalMetadataCallback done) {
  struct GetGlobalMetadataArgs {
    ::grpc::ClientContext context;
    GetGlobalMetadataRequest request;
    GetGlobalMetadataResponse response;
    GetGlobalMetadataCallback callback;
    std::unique_ptr<vmsdk::StopWatch> latency_sample;
  };
  auto args = std::make_unique<GetGlobalMetadataArgs>();
  args->context.set_deadline(
      absl::ToChronoTime(absl::Now() + absl::Seconds(60)));
  args->callback = std::move(done);
  args->latency_sample = SAMPLE_EVERY_N(100);
  auto args_raw = args.release();
  stub_->async()->GetGlobalMetadata(
      &args_raw->context, &args_raw->request, &args_raw->response,
      // std::function is not move-only.
      [args_raw](grpc::Status s) mutable {
        GRPCSuspensionGuard guard(GRPCSuspender::Instance());
        auto args = std::unique_ptr<GetGlobalMetadataArgs>(args_raw);
        args->callback(s, args->response);
        if (s.ok()) {
          Metrics::GetStats()
              .coordinator_client_get_global_metadata_success_cnt++;
          Metrics::GetStats()
              .coordinator_client_get_global_metadata_success_latency
              .SubmitSample(std::move(args->latency_sample));
        } else {
          Metrics::GetStats()
              .coordinator_client_get_global_metadata_failure_cnt++;
          Metrics::GetStats()
              .coordinator_client_get_global_metadata_failure_latency
              .SubmitSample(std::move(args->latency_sample));
        }
      });
}

void ClientImpl::SearchIndexPartition(
    std::unique_ptr<SearchIndexPartitionRequest> request,
    SearchIndexPartitionCallback done) {
  struct SearchIndexPartitionArgs {
    ::grpc::ClientContext context;
    std::unique_ptr<SearchIndexPartitionRequest> request;
    SearchIndexPartitionResponse response;
    SearchIndexPartitionCallback callback;
    std::unique_ptr<vmsdk::StopWatch> latency_sample;
  };
  auto args = std::make_unique<SearchIndexPartitionArgs>();
  args->context.set_deadline(absl::ToChronoTime(
      absl::Now() + absl::Milliseconds(request->timeout_ms())));
  args->callback = std::move(done);
  args->request = std::move(request);
  args->latency_sample = SAMPLE_EVERY_N(100);
  auto args_raw = args.release();
  Metrics::GetStats().coordinator_bytes_out.fetch_add(
      args_raw->request->ByteSizeLong(), std::memory_order_relaxed);
  stub_->async()->SearchIndexPartition(
      &args_raw->context, args_raw->request.get(), &args_raw->response,
      // std::function is not move-only.
      [args_raw](grpc::Status s) mutable {
        GRPCSuspensionGuard guard(GRPCSuspender::Instance());
        auto args = std::unique_ptr<SearchIndexPartitionArgs>(args_raw);
        args->callback(s, args->response);
        if (s.ok()) {
          Metrics::GetStats()
              .coordinator_client_search_index_partition_success_cnt++;
          Metrics::GetStats()
              .coordinator_client_search_index_partition_success_latency
              .SubmitSample(std::move(args->latency_sample));
          Metrics::GetStats().coordinator_bytes_in.fetch_add(
              args->response.ByteSizeLong(), std::memory_order_relaxed);
        } else {
          Metrics::GetStats()
              .coordinator_client_search_index_partition_failure_cnt++;
          Metrics::GetStats()
              .coordinator_client_search_index_partition_failure_latency
              .SubmitSample(std::move(args->latency_sample));
          // No need to count bytes on error
        }
      });
}

void ClientImpl::InfoIndexPartition(
    std::unique_ptr<InfoIndexPartitionRequest> request,
    InfoIndexPartitionCallback done,
    int timeout_ms) {
  struct InfoIndexPartitionArgs {
    ::grpc::ClientContext context;
    std::unique_ptr<InfoIndexPartitionRequest> request;
    InfoIndexPartitionResponse response;
    InfoIndexPartitionCallback callback;
    std::unique_ptr<vmsdk::StopWatch> latency_sample;
  };
  auto args = std::make_unique<InfoIndexPartitionArgs>();
  args->context.set_deadline(
      absl::ToChronoTime(absl::Now() + absl::Milliseconds(timeout_ms)));
  args->callback = std::move(done);
  args->request = std::move(request);
  args->latency_sample = SAMPLE_EVERY_N(100);
  auto args_raw = args.release();
  Metrics::GetStats().coordinator_bytes_out.fetch_add(
      args_raw->request->ByteSizeLong(), std::memory_order_relaxed);
  stub_->async()->InfoIndexPartition(
      &args_raw->context,
      args_raw->request.get(),
      &args_raw->response,
      // std::function is not move-only
      [args_raw](grpc::Status s) mutable {
        GRPCSuspensionGuard guard(GRPCSuspender::Instance());
        auto args = std::unique_ptr<InfoIndexPartitionArgs>(args_raw);
        args->callback(s, args->response);
        // (Optional) record metrics here
        if (s.ok()) {
          Metrics::GetStats().coordinator_bytes_in.fetch_add(
            args->response.ByteSizeLong(), std::memory_order_relaxed);
        }
      });
}

}  // namespace valkey_search::coordinator
