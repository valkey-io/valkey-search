/*
 * Copyright (c) 2025, ValkeySearch contributors
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
    vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
    absl::string_view address) {
  std::shared_ptr<grpc::ChannelCredentials> creds =
      grpc::InsecureChannelCredentials();
  std::shared_ptr<grpc::Channel> channel =
      grpc::CreateCustomChannel(std::string(address), creds, GetChannelArgs());
  return std::make_unique<ClientImpl>(std::move(detached_ctx), address,
                                      Coordinator::NewStub(channel));
}

ClientImpl::ClientImpl(vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
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
        } else {
          Metrics::GetStats()
              .coordinator_client_search_index_partition_failure_cnt++;
          Metrics::GetStats()
              .coordinator_client_search_index_partition_failure_latency
              .SubmitSample(std::move(args->latency_sample));
        }
      });
}

}  // namespace valkey_search::coordinator
