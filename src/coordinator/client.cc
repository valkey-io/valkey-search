/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/client.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/call_once.h"
#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "google/protobuf/arena.h"
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
#include "src/valkey_search_options.h"
#include "utils.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"

namespace valkey_search::coordinator {

// clang-format off
constexpr absl::string_view kRetryPolicy =
    "{\"methodConfig\" : [{"
    "   \"name\" : [{\"service\": \"valkey_search.coordinator.Coordinator\"}],"
    "   \"waitForReady\": true,"
    "   \"retryPolicy\": {"
    "     \"maxAttempts\": 5,"
    "     \"initialBackoff\": \"0.100s\","
    "     \"maxBackoff\": \"1s\","
    "     \"backoffMultiplier\": 1.0,"
    "     \"retryableStatusCodes\": ["
    "       \"UNAVAILABLE\","
    "       \"UNKNOWN\","
    "       \"INTERNAL\","
    "       \"DATA_LOSS\","
    "       \"NOT_FOUND\""
    "     ]"
    "    }"
    "}]}";
// clang-format on

static constexpr absl::string_view kCoordinatorQueryTimeout{
    "coordinator-query-timeout-secs"};
static constexpr int kCoordinatorQueryDefaultTimeout{25};
static constexpr int kCoordinatorQueryMinTimeout{1};
static constexpr int kCoordinatorQueryMaxTimeout{3600};

static auto query_connection_timeout =
    vmsdk::config::NumberBuilder(
        kCoordinatorQueryTimeout, kCoordinatorQueryDefaultTimeout,
        kCoordinatorQueryMinTimeout, kCoordinatorQueryMaxTimeout)
        .Build();

grpc::ChannelArguments &GetChannelArgs() {
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

ClientImpl::ClientImpl(
    vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
    absl::string_view address, std::unique_ptr<Coordinator::StubInterface> stub)
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
  auto *args_raw = args.release();
  stub_->async()->GetGlobalMetadata(
      &args_raw->context, &args_raw->request, &args_raw->response,
      // std::function is not move-only.
      [args_raw](const grpc::Status &s) mutable {
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

namespace {

// RPC-specific success / failure metrics, recorded once a call completes.
using RecordCallMetrics = void (*)(bool ok,
                                   std::unique_ptr<vmsdk::StopWatch> sample);

void RecordSearchPartitionClientMetrics(
    bool ok, std::unique_ptr<vmsdk::StopWatch> latency_sample) {
  auto &stats = Metrics::GetStats();
  if (ok) {
    stats.coordinator_client_search_index_partition_success_cnt++;
    stats.coordinator_client_search_index_partition_success_latency
        .SubmitSample(std::move(latency_sample));
  } else {
    stats.coordinator_client_search_index_partition_failure_cnt++;
    stats.coordinator_client_search_index_partition_failure_latency
        .SubmitSample(std::move(latency_sample));
  }
}

// One in-flight unary RPC with an arena-allocated response. Owns itself from
// Start() until Complete(), which the stub's completion invokes exactly once.
// Shared by the RPCs whose calls differ only in the stub method and metrics.
template <typename Request, typename Response>
class UnaryCall {
 public:
  using Done = absl::AnyInvocable<void(grpc::Status, Response &)>;

  // Counts the request bytes and arms the deadline.
  static UnaryCall *Start(std::unique_ptr<Request> request, Done done,
                          RecordCallMetrics record_metrics) {
    auto *call =
        new UnaryCall(std::move(request), std::move(done), record_metrics);
    Metrics::GetStats().coordinator_bytes_out.fetch_add(
        call->request_->ByteSizeLong(), std::memory_order_relaxed);
    return call;
  }

  ::grpc::ClientContext *context() { return &context_; }
  const Request *request() const { return request_.get(); }
  Response *response() { return response_; }

  // Hands the response to `done`, then records metrics and deletes the call.
  void Complete(const grpc::Status &status) {
    GRPCSuspensionGuard guard(GRPCSuspender::Instance());
    std::unique_ptr<UnaryCall> self(this);
    // Measured before `done`, which may consume the response.
    const uint64_t response_bytes = status.ok() ? response_->ByteSizeLong() : 0;
    done_(status, *response_);
    if (record_metrics_ != nullptr) {
      record_metrics_(status.ok(), std::move(latency_sample_));
    }
    if (status.ok()) {
      Metrics::GetStats().coordinator_bytes_in.fetch_add(
          response_bytes, std::memory_order_relaxed);
    }
  }

 private:
  UnaryCall(std::unique_ptr<Request> request, Done done,
            RecordCallMetrics record_metrics)
      : request_(std::move(request)),
        response_(google::protobuf::Arena::Create<Response>(&arena_)),
        done_(std::move(done)),
        record_metrics_(record_metrics),
        latency_sample_(SAMPLE_EVERY_N(100)) {
    context_.set_deadline(absl::ToChronoTime(
        absl::Now() + absl::Seconds(query_connection_timeout->GetValue())));
  }

  ::grpc::ClientContext context_;
  std::unique_ptr<Request> request_;
  google::protobuf::Arena arena_;
  Response *response_;
  Done done_;
  RecordCallMetrics record_metrics_;
  std::unique_ptr<vmsdk::StopWatch> latency_sample_;
};

}  // namespace

void ClientImpl::SearchIndexPartition(
    std::unique_ptr<SearchIndexPartitionRequest> request,
    SearchIndexPartitionCallback done) {
  auto *call =
      UnaryCall<SearchIndexPartitionRequest, SearchIndexPartitionResponse>::
          Start(std::move(request), std::move(done),
                &RecordSearchPartitionClientMetrics);
  stub_->async()->SearchIndexPartition(
      call->context(), call->request(), call->response(),
      [call](const grpc::Status &status) { call->Complete(status); });
}

void ClientImpl::FetchContent(std::unique_ptr<FetchContentRequest> request,
                              SearchIndexPartitionCallback done) {
  // Not a search partition, so no search partition metrics; the bounded
  // content fan-out counts its recovery requests itself.
  auto *call =
      UnaryCall<FetchContentRequest, SearchIndexPartitionResponse>::Start(
          std::move(request), std::move(done), /*record_metrics=*/nullptr);
  stub_->async()->FetchContent(
      call->context(), call->request(), call->response(),
      [call](const grpc::Status &status) { call->Complete(status); });
}

void ClientImpl::MultiSearchIndexPartition(
    std::unique_ptr<MultiSearchIndexPartitionRequest> request,
    MultiSearchIndexPartitionCallback done) {
  struct MultiSearchIndexPartitionArgs {
    ::grpc::ClientContext context;
    std::unique_ptr<MultiSearchIndexPartitionRequest> request;
    MultiSearchIndexPartitionResponse response;
    MultiSearchIndexPartitionCallback callback;
    std::unique_ptr<vmsdk::StopWatch> latency_sample;
  };
  auto args = std::make_unique<MultiSearchIndexPartitionArgs>();
  args->context.set_deadline(absl::ToChronoTime(
      absl::Now() + absl::Seconds(query_connection_timeout->GetValue())));
  args->callback = std::move(done);
  args->request = std::move(request);
  args->latency_sample = SAMPLE_EVERY_N(100);
  auto args_raw = args.release();
  Metrics::GetStats().coordinator_bytes_out.fetch_add(
      args_raw->request->ByteSizeLong(), std::memory_order_relaxed);
  stub_->async()->MultiSearchIndexPartition(
      &args_raw->context, args_raw->request.get(), &args_raw->response,
      [args_raw](grpc::Status s) mutable {
        GRPCSuspensionGuard guard(GRPCSuspender::Instance());
        auto args = std::unique_ptr<MultiSearchIndexPartitionArgs>(args_raw);
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
        }
      });
}

void ClientImpl::InfoIndexPartition(
    std::unique_ptr<InfoIndexPartitionRequest> request,
    InfoIndexPartitionCallback done, int timeout_ms) {
  struct InfoIndexPartitionArgs {
    ::grpc::ClientContext context;
    std::unique_ptr<InfoIndexPartitionRequest> request;
    InfoIndexPartitionResponse response;
    InfoIndexPartitionCallback callback;
    std::unique_ptr<vmsdk::StopWatch> latency_sample;
  };
  auto args = std::make_unique<InfoIndexPartitionArgs>();
  args->context.set_deadline(absl::ToChronoTime(
      absl::Now() +
      absl::Milliseconds(options::GetFTInfoRpcTimeoutMs().GetValue())));
  args->callback = std::move(done);
  args->request = std::move(request);
  args->latency_sample = SAMPLE_EVERY_N(100);
  auto *args_raw = args.release();
  Metrics::GetStats().coordinator_bytes_out.fetch_add(
      args_raw->request->ByteSizeLong(), std::memory_order_relaxed);
  stub_->async()->InfoIndexPartition(
      &args_raw->context, args_raw->request.get(), &args_raw->response,
      // std::function is not move-only
      [args_raw](const grpc::Status &s) mutable {
        if (!vmsdk::IsMainThread()) {
          PAUSEPOINT("fanout_remote_pausepoint");
        }
        GRPCSuspensionGuard guard(GRPCSuspender::Instance());
        auto args = std::unique_ptr<InfoIndexPartitionArgs>(args_raw);
        const uint64_t response_bytes =
            s.ok() ? args->response.ByteSizeLong() : 0;
        args->callback(s, args->response);
        // (Optional) record metrics here
        if (s.ok()) {
          Metrics::GetStats().coordinator_bytes_in.fetch_add(
              response_bytes, std::memory_order_relaxed);
        }
      });
}

}  // namespace valkey_search::coordinator
