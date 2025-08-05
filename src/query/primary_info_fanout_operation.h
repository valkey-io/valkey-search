#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "src/coordinator/client_pool.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/fanout_operation_base.h"
#include "src/query/fanout_template.h"

namespace valkey_search::query::primary_info_fanout {

struct PrimaryInfoParameters {
  std::string index_name;
  int timeout_ms;
};

struct PrimaryInfoResult {
  bool exists = false;
  std::string index_name;
  uint64_t num_docs = 0;
  uint64_t num_records = 0;
  uint64_t hash_indexing_failures = 0;
  std::string error;
  std::optional<uint64_t> schema_fingerprint;
  bool has_schema_mismatch = false;
  std::optional<uint32_t> encoding_version;
  bool has_version_mismatch = false;
};

struct PrimaryInfoAsyncResult {
  absl::StatusOr<PrimaryInfoResult> info;
  std::unique_ptr<PrimaryInfoParameters> parameters;

  static int DoReply(ValkeyModuleCtx* ctx, PrimaryInfoAsyncResult* res,
                     ValkeyModuleString** argv, int argc) {
    if (!res) {
      return ValkeyModule_ReplyWithError(ctx, "No reply data");
    }
    if (!res->info.ok()) {
      return ValkeyModule_ReplyWithError(ctx,
                                         res->info.status().message().data());
    }
    const auto& info = res->info.value();
    const auto* index_name = res->parameters->index_name.c_str();

    if (!info.exists) {
      std::string error_msg =
          absl::StrFormat("Primary index with name '%s' not found", index_name);
      return ValkeyModule_ReplyWithError(ctx, error_msg.c_str());
    }

    if (info.has_schema_mismatch) {
      return ValkeyModule_ReplyWithError(
          ctx, "ERR found primary index schema inconsistency in the cluster");
    }
    if (info.has_version_mismatch) {
      return ValkeyModule_ReplyWithError(
          ctx, "ERR found index schema version inconsistency in the cluster");
    }

    ValkeyModule_ReplyWithArray(ctx, 10);
    ValkeyModule_ReplyWithSimpleString(ctx, "mode");
    ValkeyModule_ReplyWithSimpleString(ctx, "primary");
    ValkeyModule_ReplyWithSimpleString(ctx, "index_name");
    ValkeyModule_ReplyWithSimpleString(ctx, index_name);
    ValkeyModule_ReplyWithSimpleString(ctx, "num_docs");
    ValkeyModule_ReplyWithCString(ctx, std::to_string(info.num_docs).c_str());
    ValkeyModule_ReplyWithSimpleString(ctx, "num_records");
    ValkeyModule_ReplyWithCString(ctx,
                                  std::to_string(info.num_records).c_str());
    ValkeyModule_ReplyWithSimpleString(ctx, "hash_indexing_failures");
    ValkeyModule_ReplyWithCString(
        ctx, std::to_string(info.hash_indexing_failures).c_str());

    return VALKEYMODULE_OK;
  }
};

// Operation object for primary info fanout (primary node aggregation)
class PrimaryInfoFanoutOperation
    : public fanout::FanoutOperationBase<
          coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          fanout::FanoutTargetMode::kPrimary, PrimaryInfoAsyncResult> {
 public:
  PrimaryInfoFanoutOperation(std::string index_name, int timeout_ms,
                             coordinator::ClientPool* client_pool);

  int GetTimeoutMs() const override;

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const fanout::FanoutSearchTarget&, int timeout_ms) override;

  void OnResponse(const coordinator::InfoIndexPartitionResponse& resp,
                  const fanout::FanoutSearchTarget&) override;

  void OnError(const std::string& error,
               const fanout::FanoutSearchTarget&) override;

  void FillLocalResponse(ValkeyModuleCtx* ctx,
                         const coordinator::InfoIndexPartitionRequest& request,
                         coordinator::InfoIndexPartitionResponse& resp,
                         const fanout::FanoutSearchTarget&) override;

  void InvokeRemoteRpc(
      coordinator::Client* client,
      const coordinator::InfoIndexPartitionRequest& request,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse&)>
          callback,
      int timeout_ms) override;

  void* CreateAsyncResult() override;

  void OnCompletion() override;

 private:
  // Resources & config
  coordinator::ClientPool* client_pool_;

  // Aggregation state
  bool exists_ = false;
  std::optional<uint64_t> schema_fingerprint_;
  std::optional<uint32_t> encoding_version_;
  bool has_schema_mismatch_ = false;
  bool has_version_mismatch_ = false;
  std::string error_;
  std::string index_name_;
  std::optional<int> timeout_ms_;
  uint64_t num_docs_ = 0;
  uint64_t num_records_ = 0;
  uint64_t hash_indexing_failures_ = 0;
};

}  // namespace valkey_search::query::primary_info_fanout
