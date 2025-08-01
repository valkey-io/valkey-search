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

// --- Parameter and Result Structs ---

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

// --- Callback Type ---

using PrimaryInfoResponseCallback = absl::AnyInvocable<void(
    absl::StatusOr<PrimaryInfoResult>, std::unique_ptr<PrimaryInfoParameters>)>;

// Operation object for primary info fanout (primary node aggregation)
class PrimaryInfoFanoutOperation : public fanout::FanoutOperationBase<
                                       coordinator::InfoIndexPartitionRequest,
                                       coordinator::InfoIndexPartitionResponse,
                                       fanout::FanoutTargetMode::kPrimary> {
 public:
  PrimaryInfoFanoutOperation(ValkeyModuleCtx* ctx,
                             coordinator::ClientPool* client_pool,
                             std::unique_ptr<PrimaryInfoParameters> params,
                             PrimaryInfoResponseCallback callback);

  int GetTimeoutMs() const override;

  coordinator::InfoIndexPartitionRequest GenerateRequest(
      const fanout::FanoutSearchTarget&, int timeout_ms) override;

  void OnResponse(const coordinator::InfoIndexPartitionResponse& resp,
                  const fanout::FanoutSearchTarget&) override;

  void OnError(const std::string& error,
               const fanout::FanoutSearchTarget&) override;

  void OnCompletion() override;

  void FillLocalResponse(const coordinator::InfoIndexPartitionRequest& request,
                         coordinator::InfoIndexPartitionResponse& resp,
                         const fanout::FanoutSearchTarget&) override;

  void InvokeRemoteRpc(
      coordinator::Client* client,
      std::unique_ptr<coordinator::InfoIndexPartitionRequest> request_ptr,
      std::function<void(grpc::Status,
                         coordinator::InfoIndexPartitionResponse&)>
          callback,
      int timeout_ms) override;

 private:
  // Resources & config
  coordinator::ClientPool* client_pool_;
  std::unique_ptr<PrimaryInfoParameters> parameters_;
  PrimaryInfoResponseCallback callback_;

  // Aggregation state
  bool exists_ = false;
  std::optional<uint64_t> schema_fingerprint_;
  std::optional<uint32_t> encoding_version_;
  bool has_schema_mismatch_ = false;
  bool has_version_mismatch_ = false;
  std::string error_;
  std::string index_name_;
  uint64_t num_docs_ = 0;
  uint64_t num_records_ = 0;
  uint64_t hash_indexing_failures_ = 0;
};

}  // namespace valkey_search::query::primary_info_fanout
