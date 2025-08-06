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

class PrimaryInfoFanoutOperation
    : public fanout::FanoutOperationBase<
          PrimaryInfoFanoutOperation, coordinator::InfoIndexPartitionRequest,
          coordinator::InfoIndexPartitionResponse,
          fanout::FanoutTargetMode::kPrimary> {
 public:
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

  int GenerateReply(ValkeyModuleCtx* ctx, ValkeyModuleString** argv,
                    int argc) override;

  void OnCompletion() override;

 private:
  coordinator::ClientPool* client_pool_;
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
