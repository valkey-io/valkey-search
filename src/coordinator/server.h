/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_SERVER_H_
#define VALKEYSEARCH_SRC_COORDINATOR_SERVER_H_

#include <cstdint>
#include <memory>
#include <utility>

#include "grpcpp/server.h"
#include "grpcpp/server_context.h"
#include "grpcpp/support/server_callback.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::coordinator {

class Service final : public Coordinator::CallbackService {
 public:
  Service(vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
          vmsdk::ThreadPool* reader_thread_pool)
      : detached_ctx_(std::move(detached_ctx)),
        reader_thread_pool_(reader_thread_pool) {}
  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  ~Service() override = default;

  static std::pair<grpc::Status, coordinator::InfoIndexPartitionResponse> GenerateInfoResponse(
      const coordinator::InfoIndexPartitionRequest& request);

  grpc::ServerUnaryReactor* GetGlobalMetadata(
      grpc::CallbackServerContext* context,
      const GetGlobalMetadataRequest* request,
      GetGlobalMetadataResponse* response) override;

  grpc::ServerUnaryReactor* SearchIndexPartition(
      grpc::CallbackServerContext* context,
      const SearchIndexPartitionRequest* request,
      SearchIndexPartitionResponse* response) override;

  grpc::ServerUnaryReactor* InfoIndexPartition(
      grpc::CallbackServerContext* context,
      const InfoIndexPartitionRequest* request,
      InfoIndexPartitionResponse* response) override;

 private:
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  vmsdk::ThreadPool* reader_thread_pool_;
};

class Server {
 public:
  virtual uint16_t GetPort() const = 0;
  virtual ~Server() = default;
};

class ServerImpl final : public Server {
 public:
  static std::unique_ptr<Server> Create(ValkeyModuleCtx* ctx,
                                        vmsdk::ThreadPool* reader_thread_pool,
                                        uint16_t port);
  ServerImpl(const ServerImpl&) = delete;
  ServerImpl& operator=(const ServerImpl&) = delete;
  uint16_t GetPort() const override { return port_; }

 private:
  ServerImpl(std::unique_ptr<Service> coordinator_service,
             std::unique_ptr<grpc::Server> server, uint16_t port);

  std::unique_ptr<Service> coordinator_service_{nullptr};
  std::unique_ptr<grpc::Server> server_{nullptr};
  uint16_t port_;
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_SERVER_H_
