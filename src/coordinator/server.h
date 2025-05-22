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
  Service(vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
          vmsdk::ThreadPool* reader_thread_pool)
      : detached_ctx_(std::move(detached_ctx)),
        reader_thread_pool_(reader_thread_pool) {}
  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  ~Service() override = default;

  grpc::ServerUnaryReactor* GetGlobalMetadata(
      grpc::CallbackServerContext* context,
      const GetGlobalMetadataRequest* request,
      GetGlobalMetadataResponse* response) override;

  grpc::ServerUnaryReactor* SearchIndexPartition(
      grpc::CallbackServerContext* context,
      const SearchIndexPartitionRequest* request,
      SearchIndexPartitionResponse* response) override;

 private:
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  vmsdk::ThreadPool* reader_thread_pool_;
};

class Server {
 public:
  virtual uint16_t GetPort() const = 0;
  virtual ~Server() = default;
};

class ServerImpl final : public Server {
 public:
  static std::unique_ptr<Server> Create(RedisModuleCtx* ctx,
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
