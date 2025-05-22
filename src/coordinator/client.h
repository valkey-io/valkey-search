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

#ifndef VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_
#define VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_

#include <memory>
#include <string>

#include "absl/functional/any_invocable.h"
#include "absl/strings/string_view.h"
#include "grpcpp/support/status.h"
#include "src/coordinator/coordinator.grpc.pb.h"
#include "src/coordinator/coordinator.pb.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

using GetGlobalMetadataCallback =
    absl::AnyInvocable<void(grpc::Status, GetGlobalMetadataResponse&)>;
using SearchIndexPartitionCallback =
    absl::AnyInvocable<void(grpc::Status, SearchIndexPartitionResponse&)>;

class Client {
 public:
  virtual ~Client() = default;
  virtual void GetGlobalMetadata(GetGlobalMetadataCallback done) = 0;
  virtual void SearchIndexPartition(
      std::unique_ptr<SearchIndexPartitionRequest> request,
      SearchIndexPartitionCallback done) = 0;
};

class ClientImpl : public Client {
 public:
  ClientImpl(vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
             absl::string_view address,
             std::unique_ptr<Coordinator::Stub> stub);
  static std::shared_ptr<Client> MakeInsecureClient(
      vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx,
      absl::string_view address);

  ClientImpl(const ClientImpl&) = delete;
  ClientImpl& operator=(const ClientImpl&) = delete;

  void GetGlobalMetadata(GetGlobalMetadataCallback done) override;
  void SearchIndexPartition(
      std::unique_ptr<SearchIndexPartitionRequest> request,
      SearchIndexPartitionCallback done) override;

 private:
  vmsdk::UniqueRedisDetachedThreadSafeContext detached_ctx_;
  std::string address_;
  std::unique_ptr<Coordinator::Stub> stub_;
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_
