/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
using InfoIndexPartitionCallback = 
    absl::AnyInvocable<void(grpc::Status, InfoIndexPartitionResponse&)>;

class Client {
 public:
  virtual ~Client() = default;
  virtual void GetGlobalMetadata(GetGlobalMetadataCallback done) = 0;
  virtual void SearchIndexPartition(
      std::unique_ptr<SearchIndexPartitionRequest> request,
      SearchIndexPartitionCallback done) = 0;
  virtual void InfoIndexPartition(
      std::unique_ptr<InfoIndexPartitionRequest> request,
      InfoIndexPartitionCallback done) = 0;
};

class ClientImpl : public Client {
 public:
  ClientImpl(vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
             absl::string_view address,
             std::unique_ptr<Coordinator::Stub> stub);
  static std::shared_ptr<Client> MakeInsecureClient(
      vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
      absl::string_view address);

  ClientImpl(const ClientImpl&) = delete;
  ClientImpl& operator=(const ClientImpl&) = delete;

  void GetGlobalMetadata(GetGlobalMetadataCallback done) override;
  void SearchIndexPartition(
      std::unique_ptr<SearchIndexPartitionRequest> request,
      SearchIndexPartitionCallback done) override;
  void InfoIndexPartition(
      std::unique_ptr<InfoIndexPartitionRequest> request,
      InfoIndexPartitionCallback done) override;

 private:
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  std::string address_;
  std::unique_ptr<Coordinator::Stub> stub_;
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_CLIENT_H_
