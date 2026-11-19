/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_
#define VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "grpcpp/security/credentials.h"
#include "src/coordinator/client.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

class ClientPool {
 public:
  // `creds` secures the channels to peers; plaintext when null.
  ClientPool(vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx,
             std::shared_ptr<grpc::ChannelCredentials> creds = nullptr)
      : detached_ctx_(std::move(detached_ctx)), creds_(std::move(creds)) {}
  virtual ~ClientPool() = default;

  virtual std::shared_ptr<Client> GetClient(absl::string_view address) {
    auto mutex = absl::MutexLock(&client_pool_mutex_);
    auto itr = client_pool_.find(address);
    if (itr == client_pool_.end()) {
      auto client = ClientImpl::MakeClient(
          vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(detached_ctx_.get()),
          address, creds_ ? creds_ : grpc::InsecureChannelCredentials());
      client_pool_[address] = std::move(client);
    }
    return client_pool_[address];
  }

 private:
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  std::shared_ptr<grpc::ChannelCredentials> creds_;
  absl::Mutex client_pool_mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<Client>> client_pool_
      ABSL_GUARDED_BY(client_pool_mutex_);
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_
