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

#ifndef VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_
#define VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_

#include <memory>
#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/coordinator/client.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::coordinator {

class ClientPool {
 public:
  ClientPool(vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx)
      : detached_ctx_(std::move(detached_ctx)) {}
  virtual ~ClientPool() = default;

  virtual std::shared_ptr<Client> GetClient(absl::string_view address) {
    auto mutex = absl::MutexLock(&client_pool_mutex_);
    auto itr = client_pool_.find(address);
    if (itr == client_pool_.end()) {
      auto client = ClientImpl::MakeInsecureClient(
          vmsdk::MakeUniqueValkeyDetachedThreadSafeContext(detached_ctx_.get()),
          address);
      client_pool_[address] = std::move(client);
    }
    return client_pool_[address];
  }

 private:
  vmsdk::UniqueValkeyDetachedThreadSafeContext detached_ctx_;
  absl::Mutex client_pool_mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<Client>> client_pool_
      ABSL_GUARDED_BY(client_pool_mutex_);
};

}  // namespace valkey_search::coordinator

#endif  // VALKEYSEARCH_SRC_COORDINATOR_CLIENT_POOL_H_
