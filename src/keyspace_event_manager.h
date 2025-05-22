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

#ifndef VALKEYSEARCH_SRC_KEYSPACE_EVENT_MANAGER_H_
#define VALKEYSEARCH_SRC_KEYSPACE_EVENT_MANAGER_H_

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "src/attribute_data_type.h"
#include "src/utils/patricia_tree.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
using StartSubscriptionFunction =
    std::function<absl::Status(RedisModuleCtx *, int)>;

// KeyspaceEventSubscription is an interface for classes that want to subscribe
// to keyspace events.
class KeyspaceEventSubscription {
 public:
  virtual ~KeyspaceEventSubscription() = default;
  virtual const AttributeDataType &GetAttributeDataType() const = 0;

  // GetKeyPrefixes should return a vector containing >= 1 prefixes to subscribe
  // to. Note that this should not be empty. If no prefixes are needed, this
  // should still return a vector with the single value "".
  //
  // The value "*" will match the character "*". If matching all prefixes is
  // desired, "" should be used.
  //
  // The returned vector should contain no two strings A and B, such that A is
  // completely prefixed by B. Otherwise, duplicate events may fire.
  virtual const std::vector<std::string> &GetKeyPrefixes() const = 0;

  virtual void OnKeyspaceNotification(RedisModuleCtx *ctx, int type,
                                      const char *event,
                                      RedisModuleString *key) = 0;
};

class KeyspaceEventManager {
 public:
  KeyspaceEventManager() = default;
  void NotifySubscribers(RedisModuleCtx *ctx, int type, const char *event,
                         RedisModuleString *key);

  absl::Status InsertSubscription(RedisModuleCtx *ctx,
                                  KeyspaceEventSubscription *subscription);

  absl::Status RemoveSubscription(KeyspaceEventSubscription *subscription);

  inline bool HasSubscription(KeyspaceEventSubscription *subscription) const {
    return subscriptions_.Get().contains(subscription);
  }
  static void InitInstance(std::unique_ptr<KeyspaceEventManager> instance);
  static KeyspaceEventManager &Instance();

 private:
  absl::Status StartRedisSubscriptionIfNeeded(RedisModuleCtx *ctx, int types);

  static inline int OnRedisKeyspaceNotification(RedisModuleCtx *ctx, int type,
                                                const char *event,
                                                RedisModuleString *key) {
    Instance().NotifySubscribers(ctx, type, event, key);
    return REDISMODULE_OK;
  }

  vmsdk::MainThreadAccessGuard<absl::flat_hash_set<KeyspaceEventSubscription *>>
      subscriptions_;
  // TODO: b/355561165 - Migrate to PatriciaTreeSet
  vmsdk::MainThreadAccessGuard<PatriciaTree<KeyspaceEventSubscription *>>
      subscription_trie_{/*case_sensitive=*/true};
  int subscribed_types_bit_mask_{0};
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_KEYSPACE_EVENT_MANAGER_H_
