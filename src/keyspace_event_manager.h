/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
    std::function<absl::Status(ValkeyModuleCtx *, int)>;

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

  virtual void OnKeyspaceNotification(ValkeyModuleCtx *ctx, int type,
                                      const char *event,
                                      ValkeyModuleString *key) = 0;
};

class KeyspaceEventManager {
 public:
  KeyspaceEventManager() = default;
  void NotifySubscribers(ValkeyModuleCtx *ctx, int type, const char *event,
                         ValkeyModuleString *key);

  absl::Status InsertSubscription(ValkeyModuleCtx *ctx,
                                  KeyspaceEventSubscription *subscription);

  absl::Status RemoveSubscription(KeyspaceEventSubscription *subscription);

  inline bool HasSubscription(KeyspaceEventSubscription *subscription) const {
    return subscriptions_.Get().contains(subscription);
  }
  static void InitInstance(std::unique_ptr<KeyspaceEventManager> instance);
  static KeyspaceEventManager &Instance();

 private:
  absl::Status StartValkeySubscriptionIfNeeded(ValkeyModuleCtx *ctx, int types);

  static inline int OnValkeyKeyspaceNotification(ValkeyModuleCtx *ctx, int type,
                                                 const char *event,
                                                 ValkeyModuleString *key) {
    Instance().NotifySubscribers(ctx, type, event, key);
    return VALKEYMODULE_OK;
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
