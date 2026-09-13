/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/keyspace_event_manager.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "src/indexes/vector_base.h"
#include "src/utils/string_interning.h"
#include "src/vector_registry.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {
constexpr absl::string_view kRenameFromEvent = "rename_from";
constexpr absl::string_view kRenameToEvent = "rename_to";
constexpr absl::string_view kMoveFromEvent = "move_from";
constexpr absl::string_view kMoveToEvent = "move_to";

inline bool IsMoveFromEvent(const char *event) {
  return event && (event == kRenameFromEvent || event == kMoveFromEvent);
}

inline bool IsMoveToEvent(const char *event) {
  return event && (event == kRenameToEvent || event == kMoveToEvent);
}
}  // namespace

static absl::NoDestructor<std::unique_ptr<KeyspaceEventManager>>
    keyspace_event_manager_instance;

KeyspaceEventManager &KeyspaceEventManager::Instance() {
  return **keyspace_event_manager_instance;
}
void KeyspaceEventManager::InitInstance(
    std::unique_ptr<KeyspaceEventManager> instance) {
  *keyspace_event_manager_instance = std::move(instance);
}

std::vector<KeyspaceEventSubscription *>
KeyspaceEventManager::GetMatchingSubscriptions(absl::string_view key, int type,
                                               int db_num) {
  std::vector<KeyspaceEventSubscription *> subscriptions_to_notify;
  for (auto match_itr = subscription_trie_.Get().PathIterator(key);
       !match_itr.Done(); match_itr.Next()) {
    for (const auto &subscription : *match_itr.Value().value) {
      if (db_num >= 0 && !subscription->IsInDB(db_num)) {
        continue;
      }
      if (subscription->GetAttributeDataType().GetValkeyEventTypes() & type) {
        subscriptions_to_notify.push_back(subscription);
      }
    }
  }
  return subscriptions_to_notify;
}

void KeyspaceEventManager::ProcessMoveNotification(ValkeyModuleCtx *ctx,
                                                   int type,
                                                   ValkeyModuleString *dst_key,
                                                   const char *dst_event) {
  int dst_db_num = ValkeyModule_GetSelectedDb(ctx);
  auto cleanup = absl::MakeCleanup([this]() {
    moved_src_key_.reset();
    src_db_num_ = -1;
  });

  auto src_key_view = vmsdk::ToStringView(moved_src_key_.get());
  auto src_subscriptions =
      GetMatchingSubscriptions(src_key_view, type, src_db_num_);
  if (src_subscriptions.empty()) {
    return;
  }

  auto dst_key_view = vmsdk::ToStringView(dst_key);
  auto src_interned_key = StringInternStore::Intern(src_key_view);

  // Classify each source vector attribute as either kMove or kUnshare.
  vmsdk::UniqueValkeyOpenKey dst_key_obj;
  absl::flat_hash_map<InternedStringPtr, VectorRegistry::Action> actions;
  for (const auto *sub : src_subscriptions) {
    for (const auto *src_index : sub->GetVectorIndexes()) {
      const auto &attr_id = src_index->GetInternedAttributeIdentifier();
      auto it =
          actions.try_emplace(attr_id, VectorRegistry::Action::kUnshare).first;
      if (it->second == VectorRegistry::Action::kUnshare) {
        if (!dst_key_obj) {
          dst_key_obj = vmsdk::MakeUniqueValkeyOpenKey(
              ctx, dst_key,
              VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
        }
        if (dst_key_obj &&
            VectorRegistry::Instance().HasMatchingVectorIndex(
                dst_db_num, dst_key_view, attr_id, dst_key_obj.get(),
                src_index->GetVectorDataSize())) {
          it->second = VectorRegistry::Action::kMove;
        }
      }
    }
  }

  VectorRegistry::Instance().MoveKey(src_db_num_, src_interned_key, dst_db_num,
                                     dst_key, actions);

  // Source subscriptions must be notified with the source event (e.g.
  // rename_from or move_from) in the context of their original database
  // (src_db_num_). We temporarily switch databases if moving across databases
  // and restore dst_db_num afterward. Destination subscriptions will be
  // notified with the destination event subsequently in NotifySubscribers.
  const char *src_event = (dst_event && dst_event == kMoveToEvent)
                              ? kMoveFromEvent.data()
                              : kRenameFromEvent.data();
  {
    vmsdk::ValkeySelectDbGuard db_guard(ctx, src_db_num_);
    for (const auto &subscription : src_subscriptions) {
      subscription->OnKeyspaceNotification(ctx, type, src_event,
                                           moved_src_key_.get());
    }
  }
}

void KeyspaceEventManager::NotifySubscribers(ValkeyModuleCtx *ctx, int type,
                                             const char *event,
                                             ValkeyModuleString *key) {
  if (IsMoveFromEvent(event)) {
    moved_src_key_ = vmsdk::RetainUniqueValkeyString(key);
    src_db_num_ = ValkeyModule_GetSelectedDb(ctx);
    return;
  }

  if (IsMoveToEvent(event)) {
    CHECK(moved_src_key_);
    ProcessMoveNotification(ctx, type, key, event);
  }
  CHECK(!moved_src_key_);

  auto subscriptions_to_notify =
      GetMatchingSubscriptions(vmsdk::ToStringView(key), type);
  for (const auto &subscription : subscriptions_to_notify) {
    subscription->OnKeyspaceNotification(ctx, type, event, key);
  }
}

absl::Status KeyspaceEventManager::RemoveSubscription(
    KeyspaceEventSubscription *subscription) {
  auto &subscriptions = subscriptions_.Get();
  if (!subscriptions.contains(subscription)) {
    return absl::NotFoundError("Subscription not found");
  }

  auto key_prefixes = subscription->GetKeyPrefixes();
  DCHECK(!key_prefixes.empty());
  auto &subscription_trie = subscription_trie_.Get();
  for (const auto &prefix : key_prefixes) {
    subscription_trie.Remove(prefix, subscription);
    // TODO - we need to support unsubscribe to keyspace events
  }

  subscriptions.erase(subscription);
  return absl::OkStatus();
}

absl::Status KeyspaceEventManager::InsertSubscription(
    ValkeyModuleCtx *ctx, KeyspaceEventSubscription *subscription) {
  VMSDK_RETURN_IF_ERROR(StartValkeySubscriptionIfNeeded(
      ctx, subscription->GetAttributeDataType().GetValkeyEventTypes()));

  auto key_prefixes = subscription->GetKeyPrefixes();
  CHECK(!key_prefixes.empty());
  auto &subscription_trie = subscription_trie_.Get();
  for (const auto &prefix : key_prefixes) {
    subscription_trie.AddKeyValue(prefix, subscription);
  }

  subscriptions_.Get().insert(subscription);
  return absl::OkStatus();
}

absl::Status KeyspaceEventManager::StartValkeySubscriptionIfNeeded(
    ValkeyModuleCtx *ctx, int types) {
  int to_subscribe = types & ~subscribed_types_bit_mask_;
  if (!to_subscribe) {
    return absl::OkStatus();
  }
  if (ValkeyModule_SubscribeToKeyspaceEvents(
          ctx, to_subscribe, OnValkeyKeyspaceNotification) != VALKEYMODULE_OK) {
    return absl::InternalError("failed to subscribe to keyspace events");
  }
  subscribed_types_bit_mask_ |= types;

  return absl::OkStatus();
}

}  // namespace valkey_search
