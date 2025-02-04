/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#include "src/keyspace_event_manager.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/indexes/index_base.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using testing::_;
using testing::Return;
using testing::ReturnRef;

struct KeyspaceEventSubscriptionTestCase {
  std::string subscription_id;
  std::vector<std::string> key_prefixes_to_subscribe;
  int types_to_subscribe;
  absl::optional<int> expected_type_subscriptions;
  indexes::IndexBase *fake_index;
};

struct KeyspaceEventNotificationTestCase {
  std::string notification_key;
  int notification_type;
  std::vector<std::string> expected_subscriptions_with_notifications;
};

struct KeyspaceEventManagerTestCase {
  std::string test_name;
  std::vector<KeyspaceEventSubscriptionTestCase> subscriptions;
  std::vector<KeyspaceEventNotificationTestCase> notifications;
};

class KeyspaceEventManagerTest
    : public ValkeySearchTestWithParam<KeyspaceEventManagerTestCase> {
 protected:
  void TearDown() override {
    ValkeySearchTestWithParam<KeyspaceEventManagerTestCase>::TearDown();
  }
};

TEST_P(KeyspaceEventManagerTest, SubscriptionAndNotificationTest) {
  const KeyspaceEventManagerTestCase &test_case = GetParam();
  absl::string_view event_name = "event";

  std::vector<std::unique_ptr<MockAttributeDataType>> mock_attribute_data_types;
  absl::flat_hash_map<std::string,
                      std::unique_ptr<MockKeyspaceEventSubscription>>
      mock_subscriptions;
  absl::flat_hash_map<std::string, KeyspaceEventSubscriptionTestCase>
      subscription_test_cases;
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();

  for (const KeyspaceEventSubscriptionTestCase &subscription :
       test_case.subscriptions) {
    if (subscription.expected_type_subscriptions.has_value()) {
      EXPECT_CALL(
          *kMockRedisModule,
          SubscribeToKeyspaceEvents(
              &fake_ctx_, subscription.expected_type_subscriptions.value(), _))
          .WillOnce(Return(REDISMODULE_OK));
    }
    auto mock_subscription = std::make_unique<MockKeyspaceEventSubscription>();
    auto mock_attribute_data_type = std::make_unique<MockAttributeDataType>();
    EXPECT_CALL(*mock_attribute_data_type, GetRedisEventTypes())
        .WillRepeatedly(Return(subscription.types_to_subscribe));
    EXPECT_CALL(*mock_subscription, GetAttributeDataType())
        .WillRepeatedly(ReturnRef(*mock_attribute_data_type));
    EXPECT_CALL(*mock_subscription, GetKeyPrefixes())
        .WillRepeatedly(ReturnRef(subscription.key_prefixes_to_subscribe));
    VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
        &fake_ctx_, mock_subscription.get()));
    subscription_test_cases[subscription.subscription_id] = subscription;
    mock_subscriptions[subscription.subscription_id] =
        std::move(mock_subscription);
    mock_attribute_data_types.push_back(std::move(mock_attribute_data_type));
  }

  for (const KeyspaceEventNotificationTestCase &notification :
       test_case.notifications) {
    RedisModuleString *key = TestRedisModule_CreateStringPrintf(
        &fake_ctx_, "%s", notification.notification_key.data());
    for (const std::string &subscription_id :
         notification.expected_subscriptions_with_notifications) {
      EXPECT_CALL(
          *mock_subscriptions[subscription_id],
          OnKeyspaceNotification(&fake_ctx_, notification.notification_type,
                                 event_name.data(), key))
          .WillOnce(Return());
    }
    keyspace_event_manager->NotifySubscribers(
        &fake_ctx_, notification.notification_type, event_name.data(), key);
    TestRedisModule_FreeString(nullptr, key);
  }

  for (const KeyspaceEventSubscriptionTestCase &subscription :
       test_case.subscriptions) {
    VMSDK_EXPECT_OK(keyspace_event_manager->RemoveSubscription(
        mock_subscriptions[subscription.subscription_id].get()));
  }

  // Check everything is cleaned up. We should see no calls
  for (const KeyspaceEventNotificationTestCase &notification :
       test_case.notifications) {
    RedisModuleString *key = TestRedisModule_CreateStringPrintf(
        &fake_ctx_, "%s", notification.notification_key.data());
    keyspace_event_manager->NotifySubscribers(
        &fake_ctx_, notification.notification_type, event_name.data(), key);
    TestRedisModule_FreeString(nullptr, key);
  }
}

TEST_F(KeyspaceEventManagerTest, RemoveSubscriptionNotExists) {
  TestableKeyspaceEventManager test_keyspace_event_manager;
  EXPECT_EQ(test_keyspace_event_manager
                .RemoveSubscription((KeyspaceEventSubscription *)0xBAADF00D)
                .code(),
            absl::StatusCode::kNotFound);
}

INSTANTIATE_TEST_SUITE_P(
    KeyspaceEventManagerTests, KeyspaceEventManagerTest,
    testing::ValuesIn<KeyspaceEventManagerTestCase>({
        {
            .test_name = "single_match",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {"prefix:"},
                .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = REDISMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = REDISMODULE_NOTIFY_HASH,
                .expected_subscriptions_with_notifications =
                    {"subscription_id"},
            }},
        },
        {
            .test_name = "no_prefix_match",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {"prefix1:"},
                .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = REDISMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = REDISMODULE_NOTIFY_HASH,
                .expected_subscriptions_with_notifications = {},
            }},
        },
        {
            .test_name = "no_type_match",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {"prefix:"},
                .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = REDISMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = REDISMODULE_NOTIFY_EVICTED,
                .expected_subscriptions_with_notifications = {},
            }},
        },
        {
            .test_name = "empty_prefix",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {""},
                .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = REDISMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                                  .notification_key = "prefix:key",
                                  .notification_type = REDISMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id"},
                              },
                              {
                                  .notification_key = "different:key",
                                  .notification_type = REDISMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id"},
                              }},
        },
        {
            .test_name = "two_subscriptions_same_types",
            .subscriptions = {{
                                  .subscription_id = "subscription_id_0",
                                  .key_prefixes_to_subscribe = {"prefix:"},
                                  .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                                  .expected_type_subscriptions =
                                      REDISMODULE_NOTIFY_HASH,
                              },
                              {
                                  .subscription_id = "subscription_id_1",
                                  .key_prefixes_to_subscribe = {"prefix:"},
                                  .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                                  .expected_type_subscriptions = absl::nullopt,
                              }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = REDISMODULE_NOTIFY_HASH,
                .expected_subscriptions_with_notifications =
                    {"subscription_id_0", "subscription_id_1"},
            }},
        },
        {
            .test_name = "two_subscriptions_overlapping_types",
            .subscriptions =
                {{
                     .subscription_id = "subscription_id_0",
                     .key_prefixes_to_subscribe = {"prefix:"},
                     .types_to_subscribe = REDISMODULE_NOTIFY_HASH |
                                           REDISMODULE_NOTIFY_STREAM,
                     .expected_type_subscriptions = REDISMODULE_NOTIFY_HASH |
                                                    REDISMODULE_NOTIFY_STREAM,
                 },
                 {
                     .subscription_id = "subscription_id_1",
                     .key_prefixes_to_subscribe = {"prefix:"},
                     .types_to_subscribe = REDISMODULE_NOTIFY_HASH |
                                           REDISMODULE_NOTIFY_ZSET,
                     .expected_type_subscriptions = REDISMODULE_NOTIFY_ZSET,
                 }},
            .notifications =
                {{
                     .notification_key = "prefix:key",
                     .notification_type = REDISMODULE_NOTIFY_HASH,
                     .expected_subscriptions_with_notifications =
                         {"subscription_id_0", "subscription_id_1"},
                 },
                 {
                     .notification_key = "prefix:key",
                     .notification_type = REDISMODULE_NOTIFY_ZSET,
                     .expected_subscriptions_with_notifications =
                         {"subscription_id_1"},
                 },
                 {
                     .notification_key = "prefix:key",
                     .notification_type = REDISMODULE_NOTIFY_STREAM,
                     .expected_subscriptions_with_notifications =
                         {"subscription_id_0"},
                 }},
        },
        {
            .test_name = "two_subscriptions_prefix_partial_match",
            .subscriptions = {{
                                  .subscription_id = "subscription_id_0",
                                  .key_prefixes_to_subscribe = {"prefix1"},
                                  .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                                  .expected_type_subscriptions =
                                      REDISMODULE_NOTIFY_HASH,
                              },
                              {
                                  .subscription_id = "subscription_id_1",
                                  .key_prefixes_to_subscribe = {"prefix11"},
                                  .types_to_subscribe = REDISMODULE_NOTIFY_HASH,
                                  .expected_type_subscriptions = absl::nullopt,
                              }},
            .notifications = {{
                                  .notification_key = "prefix11:key",
                                  .notification_type = REDISMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id_0",
                                       "subscription_id_1"},
                              },
                              {
                                  .notification_key = "prefix1:key",
                                  .notification_type = REDISMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id_0"},
                              }},
        },
    }),
    [](const testing::TestParamInfo<KeyspaceEventManagerTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace valkey_search
