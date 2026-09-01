/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
          *kMockValkeyModule,
          SubscribeToKeyspaceEvents(
              &fake_ctx_, subscription.expected_type_subscriptions.value(), _))
          .WillOnce(Return(VALKEYMODULE_OK));
    }
    auto mock_subscription = std::make_unique<MockKeyspaceEventSubscription>();
    auto mock_attribute_data_type = std::make_unique<MockAttributeDataType>();
    EXPECT_CALL(*mock_attribute_data_type, GetValkeyEventTypes())
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
    ValkeyModuleString *key = TestValkeyModule_CreateStringPrintf(
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
    TestValkeyModule_FreeString(nullptr, key);
  }

  for (const KeyspaceEventSubscriptionTestCase &subscription :
       test_case.subscriptions) {
    VMSDK_EXPECT_OK(keyspace_event_manager->RemoveSubscription(
        mock_subscriptions[subscription.subscription_id].get()));
  }

  // Check everything is cleaned up. We should see no calls
  for (const KeyspaceEventNotificationTestCase &notification :
       test_case.notifications) {
    ValkeyModuleString *key = TestValkeyModule_CreateStringPrintf(
        &fake_ctx_, "%s", notification.notification_key.data());
    keyspace_event_manager->NotifySubscribers(
        &fake_ctx_, notification.notification_type, event_name.data(), key);
    TestValkeyModule_FreeString(nullptr, key);
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
                .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = VALKEYMODULE_NOTIFY_HASH,
                .expected_subscriptions_with_notifications =
                    {"subscription_id"},
            }},
        },
        {
            .test_name = "no_prefix_match",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {"prefix1:"},
                .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = VALKEYMODULE_NOTIFY_HASH,
                .expected_subscriptions_with_notifications = {},
            }},
        },
        {
            .test_name = "no_type_match",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {"prefix:"},
                .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = VALKEYMODULE_NOTIFY_EVICTED,
                .expected_subscriptions_with_notifications = {},
            }},
        },
        {
            .test_name = "empty_prefix",
            .subscriptions = {{
                .subscription_id = "subscription_id",
                .key_prefixes_to_subscribe = {""},
                .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH,
            }},
            .notifications = {{
                                  .notification_key = "prefix:key",
                                  .notification_type = VALKEYMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id"},
                              },
                              {
                                  .notification_key = "different:key",
                                  .notification_type = VALKEYMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id"},
                              }},
        },
        {
            .test_name = "two_subscriptions_same_types",
            .subscriptions =
                {{
                     .subscription_id = "subscription_id_0",
                     .key_prefixes_to_subscribe = {"prefix:"},
                     .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                     .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH,
                 },
                 {
                     .subscription_id = "subscription_id_1",
                     .key_prefixes_to_subscribe = {"prefix:"},
                     .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                     .expected_type_subscriptions = absl::nullopt,
                 }},
            .notifications = {{
                .notification_key = "prefix:key",
                .notification_type = VALKEYMODULE_NOTIFY_HASH,
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
                     .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH |
                                           VALKEYMODULE_NOTIFY_STREAM,
                     .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH |
                                                    VALKEYMODULE_NOTIFY_STREAM,
                 },
                 {
                     .subscription_id = "subscription_id_1",
                     .key_prefixes_to_subscribe = {"prefix:"},
                     .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH |
                                           VALKEYMODULE_NOTIFY_ZSET,
                     .expected_type_subscriptions = VALKEYMODULE_NOTIFY_ZSET,
                 }},
            .notifications =
                {{
                     .notification_key = "prefix:key",
                     .notification_type = VALKEYMODULE_NOTIFY_HASH,
                     .expected_subscriptions_with_notifications =
                         {"subscription_id_0", "subscription_id_1"},
                 },
                 {
                     .notification_key = "prefix:key",
                     .notification_type = VALKEYMODULE_NOTIFY_ZSET,
                     .expected_subscriptions_with_notifications =
                         {"subscription_id_1"},
                 },
                 {
                     .notification_key = "prefix:key",
                     .notification_type = VALKEYMODULE_NOTIFY_STREAM,
                     .expected_subscriptions_with_notifications =
                         {"subscription_id_0"},
                 }},
        },
        {
            .test_name = "two_subscriptions_prefix_partial_match",
            .subscriptions =
                {{
                     .subscription_id = "subscription_id_0",
                     .key_prefixes_to_subscribe = {"prefix1"},
                     .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                     .expected_type_subscriptions = VALKEYMODULE_NOTIFY_HASH,
                 },
                 {
                     .subscription_id = "subscription_id_1",
                     .key_prefixes_to_subscribe = {"prefix11"},
                     .types_to_subscribe = VALKEYMODULE_NOTIFY_HASH,
                     .expected_type_subscriptions = absl::nullopt,
                 }},
            .notifications = {{
                                  .notification_key = "prefix11:key",
                                  .notification_type = VALKEYMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id_0",
                                       "subscription_id_1"},
                              },
                              {
                                  .notification_key = "prefix1:key",
                                  .notification_type = VALKEYMODULE_NOTIFY_HASH,
                                  .expected_subscriptions_with_notifications =
                                      {"subscription_id_0"},
                              }},
        },
    }),
    [](const testing::TestParamInfo<KeyspaceEventManagerTestCase> &info) {
      return info.param.test_name;
    });

TEST_F(KeyspaceEventManagerTest, RenameLifecycleMatchingSubscribers) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_subscription = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attribute_data_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes = {"doc:"};
  EXPECT_CALL(*mock_attribute_data_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_subscription, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attribute_data_type));
  EXPECT_CALL(*mock_subscription, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes));

  MockIndex mock_index(2, "vec", 0);
  mock_subscription->vector_indexes_ = {&mock_index};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_subscription.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:2");

  // Step 1: rename_from should not notify subscriber yet
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  // Step 2: rename_to should notify src first with rename_from, then dst with
  // rename_to
  testing::InSequence seq;
  EXPECT_CALL(*mock_subscription,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_subscription,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);

  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_subscription.get()));
}

TEST_F(KeyspaceEventManagerTest, RenameLifecycleCrossDatabaseMove) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_db0 = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_db1 = std::make_unique<MockKeyspaceEventSubscription>();
  mock_sub_db0->db_num_ = 0;
  mock_sub_db1->db_num_ = 1;
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes = {"doc:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_db0, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_db0, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes));
  EXPECT_CALL(*mock_sub_db1, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_db1, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes));

  MockIndex mock_index_db0(2, "vec", 0);
  MockIndex mock_index_db1(2, "vec", 1);
  mock_sub_db0->vector_indexes_ = {&mock_index_db0};
  mock_sub_db1->vector_indexes_ = {&mock_index_db1};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_db0.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_db1.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:2");

  // rename_from on DB 0
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillOnce(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  // rename_to on DB 1
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 0))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*mock_sub_db0,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 1))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*mock_sub_db0,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_db1,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);

  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_db0.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_db1.get()));
}

TEST_F(KeyspaceEventManagerTest, MoveLifecycleCrossDatabaseMove) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_db0 = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_db1 = std::make_unique<MockKeyspaceEventSubscription>();
  mock_sub_db0->db_num_ = 0;
  mock_sub_db1->db_num_ = 1;
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes = {"doc:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_db0, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_db0, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes));
  EXPECT_CALL(*mock_sub_db1, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_db1, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes));

  MockIndex mock_index_db0(2, "vec", 0);
  MockIndex mock_index_db1(2, "vec", 1);
  mock_sub_db0->vector_indexes_ = {&mock_index_db0};
  mock_sub_db1->vector_indexes_ = {&mock_index_db1};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_db0.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_db1.get()));

  ValkeyModuleString *key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:1");

  // move_from on DB 0
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillOnce(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "move_from", key);

  // move_to on DB 1
  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(1));
  EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 0))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*mock_sub_db0,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("move_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*kMockValkeyModule, SelectDb(&fake_ctx_, 1))
      .WillOnce(Return(VALKEYMODULE_OK));
  EXPECT_CALL(*mock_sub_db0,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("move_to"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_db1,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("move_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "move_to", key);

  TestValkeyModule_FreeString(nullptr, key);

  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_db0.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_db1.get()));
}

TEST_F(KeyspaceEventManagerTest, RenameDimensionMismatchRetainsUnshareAction) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_src = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_dst = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_src = {"src:"};
  std::vector<std::string> prefixes_dst = {"dst:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_src, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));
  EXPECT_CALL(*mock_sub_dst, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_dst, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_dst));

  MockIndex mock_index_src(2, "vec", 0);
  MockIndex mock_index_dst(4, "vec", 0);  // Dimension mismatch (4 vs 2)
  mock_sub_src->vector_indexes_ = {&mock_index_src};
  mock_sub_dst->vector_indexes_ = {&mock_index_dst};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_dst.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "src:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "dst:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_src,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_dst,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_dst.get()));
}

TEST_F(KeyspaceEventManagerTest,
       RenameAttributeIdentifierMismatchRetainsUnshareAction) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_src = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_dst = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_src = {"src:"};
  std::vector<std::string> prefixes_dst = {"dst:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_src, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));
  EXPECT_CALL(*mock_sub_dst, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_dst, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_dst));

  MockIndex mock_index_src(2, "vec_a", 0);
  MockIndex mock_index_dst(2, "vec_b", 0);  // Attribute name mismatch
  mock_sub_src->vector_indexes_ = {&mock_index_src};
  mock_sub_dst->vector_indexes_ = {&mock_index_dst};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_dst.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "src:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "dst:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_src,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_dst,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_dst.get()));
}

TEST_F(KeyspaceEventManagerTest, RenameMultiAttributeMixedMoveAndUnshare) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_src = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_dst = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_src = {"src:"};
  std::vector<std::string> prefixes_dst = {"dst:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_src, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));
  EXPECT_CALL(*mock_sub_dst, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_dst, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_dst));

  MockIndex mock_index_src1(2, "vec1", 0);
  MockIndex mock_index_src2(4, "vec2", 0);
  MockIndex mock_index_dst1(2, "vec1", 0);  // dst only has vec1
  mock_sub_src->vector_indexes_ = {&mock_index_src1, &mock_index_src2};
  mock_sub_dst->vector_indexes_ = {&mock_index_dst1};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_dst.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "src:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "dst:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_src,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_dst,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_dst.get()));
}

TEST_F(KeyspaceEventManagerTest,
       RenameMultipleSourceSchemasDeduplicatesAttributes) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_src1 = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_src2 = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_dst = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_src = {"doc:"};
  std::vector<std::string> prefixes_dst = {"dest:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_src1, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src1, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));
  EXPECT_CALL(*mock_sub_src2, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src2, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));
  EXPECT_CALL(*mock_sub_dst, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_dst, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_dst));

  MockIndex mock_index_src1(2, "vec", 0);
  MockIndex mock_index_src2(2, "vec", 0);
  MockIndex mock_index_dst(2, "vec", 0);
  mock_sub_src1->vector_indexes_ = {&mock_index_src1};
  mock_sub_src2->vector_indexes_ = {&mock_index_src2};
  mock_sub_dst->vector_indexes_ = {&mock_index_dst};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src1.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src2.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_dst.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "dest:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_src1,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_src2,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_dst,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src1.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src2.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_dst.get()));
}

TEST_F(KeyspaceEventManagerTest, RenameToUnindexedDestination) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_src = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_src = {"doc:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_src, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));

  MockIndex mock_index_src(2, "vec", 0);
  mock_sub_src->vector_indexes_ = {&mock_index_src};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "outside:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_src,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src.get()));
}

TEST_F(KeyspaceEventManagerTest, RenameFromUnindexedSource) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_dst = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_dst = {"doc:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_dst, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_dst, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_dst));

  MockIndex mock_index_dst(2, "vec", 0);
  mock_sub_dst->vector_indexes_ = {&mock_index_dst};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_dst.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "outside:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "doc:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_dst,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_dst.get()));
}

TEST_F(KeyspaceEventManagerTest, RenameToKeyWithIncompatibleType) {
  auto keyspace_event_manager =
      std::make_unique<TestableKeyspaceEventManager>();
  auto mock_sub_src = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_sub_dst = std::make_unique<MockKeyspaceEventSubscription>();
  auto mock_attr_type = std::make_unique<MockAttributeDataType>();

  std::vector<std::string> prefixes_src = {"src:"};
  std::vector<std::string> prefixes_dst = {"dst:"};
  EXPECT_CALL(*mock_attr_type, GetValkeyEventTypes())
      .WillRepeatedly(
          Return(VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC));
  EXPECT_CALL(*mock_sub_src, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_src, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_src));
  EXPECT_CALL(*mock_sub_dst, GetAttributeDataType())
      .WillRepeatedly(ReturnRef(*mock_attr_type));
  EXPECT_CALL(*mock_sub_dst, GetKeyPrefixes())
      .WillRepeatedly(ReturnRef(prefixes_dst));

  // Destination key type is incompatible (IsProperType returns false)
  EXPECT_CALL(*mock_attr_type, IsProperType(testing::_))
      .WillRepeatedly(Return(false));

  MockIndex mock_index_src(2, "vec", 0);
  MockIndex mock_index_dst(2, "vec", 0);
  mock_sub_src->vector_indexes_ = {&mock_index_src};
  mock_sub_dst->vector_indexes_ = {&mock_index_dst};

  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_src.get()));
  VMSDK_EXPECT_OK(keyspace_event_manager->InsertSubscription(
      &fake_ctx_, mock_sub_dst.get()));

  ValkeyModuleString *src_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "src:1");
  ValkeyModuleString *dst_key =
      TestValkeyModule_CreateStringPrintf(&fake_ctx_, "dst:1");

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(Return(0));
  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_from", src_key);

  EXPECT_CALL(*mock_sub_src,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_from"), testing::_))
      .WillOnce(Return());
  EXPECT_CALL(*mock_sub_dst,
              OnKeyspaceNotification(&fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC,
                                     testing::StrEq("rename_to"), testing::_))
      .WillOnce(Return());

  keyspace_event_manager->NotifySubscribers(
      &fake_ctx_, VALKEYMODULE_NOTIFY_GENERIC, "rename_to", dst_key);

  TestValkeyModule_FreeString(nullptr, src_key);
  TestValkeyModule_FreeString(nullptr, dst_key);
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_src.get()));
  VMSDK_EXPECT_OK(
      keyspace_event_manager->RemoveSubscription(mock_sub_dst.get()));
}
}  // namespace

}  // namespace valkey_search
