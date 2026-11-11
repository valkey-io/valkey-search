/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/server_events.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_registry.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::server_events {
#ifndef TESTING_TMP_DISABLED
// SubscribesToServerEvents leads to segfault due to a call to pthread_atfork
class MockPthreadAtfork {
 public:
  MOCK_METHOD(int, pthread_atfork,
              (void (*__prepare)(), void (*__parent)(), void (*__child)()), ());
};
MockPthreadAtfork mock_pthread_atfork;

int pthread_atfork(void (*prepare)(), void (*parent)(), void (*child)()) {
  return mock_pthread_atfork.pthread_atfork(prepare, parent, child);
}
#endif  // TESTING_TMP_DISABLED

class ServerEventsTest : public ValkeySearchTest {};

TEST_F(ServerEventsTest, SubscribesToServerEvents) {
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_CronLoop),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_ForkChild),
          testing::_))
      .WillOnce(testing::Return(1));
#ifndef TESTING_TMP_DISABLED
  EXPECT_CALL(mock_pthread_atfork,
              pthread_atfork(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(1));
#endif  // TESTING_TMP_DISABLED
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_SwapDB),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_Loading),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_FlushDB),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_Shutdown),
          testing::_))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockValkeyModule,
              SubscribeToServerEvent(
                  testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_Key),
                  testing::_))
      .Times(0);
  SubscribeToServerEvents();
}

TEST_F(ServerEventsTest, SubscribesToKeyEventWhenSharingActive) {
  auto &enable_sharing =
      const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
  VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
  VectorRegistry::Construct(&registry_ctx_);

  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_CronLoop),
          testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_ForkChild),
          testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
#ifndef TESTING_TMP_DISABLED
  EXPECT_CALL(mock_pthread_atfork,
              pthread_atfork(testing::_, testing::_, testing::_))
      .WillOnce(testing::Return(0));
#endif  // TESTING_TMP_DISABLED
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_SwapDB),
          testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_Loading),
          testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_FlushDB),
          testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(
      *kMockValkeyModule,
      SubscribeToServerEvent(
          testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_Shutdown),
          testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  EXPECT_CALL(*kMockValkeyModule,
              SubscribeToServerEvent(
                  testing::_, vmsdk::IsValkeyModuleEvent(ValkeyModuleEvent_Key),
                  testing::_))
      .WillOnce(testing::Return(VALKEYMODULE_OK));
  SubscribeToServerEvents();
}

TEST_F(ServerEventsTest,
       OnKeyCallbackCleansUpExpiredEvictedAndOverwrittenKeys) {
  auto &enable_sharing =
      const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
  VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
  VectorRegistry::Construct(&registry_ctx_);
  auto &registry = VectorRegistry::Instance();

  auto attr = StringInternStore::Intern("vec");
  MockIndex index(2, attr->Str(), 0);
  std::vector<float> vec_data = {1.0f, 2.0f};
  auto valkey_vec = vmsdk::MakeUniqueValkeyString(
      std::string(reinterpret_cast<const char *>(vec_data.data()),
                  vec_data.size() * sizeof(float)));

  for (const char *name : {"k_exp", "k_evict", "k_overwrite", "k_del"}) {
    registry.DedupOrConstruct(
        StringInternStore::Intern(name), valkey_vec.get(),
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH, 0, &index);
  }
  ASSERT_EQ(registry.GetStats().entry_cnt, 4);

  EXPECT_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
      .WillRepeatedly(testing::Return(0));

  auto fire_key_event = [&](uint64_t subevent, const std::string &key_name) {
    ValkeyModuleKey mod_key{.ctx = &fake_ctx_, .key = key_name};
    ValkeyModuleKeyInfo key_info{
        .version = VALKEYMODULE_KEYINFO_VERSION,
        .key = &mod_key,
    };
    OnKeyCallback(&fake_ctx_, ValkeyModuleEvent_Key, subevent, &key_info);
  };

  // DELETED is ignored so RENAME/MOVE can move or unshare before cleanup.
  fire_key_event(VALKEYMODULE_SUBEVENT_KEY_DELETED, "k_del");
  EXPECT_EQ(registry.GetStats().entry_cnt, 4);

  fire_key_event(VALKEYMODULE_SUBEVENT_KEY_EXPIRED, "k_exp");
  EXPECT_EQ(registry.GetStats().entry_cnt, 3);

  fire_key_event(VALKEYMODULE_SUBEVENT_KEY_EVICTED, "k_evict");
  EXPECT_EQ(registry.GetStats().entry_cnt, 2);

  fire_key_event(VALKEYMODULE_SUBEVENT_KEY_OVERWRITTEN, "k_overwrite");
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);

  // Null guards on data / key_info->key do not crash or mutate remaining entry.
  OnKeyCallback(&fake_ctx_, ValkeyModuleEvent_Key,
                VALKEYMODULE_SUBEVENT_KEY_EXPIRED, nullptr);
  ValkeyModuleKeyInfo empty_info{.version = VALKEYMODULE_KEYINFO_VERSION,
                                 .key = nullptr};
  OnKeyCallback(&fake_ctx_, ValkeyModuleEvent_Key,
                VALKEYMODULE_SUBEVENT_KEY_EXPIRED, &empty_info);
  EXPECT_EQ(registry.GetStats().entry_cnt, 1);
}

}  // namespace valkey_search::server_events
