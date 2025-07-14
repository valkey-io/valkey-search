/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/server_events.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "testing/common.h"
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
  SubscribeToServerEvents();
}

}  // namespace valkey_search::server_events
