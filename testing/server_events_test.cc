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
