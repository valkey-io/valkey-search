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

#include "vmsdk/src/utils.h"

#include <string>

#include "absl/synchronization/blocking_counter.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {

class UtilsTest : public vmsdk::RedisTest {};

TEST_F(UtilsTest, RunByMain) {
  absl::BlockingCounter blocking_refcount(1);
  ThreadPool thread_pool("test-pool", 1);
  thread_pool.StartWorkers();
  RedisModuleEventLoopOneShotFunc captured_callback;
  void *captured_data;
  EXPECT_CALL(*kMockRedisModule, EventLoopAddOneShot(testing::_, testing::_))
      .WillOnce([&](RedisModuleEventLoopOneShotFunc callback, void *data) {
        captured_callback = callback;
        captured_data = data;
        blocking_refcount.DecrementCount();
        return 0;
      });
  bool run = false;
  EXPECT_TRUE(thread_pool.Schedule(
      [&]() {
        RunByMain([&run] {
          EXPECT_TRUE(IsMainThread());
          run = true;
        });
      },
      ThreadPool::Priority::kLow));
  blocking_refcount.Wait();
  captured_callback(captured_data);
  EXPECT_TRUE(run);
  thread_pool.JoinWorkers();
}

TEST_F(UtilsTest, RunByMainWhileInMain) {
  absl::BlockingCounter blocking_refcount(1);
  EXPECT_CALL(*kMockRedisModule, EventLoopAddOneShot(testing::_, testing::_))
      .Times(0);
  bool run = false;
  RunByMain([&blocking_refcount, &run] {
    EXPECT_TRUE(IsMainThread());
    blocking_refcount.DecrementCount();
    run = true;
  });
  blocking_refcount.Wait();
  EXPECT_TRUE(run);
}

}  // namespace

}  // namespace vmsdk
