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

#include "vmsdk/src/blocked_client.h"

#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {
struct BlockedClientTestCase {
  std::string test_name;
  size_t client_id_cnt;
  std::vector<size_t> tracked_blocked_clients;
  bool use_same_client_id{false};
};

class BlockedClientTest
    : public vmsdk::RedisTestWithParam<BlockedClientTestCase> {
 protected:
};

std::vector<size_t> FetchTrackedBlockedClients() {
  std::vector<size_t> tracked_bc_cnt;
  for (auto &entry : TrackedBlockedClients()) {
    tracked_bc_cnt.push_back(entry.second.cnt);
  }
  return tracked_bc_cnt;
}

TEST_P(BlockedClientTest, EngineVersion) {
  const BlockedClientTestCase &test_case = GetParam();
  RedisModuleCtx fake_ctx;
  EXPECT_TRUE(FetchTrackedBlockedClients().empty());
  std::vector<unsigned long long> client_ids(test_case.client_id_cnt);
  std::vector<RedisModuleBlockedClient> bc_ptr(test_case.client_id_cnt);
  {
    std::vector<BlockedClient> blocked_clients;
    if (test_case.tracked_blocked_clients.empty()) {
      EXPECT_CALL(*kMockRedisModule, UnblockClient(testing::_, nullptr))
          .Times(0);
    } else {
      for (size_t i = 0; i < test_case.client_id_cnt; ++i) {
        if (i == 0 || !test_case.use_same_client_id) {
          EXPECT_CALL(*kMockRedisModule, UnblockClient(&bc_ptr[i], nullptr))
              .Times(1);
        }
      }
    }
    for (size_t i = 0; i < test_case.client_id_cnt; ++i) {
      auto ctx = test_case.use_same_client_id ? &client_ids[0] : &client_ids[i];
      if (test_case.tracked_blocked_clients.empty()) {
        EXPECT_CALL(*kMockRedisModule,
                    BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
            .Times(0);

      } else {
        if (test_case.use_same_client_id) {
          EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
              .WillOnce(testing::Return(1));
        } else {
          EXPECT_CALL(*kMockRedisModule, GetClientId(&fake_ctx))
              .WillOnce(testing::Return(i + 1));
        }
        if (i == 0 || !test_case.use_same_client_id) {
          EXPECT_CALL(*kMockRedisModule,
                      BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
              .WillOnce(
                  [&bc_ptr, i](RedisModuleCtx *ctx,
                               RedisModuleCmdFunc reply_callback,
                               RedisModuleCmdFunc timeout_callback,
                               void (*free_privdata)(RedisModuleCtx *, void *),
                               long long timeout_ms) { return &bc_ptr[i]; });
        }
      }
      BlockedClient bc(&fake_ctx, true);
      blocked_clients.emplace_back(std::move(bc));
    }
    auto tracked_bc_cnt = FetchTrackedBlockedClients();
    EXPECT_EQ(tracked_bc_cnt, test_case.tracked_blocked_clients);
  }
  EXPECT_TRUE(FetchTrackedBlockedClients().empty());
}

INSTANTIATE_TEST_SUITE_P(
    BlockedClientTests, BlockedClientTest,
    testing::ValuesIn<BlockedClientTestCase>(
        {{
             .test_name = "happy_path_1",
             .client_id_cnt = 1,
             .tracked_blocked_clients = {1},

         },
         {
             .test_name = "happy_path_2",
             .client_id_cnt = 1,
             .tracked_blocked_clients = {1},

         },
         {
             .test_name = "two_blocked_clients",
             .client_id_cnt = 2,
             .tracked_blocked_clients = {1, 1},

         },
         {
             .test_name = "two_blocked_clients_same",
             .client_id_cnt = 2,
             .tracked_blocked_clients = {2},
             .use_same_client_id = true,

         }}),
    [](const testing::TestParamInfo<BlockedClientTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace

}  // namespace vmsdk
