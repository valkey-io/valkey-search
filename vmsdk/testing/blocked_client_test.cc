/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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
    : public vmsdk::ValkeyTestWithParam<BlockedClientTestCase> {
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
  ValkeyModuleCtx fake_ctx;
  EXPECT_TRUE(FetchTrackedBlockedClients().empty());
  std::vector<unsigned long long> client_ids(test_case.client_id_cnt);
  std::vector<ValkeyModuleBlockedClient> bc_ptr(test_case.client_id_cnt);
  {
    std::vector<BlockedClient> blocked_clients;
    if (test_case.tracked_blocked_clients.empty()) {
      EXPECT_CALL(*kMockValkeyModule, UnblockClient(testing::_, nullptr))
          .Times(0);
    } else {
      for (size_t i = 0; i < test_case.client_id_cnt; ++i) {
        if (i == 0 || !test_case.use_same_client_id) {
          EXPECT_CALL(*kMockValkeyModule, UnblockClient(&bc_ptr[i], nullptr))
              .Times(1);
        }
      }
    }
    for (size_t i = 0; i < test_case.client_id_cnt; ++i) {
      auto ctx = test_case.use_same_client_id ? &client_ids[0] : &client_ids[i];
      if (test_case.tracked_blocked_clients.empty()) {
        EXPECT_CALL(*kMockValkeyModule,
                    BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
            .Times(0);

      } else {
        if (test_case.use_same_client_id) {
          EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
              .WillOnce(testing::Return(1));
        } else {
          EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
              .WillOnce(testing::Return(i + 1));
        }
        if (i == 0 || !test_case.use_same_client_id) {
          EXPECT_CALL(*kMockValkeyModule,
                      BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
              .WillOnce(
                  [&bc_ptr, i](ValkeyModuleCtx *ctx,
                               ValkeyModuleCmdFunc reply_callback,
                               ValkeyModuleCmdFunc timeout_callback,
                               void (*free_privdata)(ValkeyModuleCtx *, void *),
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
