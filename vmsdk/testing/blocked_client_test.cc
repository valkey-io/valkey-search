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

std::vector<size_t> FetchTrackedBlockedClients(
    BlockedClientCategory category = BlockedClientCategory::kOther) {
  std::vector<size_t> tracked_bc_cnt;
  const auto &category_map = BlockedClientTracker::GetInstance()[category];
  for (auto &entry : category_map) {
    tracked_bc_cnt.push_back(entry.second.cnt);
  }
  return tracked_bc_cnt;
}

bool AreAllCategoriesEmpty() {
  return BlockedClientTracker::GetInstance().GetClientCount(
             BlockedClientCategory::kHash) == 0 &&
         BlockedClientTracker::GetInstance().GetClientCount(
             BlockedClientCategory::kJson) == 0 &&
         BlockedClientTracker::GetInstance().GetClientCount(
             BlockedClientCategory::kOther) == 0;
}

TEST_P(BlockedClientTest, EngineVersion) {
  const BlockedClientTestCase &test_case = GetParam();
  ValkeyModuleCtx fake_ctx;
  EXPECT_TRUE(AreAllCategoriesEmpty());
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
  EXPECT_TRUE(AreAllCategoriesEmpty());
}

// Test for category tracking
TEST_F(BlockedClientTest, CategoryTracking) {
  ValkeyModuleCtx fake_ctx;

  // Verify initial counts are zero
  EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                BlockedClientCategory::kHash),
            0);
  EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                BlockedClientCategory::kJson),
            0);
  EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                BlockedClientCategory::kOther),
            0);

  // Set up mock expectations
  EXPECT_CALL(*kMockValkeyModule, GetClientId(&fake_ctx))
      .WillOnce(testing::Return(1))
      .WillOnce(testing::Return(2))
      .WillOnce(testing::Return(3));

  EXPECT_CALL(*kMockValkeyModule,
              BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
      .WillOnce(testing::Return((ValkeyModuleBlockedClient *)0x1))
      .WillOnce(testing::Return((ValkeyModuleBlockedClient *)0x2))
      .WillOnce(testing::Return((ValkeyModuleBlockedClient *)0x3));

  EXPECT_CALL(*kMockValkeyModule, UnblockClient(testing::_, nullptr)).Times(3);

  {
    // Create clients with different categories
    BlockedClient hash_client(&fake_ctx, true, BlockedClientCategory::kHash);

    // Check hash client count increased
    EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                  BlockedClientCategory::kHash),
              1);
    EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                  BlockedClientCategory::kJson),
              0);
    EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                  BlockedClientCategory::kOther),
              0);

    {
      BlockedClient json_client(&fake_ctx, true, BlockedClientCategory::kJson);

      // Check json client count increased
      EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                    BlockedClientCategory::kHash),
                1);
      EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                    BlockedClientCategory::kJson),
                1);
      EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                    BlockedClientCategory::kOther),
                0);

      {
        BlockedClient other_client(&fake_ctx, true,
                                   BlockedClientCategory::kOther);

        // Check other client count increased
        EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                      BlockedClientCategory::kHash),
                  1);
        EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                      BlockedClientCategory::kJson),
                  1);
        EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                      BlockedClientCategory::kOther),
                  1);
      }

      // Check other client count decreased
      EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                    BlockedClientCategory::kHash),
                1);
      EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                    BlockedClientCategory::kJson),
                1);
      EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                    BlockedClientCategory::kOther),
                0);
    }

    // Check json client count decreased
    EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                  BlockedClientCategory::kHash),
              1);
    EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                  BlockedClientCategory::kJson),
              0);
    EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                  BlockedClientCategory::kOther),
              0);
  }

  // Check all counts are zero
  EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                BlockedClientCategory::kHash),
            0);
  EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                BlockedClientCategory::kJson),
            0);
  EXPECT_EQ(BlockedClientTracker::GetInstance().GetClientCount(
                BlockedClientCategory::kOther),
            0);
}

// Test for GetCategory
TEST_F(BlockedClientTest, GetCategory) {
  ValkeyModuleCtx fake_ctx;

  BlockedClient hash_client(&fake_ctx, false, BlockedClientCategory::kHash);
  EXPECT_EQ(hash_client.GetCategory(), BlockedClientCategory::kHash);

  BlockedClient json_client(&fake_ctx, false, BlockedClientCategory::kJson);
  EXPECT_EQ(json_client.GetCategory(), BlockedClientCategory::kJson);

  BlockedClient other_client(&fake_ctx, false, BlockedClientCategory::kOther);
  EXPECT_EQ(other_client.GetCategory(), BlockedClientCategory::kOther);

  // Test default category
  BlockedClient default_client(&fake_ctx, false);
  EXPECT_EQ(default_client.GetCategory(), BlockedClientCategory::kOther);
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
