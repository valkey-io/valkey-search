/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/blocked_client.h"

#include <memory>
#include <utility>

#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace {

class BlockedClientTest : public vmsdk::ValkeyTest {};

TEST_F(BlockedClientTest, SharedOwnerUnblocksWhenLastReferenceIsReleased) {
  ValkeyModuleCtx fake_ctx;
  auto *blocked_client = reinterpret_cast<ValkeyModuleBlockedClient *>(0x1);
  EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kHash),
            0);
  EXPECT_CALL(*kMockValkeyModule,
              BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
      .WillOnce(testing::Return(blocked_client));
  EXPECT_CALL(*kMockValkeyModule, UnblockClient(blocked_client, nullptr))
      .WillOnce(testing::Return(1));

  auto owner =
      std::make_shared<BlockedClient>(&fake_ctx, BlockedClientCategory::kHash);
  EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kHash),
            1);
  {
    auto additional_owner = owner;
    owner.reset();
    EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kHash),
              1);
  }

  EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kHash),
            0);
}

TEST_F(BlockedClientTest, FailedBlockDoesNotChangeActiveCount) {
  ValkeyModuleCtx fake_ctx;
  EXPECT_CALL(*kMockValkeyModule,
              BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
      .WillOnce(testing::Return(nullptr));
  EXPECT_CALL(*kMockValkeyModule, UnblockClient(testing::_, testing::_))
      .Times(0);

  {
    BlockedClient blocked_client(&fake_ctx, BlockedClientCategory::kJson);
    EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kJson),
              0);
  }

  EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kJson),
            0);
}

TEST_F(BlockedClientTest, MoveAssignmentReleasesOldHandleAndTakesNewHandle) {
  ValkeyModuleCtx fake_ctx;
  auto *first_handle = reinterpret_cast<ValkeyModuleBlockedClient *>(0x1);
  auto *second_handle = reinterpret_cast<ValkeyModuleBlockedClient *>(0x2);
  EXPECT_CALL(*kMockValkeyModule,
              BlockClient(&fake_ctx, nullptr, nullptr, nullptr, 0))
      .WillOnce(testing::Return(first_handle))
      .WillOnce(testing::Return(second_handle));
  EXPECT_CALL(*kMockValkeyModule, UnblockClient(first_handle, nullptr))
      .WillOnce(testing::Return(1));
  EXPECT_CALL(*kMockValkeyModule, UnblockClient(second_handle, nullptr))
      .WillOnce(testing::Return(1));

  {
    BlockedClient first(&fake_ctx, BlockedClientCategory::kHash);
    BlockedClient second(&fake_ctx, BlockedClientCategory::kJson);
    first = std::move(second);

    EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kHash),
              0);
    EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kJson),
              1);
  }

  EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kHash),
            0);
  EXPECT_EQ(BlockedClient::GetActiveClientCount(BlockedClientCategory::kJson),
            0);
}

}  // namespace
}  // namespace vmsdk
