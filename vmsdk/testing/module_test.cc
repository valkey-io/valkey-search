/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/testing_infra/module.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

namespace {
#define INSTALLED_MODULES 3

class ModuleTest : public vmsdk::ValkeyTest {
 protected:
  ValkeyModuleCtx fake_ctx;
  ValkeyModuleCallReply json_key;
  ValkeyModuleCallReply json_value;
  ValkeyModuleCallReply some_key;
  ValkeyModuleCallReply reply_internal[INSTALLED_MODULES];
  void InstallCheckers();
};

void ModuleTest::InstallCheckers() {
  auto reply = new ValkeyModuleCallReply;
  EXPECT_CALL(*kMockValkeyModule,
              Call(&fake_ctx, testing::StrEq("MODULE"), testing::StrEq("c"),
                   testing::StrEq("LIST")))
      .WillOnce([reply](ValkeyModuleCtx* ctx, const char* cmd, const char* fmt,
                        const char* arg1) -> ValkeyModuleCallReply* {
        return reply;
      });
  EXPECT_CALL(*kMockValkeyModule, FreeCallReply(reply))
      .WillOnce([](ValkeyModuleCallReply* reply) { delete reply; });

  EXPECT_CALL(*kMockValkeyModule, CallReplyType(testing::_))
      .WillRepeatedly([](ValkeyModuleCallReply* reply) {
        return VALKEYMODULE_REPLY_ARRAY;
      });
  EXPECT_CALL(*kMockValkeyModule, CallReplyLength(testing::_))
      .WillRepeatedly([reply](ValkeyModuleCallReply* in_reply) -> size_t {
        if (reply == in_reply) {
          return INSTALLED_MODULES;
        }
        return 10;
      });

  EXPECT_CALL(*kMockValkeyModule, CallReplyArrayElement(testing::_, testing::_))
      .WillRepeatedly([reply, this](ValkeyModuleCallReply* in_reply,
                                    size_t indx) -> ValkeyModuleCallReply* {
        if (reply == in_reply) {
          return &reply_internal[indx];
        }
        if (in_reply == &reply_internal[1]) {
          if (indx == 2) {
            return &json_key;
          } else if (indx == 3) {
            return &json_value;
          }
        }
        return &some_key;
      });

  EXPECT_CALL(*kMockValkeyModule, CallReplyStringPtr(testing::_, testing::_))
      .WillRepeatedly(
          [&](ValkeyModuleCallReply* in_reply, size_t* len) -> const char* {
            if (in_reply == &json_key) {
              static const std::string str("name");
              *len = str.length();
              return str.c_str();
            }
            if (in_reply == &json_value) {
              static const std::string str("json");
              *len = str.length();
              return str.c_str();
            }
            static const std::string str("a_module_field");
            *len = str.length();
            return str.c_str();
          });
}

TEST_F(ModuleTest, ModuleLoaded) {
  InstallCheckers();
  EXPECT_TRUE(vmsdk::IsModuleLoaded(&fake_ctx, "json"));
  EXPECT_TRUE(vmsdk::IsModuleLoaded(&fake_ctx, "json"));
}

TEST_F(ModuleTest, ModuleNotLoaded) {
  InstallCheckers();
  EXPECT_FALSE(vmsdk::IsModuleLoaded(&fake_ctx, "json11"));
}

}  // namespace

}  // namespace vmsdk
