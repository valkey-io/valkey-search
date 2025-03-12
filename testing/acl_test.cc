

#include "src/acl.h"

#include "absl/log/log.h"
#include "gtest/gtest.h"
#include "testing/common.h"

namespace valkey_search::acl {

namespace {
using testing::TestParamInfo;
using testing::ValuesIn;

class AclPrefixCheckFuzzTest : public ValkeySearchTest {};

// This test is copied from valkey-io/valkey/blob/unstable/src/util.c
TEST_F(AclPrefixCheckFuzzTest, AclPrefixCheckTests) {
  char str[32];
  char pat[32];
  int cycles = 10000000;
  int total_matches = 0;
  while (cycles--) {
    int strlen = rand() % sizeof(str);
    int patlen = rand() % sizeof(pat);
    for (int j = 0; j < strlen; j++) {
      str[j] = rand() % 128;
    }
    for (int j = 0; j < patlen; j++) {
      pat[j] = rand() % 128;
    }
    total_matches += StringEndsWithWildCardMatch(pat, patlen, str, strlen);
  }

  LOG(INFO) << "AclPrefixCheck total matches: " << total_matches;
  // OK if not crashed
}

struct ValkeyAclGetUserOutput {
  std::string cmds;
  std::string keys;
};

struct AclPrefixCheckTestCase {
  std::string test_name;
  std::unordered_set<absl::string_view> module_allowed_commands;
  std::vector<std::string> prefixes;
  std::vector<ValkeyAclGetUserOutput> acls;
  absl::Status expected_return;
};

class AclPrefixCheckTest
    : public ValkeySearchTestWithParam<AclPrefixCheckTestCase> {};

RedisModuleCtx fake_ctx;

TEST_P(AclPrefixCheckTest, AclPrefixCheckTests) {
  const AclPrefixCheckTestCase &test_case = GetParam();

  EXPECT_CALL(*kMockRedisModule, GetCurrentUserName(testing::_))
      .WillOnce([](RedisModuleCtx *ctx) {
        return new RedisModuleString(std::string("alice"));
      });

  CallReplyMap reply_map;

  CallReplyArray flags;
  flags.emplace_back(CreateRedisModuleCallReply("on"));
  AddElementToCallReplyMap(reply_map, "flags", std::move(flags));

  CallReplyArray pass;
  pass.emplace_back(CreateRedisModuleCallReply("pass"));
  AddElementToCallReplyMap(reply_map, "passwords", std::move(pass));

  AddElementToCallReplyMap(reply_map, "commands", test_case.acls[0].cmds);
  AddElementToCallReplyMap(reply_map, "keys", test_case.acls[0].keys);

  AddElementToCallReplyMap(reply_map, "channels", "&");

  if (test_case.acls.size() > 1) {
    CallReplyArray selectors;
    for (int i = 1; i < test_case.acls.size(); i++) {
      CallReplyMap selector;
      AddElementToCallReplyMap(selector, "commands", test_case.acls[i].cmds);
      AddElementToCallReplyMap(selector, "keys", test_case.acls[i].keys);
      AddElementToCallReplyMap(selector, "channels", "&");
      selectors.emplace_back(CreateRedisModuleCallReply(std::move(selector)));
    }
    AddElementToCallReplyMap(reply_map, "selectors", std::move(selectors));
  } else {
    AddElementToCallReplyMap(reply_map, "selectors", nullptr);
  }
  std::unique_ptr<RedisModuleCallReply> reply =
      CreateRedisModuleCallReply(std::move(reply_map));

  EXPECT_CALL(*kMockRedisModule,
              Call(testing::_, testing::StrEq(std::string("ACL")),
                   testing::StrEq("cs3"), testing::StrEq("GETUSER"),
                   testing::StrEq("alice")))
      .WillOnce([&reply](RedisModuleCtx *ctx, const char *cmd, const char *fmt,
                         const char *arg1,
                         const char *arg2) { return (reply.get()); });
  EXPECT_EQ(test_case.expected_return,
            AclPrefixCheck(&fake_ctx_, test_case.module_allowed_commands,
                           test_case.prefixes));
}

INSTANTIATE_TEST_SUITE_P(
    AclPrefixCheckTests, AclPrefixCheckTest,
    ValuesIn<AclPrefixCheckTestCase>({
        {
            .test_name = "all_key",
            .module_allowed_commands = {"@search"},
            .prefixes = {},
            .acls =
                {
                    {
                        .cmds = "+@all",
                        .keys = "~*",
                    },
                },
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "all_key_alias",
            .module_allowed_commands = {"@search"},
            .prefixes = {},
            .acls = {{
                .cmds = "+@all",
                .keys = "allkeys",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "all_key_smaller",
            .module_allowed_commands = {"@search"},
            .prefixes = {},
            .acls = {{
                .cmds = "+@all",
                .keys = "~a*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user doesn't have a permission to execute a command"),
        },
        {
            .test_name = "same_key",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "resetkeys",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~* allkeys ~abc:* resetkeys",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user doesn't have a permission to execute a command"),
        },
        {
            .test_name = "resetkeys_same",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~* allkeys ~abc:* resetkeys ~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~a*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_question",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~a??:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_oneof",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~ab[abc]:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_ranged_oneof",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~ab[a-d]:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_negative_oneof",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~ab[^xyz]:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "wrongs",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc: ~xyz: ~xyz:* ~ab ~abcd ~abcd* ~abc:? ~a??? "
                        "~ab[xyz]:* ~ab[d-z]:* ~ab[^abc]:* %R~xyz:* %RW~xyz:* "
                        "%W~xyz:*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user doesn't have a permission to execute a command"),
        },
        {
            .test_name = "union_same_but_fail",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc:[ab]* ~abc:[^ab]*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user doesn't have a permission to execute a command"),
        },
        {
            .test_name = "readonly_same",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "%R~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "readwrite_same",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "%RW~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "writeonly_same",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "%W~abc:*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user doesn't have a permission to execute a command"),
        },
        {
            .test_name = "cmd_allowed",
            .module_allowed_commands = {"@search"},
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "-@all +@search",
                .keys = "allkeys",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "cmd_allowed_multiple_rules",
            .module_allowed_commands = {"@search", "@write"},
            .prefixes = {"abc:"},
            .acls = {{
                         .cmds = "-@all +@search",
                         .keys = "~xyz:*",
                     },
                     {
                         .cmds = "-@all +@write",
                         .keys = "~abc:*",
                     }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "cmd_allowed_one_command",
            .module_allowed_commands = {"@search", "@write", "FT.CREATE"},
            .prefixes = {"abc:"},
            .acls = {{
                         .cmds = "-@all +@search",
                         .keys = "~xyz:*",
                     },
                     {
                         .cmds = "-@all +@write",
                         .keys = "~xyz:*",
                     },
                     {
                         .cmds = "-@all +FT.CREATE",
                         .keys = "~abc:*",
                     }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "cmd_not_allowed",
            .module_allowed_commands = {"@search", "@write", "FT.CREATE"},
            .prefixes = {"abc:"},
            .acls =
                {
                    {
                        .cmds = "+@search +@write +FT.CREATE -@all",
                        .keys = "~abc:*",
                    },
                    {
                        .cmds = "+@all -@search",
                        .keys = "~abc:*",
                    },
                    {
                        .cmds = "+@all -FT.CREATE",
                        .keys = "~abc:*",
                    },
                    {
                        .cmds = "-@all",
                        .keys = "~abc:*",
                    },
                    {
                        .cmds = "-@all +@read",
                        .keys = "~abc:*",
                    },
                    {
                        .cmds = "-@all +FT.SEARCH",
                        .keys = "~abc:*",
                    },
                    {
                        .cmds = "-@all +@search +@write +FT.CREATE nocommands",
                        .keys = "~abc:*",
                    },
                },
            .expected_return = absl::PermissionDeniedError(
                "The user doesn't have a permission to execute a command"),
        },
    }),
    [](const TestParamInfo<AclPrefixCheckTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search::acl