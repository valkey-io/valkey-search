

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
  absl::string_view access;
  std::vector<std::string> prefixes;
  std::vector<ValkeyAclGetUserOutput> acls;
  absl::Status expected_return;
};

class AclPrefixCheckTest
    : public ValkeySearchTestWithParam<AclPrefixCheckTestCase> {};

ValkeyModuleCtx fake_ctx;

TEST_P(AclPrefixCheckTest, AclPrefixCheckTests) {
  const AclPrefixCheckTestCase &test_case = GetParam();

  EXPECT_CALL(*kMockValkeyModule, GetCurrentUserName(testing::_))
      .WillOnce([](ValkeyModuleCtx *ctx) {
        return new ValkeyModuleString(std::string("alice"));
      });

  EXPECT_CALL(*kMockValkeyModule, GetClientId(testing::_))
      .WillOnce([](ValkeyModuleCtx *ctx) { return 3; });

  CallReplyMap reply_map;

  CallReplyArray flags;
  flags.emplace_back(CreateValkeyModuleCallReply("on"));
  AddElementToCallReplyMap(reply_map, "flags", std::move(flags));

  CallReplyArray pass;
  pass.emplace_back(CreateValkeyModuleCallReply("pass"));
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
      selectors.emplace_back(CreateValkeyModuleCallReply(std::move(selector)));
    }
    AddElementToCallReplyMap(reply_map, "selectors", std::move(selectors));
  } else {
    AddElementToCallReplyMap(reply_map, "selectors", nullptr);
  }
  std::unique_ptr<ValkeyModuleCallReply> reply =
      CreateValkeyModuleCallReply(std::move(reply_map));

  EXPECT_CALL(*kMockValkeyModule,
              Call(testing::_, testing::StrEq(std::string("ACL")),
                   testing::StrEq("cs3"), testing::StrEq("GETUSER"),
                   testing::StrEq("alice")))
      .WillOnce([&reply](ValkeyModuleCtx *ctx, const char *cmd, const char *fmt,
                         const char *arg1,
                         const char *arg2) { return (reply.get()); });
  EXPECT_EQ(test_case.expected_return,
            AclPrefixCheck(&fake_ctx_, test_case.access, test_case.prefixes));
}

INSTANTIATE_TEST_SUITE_P(
    AclPrefixCheckTests, AclPrefixCheckTest,
    ValuesIn<AclPrefixCheckTestCase>({
        {
            .test_name = "all_key",
            .access = "@write",
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
            .access = "@write",
            .prefixes = {},
            .acls = {{
                .cmds = "+@all",
                .keys = "allkeys",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "all_key_smaller",
            .access = "@write",
            .prefixes = {},
            .acls = {{
                .cmds = "+@all",
                .keys = "~a*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user does not have permission to access the key prefix"),
        },
        {
            .test_name = "same_key",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "resetkeys",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~* allkeys ~abc:* resetkeys",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user does not have permission to access the key prefix"),
        },
        {
            .test_name = "resetkeys_same",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~* allkeys ~abc:* resetkeys ~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~a*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_question",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~a??:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_oneof",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~ab[abc]:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_ranged_oneof",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~ab[a-d]:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "bigger_key_negative_oneof",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~ab[^xyz]:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "wrongs",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc: ~xyz: ~xyz:* ~ab ~abcd ~abcd* ~abc:? ~a??? "
                        "~ab[xyz]:* ~ab[d-z]:* ~ab[^abc]:* %R~xyz:* %RW~xyz:* "
                        "%W~xyz:*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user does not have permission to access the key prefix"),
        },
        {
            .test_name = "union_same_but_fail",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc:[ab]* ~abc:[^ab]*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user does not have permission to access the key prefix"),
        },
        {
            .test_name = "readonly_same",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "%R~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "readwrite_same",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "%RW~abc:*",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "writeonly_same",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "%W~abc:*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user does not have permission to access the key prefix"),
        },
        {
            .test_name = "cmd_allowed",
            .access = "@write",
            .prefixes = {"abc:"},
            .acls = {{
                .cmds = "-@all +@search",
                .keys = "allkeys",
            }},
            .expected_return = absl::OkStatus(),
        },
        {
            .test_name = "cmd_allowed_multiple_rules",
            .access = "@write",
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
            .access = "@write",
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
            .test_name = "several_prefixes_allowed_only_one",
            .access = "@write",
            .prefixes = {"abc:", "xyz:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc:*",
            }},
            .expected_return = absl::PermissionDeniedError(
                "The user does not have permission to access the key prefix"),
        },
        {
            .test_name = "several_prefixes_allowed_all",
            .access = "@write",
            .prefixes = {"abc:", "xyz:"},
            .acls = {{
                .cmds = "+@all",
                .keys = "~abc:* ~xyz:*",
            }},
            .expected_return = absl::OkStatus(),
        },
    }),
    [](const TestParamInfo<AclPrefixCheckTestCase> &info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search::acl