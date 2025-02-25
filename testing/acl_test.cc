

#include "src/acl.h"

#include "gtest/gtest.h"
#include "testing/common.h"

namespace valkey_search::acl {

namespace {
using testing::TestParamInfo;
using testing::ValuesIn;

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

std::vector<ValkeyAclGetUserReplyView> GetAclViewsFromTest(
    const AclPrefixCheckTestCase &test_case) {
  std::vector<ValkeyAclGetUserReplyView> views;

  views.reserve(test_case.acls.size());
  for (const auto &acl : test_case.acls) {
    views.emplace_back(ValkeyAclGetUserReplyView{
        .cmds = absl::string_view(acl.cmds),
        .keys = absl::string_view(acl.keys),
    });
  }
  return views;
}

TEST_P(AclPrefixCheckTest, AclPrefixCheckTests) {
  const AclPrefixCheckTestCase &test_case = GetParam();
  auto acl = std::make_unique<TestableAclManager>();
  auto acl_views = GetAclViewsFromTest(test_case);
  acl->SetAclViews(&acl_views);
  AclManager::InitInstance(std::move(acl));

  EXPECT_EQ(
      test_case.expected_return,
      AclManager::Instance().AclPrefixCheck(
          &fake_ctx_, test_case.module_allowed_commands, test_case.prefixes));
}

INSTANTIATE_TEST_SUITE_P(
    AclPrefixCheckTests, AclPrefixCheckTest,
    ValuesIn<AclPrefixCheckTestCase>({
        {
            .test_name = "all_key",
            .module_allowed_commands = {"@search"},
            .prefixes = {},
            .acls = {{
                .cmds = "+@all",
                .keys = "~*",
            }},
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