#ifndef VALKEYSEARCH_SRC_COMMANDS_ACL_H_
#define VALKEYSEARCH_SRC_COMMANDS_ACL_H_

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace acl {

struct ValkeyAclGetUserReplyView {
  absl::string_view cmds;
  absl::string_view keys;
};

/* Expose this function for tests */
bool StringEndsWithWildCardMatch(const char *pattern, int pattern_len,
                                 const char *string, int string_len);
}  // namespace acl

/*
Check if
    * the user behind the connection in the context `ctx`,
    * can run a command with command categories defined in
`module_allowed_cmds`, which are composed of commands and commands categories,
    * against ALL POSSIBLE keys with key prefixes, defined as
`module_prefixes`,
    * according to the ACL rules defined in the server.
*/
absl::Status AclPrefixCheck(
    ValkeyModuleCtx *ctx,
    const absl::flat_hash_set<absl::string_view> &module_allowed_cmds,
    const std::vector<std::string> &module_prefixes);

absl::Status AclPrefixCheck(
    ValkeyModuleCtx *ctx,
    const absl::flat_hash_set<absl::string_view> &module_allowed_cmds,
    const data_model::IndexSchema &index_schema_proto);

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_ACL_H_