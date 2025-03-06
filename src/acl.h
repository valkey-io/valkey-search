#ifndef VALKEYSEARCH_SRC_COMMANDS_ACL_H_
#define VALKEYSEARCH_SRC_COMMANDS_ACL_H_

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace acl {
struct ValkeyAclGetUserReplyView {
  absl::string_view cmds;
  absl::string_view keys;
};
}  // namespace acl

class AclManager {
 public:
  AclManager() = default;
  virtual ~AclManager() = default;

  static AclManager &Instance();
  static void InitInstance(std::unique_ptr<AclManager> instance);

  /*
  Check if
      * the user behind the connection in the context `ctx`,
      * can run a command with command categories defined in
  `module_allowed_cmds`, which are composed of commands and commands categories,
      * against ALL POSSBILE keys with key prefixes, defined as
  `module_prefixes`,
      * according to the ACL rules defined in the server.
  */
  absl::Status AclPrefixCheck(
      RedisModuleCtx *ctx,
      const std::unordered_set<absl::string_view> &module_allowed_cmds,
      const std::vector<std::string> &module_prefixes);

  absl::Status AclPrefixCheck(
      RedisModuleCtx *ctx,
      const std::unordered_set<absl::string_view> &module_allowed_cmds,
      const data_model::IndexSchema &index_schema_proto);
  /*
  Internally, we use RedisModule_Call API to get the ACL ruls from the server,
  i.e. "ACL GETUSER alice"

  The reply of RedisModule_Call API is complicated,
  composed of a map of strings, sets and lists (of other lists and maps nested)
  due to its underlying structure. See the Valkey ACL document for details.

  The reply, RedisModuleCallReply, is opaque and should be parsed with various
  RedisModule APIs. Unit testing with Mock APIs in this case is painful.
  Structuring the mock behaviors with arbitrary structured RedisModuleCallReply
  is doable but complicated.

  Also re-implementing the RedisModuleCallReply generator/decoder with mocks
  doesn't add much value to the unit tests, since there's no guarantee that the
  structure of the reply and the mock behaviors are exactly the same as the
  server's.

  Therefore, the reply decoding part is separated and will be tested by
  integration tests (in the future). The only separated logic part is unit
  tested. The function pointer, GetAclViewFromCallReply, is defined here to
  provide the test inputs.
  */
  virtual absl::StatusOr<std::vector<acl::ValkeyAclGetUserReplyView>>
  GetAclViewFromCallReply(RedisModuleCallReply *reply);

  friend absl::NoDestructor<AclManager>;
};

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_ACL_H_