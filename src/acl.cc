#include "src/acl.h"

#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace acl {
/* This pattern matching code mostly came from valkey/src/util.c,
 with some modifications for a pattern-to-pattern match, not a pattern-to-string
 match. It returns true when (1) the pattern matches the string, AND (2) the
 pattern ends with wildcards.
 */
bool StringEndsWithWildCardMatch(const char *pattern, int pattern_len,
                                 const char *string, int string_len,
                                 bool nocase, bool *skip_longer_matches) {
  while (pattern_len && string_len) {
    switch (pattern[0]) {
      case '*':
        while (pattern_len >= 2 && pattern[1] == '*') {
          pattern++;
          pattern_len--;
        }
        if (pattern_len == 1) {
          return true; /* match */
        }
        while (string_len) {
          if (StringEndsWithWildCardMatch(pattern + 1, pattern_len - 1, string,
                                          string_len, nocase,
                                          skip_longer_matches)) {
            return true; /* match */
          }
          if (*skip_longer_matches) {
            return false; /* no match */
          }
          string++;
          string_len--;
        }
        /* There was no match for the rest of the pattern starting
         * from anywhere in the rest of the string. If there were
         * any '*' earlier in the pattern, we can terminate the
         * search early without trying to match them to longer
         * substrings. This is because a longer match for the
         * earlier part of the pattern would require the rest of the
         * pattern to match starting later in the string, and we
         * have just determined that there is no match for the rest
         * of the pattern starting from anywhere in the current
         * string. */
        *skip_longer_matches = true;
        return false; /* no match */
        break;
      case '?':
        string++;
        string_len--;
        break;
      case '[': {
        bool negate, match;

        match = false;
        if (pattern_len == 1) {
          break;
        }

        pattern++;
        pattern_len--;
        negate = pattern[0] == '^';
        if (negate) {
          if (pattern_len == 1) { /* [^$ is invalid syntax */
            return false;
          }
          pattern++;
          pattern_len--;
        }
        while (1) {
          if (pattern_len >= 2 && pattern[0] == '\\') {
            pattern++;
            pattern_len--;
            if (pattern[0] == string[0]) {
              match = true;
            }
          } else if (pattern[0] == ']') {
            break;
          } else if (pattern_len == 0) {
            pattern--;
            pattern_len++;
            break;
          } else if (pattern_len >= 3 && pattern[1] == '-') {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            pattern_len -= 2;
            if (c >= start && c <= end) {
              match = true;
            }
          } else {
            if (!nocase) {
              if (pattern[0] == string[0]) {
                match = true;
              }
            } else {
              if (tolower((int)pattern[0]) == tolower((int)string[0])) {
                match = true;
              }
            }
          }
          pattern++;
          pattern_len--;
        }
        if (negate) {
          match = !match;
        }
        if (!match) {
          return false; /* no match */
        }
        string++;
        string_len--;
        break;
      }
      case '\\':
        if (pattern_len >= 2) {
          pattern++;
          pattern_len--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0]) {
            return false; /* no match */
          }
        } else {
          if (tolower((int)pattern[0]) != tolower((int)string[0])) {
            return false; /* no match */
          }
        }
        string++;
        string_len--;
        break;
    }
    pattern++;
    pattern_len--;
  }
  if (string_len == 0 && pattern_len > 0) {
    while (*pattern == '*') {
      if (pattern_len == 1) {
        return true;
      }
      pattern++;
      pattern_len--;
    }
  }
  return false;
}

bool StringEndsWithWildCardMatch(const char *pattern, int pattern_len,
                                 const char *string, int string_len) {
  bool skip_longer_matches = false;
  return StringEndsWithWildCardMatch(pattern, pattern_len, string, string_len,
                                     false, &skip_longer_matches);
}

}  // namespace acl

namespace {

bool IsAllowedCommands(
    const absl::flat_hash_set<absl::string_view> &module_allowed_cmds,
    absl::string_view acl_cmds) {
  bool allowed = false;

  for (const auto acl_cmd : absl::StrSplit(acl_cmds, ' ', absl::SkipEmpty())) {
    auto cmd = absl::string_view(acl_cmd.data(), acl_cmd.size());
    if (absl::EqualsIgnoreCase(cmd, "+@all") ||
        absl::EqualsIgnoreCase(cmd, "allcommands")) {
      allowed = true;
    } else if (absl::EqualsIgnoreCase(cmd, "-@all") ||
               absl::EqualsIgnoreCase(cmd, "nocommands")) {
      allowed = false;
    } else if (!acl_cmd.empty() &&
               module_allowed_cmds.contains(
                   absl::string_view(acl_cmd.data() + 1, acl_cmd.size() - 1))) {
      if (cmd.starts_with('+')) {
        allowed = true;
      } else if (cmd.starts_with('-')) {
        allowed = false;
      } else {
        // This should not happen. All ACL command expression starts with +, -,
        // or all/no command alias.
        CHECK(false);
      }
    }
  }
  return allowed;
}

absl::string_view CallReplyStringToStringView(RedisModuleCallReply *r) {
  size_t l;
  const char *r_c_str = RedisModule_CallReplyStringPtr(r, &l);
  return {r_c_str, l};
}

/*
The ACL rules provided by the Valkey Server ACL GETUSER command, are a list of
rules contain commands and key patterns, which describe if the commands can run
against the key patterns. We only need the key patterns that the corresponding
commands matches our module commands.
*/
std::vector<absl::string_view> GetKeysWithAllowedCommands(
    const absl::flat_hash_set<absl::string_view> &module_allowed_cmds,
    const std::vector<acl::ValkeyAclGetUserReplyView> &acl_views) {
  std::vector<absl::string_view> keys;
  for (const auto &acl_view : acl_views) {
    if (IsAllowedCommands(module_allowed_cmds, acl_view.cmds)) {
      for (const auto acl_key :
           absl::StrSplit(acl_view.keys, ' ', absl::SkipEmpty())) {
        keys.insert(keys.end(),
                    absl::string_view(acl_key.data(), acl_key.size()));
      }
    }
  }
  return keys;
}

/*
Check if a given prefix matches ANY given acl patterns.

The prefix, for example `abc:`, is logically the same as `~abc:*` with an ACL
glob expression. This function finds if the given prefix, `~abc:*` is a subset
of ANY one of acl patterns (i.e. `allkeys` or `~ab*`)

%RW flags are different from @read or @write, and they are key level flags
permit read and write operations. Vector Search only read the keys with given
prefixes and never writes, so in here we assume that we need %R permission is
required, not %W.
*/
bool IsPrefixAllowed(const absl::string_view &module_prefix,
                     const std::vector<absl::string_view> &acl_keys) {
  bool result = false;
  for (const auto &acl_key : acl_keys) {
    if (acl_key == "allkeys") {
      result = true;
    } else if (acl_key == "resetkeys") {
      result = false;
    } else if (acl_key.starts_with("%W")) {
      continue;
    } else {
      int pos = acl_key.find('~');
      /* This should not happen assuming Valkey server generated output is
       * always valid */
      CHECK(pos != absl::string_view::npos);
      int offset = pos + 1; /* either one of ~, %R~, and %RW~ */
      CHECK(acl_key.length() - offset > 0);
      result = result || acl::StringEndsWithWildCardMatch(
                             acl_key.data() + offset, acl_key.length() - offset,
                             module_prefix.data(), module_prefix.length());
    }
  }
  return result;
}

void GetAclViewFromCallReplyImpl(
    std::vector<acl::ValkeyAclGetUserReplyView> &acl_views,
    RedisModuleCallReply *reply) {
  acl_views.emplace_back(acl::ValkeyAclGetUserReplyView{});
  auto &acl_view = acl_views.back();

  RedisModuleCallReply *map_key;
  RedisModuleCallReply *map_val;
  int idx = 0;

  while (RedisModule_CallReplyMapElement(reply, idx, &map_key, &map_val) ==
         REDISMODULE_OK) {
    if (map_key &&
        RedisModule_CallReplyType(map_key) == REDISMODULE_REPLY_STRING) {
      absl::string_view key = CallReplyStringToStringView(map_key);

      if (absl::EqualsIgnoreCase(key, "commands")) {
        acl_view.cmds = CallReplyStringToStringView(map_val);
      } else if (absl::EqualsIgnoreCase(key, "keys")) {
        acl_view.keys = CallReplyStringToStringView(map_val);
      } else if (absl::EqualsIgnoreCase(key, "selectors")) {
        int selector_idx = 0;
        RedisModuleCallReply *selector_reply;
        while ((selector_reply =
                    RedisModule_CallReplyArrayElement(map_val, selector_idx))) {
          GetAclViewFromCallReplyImpl(acl_views, selector_reply);
          selector_idx++;
        }
      }
    }
    idx++;
  }
}

}  // namespace

absl::StatusOr<std::vector<acl::ValkeyAclGetUserReplyView>>
GetAclViewFromCallReply(RedisModuleCallReply *reply) {
  if (!reply || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_MAP) {
    return absl::FailedPreconditionError(
        "Cannot get an ACL from the server, got an invalid response");
  }

  std::vector<acl::ValkeyAclGetUserReplyView> acl_views;
  GetAclViewFromCallReplyImpl(acl_views, reply);
  return acl_views;
}

absl::Status AclPrefixCheck(
    RedisModuleCtx *ctx,
    const absl::flat_hash_set<absl::string_view> &module_allowed_cmds,
    const std::vector<std::string> &module_prefixes) {
  auto username = vmsdk::UniqueRedisString(RedisModule_GetCurrentUserName(ctx));
  auto reply = vmsdk::UniquePtrRedisCallReply(
      RedisModule_Call(ctx, "ACL", "cs3", "GETUSER", username.get()));
  VMSDK_ASSIGN_OR_RETURN(auto acl_views, GetAclViewFromCallReply(reply.get()));

  auto acl_keys = GetKeysWithAllowedCommands(module_allowed_cmds, acl_views);

  // If no index prefixes are given from the module, it should have permission
  // to access ALL keys.
  if (module_prefixes.empty() && IsPrefixAllowed("", acl_keys)) {
    return absl::OkStatus();
  }

  for (const auto &module_prefix : module_prefixes) {
    if (IsPrefixAllowed(module_prefix, acl_keys)) {
      return absl::OkStatus();
    }
  }
  return absl::PermissionDeniedError(
      "The user doesn't have a permission to execute a command");
}

absl::Status AclPrefixCheck(
    RedisModuleCtx *ctx,
    const absl::flat_hash_set<absl::string_view> &module_allowed_cmds,
    const data_model::IndexSchema &index_schema_proto) {
  std::vector<std::string> module_prefixes;
  module_prefixes.reserve(index_schema_proto.subscribed_key_prefixes_size());
  for (const auto &prefix : index_schema_proto.subscribed_key_prefixes()) {
    module_prefixes.emplace_back(prefix);
  }
  return AclPrefixCheck(ctx, module_allowed_cmds, module_prefixes);
}

}  // namespace valkey_search
