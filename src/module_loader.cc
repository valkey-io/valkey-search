/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <memory>

#include "src/commands/commands.h"
#include "src/keyspace_event_manager.h"
#include "src/valkey_search.h"
#include "src/version.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace {

// Strip the '@' prefix from command categories (e.g., @read)
// to format them for Valkey Search's prefix ACL rules (e.g., read).
inline std::list<absl::string_view> ACLPermissionFormatter(
    const absl::flat_hash_set<absl::string_view> &cmd_permissions) {
  std::list<absl::string_view> permissions;
  for (auto permission : cmd_permissions) {
    CHECK(permission[0] == '@');
    permissions.push_back(permission.substr(1));
  }
  return permissions;
}

// ============================================================
// Command Info - static data tables
// ============================================================
//
// Each command's argument structure is defined as static arrays of
// ValkeyModuleCommandArg terminated by a zero entry. These are wired
// into ValkeyModuleCommandInfo structs and registered via
// ValkeyModule_SetCommandInfo in RegisterCommandInfo().

// ---------- FT.CREATE ----------
//
// FT.CREATE <index-name>
//     [ON HASH | ON JSON]
//     [PREFIX <count> <prefix> [<prefix>...]]
//     [SCORE default_value]
//     [LANGUAGE <language>]
//     [SKIPINITIALSCAN]
//     [MINSTEMSIZE <min_stem_size>]
//     [WITHOFFSETS | NOOFFSETS]
//     [NOSTOPWORDS | STOPWORDS <count> <word> word ...]
//     [PUNCTUATION <punctuation>]
//     SCHEMA
//         ( <field-identifier> [AS <field-alias>]
//               NUMERIC
//             | TAG [SEPARATOR <sep>] [CASESENSITIVE]
//             | TEXT [NOSTEM] [WITHSUFFIXTRIE | NOSUFFIXTRIE] [WEIGHT <weight>]
//             | VECTOR [HNSW | FLAT] <attr_count> [<attr_name> <attr_value>]+
//           [SORTABLE]
//         )+

static ValkeyModuleCommandArg ft_create_on_subargs[] = {
    {.name = "hash",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "HASH"},
    {.name = "json",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "JSON"},
    {},
};

static ValkeyModuleCommandArg ft_create_prefix_subargs[] = {
    {.name = "count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "count"},
    {.name = "prefix",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE},
    {},
};

static ValkeyModuleCommandArg ft_create_stopwords_words_subargs[] = {
    {.name = "count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "count"},
    {.name = "word",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE},
    {},
};

static ValkeyModuleCommandArg ft_create_tag_subargs[] = {
    {.name = "separator",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "SEPARATOR",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "casesensitive",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "CASESENSITIVE",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {},
};

static ValkeyModuleCommandArg ft_create_text_suffixtrie_subargs[] = {
    {.name = "withsuffixtrie",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "WITHSUFFIXTRIE"},
    {.name = "nosuffixtrie",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "NOSUFFIXTRIE"},
    {},
};

static ValkeyModuleCommandArg ft_create_text_subargs[] = {
    {.name = "nostem",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "NOSTEM",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "suffixtrie",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_create_text_suffixtrie_subargs},
    {.name = "weight",
     .type = VALKEYMODULE_ARG_TYPE_DOUBLE,
     .key_spec_index = -1,
     .token = "WEIGHT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {},
};

static ValkeyModuleCommandArg ft_create_vector_algorithm_subargs[] = {
    {.name = "hnsw",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "HNSW"},
    {.name = "flat",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "FLAT"},
    {},
};

static ValkeyModuleCommandArg ft_create_vector_subargs[] = {
    {.name = "algorithm",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .subargs = ft_create_vector_algorithm_subargs},
    {.name = "attr-count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "attribute",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE},
    {},
};

static ValkeyModuleCommandArg ft_create_fieldtype_subargs[] = {
    {.name = "numeric",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "NUMERIC"},
    {.name = "tag",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "TAG",
     .subargs = ft_create_tag_subargs},
    {.name = "text",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "TEXT",
     .subargs = ft_create_text_subargs},
    {.name = "vector",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "VECTOR",
     .subargs = ft_create_vector_subargs},
    {},
};

static ValkeyModuleCommandArg ft_create_field_subargs[] = {
    {.name = "identifier",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "alias",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "AS",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "type",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .subargs = ft_create_fieldtype_subargs},
    {.name = "sortable",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "SORTABLE",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {},
};

static ValkeyModuleCommandArg ft_create_offsets_subargs[] = {
    {.name = "withoffsets",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "WITHOFFSETS"},
    {.name = "nooffsets",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "NOOFFSETS"},
    {},
};

static ValkeyModuleCommandArg ft_create_stopwords_policy_subargs[] = {
    {.name = "nostopwords",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "NOSTOPWORDS"},
    {.name = "stopwords",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "STOPWORDS",
     .subargs = ft_create_stopwords_words_subargs},
    {},
};

static ValkeyModuleCommandArg ft_create_args[] = {
    {.name = "index",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "index"},
    {.name = "on",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .token = "ON",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_create_on_subargs},
    {.name = "prefix",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "PREFIX",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_create_prefix_subargs},
    {.name = "score",
     .type = VALKEYMODULE_ARG_TYPE_DOUBLE,
     .key_spec_index = -1,
     .token = "SCORE",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "language",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "LANGUAGE",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "skipinitialscan",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "SKIPINITIALSCAN",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "minstemsize",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "MINSTEMSIZE",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "offsets",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_create_offsets_subargs},
    {.name = "stopwords-policy",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_create_stopwords_policy_subargs},
    {.name = "punctuation",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "PUNCTUATION",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "schema",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "SCHEMA"},
    {.name = "field",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
     .subargs = ft_create_field_subargs},
    {},
};

static ValkeyModuleCommandInfo ft_create_info = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "Create an index",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = -3,
    .args = ft_create_args,
};

// ---------- FT.DROPINDEX ----------
//
// FT.DROPINDEX <index-name>

static ValkeyModuleCommandArg ft_dropindex_args[] = {
    {.name = "index",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {},
};

static ValkeyModuleCommandInfo ft_dropindex_info = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "Delete an index",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = 2,
    .args = ft_dropindex_args,
};

// ---------- FT.INFO ----------
//
// FT.INFO <index-name>
//   [LOCAL | PRIMARY | CLUSTER]
//   [ALLSHARDS | SOMESHARDS]
//   [CONSISTENT | INCONSISTENT]

static ValkeyModuleCommandArg ft_info_scope_subargs[] = {
    {.name = "local",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "LOCAL"},
    {.name = "primary",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "PRIMARY"},
    {.name = "cluster",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "CLUSTER"},
    {},
};

static ValkeyModuleCommandArg ft_info_shards_subargs[] = {
    {.name = "allshards",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "ALLSHARDS"},
    {.name = "someshards",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "SOMESHARDS"},
    {},
};

static ValkeyModuleCommandArg ft_info_consistency_subargs[] = {
    {.name = "consistent",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "CONSISTENT"},
    {.name = "inconsistent",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "INCONSISTENT"},
    {},
};

static ValkeyModuleCommandArg ft_info_args[] = {
    {.name = "index",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "index"},
    {.name = "scope",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_info_scope_subargs},
    {.name = "shards",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_info_shards_subargs},
    {.name = "consistency",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_info_consistency_subargs},
    {},
};

static ValkeyModuleCommandInfo ft_info_info = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "Return information about an index",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = -2,
    .args = ft_info_args,
};

// ---------- FT._LIST ----------
//
// FT._LIST

static ValkeyModuleCommandInfo ft_list_info = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "List current index names",
    .complexity = "O(1)",
    .since = "1.0.0",
    .arity = 1,
};

// ---------- FT.SEARCH ----------
//
// FT.SEARCH <index> <query>
//   [ALLSHARDS | SOMESHARDS]
//   [CONSISTENT | INCONSISTENT]
//   [DIALECT <dialect>]
//   [INORDER]
//   [LIMIT <offset> <num>]
//   [NOCONTENT]
//   [PARAMS <count> <name> <value> [ <name> <value> ...]]
//   [RETURN <count> <field> [AS <name>] <field> [AS <name>]...]
//   [SLOP <slop>]
//   [SORTBY <field> [ ASC | DESC]]
//   [TIMEOUT <timeout>]
//   [VERBATIM]
//   [WITHSORTKEYS]

static ValkeyModuleCommandArg ft_search_limit_subargs[] = {
    {.name = "offset",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "num",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {},
};

static ValkeyModuleCommandArg ft_search_params_pair_subargs[] = {
    {.name = "name",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "value",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {},
};

static ValkeyModuleCommandArg ft_search_params_subargs[] = {
    {.name = "count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "param",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
     .subargs = ft_search_params_pair_subargs},
    {},
};

static ValkeyModuleCommandArg ft_search_return_field_subargs[] = {
    {.name = "field",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "alias",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "AS",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {},
};

static ValkeyModuleCommandArg ft_search_return_subargs[] = {
    {.name = "count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "field-spec",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
     .subargs = ft_search_return_field_subargs},
    {},
};

static ValkeyModuleCommandArg ft_search_sortby_dir_subargs[] = {
    {.name = "asc",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "ASC"},
    {.name = "desc",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "DESC"},
    {},
};

static ValkeyModuleCommandArg ft_search_sortby_subargs[] = {
    {.name = "field",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "direction",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_sortby_dir_subargs},
    {},
};

static ValkeyModuleCommandArg ft_search_shards_subargs[] = {
    {.name = "allshards",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "ALLSHARDS"},
    {.name = "someshards",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "SOMESHARDS"},
    {},
};

static ValkeyModuleCommandArg ft_search_consistency_subargs[] = {
    {.name = "consistent",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "CONSISTENT"},
    {.name = "inconsistent",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "INCONSISTENT"},
    {},
};

static ValkeyModuleCommandArg ft_search_args[] = {
    {.name = "index",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "index"},
    {.name = "query",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "shards",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_shards_subargs},
    {.name = "consistency",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_consistency_subargs},
    {.name = "dialect",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "DIALECT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "inorder",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "INORDER",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "limit",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "LIMIT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_limit_subargs},
    {.name = "nocontent",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "NOCONTENT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "params",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "PARAMS",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_params_subargs},
    {.name = "return",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "RETURN",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_return_subargs},
    {.name = "slop",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "SLOP",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "sortby",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "SORTBY",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_search_sortby_subargs},
    {.name = "timeout",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "TIMEOUT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "verbatim",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "VERBATIM",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "withsortkeys",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "WITHSORTKEYS",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {},
};

static ValkeyModuleCommandInfo ft_search_info = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "Search an index",
    .complexity = "O(N)",
    .since = "1.0.0",
    .arity = -3,
    .args = ft_search_args,
};

// ---------- FT.AGGREGATE ----------
//
// FT.AGGREGATE <index-name> <query>
//     [DIALECT <dialect>]
//     [INORDER]
//     [LOAD * | LOAD <count> <field> [<field> ...]]
//     [PARAMS <count> <name> <value> [ <name> <value> ...]]
//     [SLOP <slop>]
//     [TIMEOUT <timeout>]
//     [VERBATIM]
//     (
//       | APPLY <expression> AS <field>
//       | FILTER <expression>
//       | GROUPBY <count> <field> [<field>...]
//           [REDUCE <reducer> <count> [<expression>...]]...
//       | LIMIT <offset> <count>
//       | SORTBY <count> <expression> [ASC|DESC] ... [MAX <num>]
//     )+

static ValkeyModuleCommandArg ft_aggregate_load_fields_subargs[] = {
    {.name = "count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "field",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE},
    {},
};

static ValkeyModuleCommandArg ft_aggregate_load_subargs[] = {
    {.name = "all",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "*"},
    {.name = "fields",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .subargs = ft_aggregate_load_fields_subargs},
    {},
};

static ValkeyModuleCommandArg ft_aggregate_params_pair_subargs[] = {
    {.name = "name",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "value",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {},
};

static ValkeyModuleCommandArg ft_aggregate_params_subargs[] = {
    {.name = "count",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "param",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
     .subargs = ft_aggregate_params_pair_subargs},
    {},
};

// APPLY <expression> AS <field>
static ValkeyModuleCommandArg ft_aggregate_apply_subargs[] = {
    {.name = "expression",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "name",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "AS"},
    {},
};

// REDUCE <reducer> <count> [<expression>...]
static ValkeyModuleCommandArg ft_aggregate_reduce_subargs[] = {
    {.name = "function",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "nargs",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "arg",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL | VALKEYMODULE_CMD_ARG_MULTIPLE},
    {},
};

// GROUPBY <count> <field>... [REDUCE ...]...
static ValkeyModuleCommandArg ft_aggregate_groupby_subargs[] = {
    {.name = "nargs",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "property",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE},
    {.name = "reduce",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "REDUCE",
     .flags =
         VALKEYMODULE_CMD_ARG_OPTIONAL | VALKEYMODULE_CMD_ARG_MULTIPLE_TOKEN,
     .subargs = ft_aggregate_reduce_subargs},
    {},
};

// LIMIT <offset> <count>
static ValkeyModuleCommandArg ft_aggregate_limit_subargs[] = {
    {.name = "offset",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "num",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {},
};

// SORTBY sort-spec: <expression> [ASC|DESC]
static ValkeyModuleCommandArg ft_aggregate_sortby_dir_subargs[] = {
    {.name = "asc",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "ASC"},
    {.name = "desc",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "DESC"},
    {},
};

static ValkeyModuleCommandArg ft_aggregate_sortby_expr_subargs[] = {
    {.name = "expression",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "direction",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_aggregate_sortby_dir_subargs},
    {},
};

static ValkeyModuleCommandArg ft_aggregate_sortby_subargs[] = {
    {.name = "nargs",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1},
    {.name = "sort-spec",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
     .subargs = ft_aggregate_sortby_expr_subargs},
    {.name = "max",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "MAX",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {},
};

// Processing stages (ONEOF, repeatable)
static ValkeyModuleCommandArg ft_aggregate_stage_subargs[] = {
    {.name = "apply",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "APPLY",
     .subargs = ft_aggregate_apply_subargs},
    {.name = "filter",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "FILTER"},
    {.name = "groupby",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "GROUPBY",
     .subargs = ft_aggregate_groupby_subargs},
    {.name = "limit",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "LIMIT",
     .subargs = ft_aggregate_limit_subargs},
    {.name = "sortby",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "SORTBY",
     .subargs = ft_aggregate_sortby_subargs},
    {},
};

static ValkeyModuleCommandArg ft_aggregate_args[] = {
    {.name = "index",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1,
     .token = "index"},
    {.name = "query",
     .type = VALKEYMODULE_ARG_TYPE_STRING,
     .key_spec_index = -1},
    {.name = "dialect",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "DIALECT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "inorder",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "INORDER",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "load",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .token = "LOAD",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_aggregate_load_subargs},
    {.name = "params",
     .type = VALKEYMODULE_ARG_TYPE_BLOCK,
     .key_spec_index = -1,
     .token = "PARAMS",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
     .subargs = ft_aggregate_params_subargs},
    {.name = "slop",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "SLOP",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "timeout",
     .type = VALKEYMODULE_ARG_TYPE_INTEGER,
     .key_spec_index = -1,
     .token = "TIMEOUT",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "verbatim",
     .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
     .key_spec_index = -1,
     .token = "VERBATIM",
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL},
    {.name = "stage",
     .type = VALKEYMODULE_ARG_TYPE_ONEOF,
     .key_spec_index = -1,
     .flags = VALKEYMODULE_CMD_ARG_OPTIONAL | VALKEYMODULE_CMD_ARG_MULTIPLE,
     .subargs = ft_aggregate_stage_subargs},
    {},
};

static ValkeyModuleCommandInfo ft_aggregate_info = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "Perform aggregate operations on an index",
    .complexity = "O(N)",
    .since = "1.1.0",
    .arity = -3,
    .args = ft_aggregate_args,
};

}  // namespace

vmsdk::module::Options options = {
    .name = "search",
    .acl_categories = ACLPermissionFormatter({
        valkey_search::kSearchCategory,
    }),
    .version = kModuleVersion,
    .minimum_valkey_server_version = kMinimumServerVersion,
    .info = valkey_search::ModuleInfo,
    .commands =
        {
            {
                .cmd_name = valkey_search::kCreateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kCreateCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kFastFlag,
                          vmsdk::module::kDenyOOMFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTCreateCmd>,
                .command_info = &ft_create_info,
            },
            {
                .cmd_name = valkey_search::kDropIndexCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kDropIndexCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kFastFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTDropIndexCmd>,
                .command_info = &ft_dropindex_info,
            },
            {
                .cmd_name = valkey_search::kInfoCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kInfoCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kFastFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTInfoCmd>,
                .command_info = &ft_info_info,
            },
            {
                .cmd_name = valkey_search::kListCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kListCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kAdminFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTListCmd>,
                .command_info = &ft_list_info,
            },
            {
                .cmd_name = valkey_search::kSearchCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTSearchCmd>,
                .command_info = &ft_search_info,
            },
            {
                .cmd_name = valkey_search::kDebugCommand,
                .permissions =
                    ACLPermissionFormatter(valkey_search::kDebugCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag,
                          vmsdk::module::kAdminFlag},
                .cmd_func = &vmsdk::CreateCommand<valkey_search::FTDebugCmd>,
            },
            {
                .cmd_name = valkey_search::kInternalUpdateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kInternalUpdateCmdPermissions),
                .flags = {vmsdk::module::kWriteFlag, vmsdk::module::kAdminFlag,
                          vmsdk::module::kFastFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTInternalUpdateCmd>,
            },
            {
                .cmd_name = valkey_search::kAggregateCommand,
                .permissions = ACLPermissionFormatter(
                    valkey_search::kSearchCmdPermissions),
                .flags = {vmsdk::module::kReadOnlyFlag},
                .cmd_func =
                    &vmsdk::CreateCommand<valkey_search::FTAggregateCmd>,
                .command_info = &ft_aggregate_info,
            },
        },
    .on_load =
        [](ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::KeyspaceEventManager::InitInstance(
              std::make_unique<valkey_search::KeyspaceEventManager>());
          valkey_search::ValkeySearch::InitInstance(
              std::make_unique<valkey_search::ValkeySearch>());

          return valkey_search::ValkeySearch::Instance().OnLoad(ctx, argv,
                                                                argc);
        },
    .on_unload =
        [](ValkeyModuleCtx *ctx,
           [[maybe_unused]] const vmsdk::module::Options &options) {
          valkey_search::ValkeySearch::Instance().OnUnload(ctx);
        },
};
VALKEY_MODULE(options);
