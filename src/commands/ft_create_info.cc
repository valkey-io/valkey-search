/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/commands.h"

namespace valkey_search {

ValkeyModuleCommandArg ftCreateOnSubargs[] = {
    {
        .name = "hash",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "HASH",
        .summary = "Index HASH data type",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "json",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "JSON",
        .summary = "Index JSON data type",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

ValkeyModuleCommandArg ftCreatePrefixSubargs[] = {
    {
        .name = "count",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Number of prefixes",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "prefix",
        .type = VALKEYMODULE_ARG_TYPE_STRING,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Key prefix to index",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

ValkeyModuleCommandArg ftCreateTagSubargs[] = {
    {
        .name = "separator",
        .type = VALKEYMODULE_ARG_TYPE_STRING,
        .key_spec_index = -1,
        .token = "SEPARATOR",
        .summary = "Tag separator character",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "casesensitive",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "CASESENSITIVE",
        .summary = "Make tag matching case sensitive",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

// Distance metric options for vector algorithms
ValkeyModuleCommandArg ftCreateDistanceMetricOptions[] = {
    {
        .name = "l2",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "L2",
        .summary = "L2 (Euclidean) distance",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "ip",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "IP",
        .summary = "Inner product distance",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "cosine",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "COSINE",
        .summary = "Cosine distance",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

// HNSW algorithm subargs
ValkeyModuleCommandArg ftCreateHnswSubargs[] = {
    {
        .name = "dim",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "DIM",
        .summary = "Vector dimensions (required)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "type",
        .type = VALKEYMODULE_ARG_TYPE_DOUBLE,
        .key_spec_index = -1,
        .token = "TYPE",
        .summary = "Vector data type (Currently Only for FLOAT32)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "distance_metric",
        .type = VALKEYMODULE_ARG_TYPE_ONEOF,
        .key_spec_index = -1,
        .token = "DISTANCE_METRIC",
        .summary = "Distance algorithm",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateDistanceMetricOptions,
    },
    {
        .name = "initial_cap",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "INITIAL_CAP",
        .summary = "Initial index size (optional)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "m",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "M",
        .summary = "Maximum outgoing edges per node (default 16, max 512)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "ef_construction",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "EF_CONSTRUCTION",
        .summary = "Vectors examined during index creation (default 200, max 4096)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "ef_runtime",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "EF_RUNTIME",
        .summary = "Vectors examined during query (default 10, max 4096)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

// FLAT algorithm subargs
ValkeyModuleCommandArg ftCreateFlatSubargs[] = {
    {
        .name = "dim",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "DIM",
        .summary = "Vector dimensions (required)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "type",
        .type = VALKEYMODULE_ARG_TYPE_DOUBLE,
        .key_spec_index = -1,
        .token = "TYPE",
        .summary = "Vector data type (FLOAT32)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "distance_metric",
        .type = VALKEYMODULE_ARG_TYPE_ONEOF,
        .key_spec_index = -1,
        .token = "DISTANCE_METRIC",
        .summary = "Distance algorithm",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateDistanceMetricOptions,
    },
    {
        .name = "initial_cap",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = "INITIAL_CAP",
        .summary = "Initial index size (optional)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

// Vector algorithm options
ValkeyModuleCommandArg ftCreateVectorAlgorithms[] = {
    {
        .name = "hnsw",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = "HNSW",
        .summary = "HNSW vector algorithm",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateHnswSubargs,
    },
    {
        .name = "flat",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = "FLAT",
        .summary = "FLAT vector algorithm",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateFlatSubargs,
    },
    {nullptr}  // Sentinel
};

ValkeyModuleCommandArg ftCreateVectorSubargs[] = {
    {
        .name = "algorithm",
        .type = VALKEYMODULE_ARG_TYPE_ONEOF,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Vector algorithm (HNSW or FLAT)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateVectorAlgorithms,
    },
    {
        .name = "attribute_count",
        .type = VALKEYMODULE_ARG_TYPE_INTEGER,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Number of vector attributes",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "attributes",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Vector attribute name-value pairs",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

ValkeyModuleCommandArg ftCreateSchemaFieldTypes[] = {
    {
        .name = "numeric",
        .type = VALKEYMODULE_ARG_TYPE_PURE_TOKEN,
        .key_spec_index = -1,
        .token = "NUMERIC",
        .summary = "Numeric field type",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "tag",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = "TAG",
        .summary = "Tag field type",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateTagSubargs,
    },
    {
        .name = "vector",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = "VECTOR",
        .summary = "Vector field type",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateVectorSubargs,
    },
    {nullptr}  // Sentinel
};

ValkeyModuleCommandArg ftCreateSchemaSubargs[] = {
    {
        .name = "field_identifier",
        .type = VALKEYMODULE_ARG_TYPE_STRING,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Field identifier",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "as",
        .type = VALKEYMODULE_ARG_TYPE_STRING,
        .key_spec_index = -1,
        .token = "AS",
        .summary = "Field alias",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "field_type",
        .type = VALKEYMODULE_ARG_TYPE_ONEOF,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Field type (NUMERIC, TAG, VECTOR)",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = ftCreateSchemaFieldTypes,
    },
    {
        .name = "field_options",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Field type specific options",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL | VALKEYMODULE_CMD_ARG_MULTIPLE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {nullptr}  // Sentinel
};

ValkeyModuleCommandArg ftCreateArgs[] = {
    {
        .name = "index_name",
        .type = VALKEYMODULE_ARG_TYPE_STRING,
        .key_spec_index = -1,
        .token = nullptr,
        .summary = "Name of the index",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_NONE,
        .deprecated_since = nullptr,
        .subargs = nullptr,
    },
    {
        .name = "on_data_type",
        .type = VALKEYMODULE_ARG_TYPE_ONEOF,
        .key_spec_index = -1,
        .token = "ON",
        .summary = "Data type to index",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = ftCreateOnSubargs,
    },
    {
        .name = "prefix",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = "PREFIX",
        .summary = "Key prefixes to index",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_OPTIONAL,
        .deprecated_since = nullptr,
        .subargs = ftCreatePrefixSubargs,
    },
    {
        .name = "schema",
        .type = VALKEYMODULE_ARG_TYPE_BLOCK,
        .key_spec_index = -1,
        .token = "SCHEMA",
        .summary = "Schema definition",
        .since = "1.0.0",
        .flags = VALKEYMODULE_CMD_ARG_MULTIPLE,
        .deprecated_since = nullptr,
        .subargs = ftCreateSchemaSubargs,
    },
    {nullptr}  // Sentinel
};

const ValkeyModuleCommandInfo ftCreateInfo = {
    .version = VALKEYMODULE_COMMAND_INFO_VERSION,
    .summary = "Creates an empty search index and initiates the backfill process",
    .complexity = "O(N log N), where N is the number of indexed items",
    .since = "1.0.0",
    .history = nullptr,
    .tips = nullptr,
    .arity = -2,
    .key_specs = nullptr,
    .args = ftCreateArgs,
};

}  // namespace valkey_search
