/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_COMMAND_INFO_H_
#define VMSDK_SRC_COMMAND_INFO_H_

#include <absl/strings/string_view.h>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace command_info {

/*

Keyspec.

*/
struct BeginSearchIndex {
    int pos;
};

struct BeginSearchKeyword {
    std::string keyword;
    int startfrom;
};

struct FindKeysRange {
    int lastkey;
    int keystep;
    int limit;
};

struct FindKeysNum {
    int keynumidx;
    int firstkey;
    int keystep;
};

struct KeySpec {
    std::string notes;
    uint64_t flags;
    std::variant<std::monostate, BeginSearchIndex, BeginSearchKeyword> beginsearch;
    std::variant<std::monostate, FindKeysRange, FindKeysNum> findkeys;
};

struct ArgDescription {
    std::string name;
    ValkeyModuleCommandArgType type;
    std::optional<int> key_spec_index;
    std::optional<std::string> token;
    std::optional<std::string> summary;
    std::optional<std::string> since;
    int flags{0};
    std::optional<std::string> deprecated_since;
    std::optional<std::vector<ArgDescription>> subargs;
    std::optional<std::string> display_text;
};

struct History {
    std::string since;
    std::string changes;
};

struct Info {
    std::string version;
    std::optional<std::string> summary;
    std::optional<std::string> complexity;
    std::optional<std::string> since;
    std::optional<std::vector<History>> history;
    std::optional<std::string> tips;
    int arity;
    std::vector<KeySpec> key_specs;
    std::vector<ArgDescription> args;
};

void Set(ValkeyModuleCtx *ctx, ValkeyModuleCommand *cmd, absl::string_view name, const Info& info);

}   
}

#endif