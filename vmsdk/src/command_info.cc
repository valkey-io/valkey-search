/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/command_info.h"

#include <span>

#include "absl/log/check.h"

namespace vmsdk {
namespace command_info {

// The lifetime of the command_info data structures isn't clear, so we'll make copies and retain them locally.

static std::vector<std::vector<char>> pod_data_;
static std::vector<std::unique_ptr<Info>> info_storage_;

template<typename T> 
std::span<T> MakePODArray(size_t size) {
    // Make an array, ensure there's always a fully zero-ed entry at the end of the array
    // as many of the Command Info sub-structures use that to identify the end of an array
    std::vector<char> pod_data(sizeof(T) * (size + 1), 0);
    std::span<T> result(static_cast<T *>(static_cast<void *>(pod_data.data())), size);
    pod_data_.push_back(std::move(pod_data));
    return result;
}

const char *OptionalString(const std::optional<std::string>& str) {
    return str ? str->data() : nullptr;
}

ValkeyModuleCommandArg* ProcessArgDescription(const std::vector<ArgDescription>& args) {
    std::span<ValkeyModuleCommandArg> vk_args = MakePODArray<ValkeyModuleCommandArg>(args.size());
    for (size_t i = 0; i < args.size(); ++i) {
        auto& vk_arg = vk_args[i];
        const auto& arg = args[i];

        vk_arg.name = arg.name.data();
        vk_arg.key_spec_index = arg.key_spec_index ? *arg.key_spec_index : -1;
        vk_arg.token = OptionalString(arg.token);
        vk_arg.summary = OptionalString(arg.summary);
        vk_arg.since = OptionalString(arg.since);

        CHECK(arg.flags == 
            (arg.flags & 
                (VALKEYMODULE_CMD_ARG_OPTIONAL | VALKEYMODULE_CMD_ARG_MULTIPLE | VALKEYMODULE_CMD_ARG_MULTIPLE_TOKEN)
            )
        );
        vk_arg.flags = arg.flags;
        vk_arg.type = arg.type;
        vk_arg.deprecated_since = OptionalString(arg.deprecated_since);
        vk_arg.subargs = arg.subargs ? ProcessArgDescription((*arg.subargs)) : nullptr;
        vk_arg.display_text = OptionalString(arg.display_text);
    }
    return vk_args.data();
}

void Set(ValkeyModuleCtx *ctx, ValkeyModuleCommand *cmd, absl::string_view name, const Info& info_arg) {
    info_storage_.push_back(std::make_unique<Info>(info_arg));
    const Info& info = *info_storage_.back();

    // Now do the keyspecs

    auto vk_keyspecs = MakePODArray<ValkeyModuleCommandKeySpec>(info.key_specs.size());
    for (size_t i = 0; i < vk_keyspecs.size(); ++i) {
        auto& vk_keyspec = vk_keyspecs[i];
        const auto& keyspec = info.key_specs[i];

        vk_keyspec.notes = OptionalString(keyspec.notes);
        vk_keyspec.flags = keyspec.flags;
        switch(keyspec.beginsearch.index()) {
            case 0: 
                vk_keyspec.begin_search_type = VALKEYMODULE_KSPEC_BS_UNKNOWN;
                break;
            case 1: {
                const BeginSearchIndex& si = std::get<1>(keyspec.beginsearch);
                vk_keyspec.begin_search_type = VALKEYMODULE_KSPEC_BS_INDEX;
                vk_keyspec.bs.index.pos = si.pos;
                break;
            }
            case 2: {
                const BeginSearchKeyword& key = std::get<2>(keyspec.beginsearch);
                vk_keyspec.begin_search_type = VALKEYMODULE_KSPEC_BS_KEYWORD;
                vk_keyspec.bs.keyword.keyword = key.keyword.data();
                vk_keyspec.bs.keyword.startfrom = key.startfrom;
                break;
            }
            default:
                CHECK(false);
                break;
        }
        switch(keyspec.findkeys.index()) {
            case 0:
                vk_keyspec.find_keys_type = VALKEYMODULE_KSPEC_FK_UNKNOWN;
                break;
            case 1: {
                const FindKeysRange& fk = std::get<1>(keyspec.findkeys);
                vk_keyspec.find_keys_type = VALKEYMODULE_KSPEC_FK_RANGE;
                vk_keyspec.fk.range.keystep = fk.keystep;
                vk_keyspec.fk.range.lastkey = fk.lastkey;
                vk_keyspec.fk.range.limit = fk.limit;
                break;
            }
            case 2: {
                const FindKeysNum& fk = std::get<2>(keyspec.findkeys);
                vk_keyspec.find_keys_type = VALKEYMODULE_KSPEC_FK_KEYNUM;
                vk_keyspec.fk.keynum.keynumidx = fk.keynumidx;
                vk_keyspec.fk.keynum.keystep = fk.keystep;
                vk_keyspec.fk.keynum.firstkey = fk.firstkey;
                break;
            }
            default:
                CHECK(false);
                break;
        }
    }

    // Now do the Info object itself

    auto info_ptr = MakePODArray<ValkeyModuleCommandInfo>(1);
    auto& vk_info = info_ptr[0];
    
    vk_info.version = VALKEYMODULE_COMMAND_INFO_VERSION;
    vk_info.summary = OptionalString(info.summary);
    vk_info.complexity = OptionalString(info.complexity);
    vk_info.since = OptionalString(info.since);
    if (info.history) {
        auto vk_hist = MakePODArray<ValkeyModuleCommandHistoryEntry>(info.history->size());
        for (size_t i = 0; i < vk_hist.size(); ++i) {
            const auto& hist = (*info.history)[i];
            vk_hist[i].changes = hist.changes.data();
            vk_hist[i].since = hist.since.data();
        }
        vk_info.history = vk_hist.data();
    }
    vk_info.tips = OptionalString(info.tips);
    vk_info.arity = info.arity;
    vk_info.args = ProcessArgDescription(info.args);
    vk_info.key_specs = vk_keyspecs.data();

    if (ValkeyModule_SetCommandInfo(cmd, &vk_info) != VALKEYMODULE_OK) {
        CHECK(false) << "Unable to set command info for " << name << " See Valkey log for details.";
    }
}

}
}