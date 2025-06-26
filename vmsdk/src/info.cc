
/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include <map>
#include <set>
#include <string>

#include "vmsdk/src/info.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/module_config.h"

namespace vmsdk {
namespace info_field {

using FieldMap = std::map<std::string, const Base *>;

struct SectionInfo {
    bool handled_{false};
    FieldMap fields_;
};

using SectionMap = std::map<std::string, SectionInfo>;

//
// This trick allows fields to be declared globally static.
//
static SectionMap& GetSectionMap() {
    static SectionMap section_map;
    return section_map;
}

static const char *bad_field_reason = nullptr;

static bool IsValidName(const std::string& str) {
    for (auto c : str) {
        if (!std::isprint(c) || c == ':') {
            return false;
        }
    }
    return true;
}

static bool doing_startup = true;

Base::Base(absl::string_view section, absl::string_view name, Flags flags) 
: section_(section), name_(name), flags_(flags) {

    CHECK(doing_startup || IsMainThread());

    SectionMap& section_map = GetSectionMap();
    FieldMap& field_map = section_map[section_].fields_;
    if (field_map.contains(name_)) {
        bad_field_reason = "Created Duplicate Field"; // We're toast ;-) but we'll fail later with a nice error message
    } else {
        field_map[name_] = this;
    }
}

Base::~Base() {
    vmsdk::VerifyMainThread();

    SectionMap& section_map = GetSectionMap();
    if (!section_map.contains(section_)) {
        bad_field_reason = "section map corrupted"; // We're toast ;-) but we'll fail later with a nice error message
    } else {
        FieldMap& field_map = section_map[section_].fields_;
        if (!field_map.contains(name_)) {
            bad_field_reason = "field map corrupted, probably a duplicate field";
        } else {
            field_map.erase(name_);
            if (field_map.empty()) {
                section_map.erase(section_);
            }
        }
    }
}

static auto show_developer = vmsdk::config::Boolean("info-developer-visible", false);

//
// Dump the sections that haven't been already dumped.
//
// No locks or memory allocations are permitted here.
//
void DoRemainingSections(RedisModuleInfoCtx *ctx, int for_crash_report) {
    SectionMap& section_map = GetSectionMap();

    //
    // See if anything will get displayed in this section
    //
    for (auto& [section, section_info] : section_map) {
        if (section_info.handled_) {
            section_info.handled_ = false;
            continue;
        }
        bool do_section = false;
        if (show_developer.GetValue()) {
            do_section = true;
        } else {
            for (auto& [name, field] : section_info.fields_) {
                if ((!for_crash_report || (field->GetFlags() & kCrashSafe)) && field->IsVisible()) {
                    if (field->GetFlags() & kApplication) {
                        do_section = true;
                        break;
                    }
                }
            }
        }
        if (do_section) {
            continue;
        }
        DoSection(ctx, section, for_crash_report);
        section_info.handled_ = false;
    }
}

//
// Begin a specific section. This is usually done because the caller wants to add his own fields
// outside of this machinery.
// The section is marked as "handled" so that the DoRemainingSections knows to avoid repeating
// this section.
//
// No memory allocations or locks are permitted here.
//
void DoSection(RedisModuleInfoCtx *ctx, absl::string_view section, int for_crash_report) {
    if (RedisModule_InfoAddSection(ctx, section.data()) == REDISMODULE_ERR) {
        VMSDK_LOG(DEBUG, nullptr) << "Info Section " << section << " Skipped";
    } else {
        // Find the section without a memory allocation....
        auto& section_map = GetSectionMap();
        for (auto& [name, section_info] : section_map) {
            if (section == name) {
                // Found it....
                section_info.handled_ = true;
                for (auto& [name, field] : section_info.fields_) {
                    if ((!for_crash_report || (field->GetFlags() & kCrashSafe)) && field->IsVisible()) {
                        if (show_developer.GetValue() || (field->GetFlags() & kApplication)) {
                            field->Dump(ctx);
                        }
                    }
                }
            }
        }
    }
}

bool Validate(RedisModuleCtx *ctx) {
    doing_startup = false; // Done.
    bool failed = false;
    if (bad_field_reason) {
        LOG(WARNING) << "Invalid INFO Section Configuration detected, first error was: " << bad_field_reason << "\n";
        failed = true;
    }
    std::set<std::string> unique_names; // Python info parsing requires that names are unique across sections.
    
    SectionMap section_map = GetSectionMap();
    for (auto& [section, section_info] : section_map) {
        if (!IsValidName(section)) {
            VMSDK_LOG(WARNING, ctx) << "Invalid characters in section name: " << section;
            failed = true;
        }
        for (auto& [name, info] : section_info.fields_) {
            if (name != info->GetName()) {
                VMSDK_LOG(WARNING, ctx) << "Map corruption";
                return true;
            }
            if (!(info->GetFlags() ^ (Flags::kDeveloper | Flags::kApplication))) {
                VMSDK_LOG(WARNING, ctx) << "Incorrect flags set for INFO Section:" << section << " Name:" << name;
                failed = true;
            }
            if (!IsValidName(name)) {
                VMSDK_LOG(WARNING, ctx) << "Invalid characters in info field name: " << name;
                failed = true;
            }
            if (unique_names.contains(name)) {
                VMSDK_LOG(WARNING, ctx) << "Non-unique name: " << name;
                failed = true;
            }
            VMSDK_LOG(WARNING, ctx) << "Defined Info Field: " << name << " Flags:" << info->GetFlags();
        }
    }
    return !failed;
}

Numeric::Numeric(absl::string_view section, absl::string_view name, NumericBuilder builder)
    : Base(section, name, builder.flags_),
      visible_func_(builder.visible_func_),
      compute_func_(builder.compute_func_) {
      }

void Numeric::Dump(RedisModuleInfoCtx *ctx) const {
    long long value = compute_func_ ? (*compute_func_)() : Get();
    VMSDK_LOG(WARNING, nullptr) << "Numeric::Dump " << GetName() << " Value:" << value << " Flags:" << GetFlags();
    if (GetFlags() & Flags::kSIBytes) {
        char buffer[100];
        size_t used = vmsdk::DisplayAsSIBytes(value, buffer, sizeof(buffer));
        RedisModule_InfoAddFieldCString(ctx, GetName().data(), buffer);
    } else {
        RedisModule_InfoAddFieldLongLong(ctx, GetName().data(), value);
    }
}

bool Numeric::IsVisible() const {
    return visible_func_? (*visible_func_)() : true;
}

String::String(absl::string_view section, absl::string_view name, StringBuilder builder)
    : Base(section, name, builder.flags_),
      visible_func_(builder.visible_func_),
      compute_string_func_(builder.compute_string_func_),
      compute_char_func_(builder.compute_char_func_)
       {}

void String::Dump(RedisModuleInfoCtx *ctx) const {
    if (compute_char_func_) {
        const char *str = (*compute_char_func_)();
        if (str) {
            RedisModule_InfoAddFieldCString(ctx, GetName().data(), (*compute_char_func_)());
        }
    } else if (compute_string_func_) {
        std::string s = (*compute_string_func_)();
        RedisModule_InfoAddFieldCString(ctx, GetName().data(), s.data());
    } else {
        VMSDK_LOG(WARNING, nullptr) << "Invalid state for Info String: " << GetSection() << "/" << GetName();
    }
}

bool String::IsVisible() const {
    return visible_func_? (*visible_func_)() : true;
}

}
}