
/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/info.h"

#include <string>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_set.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/utils.h"

namespace vmsdk {

namespace info_field {

using FieldMap = absl::btree_map<std::string, const Base*>;

struct SectionInfo {
  bool handled_{false};
  FieldMap fields_;
};

using SectionMap = absl::btree_map<std::string, SectionInfo>;

//
// This trick allows fields to be declared globally static.
//
static SectionMap& GetSectionMap() {
  static SectionMap section_map;
  return section_map;
}

static std::optional<std::string> bad_field_reason;

static bool IsValidName(const std::string& str) {
  for (auto c : str) {
    if (!std::isprint(c) || c == ':') {
      return false;
    }
  }
  return true;
}

static bool doing_startup = true;

Base::Base(absl::string_view section, absl::string_view name, Flags flags,
           Units units)
    : section_(section), name_(name), flags_(flags), units_(units) {
  CHECK(doing_startup || IsMainThread());

  SectionMap& section_map = GetSectionMap();
  FieldMap& field_map = section_map[section_].fields_;
  if (field_map.contains(name_)) {
    bad_field_reason =
        "Created Duplicate Field";  // We're toast ;-) but we'll fail later with
                                    // a nice error message
  } else {
    field_map[name_] = this;
  }
}

Base::~Base() {
  vmsdk::VerifyMainThread();

  SectionMap& section_map = GetSectionMap();
  if (!section_map.contains(section_)) {
    bad_field_reason =
        "section map corrupted";  // We're toast ;-) but we'll fail later with a
                                  // nice error message
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

static auto show_developer =
    vmsdk::config::Boolean("info-developer-visible", false);

//
// Dump the sections that haven't been already dumped.
//
// No locks or memory allocations are permitted here.
//
void DoRemainingSections(ValkeyModuleInfoCtx* ctx, int for_crash_report) {
  SectionMap& section_map = GetSectionMap();

  //
  // See if anything will get displayed in this section
  //
  for (auto& [section, section_info] : section_map) {
    if (section_info.handled_) {
      VMSDK_LOG(DEBUG, nullptr)
          << "Skipping Section " << section << " as it was already handled";
      section_info.handled_ = false;
      continue;
    }
    bool do_section = false;
    if (show_developer.GetValue()) {
      do_section = true;
    } else {
      for (auto& [name, field] : section_info.fields_) {
        if ((!for_crash_report || field->IsCrashSafe()) && field->IsVisible()) {
          if (field->IsApplication()) {
            do_section = true;
            break;
          }
        }
      }
    }
    if (!do_section) {
      continue;
    }
    DoSection(ctx, section, for_crash_report);
    section_info.handled_ = false;
  }
}

//
// Begin a specific section. This is usually done because the caller wants to
// add his own fields outside of this machinery. The section is marked as
// "handled" so that the DoRemainingSections knows to avoid repeating this
// section.
//
// No memory allocations or locks are permitted here.
//
void DoSection(ValkeyModuleInfoCtx* ctx, absl::string_view section,
               int for_crash_report) {
  if (ValkeyModule_InfoAddSection(ctx, section.data()) == VALKEYMODULE_ERR) {
    VMSDK_LOG(DEBUG, nullptr) << "Info Section " << section << " Skipped";
    return;
  }
  // Find the section without a memory allocation....
  SectionMap& section_map = GetSectionMap();
  for (auto& [name, section_info] : section_map) {
    if (section != name) {
      continue;
    }
    // Found it....
    section_info.handled_ = true;
    for (auto& [name, field] : section_info.fields_) {
      if ((!for_crash_report || field->IsCrashSafe()) && field->IsVisible()) {
        if (show_developer.GetValue() || field->IsApplication()) {
          field->Dump(ctx);
        }
      }
    }
  }
}

bool Validate(ValkeyModuleCtx* ctx) {
  doing_startup = false;  // Done.
  bool failed = false;
  if (bad_field_reason) {
    LOG(WARNING)
        << "Invalid INFO Section Configuration detected, first error was: "
        << *bad_field_reason << "\n";
    failed = true;
  }
  absl::flat_hash_set<std::string>
      unique_names;  // Python info parsing requires that names are unique
                     // across sections.

  SectionMap& section_map = GetSectionMap();
  for (auto& [section, section_info] : section_map) {
    if (!IsValidName(section)) {
      VMSDK_LOG(WARNING, ctx)
          << "Invalid characters in section name: " << section;
      failed = true;
    }
    for (auto& [name, info] : section_info.fields_) {
      if (name != info->GetName()) {
        VMSDK_LOG(WARNING, ctx) << "Map corruption";
        return true;
      }
      if (!(info->GetFlags() ^ (Flags::kDeveloper | Flags::kApplication))) {
        VMSDK_LOG(WARNING, ctx)
            << "Missing App/Dev for Section:" << section << " Name:" << name;
        failed = true;
      }
      if (!IsValidName(name)) {
        VMSDK_LOG(WARNING, ctx)
            << "Invalid characters in info field name: " << name;
        failed = true;
      }
      if (unique_names.contains(name)) {
        VMSDK_LOG(WARNING, ctx) << "Non-unique name: " << name;
        failed = true;
      }
      VMSDK_LOG(WARNING, ctx)
          << "Defined Info Field: " << name << " Flags:" << info->GetFlags();
    }
  }
  return !failed;
}

template <typename T>
Numeric<T>::Numeric(absl::string_view section, absl::string_view name,
                    NumericBuilder<T> builder)
    : Base(section, name, builder.flags_, builder.units_),
      visible_func_(builder.visible_func_),
      compute_func_(builder.compute_func_) {}

template <typename T>
void Numeric<T>::Dump(ValkeyModuleInfoCtx* ctx) const {
  T value = compute_func_ ? (*compute_func_)() : Get();
  if constexpr (std::is_integral<T>::value) {
    if (GetFlags() & Flags::kSIBytes) {
      char buffer[100];
      size_t used = vmsdk::DisplayAsSIBytes(value, buffer, sizeof(buffer));
      ValkeyModule_InfoAddFieldCString(ctx, GetName().data(), buffer);
    } else {
      ValkeyModule_InfoAddFieldLongLong(ctx, GetName().data(), value);
    }
  } else if constexpr (std::is_floating_point<T>::value) {
    ValkeyModule_InfoAddFieldDouble(ctx, GetName().data(), value);
  } else {
    CHECK(false);
  }
}

template <typename T>
void Numeric<T>::Reply(ValkeyModuleCtx* ctx) const {
  T value = compute_func_ ? (*compute_func_)() : Get();
  if constexpr (std::is_integral<T>::value) {
    if (GetFlags() & Flags::kSIBytes) {
      char buffer[100];
      size_t used = vmsdk::DisplayAsSIBytes(value, buffer, sizeof(buffer));
      ValkeyModule_ReplyWithCString(ctx, buffer);
    } else {
      ValkeyModule_ReplyWithLongLong(ctx, value);
    }
  } else if constexpr (std::is_floating_point<T>::value) {
    ValkeyModule_ReplyWithDouble(ctx, value);
  } else {
    CHECK(false);
  }
}

template <typename T>
bool Numeric<T>::IsVisible() const {
  return visible_func_ ? (*visible_func_)() : true;
}

//
// Explicitly instantiate the Numeric templates for the types we use.
//
template struct Numeric<long long>;
template struct Numeric<double>;

String::String(absl::string_view section, absl::string_view name,
               StringBuilder builder)
    : Base(section, name, builder.flags_, Units::kNone),
      visible_func_(builder.visible_func_),
      compute_string_func_(builder.compute_string_func_),
      compute_char_func_(builder.compute_char_func_) {}

void String::Dump(ValkeyModuleInfoCtx* ctx) const {
  if (compute_char_func_) {
    const char* str = (*compute_char_func_)();
    if (str) {
      ValkeyModule_InfoAddFieldCString(ctx, GetName().data(),
                                       (*compute_char_func_)());
    }
  } else if (compute_string_func_) {
    std::string s = (*compute_string_func_)();
    ValkeyModule_InfoAddFieldCString(ctx, GetName().data(), s.data());
  } else {
    VMSDK_LOG(WARNING, nullptr)
        << "Invalid state for Info String: " << GetSection() << "/"
        << GetName();
  }
}

void String::Reply(ValkeyModuleCtx* ctx) const {
  if (compute_char_func_) {
    const char* str = (*compute_char_func_)();
    if (str) {
      ValkeyModule_ReplyWithCString(ctx, (*compute_char_func_)());
    }
  } else if (compute_string_func_) {
    std::string s = (*compute_string_func_)();
    ValkeyModule_ReplyWithCString(ctx, s.data());
  } else {
    VMSDK_LOG(WARNING, nullptr)
        << "Invalid state for Info String: " << GetSection() << "/"
        << GetName();
  }
}

bool String::IsVisible() const {
  return visible_func_ ? (*visible_func_)() : true;
}

static absl::flat_hash_map<Units, absl::string_view> kUnitsToString{
    {Units::kNone, ""},

    {Units::kCount, "Count"},
    {Units::kCountPerSecond, "Count/Second"},
    {Units::kPercent, "Percent"},

    {Units::kSeconds, "Seconds"},
    {Units::kMilliSeconds, "Milliseconds"},
    {Units::kMicroSeconds, "Microseconds"},

    {Units::kBytes, "Bytes"},

    {Units::kBytesPerSecond, "Bytes/Second"},
    {Units::kKiloBytesPerSecond, "KiloBytes/Second"},
    {Units::kMegaBytesPerSecond, "MegaBytes/Second"},
    {Units::kGigaBytesPerSecond, "GigaBytes/Second"},
};

static size_t DumpNames(ValkeyModuleCtx* ctx,
                        const vmsdk::module::Options& options,
                        const Base* field) {
  VMSDK_LOG(WARNING, ctx) << "Dumping Info Field: " << field->GetSection()
                          << "/" << field->GetName();
  ValkeyModule_ReplyWithCString(ctx, "Section");
  ValkeyModule_ReplyWithCString(ctx, field->GetSection().data());
  std::string external_name = options.name + "." + field->GetName();
  ValkeyModule_ReplyWithCString(ctx, "Name");
  ValkeyModule_ReplyWithCString(ctx, external_name.data());
  return 4;
}

//
// FT._DEBUG SHOW_INFO [METADATA]
//
// If Metadata is specified, then all fields are dumped.
// If Metadata is missing then only fields with values are dumped.
//
absl::Status ShowInfo(ValkeyModuleCtx* ctx, vmsdk::ArgsIterator& itr,
                      const vmsdk::module::Options& options) {
  struct ShowParameters {
    bool metadata_;
  } cmd;
  vmsdk::KeyValueParser<ShowParameters> parser;
  parser.AddParamParser("METADATA",
                        GENERATE_FLAG_PARSER(ShowParameters, metadata_));
  VMSDK_RETURN_IF_ERROR(parser.Parse(cmd, itr));
  if (itr.DistanceEnd() > 0) {
    return absl::InvalidArgumentError("Too many parameters");
  }
  ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
  size_t num_infos = 0;
  SectionMap& section_map = GetSectionMap();
  for (const auto& [section, section_info] : section_map) {
    for (const auto& [name, field] : section_info.fields_) {
      if (cmd.metadata_ || field->IsVisible()) {
        num_infos++;
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_ARRAY_LEN);
        size_t response_length = DumpNames(ctx, options, field);
        if (cmd.metadata_) {
          ValkeyModule_ReplyWithCString(ctx, "Units");
          ValkeyModule_ReplyWithCString(
              ctx, kUnitsToString[field->GetUnits()].data());
          ValkeyModule_ReplyWithCString(ctx, "Application");
          ValkeyModule_ReplyWithBool(ctx, field->IsApplication());
          ValkeyModule_ReplyWithCString(ctx, "Cumulative");
          ValkeyModule_ReplyWithBool(ctx, field->IsCumulative());
          response_length += 6;
        }
        if (field->IsVisible()) {
          ValkeyModule_ReplyWithCString(ctx, "Value");
          field->Reply(ctx);
          response_length += 2;
        }
        ValkeyModule_ReplySetArrayLength(ctx, response_length);
      }
    }
  }
  ValkeyModule_ReplySetArrayLength(ctx, num_infos);
  return absl::OkStatus();
}

}  // namespace info_field
}  // namespace vmsdk
