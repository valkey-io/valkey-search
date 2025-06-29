/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#include "vmsdk/src/module_config.h"

#include <absl/base/no_destructor.h>

#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"

namespace vmsdk {
namespace config {

namespace {

constexpr absl::string_view kUseCoordinator = "--use-coordinator";

template <typename T>
static T OnGetConfig(const char *config_name, void *priv_data) {
  auto entry = static_cast<ConfigBase<T> *>(priv_data);
  CHECK(entry) << "null private data for Boolean configuration entry.";
  return static_cast<int>(entry->GetValue());
}

template <typename T>
static int OnSetConfig(const char *config_name, T value, void *priv_data,
                       ValkeyModuleString **err) {
  auto entry = static_cast<ConfigBase<T> *>(priv_data);
  CHECK(entry) << "null private data for configuration Number entry.";
  auto res = entry->SetValue(value);  // Calls "Validate" internally
  if (!res.ok()) {
    if (err) {
      *err =
          ValkeyModule_CreateStringPrintf(nullptr, "%s", res.message().data());
    }
    return VALKEYMODULE_ERR;
  }

  entry->NotifyChanged();
  return VALKEYMODULE_OK;
}

/// Convert `vector<string>` -> `vector<const char*>`
/// IMPORTANT: `vec` must outlive the returned value
std::vector<const char *> ToCharPtrPtrVec(const std::vector<std::string> &vec) {
  std::vector<const char *> arrptr;
  arrptr.reserve(vec.size());
  for (const auto &item : vec) {
    arrptr.push_back(item.c_str());
  }
  return arrptr;
}
}  // namespace

ModuleConfigManager &ModuleConfigManager::Instance() {
  static ModuleConfigManager module_config_manager;
  return module_config_manager;
}

void ModuleConfigManager::RegisterConfig(Registerable *config_item) {
  entries_.insert({std::string{config_item->GetName()}, config_item});
}

void ModuleConfigManager::UnregisterConfig(Registerable *config_item) {
  entries_.erase(config_item->GetName());
}

absl::Status ModuleConfigManager::Init(ValkeyModuleCtx *ctx) {
  for (const auto &[_, entry] : entries_) {
    if (entry->IsHidden()) {
      continue;
    }
    VMSDK_RETURN_IF_ERROR(entry->Register(ctx));
  }
  return absl::OkStatus();
}

absl::Status ModuleConfigManager::ParseAndLoadArgv(ValkeyModuleCtx *ctx,
                                                   ValkeyModuleString **argv,
                                                   int argc) {
  vmsdk::ArgsIterator iter{argv, argc};
  while (iter.HasNext()) {
    VMSDK_ASSIGN_OR_RETURN(auto key, iter.Get());
    auto sv_key = vmsdk::ToStringView(key);
    iter.Next();  // skip the key

    absl::string_view sv_val;
    if (sv_key == kUseCoordinator) {
      // All keys must accept values. Unless it is `--use-coordinator` (we
      // keep this option for backward compatibility)
      sv_val = "yes";
      VMSDK_RETURN_IF_ERROR(UpdateConfigFromKeyVal(ctx, sv_key, sv_val));
    } else {
      // Read the value
      if (!iter.HasNext()) {
        return absl::NotFoundError(absl::StrFormat(
            "Command line argument `%s` is missing a value", sv_key));
      }
      VMSDK_ASSIGN_OR_RETURN(auto val, iter.Get());
      iter.Next();  // skip the value

      sv_val = vmsdk::ToStringView(val);
      VMSDK_RETURN_IF_ERROR(UpdateConfigFromKeyVal(ctx, sv_key, sv_val));
    }
  }
  return absl::OkStatus();
}

absl::Status ModuleConfigManager::UpdateConfigFromKeyVal(
    ValkeyModuleCtx *ctx, std::string_view key, std::string_view value) {
  if (!key.starts_with("--")) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "Command line argument: '%s' must start with '--'", key));
  }

  // Locate the configuration entry that matches this switch
  // remove the "--"
  key = key.substr(2);
  auto where = entries_.find(key);

  if (where == entries_.end()) {
    return absl::UnknownError(
        absl::StrFormat("Unknown command line argument: `%s`", key));
  }
  // update the configuration entry
  VMSDK_LOG(NOTICE, ctx) << "Parsed command line argument: " << key << " -> "
                         << value;
  VMSDK_RETURN_IF_ERROR(where->second->FromString(value));
  return absl::OkStatus();
}

Number::Number(std::string_view name, int64_t default_value, int64_t min_value,
               int64_t max_value)
    : ConfigBase(name),
      default_value_(default_value),
      min_value_(min_value),
      max_value_(max_value),
      current_value_(default_value) {}

absl::Status Number::Register(ValkeyModuleCtx *ctx) {
  if (ValkeyModule_RegisterNumericConfig(
          ctx,
          name_.data(),            // Name
          default_value_,          // Default value
          flags_,                  // Flags
          min_value_,              // Minimum value
          max_value_,              // Maximum value
          OnGetConfig<long long>,  // Get callback
          OnSetConfig<long long>,  // Set callback
          nullptr,                 // Apply callback (optional)
          this                     // privdata
          ) != VALKEYMODULE_OK) {
    return absl::InternalError(absl::StrCat(
        "Failed to register numeric configuration entry: ", name_));
  }
  return absl::OkStatus();
}

absl::Status Number::FromString(std::string_view value) {
  if (!absl::SimpleAtoi(value, &default_value_)) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Failed to convert '%s' into a number", value));
  }
  SetValueOrLog(default_value_, WARNING);
  return absl::OkStatus();
}

Enum::Enum(std::string_view name, int default_value,
           const std::vector<std::string_view> &names,
           const std::vector<int> &values)
    : ConfigBase(name),
      default_value_(default_value),
      values_(values),
      current_value_(default_value) {
  // Convert all names into lower-case
  names_.reserve(names.size());
  for (auto name : names) {
    auto lower_case_name = absl::AsciiStrToLower(name);
    names_.push_back(lower_case_name);
  }

  // Validate the values
  CHECK(values_.size() == names_.size());
  CHECK(std::find_if(values_.begin(), values_.end(), [&default_value](int v) {
          return default_value == v;
        }) != values_.end());
}

absl::Status Enum::Register(ValkeyModuleCtx *ctx) {
  auto names_array = ToCharPtrPtrVec(names_);
  if (ValkeyModule_RegisterEnumConfig(ctx,
                                      name_.data(),        // Name
                                      default_value_,      // Default value
                                      flags_,              // Flags
                                      names_array.data(),  // enumerator names
                                      values_.data(),      // enumerator values
                                      names_.size(),  // number of enumerators
                                      OnGetConfig<int>,  // Get callback
                                      OnSetConfig<int>,  // Set callback
                                      nullptr,  // Apply callback (optional)
                                      this      // privdata
                                      ) != VALKEYMODULE_OK) {
    return absl::InternalError(absl::StrCat(
        "Failed to register enumerator configuration entry: ", name_));
  }
  return absl::OkStatus();
}

absl::Status Enum::FromString(std::string_view value) {
  auto where = std::find_if(
      names_.begin(), names_.end(), [&value](const std::string &name) {
        return value == name || absl::AsciiStrToLower(value) == name;
      });
  if (where == names_.end()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Invalid enum value: '%s'", value));
  }

  size_t enumerator_index = std::distance(names_.begin(), where);
  default_value_ = values_[enumerator_index];

  // update the current value, skip the validation as we just did that
  SetValueOrLog(default_value_, WARNING);
  return absl::OkStatus();
}

Boolean::Boolean(std::string_view name, bool default_value)
    : ConfigBase(name),
      default_value_(default_value),
      current_value_(default_value) {}

absl::Status Boolean::Register(ValkeyModuleCtx *ctx) {
  if (ValkeyModule_RegisterBoolConfig(ctx,
                                      name_.data(),      // Name
                                      default_value_,    // Default value
                                      flags_,            // Flags
                                      OnGetConfig<int>,  // Get callback
                                      OnSetConfig<int>,  // Set callback
                                      nullptr,  // Apply callback (optional)
                                      this      // privdata
                                      ) != VALKEYMODULE_OK) {
    return absl::InternalError(absl::StrCat(
        "Failed to register boolean configuration entry: ", name_));
  }
  return absl::OkStatus();
}

absl::Status Boolean::FromString(std::string_view value) {
  auto value_lowercase = absl::AsciiStrToLower(value);
  if (value_lowercase == "yes" || value_lowercase == "on" ||
      value_lowercase == "true") {
    default_value_ = true;
    SetValueOrLog(default_value_, WARNING);
  } else if (value_lowercase == "no" || value_lowercase == "off" ||
             value_lowercase == "false") {
    default_value_ = false;
    SetValueOrLog(default_value_, WARNING);
  } else {
    return absl::InvalidArgumentError(
        absl::StrFormat("Invalid boolean value: '%s'", value));
  }
  return absl::OkStatus();
}

}  // namespace config
}  // namespace vmsdk
