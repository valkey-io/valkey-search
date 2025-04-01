/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "vmsdk/src/configurable.h"

#include "absl/strings/str_cat.h"

namespace vmsdk::config {

std::unique_ptr<std::map<std::string, ConfigurableBase *>>
    ConfigurableBase::bases;
bool ConfigurableBase::Initialized = false;

void ConfigurableBase::Reset() {
  Initialized = false;
  bases.reset();
}

absl::Status ConfigurableBase::OnStartup(RedisModuleCtx *ctx) {
  CHECK(!Initialized);
  CHECK(bases);
  for (auto &[name, base] : *bases) {
    int result = base->Register(ctx);
    if (result != REDISMODULE_OK) {
      std::string code;
      switch (result) {
        case EBUSY:
          code = "Internal Error";
          break;
        case EINVAL:
          code =
              "Invalid character in configurable name or invalid Flags "
              "combination";
          break;
        case EALREADY:
          code = "Internal Error, duplicate";
          break;
        default:
          code = "Unknown error code: " + std::to_string(result);
          break;
      }
      return absl::InvalidArgumentError(absl::StrCat(
          "Unable to register configurable '", name, "' Error code: ", code));
    }
    base->SetDefault();
  }
  auto result = RedisModule_LoadConfigs(ctx);
  Initialized = true;
  return (result == REDISMODULE_OK)
             ? absl::OkStatus()
             : absl::InvalidArgumentError(std::to_string(result));
}

static std::string FlagsToString(Flags flags) {
  std::string result;
  if (flags != Flags::kDefault) {
    result += '[';
    bool did_one = false;
#define DOFLAG(name)              \
  {                               \
    if (flags & Flags::k##name) { \
      if (did_one) result += ','; \
      did_one = true;             \
      result += #name;            \
    }                             \
  }
    DOFLAG(Immutable)
    DOFLAG(Sensitive)
    DOFLAG(Hidden)
    DOFLAG(Protected)
    DOFLAG(DenyLoading)
    DOFLAG(Memory)
    DOFLAG(BitFlags)
#undef DOFLAG
    result += ']';
  }
  return result;
}

std::string Enum::ToString() const {
  if (!(GetFlags() & Flags::kBitFlags)) {
    for (size_t i = 0; i < values_.size(); ++i) {
      if (valkey_value_ == values_[i]) {
        return names_[i];
      }
    }
    return "*Invalid*";
  } else {
    std::string result;
    result += '{';
    bool did_one = false;
    int all_values = 0;
    for (size_t i = 0; i < values_.size(); ++i) {
      all_values |= values_[i];
      if (c_value_ & values_[i]) {
        if (did_one) {
          result += '+';
        }
        did_one = true;
        result += names_[i];
      }
    }
    if (!(all_values & c_value_)) {
      // Extra bits set
      result += "Extra: ";
      result += std::to_string(c_value_ & ~all_values);
    }
    result += '}';
    return result;
  }
}

static const char *redacted = "**__redacted__**";

void ConfigurableBase::DumpAll(std::ostream &os) {
  size_t name_len = 0;
  size_t value_len = strlen(redacted);
  for (auto &[name, base] : *bases) {
    name_len = std::max(name_len, name.size());
    value_len = std::max(value_len, base->ToString().size());
  }
  for (auto &[name, base] : *bases) {
    os << std::setw(name_len) << name << ':' << std::setw(value_len);
    if (base->GetFlags() & Flags::kSensitive) {
      os << redacted;
    } else {
      os << base->ToString();
    }
    os << FlagsToString(base->GetFlags()) << "\n";
  }
}

std::map<std::string, std::pair<std::string, std::string>>
ConfigurableBase::GetAllAsMap() {
  std::map<std::string, std::pair<std::string, std::string>> result;
  for (auto &[name, base] : *bases) {
    result[name] = std::make_pair(
        (base->GetFlags() & Flags::kSensitive) ? redacted : base->ToString(),
        FlagsToString(base->GetFlags()));
  }
  return result;
}

};  // namespace vmsdk::config