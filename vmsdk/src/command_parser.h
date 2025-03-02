/*
 * Copyright (c) 2025, ValkeySearch contributors
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

#ifndef VMSDK_SRC_PARAM_PARSER_H_
#define VMSDK_SRC_PARAM_PARSER_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/functional/any_invocable.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

class ArgsIterator {
 public:
  ArgsIterator(RedisModuleString **argv, int argc) : argv_(argv), argc_(argc) {}
  absl::StatusOr<ArgsIterator> SubIterator(int distance) {
    if (distance <= 0) {
      return absl::InvalidArgumentError("distance must be positive");
    }
    if (itr_ + distance > argc_) {
      return absl::OutOfRangeError("Missing argument");
    }
    return ArgsIterator(argv_ + itr_, distance);
  }
  absl::StatusOr<RedisModuleString *> Get() {
    if (itr_ >= argc_) {
      return absl::OutOfRangeError("Missing argument");
    }
    return argv_[itr_];
  }

  ArgsIterator &Next(int steps = 1) {
    itr_ += steps;
    return *this;
  }

  absl::StatusOr<RedisModuleString *> PopNext(int steps = 1) {
    VMSDK_ASSIGN_OR_RETURN(auto current, Get());
    Next(steps);
    return current;
  }

  bool PopIfNextIgnoreCase(absl::string_view word) {
    auto next = Get();
    if (!next.ok() || !absl::EqualsIgnoreCase(vmsdk::ToStringView(next.value()), word)) {
      return false;
    } else {
      itr_ ++;
      return true;
    }
  }

  int DistanceEnd() { return argc_ > itr_ ? argc_ - itr_ : 0; }
  int DistanceStart() { return argc_ - DistanceEnd(); }
  bool HasNext() { return DistanceEnd() > 0; }
  int Position() const { return itr_; }

 private:
  int itr_{0};
  RedisModuleString **argv_;
  int argc_;
};

template <typename T>
inline absl::Status ParseParamValue(ArgsIterator &itr, T &value) {
  VMSDK_ASSIGN_OR_RETURN(auto value_rs, itr.Get());
  VMSDK_ASSIGN_OR_RETURN(value, vmsdk::To<T>(value_rs));
  itr.Next();
  return absl::OkStatus();
}

template <typename T>
inline absl::Status ParseParamValue(ArgsIterator &itr,
                                    std::optional<T> &value) {
  VMSDK_ASSIGN_OR_RETURN(auto value_rs, itr.Get());
  VMSDK_ASSIGN_OR_RETURN(value, vmsdk::To<T>(value_rs));
  itr.Next();
  return absl::OkStatus();
}

inline absl::Status ParseParamValue(ArgsIterator &itr,
                                    UniqueRedisString &value) {
  VMSDK_ASSIGN_OR_RETURN(auto value_rs, itr.Get());
  value = RetainUniqueRedisString(value_rs);
  itr.Next();
  return absl::OkStatus();
}

inline absl::StatusOr<bool> IsParamKeyMatch(absl::string_view key,
                                            bool mandatory, ArgsIterator &itr) {
  if (!mandatory && itr.DistanceEnd() == 0) {
    return false;
  }
  VMSDK_ASSIGN_OR_RETURN(auto key_in, itr.Get());
  auto key_in_str = vmsdk::ToStringView(key_in);
  if (!absl::EqualsIgnoreCase(key_in_str, key)) {
    if (mandatory) {
      return absl::InvalidArgumentError(
          absl::StrCat("Unknown argument `", key_in_str, "` at position ",
                       itr.DistanceStart()));
    }
    return false;
  }
  itr.Next();
  return true;
}

template <typename T>
inline absl::StatusOr<bool> ParseParam(absl::string_view key, bool mandatory,
                                       ArgsIterator &itr, T &value) {
  VMSDK_ASSIGN_OR_RETURN(auto res, IsParamKeyMatch(key, mandatory, itr));
  if (!res) {
    return false;
  }
  VMSDK_RETURN_IF_ERROR(ParseParamValue(itr, value)).SetPrepend()
      << "Bad arguments for " << key << ": ";
  return true;
}

template <typename T>
inline absl::StatusOr<bool> ParseParam(
    absl::string_view key, bool mandatory, ArgsIterator &itr, T &value,
    const absl::flat_hash_map<absl::string_view, T> &enum_by_str) {
  absl::string_view tmp;
  VMSDK_ASSIGN_OR_RETURN(auto res, ParseParam(key, mandatory, itr, tmp));
  if (res) {
    VMSDK_ASSIGN_OR_RETURN(value, ToEnum<T>(tmp, enum_by_str),
                           _.SetPrepend()
                               << "Bad arguments for " << key << ": ");
  }
  return res;
}

template <typename T>
class ParamParser {
 public:
  explicit ParamParser(
      absl::AnyInvocable<absl::Status(T &, ArgsIterator &) const> parse_fn)
      : parse_fn_(std::move(parse_fn)) {}

  absl::Status Parse(T &value, ArgsIterator &itr) const {
    return parse_fn_(value, itr);
  }

 private:
  absl::AnyInvocable<absl::Status(T &, ArgsIterator &) const> parse_fn_;
};

template <typename T>
absl::Status ParseEnumParam(
    T &value, ArgsIterator &itr,
    const absl::flat_hash_map<absl::string_view, T> *enum_by_str) {
  absl::string_view str;
  VMSDK_RETURN_IF_ERROR(ParseParamValue(itr, str));
  VMSDK_ASSIGN_OR_RETURN(value, ToEnum<T>(str, *enum_by_str));
  return absl::OkStatus();
}

#define GENERATE_VALUE_PARSER(type, field_name)                        \
  std::make_unique<::vmsdk::ParamParser<type>>(                        \
      [](type &value, ::vmsdk::ArgsIterator &itr) -> absl::Status {    \
        VMSDK_RETURN_IF_ERROR(ParseParamValue(itr, value.field_name)); \
        return absl::OkStatus();                                       \
      })

#define GENERATE_ENUM_PARSER(type, field_name, enum_by_str)            \
  std::make_unique<::vmsdk::ParamParser<type>>(                        \
      [enum_by_str_ptr = &enum_by_str](                                \
          type &value, ::vmsdk::ArgsIterator &itr) -> absl::Status {   \
        return ParseEnumParam(value.field_name, itr, enum_by_str_ptr); \
      })

#define GENERATE_FLAG_PARSER(type, field_name)                      \
  std::make_unique<::vmsdk::ParamParser<type>>(                     \
      [](type &value, ::vmsdk::ArgsIterator &itr) -> absl::Status { \
        value.field_name = true;                                    \
        return absl::OkStatus();                                    \
      })

template <typename T>
class KeyValueParser {
 public:
  void AddParamParser(absl::string_view param,
                      std::unique_ptr<ParamParser<T>> parser) {
#ifdef DEBUG
    // Verify that the parameter name is in upper case.
    CHECK(!std::any_of(param.begin(), param.end(),
                       [](unsigned char c) { return std::islower(c); }));
#endif
    param_parsers_[std::string(param)] = std::move(parser);
  }
  absl::Status Parse(T &value, ArgsIterator &itr,
                     bool error_on_unknown_param = true) const {
    while (itr.DistanceEnd() > 0) {
      VMSDK_ASSIGN_OR_RETURN(auto key_rs, itr.Get());
      auto key = ToStringView(key_rs);
      auto param_parser_itr = param_parsers_.find(key);
      if (param_parser_itr == param_parsers_.end()) {
        // Late uppercase the input string is an optimization.
        param_parser_itr = param_parsers_.find(absl::AsciiStrToUpper(key));
        if (param_parser_itr == param_parsers_.end()) {
          if (error_on_unknown_param) {
            return absl::InvalidArgumentError(
                absl::StrCat("Unexpected argument `", key, "`"));
          }
          return absl::OkStatus();
        }
      }
      itr.Next();
      VMSDK_RETURN_IF_ERROR(param_parser_itr->second->Parse(value, itr))
              .SetPrepend()
          << "Error parsing value for the parameter `" << key << "` - ";
    }
    return absl::OkStatus();
  }

 private:
  absl::flat_hash_map<std::string, std::unique_ptr<ParamParser<T>>>
      param_parsers_;
};

template <typename T>
inline absl::StatusOr<bool> ParseParam(absl::string_view key, bool mandatory,
                                       ArgsIterator &itr,
                                       const ParamParser<T> &value_parser,
                                       T &value) {
  VMSDK_ASSIGN_OR_RETURN(auto res, IsParamKeyMatch(key, mandatory, itr));
  if (!res) {
    return false;
  }
  VMSDK_RETURN_IF_ERROR(value_parser.Parse(value, itr)).SetPrepend()
      << "Bad arguments for " << key << ": ";
  return true;
}

}  // namespace vmsdk
#endif  // VMSDK_SRC_PARAM_PARSER_H_
