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

#ifndef VMSDK_SRC_TYPE_CONVERSIONS_H_
#define VMSDK_SRC_TYPE_CONVERSIONS_H_

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <string>
#include <system_error>  // NOLINT(build/c++11)

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {

template <typename T>
inline void VerifyUpperCase(
    [[maybe_unused]] const absl::flat_hash_map<absl::string_view, T> &map) {
#ifdef DEBUG
  for (const auto &[key, _] : map) {
    CHECK(!std::any_of(key.begin(), key.end(),
                       [](unsigned char c) { return std::islower(c); }));
  }
#endif
}

template <typename T>
inline absl::StatusOr<T> To(absl::string_view str) {
  CHECK(false) << "Unimplemented";
}

template <>
inline absl::StatusOr<absl::string_view> To(absl::string_view str) {
  return str;
}

template <>
inline absl::StatusOr<std::string> To(absl::string_view str) {
  return std::string(str);
}

static inline bool IsNumeric(absl::string_view str) {
  return std::all_of(str.begin(), str.end(), [](char c) {
    return c == '-' || std::isdigit(static_cast<unsigned char>(c)) != 0;
  });
}

template <typename T>
static inline absl::StatusOr<T> ToNumeric(absl::string_view str) {
  T result;
  auto [p, ec] = std::from_chars(str.data(), str.data() + str.size(), result);
  if (ec == std::errc()) {
    return result;
  }
  if (IsNumeric(str)) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", str, "` is outside acceptable bounds"));
  }
  return absl::InvalidArgumentError(
      absl::StrCat("`", str, "` is not a valid numeric value"));
}

// Evaluate if the implementation could rely on ToNumeric
template <>
inline absl::StatusOr<float> To(absl::string_view str) {
  float value;
  if (absl::AsciiStrToLower(str) == "nan" || !absl::SimpleAtof(str, &value)) {
    return absl::InvalidArgumentError(
        absl::StrCat(str, " is not a valid float"));
  }
  return value;
}

template <>
inline absl::StatusOr<int> To(absl::string_view str) {
  return ToNumeric<int>(str);
}

inline absl::string_view ToStringView(const RedisModuleString *str) {
  if (!str) {
    return {};
  }
  size_t length = 0;
  const char *str_ptr = RedisModule_StringPtrLen(str, &length);
  return {str_ptr, length};
}

template <typename T>
inline absl::StatusOr<T> To(const RedisModuleString *str) {
  return To<T>(ToStringView(str));
}

template <>
inline absl::StatusOr<bool> To(absl::string_view str) {
  return str == "true";
}

template <>
inline absl::StatusOr<uint64_t> To(absl::string_view str) {
  return ToNumeric<uint64_t>(str);
}

template <>
inline absl::StatusOr<long long> To(absl::string_view str) {
  return ToNumeric<long long>(str);
}

template <>
inline absl::StatusOr<double> To(absl::string_view str) {
  double value;
  if (absl::AsciiStrToLower(str) == "nan" || !absl::SimpleAtod(str, &value)) {
    return absl::InvalidArgumentError(
        absl::StrCat(str, " is not a valid double"));
  }
  return value;
}

template <>
inline absl::StatusOr<uint32_t> To(absl::string_view str) {
  return ToNumeric<uint32_t>(str);
}

template <>
inline absl::StatusOr<uint16_t> To(absl::string_view str) {
  return ToNumeric<uint16_t>(str);
}

template <typename T>
inline absl::StatusOr<T> ToEnum(
    absl::string_view val,
    const absl::flat_hash_map<absl::string_view, T> &map) {
  if (val.empty()) {
    return absl::InvalidArgumentError("Argument value is empty");
  }
  auto it = map.find(val);
  if (it == map.end()) {
    it = map.find(absl::AsciiStrToUpper(val));
    if (it == map.end()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Unknown argument `", val, "`"));
    }
  }
  return it->second;
}

template <typename T>
inline absl::StatusOr<T> ToEnum(
    const RedisModuleString *param,
    const absl::flat_hash_map<absl::string_view, T> &map) {
  if (!param) {
    return absl::InvalidArgumentError("unexpected nullptr");
  }
  return ToEnum(ToStringView(param), map);
}
}  // namespace vmsdk

#endif  // VMSDK_SRC_TYPE_CONVERSIONS_H_
