/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
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

template <>
inline absl::StatusOr<std::pmr::string> To(absl::string_view str) {
  return std::pmr::string(str);
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

inline absl::string_view ToStringView(const ValkeyModuleString *str) {
  if (!str) {
    return {};
  }
  size_t length = 0;
  const char *str_ptr = ValkeyModule_StringPtrLen(str, &length);
  return {str_ptr, length};
}

template <typename T>
inline absl::StatusOr<T> To(const ValkeyModuleString *str) {
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

#if defined(__clang__) && !defined(RunningClangd)
template <>
inline absl::StatusOr<unsigned long> To(absl::string_view str) {
  return ToNumeric<uint64_t>(str);
}
#endif

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
    const ValkeyModuleString *param,
    const absl::flat_hash_map<absl::string_view, T> &map) {
  if (!param) {
    return absl::InvalidArgumentError("unexpected nullptr");
  }
  return ToEnum(ToStringView(param), map);
}
}  // namespace vmsdk

#endif  // VMSDK_SRC_TYPE_CONVERSIONS_H_
