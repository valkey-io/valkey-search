/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_STATUS_SOURCE_LOCATION_H_
#define VMSDK_SRC_STATUS_SOURCE_LOCATION_H_

#include <cstdint>

namespace vmsdk {

// Class representing a specific location in the source code of a program.
// SourceLocation is copyable.
class SourceLocation {
 public:
  // Avoid this constructor; it populates the object with placeholder values.
  constexpr SourceLocation() : line_(0), file_name_(nullptr) {}

  // Wrapper to invoke the private constructor below. This should only be
  // used by the AIS_LOC macro, hence the name.
  static constexpr SourceLocation DoNotInvokeDirectly(std::uint_least32_t line,
                                                      const char* file_name) {
    return SourceLocation(line, file_name);
  }

  static constexpr SourceLocation current(
      std::uint_least32_t line = __builtin_LINE(),
      const char* file_name = __builtin_FILE()) {
    return SourceLocation(line, file_name);
  }

  // The line number of the captured source location.
  constexpr std::uint_least32_t line() const { return line_; }

  // The file name of the captured source location.
  constexpr const char* file_name() const { return file_name_; }

  // column() and function_name() are omitted because we don't have a
  // way to support them.

 private:
  // Do not invoke this constructor directly. Instead, use the
  // AIS_LOC macro below.
  //
  // file_name must outlive all copies of the SourceLocation
  // object, so in practice it should be a std::string literal.
  constexpr SourceLocation(std::uint_least32_t line, const char* file_name)
      : line_(line), file_name_(file_name) {}

  std::uint_least32_t line_;
  const char* file_name_;
};

}  // namespace vmsdk

// If a function takes a SourceLocation parameter, pass this as the argument.
#define VMSDK_STREAMS_LOC \
  vmsdk::SourceLocation::DoNotInvokeDirectly(__LINE__, __FILE__)

#define VMSDK_LOC_CURRENT_DEFAULT_ARG = vmsdk::SourceLocation::current()

#endif  // VMSDK_SRC_STATUS_SOURCE_LOCATION_H_
