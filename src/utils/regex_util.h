/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_REGEX_UTIL_H_
#define VALKEYSEARCH_SRC_UTILS_REGEX_UTIL_H_

#include <memory>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace re2 {
class RE2;
}  // namespace re2

namespace valkey_search {
namespace utils {

class Regex {
 public:
  // Returns InvalidArgument if the pattern doesn't compile.
  static absl::StatusOr<Regex> Compile(absl::string_view pattern);

  Regex(Regex &&) noexcept;
  Regex &operator=(Regex &&) noexcept;
  ~Regex();

  // Returns true if the pattern matches any substring of `text`.
  bool PartialMatch(absl::string_view text) const;

 private:
  explicit Regex(std::unique_ptr<re2::RE2> re);

  std::unique_ptr<re2::RE2> re_;
};

}  // namespace utils
}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_REGEX_UTIL_H_
