/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/regex_util.h"

#include <memory>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "re2/re2.h"

namespace valkey_search {
namespace utils {

absl::StatusOr<Regex> Regex::Compile(absl::string_view pattern) {
  RE2::Options options;
  options.set_log_errors(false);
  auto re = std::make_unique<RE2>(pattern, options);
  if (!re->ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid regular expression: ", re->error()));
  }
  return Regex(std::move(re));
}

Regex::Regex(std::unique_ptr<re2::RE2> re) : re_(std::move(re)) {}
Regex::Regex(Regex &&) noexcept = default;
Regex &Regex::operator=(Regex &&) noexcept = default;
Regex::~Regex() = default;

bool Regex::PartialMatch(absl::string_view text) const {
  return RE2::PartialMatch(text, *re_);
}

}  // namespace utils
}  // namespace valkey_search
