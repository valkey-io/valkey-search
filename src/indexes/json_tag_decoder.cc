/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/json_tag_decoder.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "vmsdk/src/utils.h"

namespace valkey_search::indexes {

namespace {

class JsonScanner {
 public:
  explicit JsonScanner(absl::string_view input) : input_(input) {}

  bool Consume(absl::string_view token) {
    SkipWhitespace();
    return absl::ConsumePrefix(&input_, token);
  }

  bool AtEnd() {
    SkipWhitespace();
    return input_.empty();
  }

  // The text of the next value, skipped as a whole.
  std::optional<absl::string_view> RawValue() {
    SkipWhitespace();
    const char *begin = input_.data();
    int depth = 0;
    while (!input_.empty()) {
      char c = input_.front();
      if (c == '"') {
        if (!String().has_value()) {
          return std::nullopt;
        }
        continue;
      }
      if (c == '[' || c == '{') {
        ++depth;
      } else if (c == ']' || c == '}') {
        if (depth == 0) {
          break;
        }
        --depth;
      } else if (c == ',' && depth == 0) {
        break;
      }
      input_.remove_prefix(1);
    }
    absl::string_view raw(begin, input_.data() - begin);
    raw = absl::StripTrailingAsciiWhitespace(raw);
    if (depth != 0 || raw.empty()) {
      return std::nullopt;
    }
    return raw;
  }

  std::optional<std::string> String() {
    if (!Consume("\"")) {
      return std::nullopt;
    }
    for (size_t i = 0; i < input_.size(); ++i) {
      if (input_[i] == '\\') {
        ++i;
      } else if (input_[i] == '"') {
        auto value = vmsdk::JsonUnquote(input_.substr(0, i));
        input_.remove_prefix(i + 1);
        return value;
      }
    }
    return std::nullopt;
  }

 private:
  void SkipWhitespace() {
    while (!input_.empty() && absl::ascii_isspace(input_.front())) {
      input_.remove_prefix(1);
    }
  }

  absl::string_view input_;
};

void AddTags(std::string value, std::optional<char> separator,
             std::vector<std::string> &tags) {
  if (!separator.has_value()) {
    if (!value.empty()) {
      tags.push_back(std::move(value));
    }
    return;
  }
  for (absl::string_view piece : absl::StrSplit(value, *separator)) {
    piece = absl::StripAsciiWhitespace(piece);
    if (!piece.empty()) {
      tags.emplace_back(piece);
    }
  }
}

// A string or null.
bool ParseScalar(JsonScanner &scanner, std::optional<char> separator,
                 std::vector<std::string> &tags) {
  if (scanner.Consume("null")) {
    return true;
  }
  auto value = scanner.String();
  if (!value.has_value()) {
    return false;
  }
  AddTags(*std::move(value), separator, tags);
  return true;
}

JsonTags Invalid() { return {.status = JsonTags::Status::kInvalid}; }

}  // namespace

JsonTags DecodeJsonTags(absl::string_view json, std::optional<char> separator) {
  JsonScanner scanner(json);
  // Only a legacy (non-`$`) path yields anything but a match array.
  if (!scanner.Consume("[")) {
    return {.status = JsonTags::Status::kMissing};
  }
  if (scanner.Consume("]")) {
    return scanner.AtEnd() ? JsonTags{.status = JsonTags::Status::kMissing}
                           : Invalid();
  }
  JsonTags result;
  size_t matches = 0;
  bool non_string_match = false;
  do {
    ++matches;
    if (scanner.Consume("true")) {
      result.tags.emplace_back("true");
      non_string_match = true;
    } else if (scanner.Consume("false")) {
      result.tags.emplace_back("false");
      non_string_match = true;
    } else if (scanner.Consume("[")) {
      non_string_match = true;
      if (!scanner.Consume("]")) {
        do {
          if (!ParseScalar(scanner, separator, result.tags)) {
            return Invalid();
          }
        } while (scanner.Consume(","));
        if (!scanner.Consume("]")) {
          return Invalid();
        }
      }
    } else if (!ParseScalar(scanner, separator, result.tags)) {
      return Invalid();
    }
  } while (scanner.Consume(","));
  if (!scanner.Consume("]") || !scanner.AtEnd() ||
      (matches > 1 && non_string_match)) {
    return Invalid();
  }
  return result;
}

JsonFirstMatch DecodeJsonFirstMatch(absl::string_view json) {
  JsonScanner scanner(json);
  if (!scanner.Consume("[") || scanner.Consume("]")) {
    return {.status = JsonTags::Status::kMissing};
  }
  if (scanner.Consume("null")) {
    return {.value = std::nullopt};
  }
  if (scanner.Consume("true")) {
    return {.value = "1"};
  }
  if (scanner.Consume("false")) {
    return {.value = "0"};
  }
  auto raw = scanner.RawValue();
  if (!raw.has_value()) {
    return {.status = JsonTags::Status::kInvalid};
  }
  if (raw->front() != '"') {
    return {.value = std::string(*raw)};
  }
  auto value = vmsdk::JsonUnquote(raw->substr(1, raw->size() - 2));
  if (!value.has_value()) {
    return {.status = JsonTags::Status::kInvalid};
  }
  return {.value = std::move(value)};
}

}  // namespace valkey_search::indexes
