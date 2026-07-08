/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/punctuation_segmenter.h"

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "src/utils/scanner.h"

namespace valkey_search::indexes::text {

PunctuationSegmenter::PunctuationSegmenter(const std::string &punctuation)
    : punct_set_(BuildPunctuationSet(punctuation)) {}

PunctuationSegmenter::PunctuationSegmenter(PunctuationSet punct_set)
    : punct_set_(std::move(punct_set)) {}

absl::StatusOr<std::vector<std::string>> PunctuationSegmenter::Segment(
    absl::string_view text) const {
  if (!utils::Scanner::IsValidUtf8(text)) {
    return absl::InvalidArgumentError("Invalid UTF-8");
  }

  std::vector<std::string> tokens;
  std::string word;
  word.reserve(64);
  size_t pos = 0;

  while (pos < text.size()) {
    // Skip leading delimiters (code-point aware)
    while (pos < text.size()) {
      if (text[pos] == '\\' && pos + 1 < text.size()) break;
      utils::Scanner s(text.substr(pos));
      auto cp = s.NextUtf8();
      CHECK(cp != utils::Scanner::kInvalidCp)
          << "Segment decoded invalid UTF-8 after IsValidUtf8 passed";
      if (!IsDelimiter(cp)) break;
      pos += s.LastUtf8ByteLen();
    }

    word.clear();

    // Build word until next delimiter boundary
    while (pos < text.size()) {
      // Handle backslash escape
      if (text[pos] == '\\' && pos + 1 < text.size()) {
        pos++;
        utils::Scanner s(text.substr(pos));
        auto esc_cp = s.NextUtf8();
        CHECK(esc_cp != utils::Scanner::kInvalidCp)
            << "Segment decoded invalid UTF-8 after IsValidUtf8 passed";
        uint8_t esc_len = s.LastUtf8ByteLen();
        // If the escaped char is not a backslash and not a delimiter,
        // but backslash itself is a delimiter, break to new token.
        if (esc_cp != '\\' && !IsDelimiter(esc_cp) && IsDelimiter('\\')) {
          break;
        }
        // Otherwise, include the escaped character literally
        word.append(text.data() + pos, esc_len);
        pos += esc_len;
        continue;
      }

      utils::Scanner s(text.substr(pos));
      auto cp = s.NextUtf8();
      CHECK(cp != utils::Scanner::kInvalidCp)
          << "Segment decoded invalid UTF-8 after IsValidUtf8 passed";
      if (IsDelimiter(cp)) break;
      uint8_t len = s.LastUtf8ByteLen();
      word.append(text.data() + pos, len);
      pos += len;
    }

    if (!word.empty()) {
      tokens.push_back(std::move(word));
      word.clear();
    }
  }
  return tokens;
}

bool PunctuationSegmenter::IsDelimiter(uint32_t cp) const {
  return punct_set_.Contains(cp);
}

}  // namespace valkey_search::indexes::text
