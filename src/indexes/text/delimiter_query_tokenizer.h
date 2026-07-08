/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_DELIMITER_QUERY_TOKENIZER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_DELIMITER_QUERY_TOKENIZER_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/punctuation_segmenter.h"
#include "src/indexes/text/query_tokenizer.h"
#include "src/utils/scanner.h"
#include "vmsdk/src/status/status_macros.h"

namespace valkey_search::indexes::text {

/// Delimiter-based query tokenizer for European/Snowball languages.
///
/// Walks codepoints one at a time, breaking on punctuation delimiters.
/// This contains the exact escape-handling and word-boundary logic that was
/// previously inline in the filter parser's token extraction methods.
///
/// Escape contract (mirrors PunctuationSegmenter::Segment):
///   - `\\` -> include literal `\` in token, continue same token
///   - `\<delimiter>` -> include the delimiter char literally, continue
///   - `\<non-delimiter>` where `\` is a delimiter -> end current token
///   - `\<non-delimiter>` where `\` is NOT a delimiter -> include char,
///   continue
///   - `\` at end of input -> error
///   - `\` + invalid UTF-8 -> replace with U+FFFD, continue
class DelimiterQueryTokenizer : public QueryTokenizer {
 public:
  explicit DelimiterQueryTokenizer(const PunctuationSegmenter& segmenter)
      : segmenter_(segmenter) {}

  absl::StatusOr<std::optional<Token>> NextQuotedToken(
      absl::string_view text, size_t pos) const override {
    if (pos >= text.size()) return std::nullopt;

    // Check if we're at a quote -- don't consume it
    {
      utils::Scanner s(text.substr(pos));
      auto cp = s.NextUtf8();
      if (cp == '"') return std::nullopt;
    }

    std::string content;
    size_t cursor = pos;

    // Skip leading delimiters (but stop on quote, escape, or end)
    while (cursor < text.size()) {
      if (text[cursor] == '"') break;
      if (text[cursor] == '\\') break;  // escape starts token content
      utils::Scanner s(text.substr(cursor));
      auto cp = s.NextUtf8();
      if (cp == utils::Scanner::kEOF) break;
      if (cp == utils::Scanner::kInvalidCp) {
        // Invalid UTF-8 -- replace with U+FFFD and treat as content
        utils::Scanner::PushBackUtf8(content, 0xFFFD);
        cursor += s.LastUtf8ByteLen();
        goto build_quoted_token;
      }
      if (!segmenter_.IsDelimiter(cp)) break;
      cursor += s.LastUtf8ByteLen();
    }

    // Build token until next boundary
  build_quoted_token:
    while (cursor < text.size()) {
      // Check for quote boundary
      if (text[cursor] == '"') break;

      // Check for backslash escape
      if (text[cursor] == '\\') {
        VMSDK_ASSIGN_OR_RETURN(auto esc_result,
                               HandleEscape(text, cursor, content));
        if (esc_result == EscapeResult::kBreakToken) break;
        continue;
      }

      utils::Scanner s(text.substr(cursor));
      auto cp = s.NextUtf8();
      if (cp == utils::Scanner::kEOF) break;
      if (cp == utils::Scanner::kInvalidCp) {
        utils::Scanner::PushBackUtf8(content, 0xFFFD);
        cursor += s.LastUtf8ByteLen();
        continue;
      }
      if (segmenter_.IsDelimiter(cp)) break;
      uint8_t len = s.LastUtf8ByteLen();
      content.append(text.data() + cursor, len);
      cursor += len;
    }

    size_t bytes_consumed = cursor - pos;
    if (bytes_consumed == 0) return std::nullopt;
    return std::make_optional(Token{std::move(content), bytes_consumed});
  }

  absl::StatusOr<std::optional<Token>> NextUnquotedToken(
      absl::string_view text, size_t pos, bool& hit_query_syntax,
      bool (*IsQuerySyntax)(uint32_t cp)) const override {
    hit_query_syntax = false;
    if (pos >= text.size()) return std::nullopt;

    // Check if we're already at a query syntax char
    {
      utils::Scanner s(text.substr(pos));
      auto cp = s.NextUtf8();
      if (cp != utils::Scanner::kEOF && cp != utils::Scanner::kInvalidCp &&
          IsQuerySyntax(cp)) {
        hit_query_syntax = true;
        return std::nullopt;
      }
    }

    std::string content;
    size_t cursor = pos;

    // Skip leading delimiters (but stop on query syntax, escape, or end)
    while (cursor < text.size()) {
      if (text[cursor] == '\\') break;  // escape starts token content
      utils::Scanner s(text.substr(cursor));
      auto cp = s.NextUtf8();
      if (cp == utils::Scanner::kEOF) break;
      if (cp == utils::Scanner::kInvalidCp) {
        utils::Scanner::PushBackUtf8(content, 0xFFFD);
        cursor += s.LastUtf8ByteLen();
        goto build_unquoted_token;
      }
      if (IsQuerySyntax(cp)) {
        hit_query_syntax = true;
        break;
      }
      if (!segmenter_.IsDelimiter(cp)) break;
      cursor += s.LastUtf8ByteLen();
    }

    if (hit_query_syntax) {
      size_t bytes_consumed = cursor - pos;
      if (bytes_consumed == 0) return std::nullopt;
      return std::make_optional(Token{"", bytes_consumed});
    }

    // Build token until next boundary
  build_unquoted_token:
    while (cursor < text.size()) {
      // Peek to check for query syntax
      {
        utils::Scanner s(text.substr(cursor));
        auto cp = s.NextUtf8();
        if (cp == utils::Scanner::kEOF) break;
        if (cp != utils::Scanner::kInvalidCp && IsQuerySyntax(cp)) {
          hit_query_syntax = true;
          break;
        }
      }

      // Check for backslash escape
      if (text[cursor] == '\\') {
        VMSDK_ASSIGN_OR_RETURN(auto esc_result,
                               HandleEscape(text, cursor, content));
        if (esc_result == EscapeResult::kBreakToken) break;
        continue;
      }

      utils::Scanner s(text.substr(cursor));
      auto cp = s.NextUtf8();
      if (cp == utils::Scanner::kEOF) break;
      if (cp == utils::Scanner::kInvalidCp) {
        utils::Scanner::PushBackUtf8(content, 0xFFFD);
        cursor += s.LastUtf8ByteLen();
        continue;
      }
      if (segmenter_.IsDelimiter(cp)) break;
      uint8_t len = s.LastUtf8ByteLen();
      content.append(text.data() + cursor, len);
      cursor += len;
    }

    size_t bytes_consumed = cursor - pos;
    if (bytes_consumed == 0) return std::nullopt;
    return std::make_optional(Token{std::move(content), bytes_consumed});
  }

 private:
  /// Result of handling a backslash escape.
  enum class EscapeResult {
    kContinue,   // Escape handled, continue building token
    kBreakToken  // Backslash was a delimiter before non-delimiter -> end token
  };

  /// Handles a backslash escape sequence at text[cursor].
  /// On kContinue, advances cursor past the consumed bytes and appends to
  /// content. On kBreakToken, cursor is advanced past the backslash only
  /// (the non-delimiter char starts the next token).
  absl::StatusOr<EscapeResult> HandleEscape(absl::string_view text,
                                            size_t& cursor,
                                            std::string& content) const {
    cursor++;  // consume backslash
    if (cursor >= text.size()) {
      return absl::InvalidArgumentError(
          "Invalid escape sequence: backslash at end of input");
    }
    utils::Scanner s(text.substr(cursor));
    auto esc_cp = s.NextUtf8();
    uint8_t esc_len = s.LastUtf8ByteLen();

    if (esc_cp == utils::Scanner::kInvalidCp) {
      utils::Scanner::PushBackUtf8(content, 0xFFFD);
      cursor += esc_len;
      return EscapeResult::kContinue;
    }
    if (esc_cp == '\\' || segmenter_.IsDelimiter(esc_cp)) {
      // Double backslash or escaped delimiter -> keep literally, continue
      content.append(text.data() + cursor, esc_len);
      cursor += esc_len;
      return EscapeResult::kContinue;
    }
    // Backslash before non-delimiter character
    if (segmenter_.IsDelimiter('\\')) {
      // Backslash is itself a delimiter -> token break.
      // Don't consume the escaped char -- it starts the next token.
      return EscapeResult::kBreakToken;
    }
    // Backslash is NOT a delimiter -> keep the non-delimiter char, continue
    content.append(text.data() + cursor, esc_len);
    cursor += esc_len;
    return EscapeResult::kContinue;
  }

  const PunctuationSegmenter& segmenter_;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_DELIMITER_QUERY_TOKENIZER_H_
