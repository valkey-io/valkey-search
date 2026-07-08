/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_QUERY_TOKENIZER_H_
#define VALKEY_SEARCH_INDEXES_TEXT_QUERY_TOKENIZER_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace valkey_search::indexes::text {

/// Abstract interface for query-path tokenization strategy.
///
/// The parser uses this to extract tokens from raw text spans between
/// query syntax characters. Two strategies exist:
///   - Delimiter-based (European): walk codepoints, break on punctuation
///   - Segmenter-delegated (CJK): extract span, delegate to Segment()
///
/// The parser handles query-syntax detection (|, @, (, ), ", *, %, etc.)
/// and delegates word-boundary logic and escape handling to this interface.
///
/// Standalone, testable, stateless — consistent with Segmenter/TokenFilter.
class QueryTokenizer {
 public:
  virtual ~QueryTokenizer() = default;

  struct Token {
    std::string content;    // the extracted token text (escapes resolved)
    size_t bytes_consumed;  // total bytes consumed from text[pos] onward
  };

  /// Extract the next token from quoted text starting at `text[pos]`.
  ///
  /// In quoted context, the only external boundary is the closing `"`.
  /// The tokenizer handles word-boundary detection, escape resolution, and
  /// invalid UTF-8 replacement within the quoted text span.
  ///
  /// The tokenizer MUST stop (without consuming) when it sees `"`.
  ///
  /// Returns:
  ///   - A Token with non-empty content if a token was extracted
  ///   - A Token with empty content if only delimiters/skippable bytes were
  ///     consumed (bytes_consumed > 0 tells the caller to advance)
  ///   - nullopt if nothing could be consumed (pos is at `"` or end)
  ///   - Error status for malformed escape sequences
  virtual absl::StatusOr<std::optional<Token>> NextQuotedToken(
      absl::string_view text, size_t pos) const = 0;

  /// Extract the next token from unquoted text starting at `text[pos]`.
  ///
  /// In unquoted context, the caller has already handled query syntax chars
  /// (|, @, (, ), ", *, %, -, {, }, [, ], :, ;, $). The tokenizer handles
  /// word-boundary detection and escape resolution for the remaining
  /// text-content codepoints.
  ///
  /// The tokenizer MUST stop (without consuming) when it encounters any
  /// character for which `IsQuerySyntax` returns true.
  ///
  /// `hit_query_syntax` is set to true if the tokenizer stopped because it
  /// encountered a query-syntax character.
  ///
  /// Returns:
  ///   - A Token with non-empty content if a token was extracted
  ///   - A Token with empty content + bytes_consumed > 0 if only
  ///     delimiters/skippable bytes were consumed
  ///   - nullopt if nothing could be consumed (pos at query syntax or end)
  ///   - Error status for malformed escape sequences
  virtual absl::StatusOr<std::optional<Token>> NextUnquotedToken(
      absl::string_view text, size_t pos, bool& hit_query_syntax,
      bool (*IsQuerySyntax)(uint32_t cp)) const = 0;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEY_SEARCH_INDEXES_TEXT_QUERY_TOKENIZER_H_
