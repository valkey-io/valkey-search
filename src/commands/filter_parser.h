/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
#define VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
#include <cstddef>
#include <memory>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.h"
#include "src/indexes/text/language_processor.h"
#include "src/query/predicate.h"
#include "src/utils/scanner.h"
#include "vmsdk/src/module_config.h"

namespace valkey_search {
namespace indexes {
class Tag;
}  // namespace indexes
using FieldMaskPredicate = uint64_t;
struct TextParsingOptions {
  bool verbatim = false;
  bool inorder = false;
  std::optional<uint32_t> slop = std::nullopt;
};
enum class QueryOperations : uint64_t {
  kNone = 0,
  kContainsOr = 1 << 0,
  kContainsAnd = 1 << 1,
  kContainsNumeric = 1 << 2,
  kContainsTag = 1 << 3,
  kContainsNegate = 1 << 4,
  kContainsText = 1 << 5,
  kContainsProximity = 1 << 6,
  kContainsNestedComposed = 1 << 7,
  kContainsTextTerm = 1 << 8,
  kContainsTextPrefix = 1 << 9,
  kContainsTextSuffix = 1 << 10,
  kContainsTextFuzzy = 1 << 11,
};

inline QueryOperations operator|(QueryOperations a, QueryOperations b) {
  return static_cast<QueryOperations>(static_cast<uint64_t>(a) |
                                      static_cast<uint64_t>(b));
}

inline QueryOperations& operator|=(QueryOperations& a, QueryOperations b) {
  return a = a | b;
}

inline bool operator&(QueryOperations a, QueryOperations b) {
  return static_cast<uint64_t>(a) & static_cast<uint64_t>(b);
}

struct FilterParseResults {
  std::unique_ptr<query::Predicate> root_predicate;
  absl::flat_hash_set<std::string> filter_identifiers;
  QueryOperations query_operations = QueryOperations::kNone;
  bool is_match_all = false;
};
class FilterParser {
 public:
  FilterParser(const IndexSchema& index_schema, absl::string_view expression,
               const TextParsingOptions& options);

  absl::StatusOr<FilterParseResults> Parse();

  // Parses query string tags using '|' as separator (query language OR syntax).
  // This is the single entry point for parsing tag strings from user queries.
  static absl::StatusOr<absl::flat_hash_set<absl::string_view>> ParseQueryTags(
      absl::string_view tag_string);

 private:
  const TextParsingOptions& options_;
  const IndexSchema& index_schema_;
  absl::string_view expression_;
  size_t pos_{0};
  size_t node_count_{0};
  absl::flat_hash_set<std::string> filter_identifiers_;
  QueryOperations query_operations_{QueryOperations::kNone};

  struct TokenResult {
    std::unique_ptr<query::TextPredicate> predicate;
    bool break_on_query_syntax;
  };
  absl::StatusOr<TokenResult> ParseQuotedTextToken(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      const std::optional<std::string>& field_or_default);

  absl::StatusOr<TokenResult> ParseUnquotedTextToken(
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      const std::optional<std::string>& field_or_default);

  absl::Status SetupTextFieldConfiguration(
      FieldMaskPredicate& field_mask,
      const std::optional<std::string>& field_name, bool with_suffix);
  absl::StatusOr<std::optional<std::unique_ptr<query::Predicate>>>
  ParseTextTokens(const std::optional<std::string>& field_for_default);
  absl::StatusOr<bool> IsMatchAllExpression();

  // Struct to hold parsing state including predicate, bracket counter, and
  // first joined flag
  struct ParseResult {
    std::unique_ptr<query::Predicate> prev_predicate;
    bool not_rightmost_bracket;
    ParseResult() : not_rightmost_bracket(false) {}
    ParseResult(std::unique_ptr<query::Predicate> pred, bool joined)
        : prev_predicate(std::move(pred)), not_rightmost_bracket(joined) {}
  };

  absl::StatusOr<ParseResult> ParseExpression(uint32_t level);
  absl::StatusOr<std::unique_ptr<query::NumericPredicate>>
  ParseNumericPredicate(const std::string& attribute_alias);
  absl::StatusOr<std::unique_ptr<query::TagPredicate>> ParseTagPredicate(
      const std::string& attribute_alias);
  absl::StatusOr<std::unique_ptr<query::TextPredicate>> ParseTextPredicate(
      const std::string& field_name);
  void SkipWhitespace();

  char Peek() const { return expression_[pos_]; }

  // A decoded-but-not-yet-consumed code point at the current position.
  // `byte_len` is always >= 1. Check `cp == utils::Scanner::kInvalidCp` to
  // detect malformed UTF-8.
  struct Peeked {
    uint32_t cp;
    uint8_t byte_len;

    bool IsValid() const { return cp != utils::Scanner::kInvalidCp; }
  };

  // Decode the code point at pos_ without advancing. Caller must ensure
  // !IsEnd(). The query string is the user-input boundary, so malformed UTF-8
  // is possible; the caller decides how to tolerate it (see the token loops).
  Peeked PeekCodepoint() const {
    CHECK(!IsEnd());
    utils::Scanner s(expression_.substr(pos_));
    utils::Scanner::Char cp = s.NextUtf8();
    // !IsEnd() guarantees at least one byte, so cp is never kEOF here.
    return {static_cast<uint32_t>(cp), s.LastUtf8ByteLen()};
  }

  // Append the peeked code point's bytes to `dest` and advance past it.
  // Keeps cp and byte_len together — callers never touch byte_len directly.
  void ConsumePeeked(const Peeked& p, std::string& dest) {
    dest.append(expression_.data() + pos_, p.byte_len);
    pos_ += p.byte_len;
  }

  // Advance past the peeked code point without copying it.
  void SkipPeeked(const Peeked& p) { pos_ += p.byte_len; }

  // Replace a malformed code point with U+FFFD and skip it. Legacy (< 1.4.0)
  // text-token path only; >= 1.4.0 rejects upfront in Parse(). Mirrors
  // Scanner::ReplaceInvalidUtf8, applied per token here.
  void ReplaceInvalidUtf8(const Peeked& p, std::string& dest) {
    utils::Scanner::PushBackUtf8(dest, 0xFFFD);
    SkipPeeked(p);
  }

  bool IsEnd() const { return pos_ >= expression_.length(); }
  bool Match(char expected, bool skip_whitespace = true);
  bool MatchInsensitive(const std::string& expected);
  absl::StatusOr<std::string> ParseFieldName();

  absl::StatusOr<double> ParseNumber();

  absl::StatusOr<absl::string_view> ParseTagString();

  absl::StatusOr<std::unique_ptr<query::Predicate>> WrapPredicate(
      std::unique_ptr<query::Predicate> prev_predicate,
      std::unique_ptr<query::Predicate> predicate, bool& negate,
      query::LogicalOperator logical_operator, bool no_prev_grp,
      bool not_rightmost_bracket);
  void FlagNestedComposedPredicate(
      std::unique_ptr<query::Predicate>& predicate);
};

// Helper function to print predicate tree structure using DFS
std::string PrintPredicateTree(const query::Predicate* predicate,
                               int indent = 0);

namespace options {

/// Return the value of the Query String Depth configuration
vmsdk::config::Number& GetQueryStringDepth();

/// Return the value of the Query String Terms Count configuration
vmsdk::config::Number& GetQueryStringTermsCount();

/// Return the value of the Fuzzy Max Distance configuration
vmsdk::config::Number& GetFuzzyMaxDistance();
}  // namespace options

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
