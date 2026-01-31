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
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.h"
#include "src/indexes/tag.h"
#include "src/indexes/text/lexer.h"
#include "src/query/predicate.h"
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

  absl::StatusOr<bool> HandleBackslashEscape(const indexes::text::Lexer& lexer,
                                             std::string& processed_content);
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
  absl::StatusOr<std::unique_ptr<query::Predicate>> ParseTextTokens(
      const std::optional<std::string>& field_for_default);
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
