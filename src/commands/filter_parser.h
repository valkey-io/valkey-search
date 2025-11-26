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
struct FilterParseResults {
  std::unique_ptr<query::Predicate> root_predicate;
  absl::flat_hash_set<std::string> filter_identifiers;
};
class FilterParser {
 public:
  FilterParser(const IndexSchema& index_schema, absl::string_view expression,
               const TextParsingOptions& options);

  absl::StatusOr<FilterParseResults> Parse();

 private:
  const TextParsingOptions& options_;
  const IndexSchema& index_schema_;
  absl::string_view expression_;
  size_t pos_{0};
  size_t node_count_{0};
  absl::flat_hash_set<std::string> filter_identifiers_;

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
      FieldMaskPredicate& field_mask, std::optional<uint32_t>& min_stem_size,
      const std::optional<std::string>& field_name, bool with_suffix = false);
  absl::StatusOr<std::unique_ptr<query::Predicate>> ParseTextTokens(
      const std::optional<std::string>& field_for_default);
  absl::StatusOr<bool> IsMatchAllExpression();
  absl::StatusOr<std::unique_ptr<query::Predicate>> ParseExpression(
      uint32_t level);
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

  absl::StatusOr<absl::flat_hash_set<absl::string_view>> ParseTags(
      absl::string_view tag_string, indexes::Tag* tag_index) const;

  std::unique_ptr<query::Predicate> WrapPredicate(
      std::unique_ptr<query::Predicate> prev_predicate,
      std::unique_ptr<query::Predicate> predicate, bool& negate,
      query::LogicalOperator logical_operator);
};

namespace options {

/// Return the value of the Query String Depth configuration
vmsdk::config::Number& GetQueryStringDepth();

/// Return the value of the Query String Terms Count configuration
vmsdk::config::Number& GetQueryStringTermsCount();

}  // namespace options

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
