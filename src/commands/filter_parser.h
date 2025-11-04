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
struct FilterParseResults {
  std::unique_ptr<query::Predicate> root_predicate;
  absl::flat_hash_set<std::string> filter_identifiers;
};
class FilterParser {
 public:
  FilterParser(const IndexSchema& index_schema, absl::string_view expression);

  absl::StatusOr<FilterParseResults> Parse();

 private:
  const IndexSchema& index_schema_;
  absl::string_view expression_;
  size_t pos_{0};
  size_t node_count_{0};
  absl::flat_hash_set<std::string> filter_identifiers_;


  struct TokenResult {
    size_t end_pos;
    std::unique_ptr<query::TextPredicate> predicate;
    bool break_on_query_syntax;
  };
  // Add these two new function declarations in the private section:
  absl::StatusOr<TokenResult> ParseQuotedToken(
        std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
        uint64_t field_mask, std::optional<uint32_t> min_stem_size);

  absl::StatusOr<TokenResult> ParseUnquotedToken(
        std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
        uint64_t field_mask, std::optional<uint32_t> min_stem_size);



  absl::StatusOr<TokenResult> ParseTokenAndBuildPredicate(
      bool in_quotes,
      std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
      uint64_t field_mask, std::optional<uint32_t> min_stem_size);
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
};

namespace options {

/// Return the value of the Query String Depth configuration
vmsdk::config::Number& GetQueryStringDepth();

/// Return the value of the Query String Terms Count configuration
vmsdk::config::Number& GetQueryStringTermsCount();

}  // namespace options

}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_COMMANDS_FILTER_PARSER_H_
