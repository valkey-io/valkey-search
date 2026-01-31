/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/filter_parser.h"

#include <cctype>
#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/query/predicate.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"

namespace valkey_search {

namespace options {

/// Register the "--query-string-depth" flag. Controls the depth of the query
/// string parsing from the FT.SEARCH cmd.
constexpr absl::string_view kQueryStringDepthConfig{"query-string-depth"};
constexpr uint32_t kDefaultQueryStringDepth{1000};
constexpr uint32_t kMinimumQueryStringDepth{1};
static auto query_string_depth =
    config::NumberBuilder(kQueryStringDepthConfig,   // name
                          kDefaultQueryStringDepth,  // default size
                          kMinimumQueryStringDepth,  // min size
                          UINT_MAX)                  // max size
        .WithValidationCallback(CHECK_RANGE(kMinimumQueryStringDepth, UINT_MAX,
                                            kQueryStringDepthConfig))
        .Build();

/// Register the "query-string-terms-count" flag. Controls the size of the
/// query string parsing from the FT.SEARCH cmd. The number of nodes in the
/// predicate tree.
constexpr absl::string_view kQueryStringTermsCountConfig{
    "query-string-terms-count"};
constexpr uint32_t kDefaultQueryTermsCount{1000};
constexpr uint32_t kMaxQueryTermsCount{10000};
static auto query_terms_count =
    config::NumberBuilder(kQueryStringTermsCountConfig,  // name
                          kDefaultQueryTermsCount,       // default size
                          1,                             // min size
                          kMaxQueryTermsCount)           // max size
        .WithValidationCallback(
            CHECK_RANGE(1, kMaxQueryTermsCount, kQueryStringTermsCountConfig))
        .Build();

vmsdk::config::Number& GetQueryStringDepth() {
  return dynamic_cast<vmsdk::config::Number&>(*query_string_depth);
}

vmsdk::config::Number& GetQueryStringTermsCount() {
  return dynamic_cast<vmsdk::config::Number&>(*query_terms_count);
}

/// Register the "fuzzy-max-distance" flag. Controls the maximum edit distance
/// for fuzzy search queries.
constexpr absl::string_view kFuzzyMaxDistanceConfig{"fuzzy-max-distance"};
constexpr uint32_t kDefaultFuzzyMaxDistance{3};
constexpr uint32_t kMinimumFuzzyMaxDistance{1};
constexpr uint32_t kMaximumFuzzyMaxDistance{50};
static auto fuzzy_max_distance =
    config::NumberBuilder(kFuzzyMaxDistanceConfig, kDefaultFuzzyMaxDistance,
                          kMinimumFuzzyMaxDistance, kMaximumFuzzyMaxDistance)
        .WithValidationCallback(CHECK_RANGE(kMinimumFuzzyMaxDistance,
                                            kMaximumFuzzyMaxDistance,
                                            kFuzzyMaxDistanceConfig))
        .Build();

vmsdk::config::Number& GetFuzzyMaxDistance() {
  return dynamic_cast<vmsdk::config::Number&>(*fuzzy_max_distance);
}
}  // namespace options

namespace {
#if defined(__clang__)
//  std::numeric_limits<..>::infinity() can not be used with clang when
// -ffast-math is enabled
constexpr double kPositiveInf = std::numeric_limits<double>::max();
constexpr double kNegativeInf = std::numeric_limits<double>::lowest();
#else
constexpr double kPositiveInf = std::numeric_limits<double>::infinity();
constexpr double kNegativeInf = -std::numeric_limits<double>::infinity();
#endif
}  // namespace

// Helper function to print predicate tree structure using DFS
std::string PrintPredicateTree(const query::Predicate* predicate, int indent) {
  std::string result;
  std::string indent_str(indent * 2, ' ');

  if (!predicate) {
    return result;
  }

  switch (predicate->GetType()) {
    case query::PredicateType::kComposedAnd: {
      const auto* composed =
          static_cast<const query::ComposedPredicate*>(predicate);
      auto slop = composed->GetSlop();
      if (composed->GetInorder() == false && !slop.has_value()) {
        result += indent_str + "AND{\n";
      } else {
        result += indent_str + "AND(slop=" +
                  (slop.has_value() ? std::to_string(slop.value()) : "none") +
                  ", inorder=" + (composed->GetInorder() ? "true" : "false") +
                  "){\n";
      }
      for (const auto& child : composed->GetChildren()) {
        result += PrintPredicateTree(child.get(), indent + 1);
      }
      result += indent_str + "}\n";
      break;
    }
    case query::PredicateType::kComposedOr: {
      const auto* composed =
          static_cast<const query::ComposedPredicate*>(predicate);
      result += indent_str + "OR{\n";
      for (const auto& child : composed->GetChildren()) {
        result += PrintPredicateTree(child.get(), indent + 1);
      }
      result += indent_str + "}\n";
      break;
    }
    case query::PredicateType::kNegate: {
      const auto* negate =
          static_cast<const query::NegatePredicate*>(predicate);
      result += indent_str + "NOT{\n";
      result += PrintPredicateTree(negate->GetPredicate(), indent + 1);
      result += indent_str + "}\n";
      break;
    }
    case query::PredicateType::kNumeric: {
      const auto* numeric =
          static_cast<const query::NumericPredicate*>(predicate);
      result +=
          indent_str + "NUMERIC(" + std::string(numeric->GetAlias()) + ")\n";
      break;
    }
    case query::PredicateType::kTag: {
      const auto* tag = static_cast<const query::TagPredicate*>(predicate);
      result += indent_str + "TAG(" + std::string(tag->GetAlias()) + ")\n";
      break;
    }
    case query::PredicateType::kText: {
      const auto* text = static_cast<const query::TextPredicate*>(predicate);
      std::string field_mask_str = std::to_string(text->GetFieldMask());

      // Determine specific text predicate type
      if (auto term = dynamic_cast<const query::TermPredicate*>(predicate)) {
        result += indent_str + "TEXT-TERM(\"" +
                  std::string(term->GetTextString()) +
                  "\", field_mask=" + field_mask_str + ")\n";
      } else if (auto prefix =
                     dynamic_cast<const query::PrefixPredicate*>(predicate)) {
        result += indent_str + "TEXT-PREFIX(\"" +
                  std::string(prefix->GetTextString()) +
                  "\", field_mask=" + field_mask_str + ")\n";
      } else if (auto suffix =
                     dynamic_cast<const query::SuffixPredicate*>(predicate)) {
        result += indent_str + "TEXT-SUFFIX(\"" +
                  std::string(suffix->GetTextString()) +
                  "\", field_mask=" + field_mask_str + ")\n";
      } else if (auto infix =
                     dynamic_cast<const query::InfixPredicate*>(predicate)) {
        result += indent_str + "TEXT-INFIX(\"" +
                  std::string(infix->GetTextString()) +
                  "\", field_mask=" + field_mask_str + ")\n";
      } else if (auto fuzzy =
                     dynamic_cast<const query::FuzzyPredicate*>(predicate)) {
        result += indent_str + "TEXT-FUZZY(\"" +
                  std::string(fuzzy->GetTextString()) +
                  "\", distance=" + std::to_string(fuzzy->GetDistance()) +
                  ", field_mask=" + field_mask_str + ")\n";
      } else {
        result += indent_str + "UNKNOWN\n";
      }
      break;
    }
    default:
      result += indent_str + "UNKNOWN\n";
      break;
  }
  return result;
}

FilterParser::FilterParser(const IndexSchema& index_schema,
                           absl::string_view expression,
                           const TextParsingOptions& options)
    : index_schema_(index_schema),
      expression_(absl::StripAsciiWhitespace(expression)),
      options_(options),
      query_operations_{QueryOperations::kNone} {}

bool FilterParser::Match(char expected, bool skip_whitespace) {
  if (skip_whitespace) {
    SkipWhitespace();
  }
  if (!IsEnd() && Peek() == expected) {
    ++pos_;
    return true;
  }
  return false;
}

bool FilterParser::MatchInsensitive(const std::string& expected) {
  auto old_pos = pos_;
  for (const auto& itr : expected) {
    if (!Match(itr, false) && !Match(absl::ascii_tolower(itr), false)) {
      pos_ = old_pos;
      return false;
    }
  }
  return true;
}

void FilterParser::SkipWhitespace() {
  while (!IsEnd() && std::isspace(Peek())) {
    ++pos_;
  }
}

absl::StatusOr<std::string> FilterParser::ParseFieldName() {
  std::string field_name;
  if (!Match('@')) {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected character at position ", pos_ + 1, ": `",
                     expression_.substr(pos_, 1), "`, expecting `@`"));
  }
  while (!IsEnd() && Peek() != ':' && !std::isspace(Peek())) {
    field_name += expression_[pos_++];
  }
  SkipWhitespace();
  if (IsEnd() || Peek() != ':') {
    return absl::InvalidArgumentError(
        absl::StrCat("Unexpected character at position ", pos_ + 1, ": `",
                     expression_.substr(pos_, 1), "`, expecting `:`"));
  }
  ++pos_;
  return field_name;
}

absl::StatusOr<double> FilterParser::ParseNumber() {
  SkipWhitespace();
  if (MatchInsensitive("-inf")) {
    return kNegativeInf;
  } else if (MatchInsensitive("+inf") || MatchInsensitive("inf")) {
    return kPositiveInf;
  }
  std::string number_str;
  double value;
  int multiplier = Match('-', false) ? -1 : 1;
  while (!IsEnd() && (std::isdigit(Peek()) || Peek() == '.')) {
    number_str += expression_[pos_++];
  }
  if (absl::AsciiStrToLower(number_str) != "nan" &&
      absl::SimpleAtod(number_str, &value)) {
    return value * multiplier;
  }
  return absl::InvalidArgumentError(
      absl::StrCat("Invalid number: ", number_str));
}

absl::StatusOr<std::unique_ptr<query::NumericPredicate>>
FilterParser::ParseNumericPredicate(const std::string& attribute_alias) {
  auto index = index_schema_.GetIndex(attribute_alias);
  if (!index.ok() ||
      index.value()->GetIndexerType() != indexes::IndexerType::kNumeric) {
    return absl::InvalidArgumentError(absl::StrCat(
        "`", attribute_alias, "` is not indexed as a numeric field"));
  }
  auto identifier = index_schema_.GetIdentifier(attribute_alias).value();
  filter_identifiers_.insert(identifier);
  bool is_inclusive_start = true;
  if (Match('(')) {
    is_inclusive_start = false;
  }
  VMSDK_ASSIGN_OR_RETURN(auto start, ParseNumber());
  if (!Match(' ', false) && !Match(',')) {
    return absl::InvalidArgumentError(
        absl::StrCat("Expected space or `|` between start and end values of a "
                     "numeric field. Position: ",
                     pos_));
  }
  bool is_inclusive_end = true;
  if (Match('(')) {
    is_inclusive_end = false;
  }
  VMSDK_ASSIGN_OR_RETURN(auto end, ParseNumber());
  if (!Match(']')) {
    return absl::InvalidArgumentError(absl::StrCat("Expected ']' got '",
                                                   expression_.substr(pos_, 1),
                                                   "'. Position: ", pos_));
  }
  if (start > end ||
      (start == end && !(is_inclusive_start && is_inclusive_end))) {
    return absl::InvalidArgumentError(
        absl::StrCat("Start and end values of a "
                     "numeric field indicate an empty range. Position: ",
                     pos_));
  }
  auto numeric_index =
      dynamic_cast<const indexes::Numeric*>(index.value().get());
  query_operations_ |= QueryOperations::kContainsNumeric;
  return std::make_unique<query::NumericPredicate>(
      numeric_index, attribute_alias, identifier, start, is_inclusive_start,
      end, is_inclusive_end);
}

absl::StatusOr<absl::string_view> FilterParser::ParseTagString() {
  SkipWhitespace();
  auto stop_pos = expression_.substr(pos_).find('}');
  if (stop_pos == std::string::npos) {
    return absl::InvalidArgumentError("Missing closing TAG bracket, '}'");
  }
  auto pos = pos_;
  pos_ += stop_pos + 1;
  return expression_.substr(pos, stop_pos);
}

absl::StatusOr<absl::flat_hash_set<absl::string_view>>
FilterParser::ParseQueryTags(absl::string_view tag_string) {
  // Parsing QUERY STRING: User-provided filter expression from FT.SEARCH.
  // In search queries, the tag separator is always '|' regardless of the
  // separator used when the index was created. This allows users to specify
  // multiple tags using the syntax: @field:{tag1|tag2|tag3}
  return indexes::Tag::ParseSearchTags(tag_string, '|');
}

absl::StatusOr<std::unique_ptr<query::TagPredicate>>
FilterParser::ParseTagPredicate(const std::string& attribute_alias) {
  auto index = index_schema_.GetIndex(attribute_alias);
  if (!index.ok() ||
      index.value()->GetIndexerType() != indexes::IndexerType::kTag) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", attribute_alias, "` is not indexed as a tag field"));
  }
  auto identifier = index_schema_.GetIdentifier(attribute_alias).value();
  filter_identifiers_.insert(identifier);

  auto tag_index = dynamic_cast<indexes::Tag*>(index.value().get());
  VMSDK_ASSIGN_OR_RETURN(auto tag_string, ParseTagString());
  VMSDK_ASSIGN_OR_RETURN(auto parsed_tags, ParseQueryTags(tag_string));
  query_operations_ |= QueryOperations::kContainsTag;
  return std::make_unique<query::TagPredicate>(
      tag_index, attribute_alias, identifier, tag_string, parsed_tags);
}

absl::Status UnexpectedChar(absl::string_view expression, size_t pos) {
  return absl::InvalidArgumentError(
      absl::StrCat("Unexpected character at position ", pos + 1, ": `",
                   std::string(expression.substr(pos, 1)), "`"));
}

absl::StatusOr<bool> FilterParser::IsMatchAllExpression() {
  pos_ = 0;
  bool open_bracket = false;
  bool close_bracket = false;
  bool found_asterisk = false;
  while (!IsEnd()) {
    SkipWhitespace();
    if (Match('*')) {
      if (found_asterisk || close_bracket) {
        return UnexpectedChar(expression_, pos_ - 1);
      }
      found_asterisk = true;
    } else if (Match('(')) {
      if (found_asterisk || close_bracket) {
        return UnexpectedChar(expression_, pos_ - 1);
      }
      if (open_bracket) {
        return false;
      }
      open_bracket = true;
    } else if (Match(')')) {
      if (!close_bracket && found_asterisk && open_bracket) {
        close_bracket = true;
      } else {
        return UnexpectedChar(expression_, pos_ - 1);
      }
    } else {
      break;
    }
  }
  if (!found_asterisk) {
    return false;
  }
  if (IsEnd()) {
    if ((open_bracket && close_bracket) || (!open_bracket && !close_bracket)) {
      return true;
    }
    return absl::InvalidArgumentError("Missing `)`");
  }
  return false;
}

void FilterParser::FlagNestedComposedPredicate(
    std::unique_ptr<query::Predicate>& predicate) {
  auto* composed = dynamic_cast<query::ComposedPredicate*>(predicate.get());
  if (!composed) return;
  for (const auto& child : composed->GetChildren()) {
    if (child->GetType() == query::PredicateType::kComposedAnd ||
        child->GetType() == query::PredicateType::kComposedOr) {
      query_operations_ |= QueryOperations::kContainsNestedComposed;
      return;
    }
  }
}

absl::StatusOr<FilterParseResults> FilterParser::Parse() {
  VMSDK_ASSIGN_OR_RETURN(auto is_match_all_expression, IsMatchAllExpression());
  FilterParseResults results;
  if (is_match_all_expression) {
    return results;
  }
  filter_identifiers_.clear();
  pos_ = 0;
  VMSDK_ASSIGN_OR_RETURN(auto parse_result, ParseExpression(0));
  if (!IsEnd()) {
    return UnexpectedChar(expression_, pos_);
  }
  results.root_predicate = std::move(parse_result.prev_predicate);
  FlagNestedComposedPredicate(results.root_predicate);
  results.filter_identifiers.swap(filter_identifiers_);
  results.query_operations = query_operations_;
  // Only generate query syntax tree output if debug logging is enabled.
  if (valkey_search::options::GetLogLevel().GetValue() ==
      static_cast<int>(LogLevel::kDebug)) {
    std::string tree_output =
        PrintPredicateTree(results.root_predicate.get(), 0);
    size_t chunk_size = 500;
    for (size_t i = 0; i < tree_output.length(); i += chunk_size) {
      VMSDK_LOG(DEBUG, nullptr)
          << "Parsed QuerySyntaxTree (Part " << (i / chunk_size + 1) << "):\n"
          << tree_output.substr(i, chunk_size);
    }
  }
  return results;
}

inline std::unique_ptr<query::Predicate> MayNegatePredicate(
    std::unique_ptr<query::Predicate> predicate, bool& negate,
    QueryOperations& query_operations) {
  if (negate) {
    negate = false;
    query_operations |= QueryOperations::kContainsNegate;
    return std::make_unique<query::NegatePredicate>(std::move(predicate));
  }
  return predicate;
}

absl::StatusOr<std::unique_ptr<query::Predicate>> FilterParser::WrapPredicate(
    std::unique_ptr<query::Predicate> prev_predicate,
    std::unique_ptr<query::Predicate> predicate, bool& negate,
    query::LogicalOperator logical_operator, bool no_prev_grp,
    bool not_rightmost_bracket) {
  auto new_predicate =
      MayNegatePredicate(std::move(predicate), negate, query_operations_);
  if (!prev_predicate) {
    return new_predicate;
  }
  // If INORDER OR SLOP, but the index schema does not support offsets, we
  // reject the query.
  if ((options_.inorder || options_.slop.has_value()) &&
      !index_schema_.HasTextOffsets()) {
    return absl::InvalidArgumentError("Index does not support offsets");
  }
  // Check if we can extend existing ComposedPredicate of the same type
  // Only extend AND nodes when we're adding with AND operator
  if (prev_predicate->GetType() == query::PredicateType::kComposedAnd &&
      logical_operator == query::LogicalOperator::kAnd && !no_prev_grp) {
    auto* composed =
        dynamic_cast<query::ComposedPredicate*>(prev_predicate.get());
    composed->AddChild(std::move(new_predicate));
    query_operations_ |= QueryOperations::kContainsAnd;
    return prev_predicate;
  }
  // Flatten OR nodes when not_rightmost_bracket is true at the same bracket
  // level. In this case, we are not creating a nested OR node since we are
  // extending the existing one.
  if (logical_operator == query::LogicalOperator::kOr &&
      not_rightmost_bracket &&
      new_predicate->GetType() == query::PredicateType::kComposedOr) {
    std::vector<std::unique_ptr<query::Predicate>> new_children;
    auto* new_composed =
        dynamic_cast<query::ComposedPredicate*>(new_predicate.get());
    auto children = new_composed->ReleaseChildren();
    new_children.reserve(1 + children.size());
    if (prev_predicate) {
      new_children.push_back(std::move(prev_predicate));
    }
    for (auto& child : children) {
      new_children.push_back(std::move(child));
    }
    query_operations_ |= QueryOperations::kContainsOr;
    return std::make_unique<query::ComposedPredicate>(
        logical_operator, std::move(new_children), options_.slop,
        options_.inorder);
  }
  // Create new ComposedPredicate only when operators differ or first
  // composition
  std::vector<std::unique_ptr<query::Predicate>> children;
  children.push_back(std::move(prev_predicate));
  children.push_back(std::move(new_predicate));
  if (logical_operator == query::LogicalOperator::kAnd) {
    query_operations_ |= QueryOperations::kContainsAnd;
    if (options_.inorder || options_.slop.has_value()) {
      query_operations_ |= QueryOperations::kContainsProximity;
    }
  } else {
    query_operations_ |= QueryOperations::kContainsOr;
  }
  return std::make_unique<query::ComposedPredicate>(
      logical_operator, std::move(children), options_.slop, options_.inorder);
};

// Handles backslash escaping for both quoted and unquoted text
// Escape Syntax:
// \\ -> \
// \<punctuation> -> <punctuation>
// \<non-punctuation> -> (break to new token)<non-punctuation>...
// \<EOL> -> Return error
absl::StatusOr<bool> FilterParser::HandleBackslashEscape(
    const indexes::text::Lexer& lexer, std::string& processed_content) {
  if (!Match('\\', false)) {
    // No backslash, continue normal processing of the same token.
    return true;
  }
  if (!IsEnd()) {
    char next_ch = Peek();
    if (next_ch == '\\' || lexer.IsPunctuation(next_ch)) {
      // If Double backslash, retain the double backslash
      // If Single backslash with punct on right, retain the char on right
      processed_content.push_back(next_ch);
      ++pos_;
      // Continue parsing the same token.
      return true;
    } else {
      // Backslash before non-punctuation
      if (lexer.IsPunctuation('\\')) {
        // Backslash is punctuation → break to new token (standard unicode
        // segmentation)
        return false;
      } else {
        // Backslash not punctuation → keep letter, continue
        processed_content.push_back(next_ch);
        ++pos_;
        return true;
      }
    }
  } else {
    // Unescaped backslash at end of input is invalid.
    return absl::InvalidArgumentError(
        "Invalid escape sequence: backslash at end of input");
  }
}

// Returns a token within an exact phrase parsing it until reaching the
// token boundary while handling escape chars.
// Quoted Text Syntax:
// word1 word2" word3 -> word1
// word2" word3 -> word2
// Token boundaries (separated by space): " <punctuation> \<non-punctuation>
absl::StatusOr<FilterParser::TokenResult> FilterParser::ParseQuotedTextToken(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    const std::optional<std::string>& field_or_default) {
  const auto& lexer = text_index_schema->GetLexer();
  std::string processed_content;
  while (!IsEnd()) {
    VMSDK_ASSIGN_OR_RETURN(bool should_continue,
                           HandleBackslashEscape(lexer, processed_content));
    if (!should_continue) {
      break;
    }
    // Break to complete an exact phrase or start a new exact phrase.
    char ch = Peek();
    if (ch == '"') break;
    if (ch == '\\') continue;  // Don't break on backslash
    if (lexer.IsPunctuation(ch)) break;
    processed_content.push_back(ch);
    ++pos_;
  }
  if (processed_content.empty()) {
    return FilterParser::TokenResult{nullptr, false};
  }
  std::string token = absl::AsciiStrToLower(processed_content);
  FieldMaskPredicate field_mask;
  VMSDK_RETURN_IF_ERROR(
      SetupTextFieldConfiguration(field_mask, field_or_default, false));
  return FilterParser::TokenResult{
      std::make_unique<query::TermPredicate>(text_index_schema, field_mask,
                                             std::move(token), true),
      false};
}

// Returns a token after parsing it until the token boundary while handling
// escape chars.
// Unquoted Text Syntax:
//   Term:    word
//   Prefix:  word*
//   Suffix:  *word
//   Infix:   *word*
//   Fuzzy:   %word% | %%word%% | %%%word%%%
// Token boundaries:
//   <punctuation> ( ) | @ " - { } [ ] : ; $
// Reserved chars:
//   { } [ ] : ; $ -> error
absl::StatusOr<FilterParser::TokenResult> FilterParser::ParseUnquotedTextToken(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    const std::optional<std::string>& field_or_default) {
  const auto& lexer = text_index_schema->GetLexer();
  std::string processed_content;
  bool starts_with_star = false;
  bool ends_with_star = false;
  size_t leading_percent_count = 0;
  size_t trailing_percent_count = 0;
  bool break_on_query_syntax = false;
  while (!IsEnd()) {
    VMSDK_ASSIGN_OR_RETURN(bool should_continue,
                           HandleBackslashEscape(lexer, processed_content));
    if (!should_continue) {
      break;
    }
    char ch = Peek();
    // Break on non text specific query syntax characters.
    if (ch == ')' || ch == '|' || ch == '(' || ch == '@') {
      break_on_query_syntax = true;
      break;
    }
    // Reject reserved characters in unquoted text
    if (ch == '{' || ch == '}' || ch == '[' || ch == ']' || ch == ':' ||
        ch == ';' || ch == '$') {
      return absl::InvalidArgumentError(
          absl::StrCat("Unexpected character at position ", pos_ + 1, ": `",
                       expression_.substr(pos_, 1), "`"));
    }
    // - characters in the middle of text tokens are not negate. If they are in
    // the beginning, break.
    if (ch == '-' && processed_content.empty()) {
      break_on_query_syntax = true;
      break;
    }
    // Break to complete an exact phrase or start a new exact phrase.
    if (ch == '"') break;
    // Handle fuzzy token boundary detection
    if (ch == '%') {
      if (processed_content.empty()) {
        // Leading percent
        while (Match('%', false)) {
          leading_percent_count++;
          if (leading_percent_count > options::GetFuzzyMaxDistance().GetValue())
            break;
        }
        continue;
      } else {
        // If there was no leading percent, we break.
        // Else, we keep consuming trailing percent (to match the leading count)
        // - count them
        while (trailing_percent_count < leading_percent_count &&
               Match('%', false)) {
          trailing_percent_count++;
        }
        break;
      }
    }
    // Handle wildcard token boundary detection
    if (Match('*', false)) {
      if (processed_content.empty() && !starts_with_star) {
        starts_with_star = true;
        continue;
      } else {
        // Trailing star
        ends_with_star = true;
        break;
      }
    }
    if (ch == '\\') continue;  // Don't break on backslash
    // Break on all punctuation characters.
    if (lexer.IsPunctuation(ch)) break;
    // Regular character
    processed_content.push_back(ch);
    ++pos_;
  }
  std::string token = absl::AsciiStrToLower(processed_content);
  FieldMaskPredicate field_mask;
  // Build predicate directly based on detected pattern
  if (leading_percent_count > 0) {
    if (trailing_percent_count == leading_percent_count &&
        leading_percent_count <= options::GetFuzzyMaxDistance().GetValue()) {
      if (token.empty()) return absl::InvalidArgumentError("Empty fuzzy token");
      VMSDK_RETURN_IF_ERROR(
          SetupTextFieldConfiguration(field_mask, field_or_default, false));
      auto fuzzy = FilterParser::TokenResult{
          std::make_unique<query::FuzzyPredicate>(text_index_schema, field_mask,
                                                  std::move(token),
                                                  leading_percent_count),
          break_on_query_syntax};
      return fuzzy;
    } else {
      return absl::InvalidArgumentError("Invalid fuzzy '%' markers");
    }
  } else if (starts_with_star) {
    if (token.empty())
      return absl::InvalidArgumentError("Invalid wildcard '*' markers");
    VMSDK_RETURN_IF_ERROR(
        SetupTextFieldConfiguration(field_mask, field_or_default, true));
    if (ends_with_star) {
      auto infix = FilterParser::TokenResult{
          std::make_unique<query::InfixPredicate>(text_index_schema, field_mask,
                                                  std::move(token)),
          break_on_query_syntax};
      return absl::InvalidArgumentError("Unsupported query operation");
    } else {
      return FilterParser::TokenResult{
          std::make_unique<query::SuffixPredicate>(
              text_index_schema, field_mask, std::move(token)),
          break_on_query_syntax};
    }
  } else if (ends_with_star) {
    if (token.empty())
      return absl::InvalidArgumentError("Invalid wildcard '*' markers");
    VMSDK_RETURN_IF_ERROR(
        SetupTextFieldConfiguration(field_mask, field_or_default, false));
    return FilterParser::TokenResult{
        std::make_unique<query::PrefixPredicate>(text_index_schema, field_mask,
                                                 std::move(token)),
        break_on_query_syntax};
  } else {
    // Term predicate handling:
    bool exact = options_.verbatim;
    if (lexer.IsStopWord(token) || token.empty()) {
      // Skip stop words and empty words.
      return FilterParser::TokenResult{nullptr, break_on_query_syntax};
    }
    VMSDK_RETURN_IF_ERROR(
        SetupTextFieldConfiguration(field_mask, field_or_default, false));
    // Apply stemming if not exact match - use schema-level min_stem_size
    // directly
    if (!exact && (index_schema_.GetStemTextFieldMask() & field_mask) != 0) {
      token = lexer.StemWord(token, true, index_schema_.GetMinStemSize(),
                             lexer.GetStemmer());
    }
    return FilterParser::TokenResult{
        std::make_unique<query::TermPredicate>(text_index_schema, field_mask,
                                               std::move(token), exact),
        break_on_query_syntax};
  }
}

absl::Status FilterParser::SetupTextFieldConfiguration(
    FieldMaskPredicate& field_mask,
    const std::optional<std::string>& field_name, bool with_suffix) {
  if (field_name.has_value()) {
    auto index = index_schema_.GetIndex(*field_name);
    if (!index.ok() ||
        index.value()->GetIndexerType() != indexes::IndexerType::kText) {
      return absl::InvalidArgumentError("Index does not have any text field");
    }
    auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
    if (with_suffix && !text_index->WithSuffixTrie()) {
      return absl::InvalidArgumentError("Field does not support suffix search");
    }
    auto identifier = index_schema_.GetIdentifier(*field_name).value();
    filter_identifiers_.insert(identifier);
    field_mask = 1ULL << text_index->GetTextFieldNumber();
  } else {
    // Set identifiers to include all text fields in the index schema.
    auto text_identifiers = index_schema_.GetAllTextIdentifiers(with_suffix);
    // Set field mask to include all text fields in the index schema.
    field_mask = index_schema_.GetAllTextFieldMask(with_suffix);
    if (text_identifiers.size() == 0 || field_mask == 0ULL) {
      if (with_suffix) {
        return absl::InvalidArgumentError("No fields support suffix search");
      }
      return absl::InvalidArgumentError("Index does not have any text field");
    }
    filter_identifiers_.reserve(filter_identifiers_.size() +
                                text_identifiers.size());
    filter_identifiers_.insert(text_identifiers.begin(),
                               text_identifiers.end());
  }
  return absl::OkStatus();
}

// This function is called when the characters detected are potentially those of
// a text predicate.
// Text Parsing Syntax:
//   Quoted: "word1 word2" -> ComposedAND(exact, slop=0, inorder=true)
//   Unquoted: word1 word2 -> TermPredicate(word1) - stops at first token
// Token boundaries for unquoted text: <punctuation> ( ) | @ " - { } [ ] : ; $
// Quoted phrases (Exact Phrase) parse all tokens within quotes, unquoted
// parsing stops after first token.
absl::StatusOr<std::unique_ptr<query::Predicate>> FilterParser::ParseTextTokens(
    const std::optional<std::string>& field_or_default) {
  auto text_index_schema = index_schema_.GetTextIndexSchema();
  if (!text_index_schema) {
    return absl::InvalidArgumentError("Index does not have any text field");
  }
  absl::InlinedVector<std::unique_ptr<query::TextPredicate>,
                      indexes::text::kProximityTermsInlineCapacity>
      terms;
  bool in_quotes = false;
  bool exact_phrase = false;
  while (!IsEnd()) {
    char c = Peek();
    if (c == '"') {
      in_quotes = !in_quotes;
      ++pos_;
      if (in_quotes && terms.empty()) {
        exact_phrase = true;
        continue;
      }
      break;
    }
    size_t token_start = pos_;
    VMSDK_ASSIGN_OR_RETURN(
        auto result,
        in_quotes
            ? ParseQuotedTextToken(text_index_schema, field_or_default)
            : ParseUnquotedTextToken(text_index_schema, field_or_default));
    if (result.predicate) {
      terms.push_back(std::move(result.predicate));
      // For unquoted text, stop after first token. For exact phrases, continue
      // parsing all tokens.
      if (!exact_phrase) break;
    }
    if (result.break_on_query_syntax) {
      break;
    }
    // If this happens, we are either done (at the end of the prefilter string)
    // or were on a punctuation character which should be consumed.
    if (token_start == pos_) {
      ++pos_;
    }
  }
  std::unique_ptr<query::Predicate> pred;
  if (terms.size() > 1) {
    // Exact phrase requires adjacent terms in order: slop=0, inorder=true
    uint32_t slop = 0;
    bool inorder = true;
    // If index schema does not support offsets, we cannot support exact phrase,
    // so reject the query.
    if (!index_schema_.HasTextOffsets()) {
      return absl::InvalidArgumentError("Index does not support offsets");
    }
    std::vector<std::unique_ptr<query::Predicate>> children;
    children.reserve(terms.size());
    for (auto& term : terms) {
      children.push_back(std::move(term));
    }
    query_operations_ |= QueryOperations::kContainsProximity;
    query_operations_ |= QueryOperations::kContainsAnd;
    query_operations_ |= QueryOperations::kContainsText;
    pred = std::make_unique<query::ComposedPredicate>(
        query::LogicalOperator::kAnd, std::move(children), slop, inorder);
    node_count_ += terms.size() + 1;
  } else {
    if (terms.empty()) {
      return absl::InvalidArgumentError("Invalid Query Syntax");
    }
    query_operations_ |= QueryOperations::kContainsText;
    pred = std::move(terms[0]);
    node_count_++;
  }
  return pred;
}

// Parsing rules:
// 1. Predicate evaluation is done with left-associative grouping while the OR
// operator has lower precedence than the AND operator. precedence. For
// example: a & b | c & d is evaluated as (a & b) | (c & d).
// 2. Brackets have the highest Precedence of all the operators -> () > AND >
// OR. example a & ( b | c ) & d is evaluated as AND (a, OR(b , c), d)
// 3. If a bracket has atleast 2 terms it will be evaluated as a
// separate nested structure.
// 4. If a bracket has no terms it will be evaluated to false.
// 5. Field name is always preceded by '@' and followed by ':'.
// 6. A numeric field has the following pattern: @field_name:[Start,End]. Both
// space and comma are valid separators between Start and End.
// 7. A tag field has the following pattern: @field_name:{tag1|tag2|tag3}.
// 8. A text field has the following pattern : @field_name:phrase. Where phrase
// can be a combination of different words, *, % for different text operations.
// 9. The tag separator character is configurable with a default value of '|'.
// 10. A field name can be wrapped with `()` to group multiple predicates.
// 11. Space between predicates is considered as AND while '|' is considered as
// OR.
// 12. A predicate can be negated by preceding it with '-'. For example:
// -@field_name:10 => NOT(@field_name:10), -(a | b) => NOT(a | b).
// 13. -inf, inf and +inf are acceptable numbers in a range. Therefore, greater
// than 100 is expressed as [(100 inf].
// 14. Numeric filters are inclusive. Exclusive min or max are expressed with (
// prepended to the number, for example, [(100 (200].
absl::StatusOr<FilterParser::ParseResult> FilterParser::ParseExpression(
    uint32_t level) {
  if (level++ >= options::GetQueryStringDepth().GetValue()) {
    return absl::InvalidArgumentError("Query string is too complex");
  }
  ParseResult result;
  // Keeps track of the rightmost bracket of a level. Used to determine the
  // WrapPredicate fn's OR logic
  result.not_rightmost_bracket = true;
  // Keeps track if first token is a bracket. Used to determine the
  // WrapPredicate fn's AND logic
  result.prev_predicate = nullptr;
  bool no_prev_grp = false;

  SkipWhitespace();
  while (!IsEnd()) {
    if (Peek() == ')') {
      break;
    }
    std::unique_ptr<query::Predicate> predicate;
    bool negate = Match('-');
    if (Match('(')) {
      VMSDK_ASSIGN_OR_RETURN(auto sub_result, ParseExpression(level));
      if (!Match(')')) {
        return absl::InvalidArgumentError(
            absl::StrCat("Expected ')' after expression got '",
                         expression_.substr(pos_, 1), "'. Position: ", pos_));
      }
      predicate = std::move(sub_result.prev_predicate);
      // When there is no term inside the brackets
      if (!predicate) {
        return absl::InvalidArgumentError(
            absl::StrCat("Empty brackets detected at Position: ", pos_ - 1));
      }
      if (result.prev_predicate) {
        node_count_++;
      }
      // If there is no Previous Predicate that means there is no term before
      // it and hence it is the first group which should branch to a separate
      // sub tree. This will be used when we encounter the next predicate with
      // AND logical operator.
      no_prev_grp = (!result.prev_predicate) ? true : false;
      VMSDK_ASSIGN_OR_RETURN(
          result.prev_predicate,
          WrapPredicate(std::move(result.prev_predicate), std::move(predicate),
                        negate, query::LogicalOperator::kAnd, false,
                        result.not_rightmost_bracket));
      // Closing bracket signifies one group is done which could be the
      // rightmost bracket. We set it to false as a flag for its potential for
      // the same.
      result.not_rightmost_bracket = false;
    } else if (Match('|')) {
      if (negate) {
        return UnexpectedChar(expression_, pos_ - 1);
      }
      VMSDK_ASSIGN_OR_RETURN(auto sub_result, ParseExpression(level));
      predicate = std::move(sub_result.prev_predicate);
      if (result.prev_predicate) {
        node_count_++;
      } else {
        return absl::InvalidArgumentError(("Missing OR term"));
      }
      // We use sub_result.not_rightmost_bracket since sub_result comes from the
      // right side so its bracket will be more towards the right than prev Pred
      VMSDK_ASSIGN_OR_RETURN(
          result.prev_predicate,
          WrapPredicate(std::move(result.prev_predicate), std::move(predicate),
                        negate, query::LogicalOperator::kOr, no_prev_grp,
                        sub_result.not_rightmost_bracket));
      no_prev_grp = false;
      // Resetting it to true since for that level we have got our rightmost
      // bracket and we do not want stale results to propagate.
      result.not_rightmost_bracket = true;
    } else {
      std::optional<std::string> field_name;
      bool non_text = false;
      if (Peek() == '@') {
        std::string parsed_field;
        VMSDK_ASSIGN_OR_RETURN(parsed_field, ParseFieldName());
        field_name = parsed_field;
        if (Match('[')) {
          node_count_++;
          VMSDK_ASSIGN_OR_RETURN(predicate, ParseNumericPredicate(*field_name));
          non_text = true;
        } else if (Match('{')) {
          node_count_++;
          VMSDK_ASSIGN_OR_RETURN(predicate, ParseTagPredicate(*field_name));
          non_text = true;
        }
      }
      if (!non_text) {
        VMSDK_ASSIGN_OR_RETURN(predicate, ParseTextTokens(field_name));
      }
      if (result.prev_predicate) {
        node_count_++;
      }
      VMSDK_ASSIGN_OR_RETURN(
          result.prev_predicate,
          WrapPredicate(std::move(result.prev_predicate), std::move(predicate),
                        negate, query::LogicalOperator::kAnd, no_prev_grp,
                        result.not_rightmost_bracket));
      // After the above wrap predicate there will always be a previous
      // predicate. Hence we set it to false.
      result.not_rightmost_bracket = false;
      no_prev_grp = false;
    }
    SkipWhitespace();
    auto max_node_count = options::GetQueryStringTermsCount().GetValue();
    VMSDK_RETURN_IF_ERROR(
        vmsdk::VerifyRange(node_count_, std::nullopt, max_node_count))
        << "Query string is too complex: max number of terms can't exceed "
        << max_node_count;
  }
  return result;
}
}  // namespace valkey_search
