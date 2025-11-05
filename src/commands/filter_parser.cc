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
constexpr uint32_t kDefaultQueryTermsCount{16};
constexpr uint32_t kMaxQueryTermsCount{32};
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

inline std::string indent_prefix(int depth, bool last) {
  std::string s;
  for (int i = 0; i < depth - 1; ++i) s += "│   ";
  if (depth > 0) s += (last ? "└── " : "├── ");
  return s;
}

// Note: This function is temporary until we support all the new text predicates
// and until we support the FT.EXPLAINCLI command to return the parsed query
// syntax tree from the query string provided.
void PrintPredicate(const query::Predicate* pred, int depth, bool last,
                    bool& valid) {
  if (!pred) {
    VMSDK_LOG(WARNING, nullptr) << indent_prefix(depth, last) << "NULL\n";
    return;
  }
  std::string prefix = indent_prefix(depth, last);
  switch (pred->GetType()) {
    case query::PredicateType::kComposedAnd:
    case query::PredicateType::kComposedOr: {
      const auto* comp = dynamic_cast<const query::ComposedPredicate*>(pred);
      VMSDK_LOG(WARNING, nullptr)
          << prefix
          << (pred->GetType() == query::PredicateType::kComposedAnd ? "AND"
                                                                    : "OR")
          << "\n";
      // Flatten same-type children for better readability
      std::vector<const query::Predicate*> children;
      std::function<void(const query::Predicate*)> collect =
          [&](const query::Predicate* node) {
            if (!node) return;
            if (node->GetType() == pred->GetType()) {
              auto c = dynamic_cast<const query::ComposedPredicate*>(node);
              collect(c->GetLhsPredicate());
              collect(c->GetRhsPredicate());
            } else {
              children.push_back(node);
            }
          };
      collect(comp);
      for (size_t i = 0; i < children.size(); ++i) {
        PrintPredicate(children[i], depth + 1, i == children.size() - 1, valid);
      }
      break;
    }
    case query::PredicateType::kNegate: {
      const auto* neg = dynamic_cast<const query::NegatePredicate*>(pred);
      VMSDK_LOG(WARNING, nullptr) << prefix << "NOT\n";
      PrintPredicate(neg->GetPredicate(), depth + 1, true, valid);
      break;
    }
    case query::PredicateType::kText: {
      if (auto prox = dynamic_cast<const query::ProximityPredicate*>(pred)) {
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "PROXIMITY(slop=" << prox->Slop()
            << ", inorder=" << prox->InOrder() << ")\n";
        const auto& terms = prox->Terms();
        for (size_t i = 0; i < terms.size(); ++i)
          PrintPredicate(terms[i].get(), depth + 1, i == terms.size() - 1,
                         valid);
      } else if (auto term = dynamic_cast<const query::TermPredicate*>(pred)) {
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "TERM(" << term->GetTextString() << ")_"
            << term->GetFieldMask() << "\n";
      } else if (auto pre = dynamic_cast<const query::PrefixPredicate*>(pred)) {
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "PREFIX(" << pre->GetTextString() << ")_"
            << pre->GetFieldMask() << "\n";
      } else if (auto pre = dynamic_cast<const query::SuffixPredicate*>(pred)) {
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "Suffix(" << pre->GetTextString() << ")_"
            << pre->GetFieldMask() << "\n";
      } else if (auto pre = dynamic_cast<const query::InfixPredicate*>(pred)) {
        valid = false;
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "Infix(" << pre->GetTextString() << ")_"
            << pre->GetFieldMask() << "\n";
      } else if (auto fuzzy =
                     dynamic_cast<const query::FuzzyPredicate*>(pred)) {
        valid = false;
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "FUZZY(" << fuzzy->GetTextString()
            << ", distance=" << fuzzy->GetDistance() << ")_"
            << fuzzy->GetFieldMask() << "\n";
      } else {
        valid = false;
        VMSDK_LOG(WARNING, nullptr) << prefix << "UNKNOWN TEXT\n";
      }
      break;
    }
    case query::PredicateType::kNumeric: {
      const auto* np = dynamic_cast<const query::NumericPredicate*>(pred);
      VMSDK_LOG(WARNING, nullptr)
          << prefix << "NUMERIC(" << np->GetStart()
          << (np->IsStartInclusive() ? "≤" : "<") << " .. " << np->GetEnd()
          << (np->IsEndInclusive() ? "≤" : "<") << ")_" << np->GetIdentifier()
          << "\n";
      break;
    }
    case query::PredicateType::kTag: {
      const auto* tp = dynamic_cast<const query::TagPredicate*>(pred);
      VMSDK_LOG(WARNING, nullptr) << prefix << "TAG(" << tp->GetTagString()
                                  << ")_" << tp->GetIdentifier() << "\n";
      break;
    }
    default:
      valid = false;
      VMSDK_LOG(WARNING, nullptr) << prefix << "UNKNOWN\n";
      break;
  }
}

FilterParser::FilterParser(const IndexSchema& index_schema,
                           absl::string_view expression,
                           const TextParsingOptions& options)
    : index_schema_(index_schema),
      expression_(absl::StripAsciiWhitespace(expression)),
      options_(options) {}

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

absl::StatusOr<absl::flat_hash_set<absl::string_view>> FilterParser::ParseTags(
    absl::string_view tag_string, indexes::Tag* tag_index) const {
  return indexes::Tag::ParseSearchTags(tag_string, tag_index->GetSeparator());
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
  VMSDK_ASSIGN_OR_RETURN(auto parsed_tags, ParseTags(tag_string, tag_index));
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

absl::StatusOr<FilterParseResults> FilterParser::Parse() {
  VMSDK_ASSIGN_OR_RETURN(auto is_match_all_expression, IsMatchAllExpression());
  FilterParseResults results;
  if (is_match_all_expression) {
    return results;
  }
  filter_identifiers_.clear();
  pos_ = 0;
  VMSDK_ASSIGN_OR_RETURN(auto predicate, ParseExpression(0));
  if (!IsEnd()) {
    return UnexpectedChar(expression_, pos_);
  }
  results.root_predicate = std::move(predicate);
  results.filter_identifiers.swap(filter_identifiers_);
  // Log the built query syntax tree.
  VMSDK_LOG(WARNING, nullptr) << "Parsed QuerySyntaxTree:";
  bool valid = true;
  PrintPredicate(results.root_predicate.get(), 0, true, valid);
  // Temporary validation until we support all the new predicates
  if (!valid) {
    return absl::InvalidArgumentError("Unsupported query operation");
  }
  return results;
}

inline std::unique_ptr<query::Predicate> MayNegatePredicate(
    std::unique_ptr<query::Predicate> predicate, bool& negate) {
  if (negate) {
    negate = false;
    return std::make_unique<query::NegatePredicate>(std::move(predicate));
  }
  return predicate;
}

std::unique_ptr<query::Predicate> WrapPredicate(
    std::unique_ptr<query::Predicate> prev_predicate,
    std::unique_ptr<query::Predicate> predicate, bool& negate,
    query::LogicalOperator logical_operator) {
  if (!prev_predicate) {
    return MayNegatePredicate(std::move(predicate), negate);
  }
  return std::make_unique<query::ComposedPredicate>(
      std::move(prev_predicate),
      MayNegatePredicate(std::move(predicate), negate), logical_operator);
};

static const uint32_t FUZZY_MAX_DISTANCE = 3;

// // Parses a single text predicate (one of either term, fuzzy, suffix, prefix,
// // infix). Includes the behavior for parsing while inquotes vs not inquotes.
// // Additionally, has punctuation handling for tokenization which can be
// escaped
// // by users. Returns back to caller site upon reaching the end of one token
// and
// // builds the predicate. Note: This can return early without a parsed
// predicate
// // if there was only punctuation without any actual text content before
// // encounting non text query syntax / the end of the expression.
// absl::StatusOr<FilterParser::TokenResult>
// FilterParser::ParseTokenAndBuildPredicate(
//     bool in_quotes,
//     std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
//     uint64_t field_mask, std::optional<uint32_t> min_stem_size) {
//   const auto& lexer = text_index_schema->GetLexer();
//   size_t current_pos = pos_;
//   size_t backslash_count = 0;
//   std::string processed_content;
//   // State tracking for predicate detection
//   bool starts_with_star = false;
//   bool ends_with_star = false;
//   size_t leading_percent_count = 0;
//   size_t trailing_percent_count = 0;
//   bool break_on_query_syntax = false;
//   while (current_pos < expression_.size()) {
//     char ch = expression_[current_pos];
//     // Handle backslashes
//     if (ch == '\\') {
//       backslash_count++;
//       ++current_pos;
//       continue;
//     }
//     // Process accumulated backslashes
//     if (backslash_count > 0) {
//       bool should_escape = false;
//       if (in_quotes) {
//         if (backslash_count % 2 == 0 || !lexer.IsPunctuation(ch)) {
//           processed_content.push_back('\\');
//         } else {
//           should_escape = true;
//         }
//       } else {
//         if (backslash_count % 2 == 0) {
//           processed_content.push_back('\\');
//         } else if (!lexer.IsPunctuation(ch)) {
//           if (backslash_count > 1) processed_content.push_back('\\');
//           break;
//         } else {
//           should_escape = true;
//         }
//       }
//       backslash_count = 0;
//       if (should_escape) {
//         processed_content.push_back(ch);
//         ++current_pos;
//         should_escape = false;
//         continue;
//       }
//     }
//     // Break on non text specific query syntax characters.
//     if (!in_quotes && (ch == ')' || ch == '|' || ch == '(' || ch == '@')) {
//       break_on_query_syntax = true;
//       break;
//     }
//     // - characters in the middle of text tokens are not negate. If they are
//     in
//     // the beginning, break.
//     if (!in_quotes && ch == '-' && processed_content.empty()) {
//       break_on_query_syntax = true;
//       break;
//     }
//     // Break to complete an exact phrase or start a new exact phrase.
//     if (ch == '"') break;
//     // Break on all punctuation characters, except text query syntax chars
//     such
//     // as % and * for non quote cases.
//     if ((!in_quotes && ch != '%' && ch != '*' || in_quotes) &&
//         lexer.IsPunctuation(ch))
//       break;
//     // Handle fuzzy token boundary detection
//     if (!in_quotes && ch == '%') {
//       if (current_pos == pos_) {
//         // Leading percent
//         while (current_pos < expression_.size() &&
//                expression_[current_pos] == '%') {
//           leading_percent_count++;
//           current_pos++;
//           if (leading_percent_count > FUZZY_MAX_DISTANCE) break;
//         }
//         continue;
//       } else {
//         // If there was no starting percent, we break.
//         // Trailing percent - count them
//         while (current_pos < expression_.size() &&
//                expression_[current_pos] == '%' &&
//                trailing_percent_count < leading_percent_count) {
//           trailing_percent_count++;
//           current_pos++;
//         }
//         break;
//       }
//     }
//     // Handle wildcard token boundary detection
//     if (!in_quotes && ch == '*') {
//       if (current_pos == pos_) {
//         starts_with_star = true;
//         current_pos++;
//         continue;
//       } else {
//         // Trailing star
//         ends_with_star = true;
//         current_pos++;
//         break;
//       }
//     }
//     // Regular character
//     processed_content.push_back(ch);
//     ++current_pos;
//   }
//   std::string token = absl::AsciiStrToLower(processed_content);
//   // Build predicate directly based on detected pattern
//   if (!in_quotes && leading_percent_count > 0) {
//     if (trailing_percent_count == leading_percent_count &&
//         leading_percent_count <= FUZZY_MAX_DISTANCE) {
//       if (token.empty()) {
//         return absl::InvalidArgumentError("Empty fuzzy token");
//       }
//       return FilterParser::TokenResult{
//           current_pos,
//           std::make_unique<query::FuzzyPredicate>(text_index_schema,
//           field_mask,
//                                                   std::move(token),
//                                                   leading_percent_count),
//           break_on_query_syntax};
//     } else {
//       return absl::InvalidArgumentError("Invalid fuzzy '%' markers");
//     }
//   } else if (!in_quotes && starts_with_star) {
//     if (token.empty()) {
//       return absl::InvalidArgumentError("Invalid wildcard '*' markers");
//     }
//     if (!text_index_schema->GetTextIndex()->suffix_.has_value()) {
//       return absl::InvalidArgumentError("Index created without Suffix Trie");
//     }
//     if (ends_with_star) {
//       return FilterParser::TokenResult{
//           current_pos,
//           std::make_unique<query::InfixPredicate>(text_index_schema,
//           field_mask,
//                                                   std::move(token)),
//           break_on_query_syntax};
//     } else {
//       return FilterParser::TokenResult{
//           current_pos,
//           std::make_unique<query::SuffixPredicate>(
//               text_index_schema, field_mask, std::move(token)),
//           break_on_query_syntax};
//     }
//   } else if (!in_quotes && ends_with_star) {
//     if (token.empty()) {
//       return absl::InvalidArgumentError("Invalid wildcard '*' markers");
//     }
//     return FilterParser::TokenResult{
//         current_pos,
//         std::make_unique<query::PrefixPredicate>(text_index_schema,
//         field_mask,
//                                                  std::move(token)),
//         break_on_query_syntax};
//   } else {
//     // Term predicate handling:
//     // Replace false with the VERBATIM flag from the FT.SEARCH.
//     bool exact = false || in_quotes;
//     // Replace false with the NOSTOPWORDS flag from the FT.SEARCH.
//     bool remove_stopwords = false || !in_quotes;
//     if ((remove_stopwords && lexer.IsStopWord(token) || token.empty())) {
//       return FilterParser::TokenResult{
//           current_pos, nullptr,
//           break_on_query_syntax};  // Skip stop words and empty words.
//     }
//     if (min_stem_size.has_value()) {
//       token = lexer.StemWord(token, !exact, *min_stem_size,
//       lexer.GetStemmer());
//     }
//     return FilterParser::TokenResult{
//         current_pos,
//         std::make_unique<query::TermPredicate>(text_index_schema, field_mask,
//                                                std::move(token), exact),
//         break_on_query_syntax};
//   }
// }

// Handle backslashes inside text content.
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
      // Single backslash with non-punct on right, consume the backslash and
      // break into a new token.
      return false;
    }
  } else {
    // Unescaped backslash at end of input is invalid.
    return absl::InvalidArgumentError(
        "Invalid escape sequence: backslash at end of input");
  }
}

absl::StatusOr<FilterParser::TokenResult> FilterParser::ParseQuotedTextToken(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::optional<uint32_t> min_stem_size) {
  const auto& lexer = text_index_schema->GetLexer();
  size_t backslash_count = 0;
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
    if (lexer.IsPunctuation(ch)) break;
    processed_content.push_back(ch);
    ++pos_;
  }
  std::string token = absl::AsciiStrToLower(processed_content);
  if (token.empty()) {
    return FilterParser::TokenResult{nullptr, false};
  }
  return FilterParser::TokenResult{
      std::make_unique<query::TermPredicate>(text_index_schema, field_mask,
                                             std::move(token), true),
      false};
}

absl::StatusOr<FilterParser::TokenResult> FilterParser::ParseUnquotedTextToken(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::optional<uint32_t> min_stem_size) {
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
          if (leading_percent_count > FUZZY_MAX_DISTANCE) break;
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
    // Break on all punctuation characters.
    if (lexer.IsPunctuation(ch)) break;
    // Regular character
    processed_content.push_back(ch);
    ++pos_;
  }
  std::string token = absl::AsciiStrToLower(processed_content);
  // Build predicate directly based on detected pattern
  if (leading_percent_count > 0) {
    if (trailing_percent_count == leading_percent_count &&
        leading_percent_count <= FUZZY_MAX_DISTANCE) {
      if (token.empty()) return absl::InvalidArgumentError("Empty fuzzy token");
      return FilterParser::TokenResult{
          std::make_unique<query::FuzzyPredicate>(text_index_schema, field_mask,
                                                  std::move(token),
                                                  leading_percent_count),
          break_on_query_syntax};
    } else {
      return absl::InvalidArgumentError("Invalid fuzzy '%' markers");
    }
  } else if (starts_with_star) {
    if (token.empty())
      return absl::InvalidArgumentError("Invalid wildcard '*' markers");
    if (!text_index_schema->GetTextIndex()->suffix_.has_value()) {
      return absl::InvalidArgumentError("Index created without Suffix Trie");
    }
    if (ends_with_star) {
      return FilterParser::TokenResult{
          std::make_unique<query::InfixPredicate>(text_index_schema, field_mask,
                                                  std::move(token)),
          break_on_query_syntax};
    } else {
      return FilterParser::TokenResult{
          std::make_unique<query::SuffixPredicate>(
              text_index_schema, field_mask, std::move(token)),
          break_on_query_syntax};
    }
  } else if (ends_with_star) {
    if (token.empty())
      return absl::InvalidArgumentError("Invalid wildcard '*' markers");
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
    if (!exact && min_stem_size.has_value()) {
      token = lexer.StemWord(token, true, *min_stem_size, lexer.GetStemmer());
    }
    return FilterParser::TokenResult{
        std::make_unique<query::TermPredicate>(text_index_schema, field_mask,
                                               std::move(token), exact),
        break_on_query_syntax};
  }
}

absl::Status FilterParser::SetupTextFieldConfiguration(
    FieldMaskPredicate& field_mask, std::optional<uint32_t>& min_stem_size,
    const std::optional<std::string>& field_name) {
  if (field_name.has_value()) {
    auto index = index_schema_.GetIndex(*field_name);
    if (!index.ok() ||
        index.value()->GetIndexerType() != indexes::IndexerType::kText) {
      return absl::InvalidArgumentError("Index does not have any text field");
    }
    auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
    auto identifier = index_schema_.GetIdentifier(*field_name).value();
    filter_identifiers_.insert(identifier);
    field_mask = 1ULL << text_index->GetTextFieldNumber();
    if (text_index->IsStemmingEnabled()) {
      min_stem_size = text_index->GetMinStemSize();
    }
  } else {
    // Set identifiers to include all text fields in the index schema.
    auto text_identifiers = index_schema_.GetAllTextIdentifiers();
    for (const auto& identifier : text_identifiers) {
      filter_identifiers_.insert(identifier);
    }
    // Set field mask to include all text fields in the index schema.
    field_mask = ~0ULL;
    // When no field was specified, we use the min stem across all text fields
    // in the index schema. This helps ensure the root of the text token can be
    // searched for.
    min_stem_size = index_schema_.MinStemSizeAcrossTextIndexes();
  }
  return absl::OkStatus();
}

// This function is called when the characters detected are potentially those of
// a text predicate. It can parse an exact phrase, or simply multiple text
// tokens (without field specifiers) and will return the grouped result of those
// predicates. Currently, this is Proximity and will be changed to the
// ComposedAND.
// When non text query syntax is detected (not escaped), it breaks out and
// returns back to the caller site with the parsed predicate.
absl::StatusOr<std::unique_ptr<query::Predicate>> FilterParser::ParseTextTokens(
    const std::optional<std::string>& field_or_default) {
  auto text_index_schema = index_schema_.GetTextIndexSchema();
  if (!text_index_schema) {
    return absl::InvalidArgumentError("Index does not have any text field");
  }
  std::vector<std::unique_ptr<query::TextPredicate>> terms;
  // Handle default / every field (no field specifier) and specific
  // field query cases.
  FieldMaskPredicate field_mask;
  std::optional<uint32_t> min_stem_size = std::nullopt;
  VMSDK_RETURN_IF_ERROR(
      SetupTextFieldConfiguration(field_mask, min_stem_size, field_or_default));
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
            ? ParseQuotedTextToken(text_index_schema, field_mask, min_stem_size)
            : ParseUnquotedTextToken(text_index_schema, field_mask,
                                     min_stem_size));
    if (result.predicate) {
      terms.push_back(std::move(result.predicate));
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
    uint32_t slop = options_.slop.value_or(0);
    bool inorder = options_.inorder;
    if (exact_phrase) {
      slop = 0;
      inorder = true;
    }
    // TODO: Swap ProximityPredicate with ComposedANDPredicate once it is
    // flattened. Once that happens, we need to add slop and inorder properties
    // to ComposedANDPredicate.
    pred = std::make_unique<query::ProximityPredicate>(std::move(terms), slop,
                                                       inorder);
    node_count_ += terms.size();
  } else {
    if (terms.empty()) {
      return absl::InvalidArgumentError("Invalid Query Syntax");
    }
    pred = std::move(terms[0]);
  }
  return pred;
}

// Parsing rules:
// 1. Predicate evaluation is done with left-associative grouping while the OR
// operator has higher precedence than the AND operator. precedence. For
// example: a & b | c & d is evaluated as (a & b) | (c & d).
// 2. Field name is always preceded by '@' and followed by ':'.
// 3. A numeric field has the following pattern: @field_name:[Start,End]. Both
// space and comma are valid separators between Start and End.
// 4. A tag field has the following pattern: @field_name:{tag1|tag2|tag3}.
// 5. The tag separator character is configurable with a default value of '|'.
// 6. A field name can be wrapped with `()` to group multiple predicates.
// 7. Space between predicates is considered as AND while '|' is considered as
// OR.
// 8. A predicate can be negated by preceding it with '-'. For example:
// -@field_name:10 => NOT(@field_name:10), -(a | b) => NOT(a | b).
// 9. -inf, inf and +inf are acceptable numbers in a range. Therefore, greater
// than 100 is expressed as [(100 inf].
// 10. Numeric filters are inclusive. Exclusive min or max are expressed with (
// prepended to the number, for example, [(100 (200].
absl::StatusOr<std::unique_ptr<query::Predicate>> FilterParser::ParseExpression(
    uint32_t level) {
  if (level++ >= options::GetQueryStringDepth().GetValue()) {
    return absl::InvalidArgumentError("Query string is too complex");
  }
  std::unique_ptr<query::Predicate> prev_predicate;

  SkipWhitespace();
  while (!IsEnd()) {
    if (Peek() == ')') {
      break;
    }
    std::unique_ptr<query::Predicate> predicate;
    bool negate = Match('-');

    if (Match('(')) {
      VMSDK_ASSIGN_OR_RETURN(predicate, ParseExpression(level));
      if (!Match(')')) {
        return absl::InvalidArgumentError(
            absl::StrCat("Expected ')' after expression got '",
                         expression_.substr(pos_, 1), "'. Position: ", pos_));
      }
      if (prev_predicate) {
        node_count_++;  // Count the ComposedPredicate Node
      }
      prev_predicate =
          WrapPredicate(std::move(prev_predicate), std::move(predicate), negate,
                        query::LogicalOperator::kAnd);
    } else if (Match('|')) {
      if (negate) {
        return UnexpectedChar(expression_, pos_ - 1);
      }
      VMSDK_ASSIGN_OR_RETURN(predicate, ParseExpression(level));
      if (prev_predicate) {
        node_count_++;  // Count the ComposedPredicate Node
      }
      prev_predicate =
          WrapPredicate(std::move(prev_predicate), std::move(predicate), negate,
                        query::LogicalOperator::kOr);
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
        node_count_++;
        VMSDK_ASSIGN_OR_RETURN(predicate, ParseTextTokens(field_name));
      }
      if (prev_predicate) {
        node_count_++;  // Count the ComposedPredicate Node
      }
      prev_predicate =
          WrapPredicate(std::move(prev_predicate), std::move(predicate), negate,
                        query::LogicalOperator::kAnd);
    }
    SkipWhitespace();
    auto max_node_count = options::GetQueryStringTermsCount().GetValue();
    VMSDK_RETURN_IF_ERROR(
        vmsdk::VerifyRange(node_count_, std::nullopt, max_node_count))
        << "Query string is too complex: max number of terms can't exceed "
        << max_node_count;
  }
  return prev_predicate;
}
}  // namespace valkey_search
