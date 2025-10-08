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
            << term->GetIdentifier() << "\n";
      } else if (auto pre = dynamic_cast<const query::PrefixPredicate*>(pred)) {
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "PREFIX(" << pre->GetTextString() << ")_"
            << pre->GetIdentifier() << "\n";
      } else if (auto pre = dynamic_cast<const query::SuffixPredicate*>(pred)) {
        valid = false;
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "Suffix(" << pre->GetTextString() << ")_"
            << pre->GetIdentifier() << "\n";
      } else if (auto pre = dynamic_cast<const query::InfixPredicate*>(pred)) {
        valid = false;
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "Infix(" << pre->GetTextString() << ")_"
            << pre->GetIdentifier() << "\n";
      } else if (auto fuzzy =
                     dynamic_cast<const query::FuzzyPredicate*>(pred)) {
        valid = false;
        VMSDK_LOG(WARNING, nullptr)
            << prefix << "FUZZY(" << fuzzy->GetTextString()
            << ", distance=" << fuzzy->GetDistance() << ")_"
            << fuzzy->GetIdentifier() << "\n";
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
                           absl::string_view expression)
    : index_schema_(index_schema),
      expression_(absl::StripAsciiWhitespace(expression)) {}

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
  return UnexpectedChar(expression_, pos_);
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

// TODO: Add Stemming support
absl::StatusOr<std::unique_ptr<query::TextPredicate>>
FilterParser::BuildSingleTextPredicate(const std::string& field_name,
                                       absl::string_view raw_token) {
  // --- Validate the field is a text index ---
  auto index = index_schema_.GetIndex(field_name);
  if (!index.ok() ||
      index.value()->GetIndexerType() != indexes::IndexerType::kText) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", field_name, "` is not indexed as a text field"));
  }
  auto identifier = index_schema_.GetIdentifier(field_name).value();
  filter_identifiers_.insert(identifier);
  auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
  absl::string_view token = absl::StripAsciiWhitespace(raw_token);
  if (token.empty()) {
    return absl::InvalidArgumentError("Empty text token");
  }
  // --- Fuzzy ---
  size_t lead_pct = 0;
  while (lead_pct < token.size() && token[lead_pct] == '%') {
    ++lead_pct;
    if (lead_pct > FUZZY_MAX_DISTANCE) {
      return absl::InvalidArgumentError("Too many leading '%' markers");
    }
  }
  size_t tail_pct = 0;
  while (tail_pct < token.size() && token[token.size() - 1 - tail_pct] == '%') {
    ++tail_pct;
    if (tail_pct > FUZZY_MAX_DISTANCE) {
      return absl::InvalidArgumentError("Too many trailing '%' markers");
    }
  }
  if (lead_pct || tail_pct) {
    if (lead_pct != tail_pct) {
      return absl::InvalidArgumentError("Mismatched fuzzy '%' markers");
    }
    absl::string_view core = token;
    core.remove_prefix(lead_pct);
    core.remove_suffix(tail_pct);
    if (core.empty()) {
      return absl::InvalidArgumentError("Empty fuzzy token");
    }
    return std::make_unique<query::FuzzyPredicate>(
        text_index, identifier, field_name, std::string(core), lead_pct);
  }
  // --- Wildcard ---
  bool starts_star = !token.empty() && token.front() == '*';
  bool ends_star = !token.empty() && token.back() == '*';
  if (starts_star || ends_star) {
    absl::string_view core = token;
    if (starts_star) core.remove_prefix(1);
    if (ends_star) core.remove_suffix(1);
    if (core.empty()) {
      return absl::InvalidArgumentError(
          "Wildcard token must contain at least one character besides '*'");
    }
    if (starts_star && ends_star) {
      return std::make_unique<query::InfixPredicate>(
          text_index, identifier, field_name, std::string(core));
    }
    if (starts_star) {
      return std::make_unique<query::SuffixPredicate>(
          text_index, identifier, field_name, std::string(core));
    }
    return std::make_unique<query::PrefixPredicate>(
        text_index, identifier, field_name, std::string(core));
  }
  // --- Term ---
  bool should_stem = true;
  std::string stemmed_token = text_index->ApplyStemming(token, should_stem);
  return std::make_unique<query::TermPredicate>(text_index, identifier,
                                                field_name, stemmed_token);
}

// // Q_TODO: Needs punctuation handing
// absl::StatusOr<std::vector<std::unique_ptr<query::TextPredicate>>>
// FilterParser::ParseOneTextAtomIntoTerms(const std::string& field_for_default) {
//   std::vector<std::unique_ptr<query::TextPredicate>> terms;
//   SkipWhitespace();
//   auto push_token = [&](std::string& tok) -> absl::Status {
//     if (tok.empty()) return absl::OkStatus();
//     // Q_TODO: convert to lower case, check if not stopword.
//     // Else skip BuildSingleTextPredicate, but do the rest of the fn.
//     VMSDK_ASSIGN_OR_RETURN(auto t,
//                            BuildSingleTextPredicate(field_for_default, tok));
//     terms.push_back(std::move(t));
//     tok.clear();
//     return absl::OkStatus();
//   };
//   // Exact Phrase / Term query parsing.
//   if (Match('"')) {
//     // Q_TODO: Do not allow the following characters in the exact phrase/term:
//     // $ % * ( ) - { } | ; : @ " (this indicates the end, unless escaped) ' [ ] ~
//     // Unless they are escaped, these are not allowed
//     std::string curr;
//     while (!IsEnd()) {
//       char c = Peek();
//       if (c == '"') {
//         ++pos_;
//         break;
//       }
//       if (std::isspace(static_cast<unsigned char>(c))) {
//         VMSDK_RETURN_IF_ERROR(push_token(curr));
//         ++pos_;
//       } else {
//         curr.push_back(c);
//         ++pos_;
//       }
//     }
//     VMSDK_RETURN_IF_ERROR(push_token(curr));
//     if (terms.empty()) return absl::InvalidArgumentError("Empty quoted string");
//     return terms;  // exact phrase realized later by proximity (slop=0,
//                    // inorder=true)
//   }
//   // Reads one raw term / token (unquoted) stopping on space, ')', '|', '{', '[', or
//   // start of '@field'
//   std::string tok;
//   bool seen_nonwildcard = false;
//   while (pos_ < expression_.size()) {
//     char c = expression_[pos_];
//     if (std::isspace(static_cast<unsigned char>(c)) || c == ')' || c == '|' ||
//         c == '{' || c == '[' || c == '@')
//       break;
//     tok.push_back(c);
//     ++pos_;
//     // If we encounter a tailing * (wildcard) after content, break to split into
//     // a new predicate.
//     if (c == '*' && seen_nonwildcard) {
//       break;
//     }
//     if (c != '*') {
//       seen_nonwildcard = true;
//     }
//   }
//   if (tok.empty()) return absl::InvalidArgumentError("Empty text token");
//   // Q_TODO: convert to lower case, check if not stopword.
//   // Else skip BuildSingleTextPredicate, but do the rest of the fn.
//   VMSDK_ASSIGN_OR_RETURN(auto t,
//                          BuildSingleTextPredicate(field_for_default, tok));
//   terms.push_back(std::move(t));
//   return terms;
// }

static const std::string kQuerySyntaxChars = "$%*()-{}|;:@\"'[]~";

bool IsSpecialSyntaxChar(char c) {
  return kQuerySyntaxChars.find(c) != std::string::npos;
}

absl::StatusOr<std::vector<std::unique_ptr<query::TextPredicate>>>
FilterParser::ParseOneTextAtomIntoTerms(const std::string& field_for_default) {
  // Get text index for punctuation and stop word configuration
  auto index = index_schema_.GetIndex(field_for_default);
  if (!index.ok() || index.value()->GetIndexerType() != indexes::IndexerType::kText) {
    return absl::InvalidArgumentError(
        absl::StrCat("`", field_for_default, "` is not indexed as a text field"));
  }
  auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
  auto text_index_schema = text_index->GetTextIndexSchema();
  std::vector<std::unique_ptr<query::TextPredicate>> terms;
  indexes::text::Lexer lexer;
  auto push_token = [&](std::string& tok) -> absl::Status {
    if (tok.empty()) return absl::OkStatus();
    std::string lower = absl::AsciiStrToLower(tok);
    if (lexer.IsStopWord(lower, text_index_schema->GetStopWordsSet())) {
      tok.clear();
      return absl::OkStatus();
    }
    VMSDK_ASSIGN_OR_RETURN(auto t, BuildSingleTextPredicate(field_for_default, lower));
    terms.push_back(std::move(t));
    tok.clear();
    return absl::OkStatus();
  };

  std::string curr;
  bool escaped = false;
  bool in_quotes = false;

  while (!IsEnd()) {
    char c = Peek();
    
    // Handle quote termination
    if (c == '"' && !escaped) {
      if (!in_quotes) {
        // Start quote mode
        in_quotes = true;
        ++pos_;
        continue;
      } else {
        // End quote mode
        ++pos_;
        break;
      }
    }
    
    // Handle escaping
    // TODO: validate
    if (escaped) {
      curr.push_back(c);
      escaped = false;
      ++pos_;
      continue;
    }
    if (c == '\\') {
      escaped = true;
      ++pos_;
      continue;
    }
    // Handle wildcard breaking (unquoted only)
    // TODO: curr.size() > 1 && curr != "*" is redundant.
    // TODO: Can we do this smarter? or do we have to do the same for fuzzy?
    if (!in_quotes && c == '*' && curr.size() > 1 && curr != "*") {
      curr.push_back(c);
      ++pos_;
      VMSDK_RETURN_IF_ERROR(push_token(curr));
      break;
    }

    if (!in_quotes && !escaped && (c == ')' || c == '|' || c == '(' || c == '@')) {
      VMSDK_RETURN_IF_ERROR(push_token(curr));
      break;
    }

    // Handle special characters (only in quotes)
    // TODO: Need to check about quotes. If they dont match outer quotes, we are good. if match, they need to be escaped
    // if they dont match, they do not need to be escaped.
    // Need to really understand how to implement the rejection logic without rejecting valid queries:
    // quick-running is valid.
    // if (!escaped && IsSpecialSyntaxChar(c)) {
    //   return absl::InvalidArgumentError(
    //       absl::StrCat("Unescaped special character '", std::string(1, c), "' in quoted string"));
    // }

    // TODO: I have concerns with punctuation including characters which should NOT be delimiters in queries.
    if (std::isspace(static_cast<unsigned char>(c)) || lexer.IsPunctuation(c, text_index_schema->GetPunctuationBitmap())) {
    // if (std::isspace(static_cast<unsigned char>(c))) {
      VMSDK_RETURN_IF_ERROR(push_token(curr));
      // Handle the case of non exact phrase.
      if (!in_quotes) break;
      ++pos_;
      continue;
    }
    
    // Regular character
    curr.push_back(c);
    ++pos_;
  }

  VMSDK_RETURN_IF_ERROR(push_token(curr));
  // TODO: In redis-search, they do not allow stop words in exact phrase
  // Also, we need to handle cases where this fn is called and a stop word if found with nothing else. vec is empty here.
  if (terms.empty()) return absl::InvalidArgumentError("Empty text token");
  return terms;
}

absl::StatusOr<std::string> FilterParser::ResolveTextFieldOrDefault(
    const std::optional<std::string>& maybe_field) {
  if (maybe_field.has_value()) return *maybe_field;
  // Placeholder for default text field
  return std::string("__default__");
}

// TODO:
// - Handle negation
// - Handle parenthesis by including terms in the proximity predicate. This
// requires folding this fn in the caller site.
// - Handle parsing and setup of default text field predicates
// - Try to move out nested standard operations (negate/numeric/tag/parenthesis)
// back to the caller site and reduce responsibilities of the text parser
// - Handle escaped characters in text tokens
absl::StatusOr<std::unique_ptr<query::Predicate>> FilterParser::ParseTextGroup(
    const std::string& initial_field) {
  std::vector<std::unique_ptr<query::TextPredicate>> all_terms;
  std::vector<std::unique_ptr<query::Predicate>> extra_terms;
  std::string current_field = initial_field;
  while (!IsEnd()) {
    SkipWhitespace();
    if (IsEnd()) break;
    bool negate = Match('-');
    char c = Peek();
    // Stop text group if next is OR
    if (c == '|') break;
    // Currently, parenthesis is not included in Proximity predicate. This needs
    // to be addressed.
    if (c == '(' || c == ')') break;
    std::optional<std::string> field_for_atom;
    if (!current_field.empty()) {
      field_for_atom = current_field;
    }
    // Field override or numeric/tag
    if (c == '@') {
      VMSDK_ASSIGN_OR_RETURN(current_field, ParseFieldName());
      field_for_atom = current_field;
      SkipWhitespace();
      if (!IsEnd()) {
        if (Match('[')) {
          VMSDK_ASSIGN_OR_RETURN(auto numeric,
                                 ParseNumericPredicate(current_field));
          extra_terms.push_back(std::move(numeric));
          continue;
        } else if (Match('{')) {
          VMSDK_ASSIGN_OR_RETURN(auto tag, ParseTagPredicate(current_field));
          extra_terms.push_back(std::move(tag));
          continue;
        }
      } else {
        return absl::InvalidArgumentError("Invalid query string");
      }
    }
    // Parse next text atom (first or subsequent)
    VMSDK_ASSIGN_OR_RETURN(auto resolved,
                           ResolveTextFieldOrDefault(field_for_atom));
    VMSDK_ASSIGN_OR_RETURN(auto terms, ParseOneTextAtomIntoTerms(resolved));
    for (auto& t : terms) all_terms.push_back(std::move(t));
    // Only use initial_field for first atom
    current_field.clear();
  }
  // Build main predicate from text terms
  std::unique_ptr<query::Predicate> prox;
  if (all_terms.size() == 1) {
    prox = std::move(all_terms[0]);
  } else if (!all_terms.empty()) {
    prox = std::make_unique<query::ProximityPredicate>(
        std::move(all_terms), /*slop=*/0, /*inorder=*/true);
  } else {
    return absl::InvalidArgumentError("Invalid query string");
  }
  // Append numeric/tag predicates
  for (auto& extra : extra_terms) {
    bool neg = false;
    prox = WrapPredicate(std::move(prox), std::move(extra), neg,
                         query::LogicalOperator::kAnd);
  }
  return prox;
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
      VMSDK_ASSIGN_OR_RETURN(auto field_name, ParseFieldName());
      if (Match('[')) {
        node_count_++;  // Count the NumericPredicate Node
        VMSDK_ASSIGN_OR_RETURN(predicate, ParseNumericPredicate(field_name));
      } else if (Match('{')) {
        node_count_++;  // Count the TagPredicate Node
        VMSDK_ASSIGN_OR_RETURN(predicate, ParseTagPredicate(field_name));
      } else {
        node_count_++;  // Count the TextPredicate Node
        VMSDK_ASSIGN_OR_RETURN(predicate, ParseTextGroup(field_name));
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
