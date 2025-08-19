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
#include "src/valkey_search_options.h"
#include "src/index_schema.h"
#include "src/indexes/index_base.h"
#include "src/indexes/text.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
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

void PrintPredicate(const query::Predicate* pred, int depth = 0, bool last = true) {
    if (!pred) {
        VMSDK_LOG(WARNING, nullptr) << indent_prefix(depth, last) << "NULL\n";
        return;
    }

    std::string prefix = indent_prefix(depth, last);

    switch (pred->GetType()) {
        case query::PredicateType::kComposedAnd:
        case query::PredicateType::kComposedOr: {
            const auto* comp = dynamic_cast<const query::ComposedPredicate*>(pred);
            VMSDK_LOG(WARNING, nullptr) << prefix
                                        << (pred->GetType() == query::PredicateType::kComposedAnd ? "AND" : "OR") << "\n";

            // Flatten same-type children for better readability
            std::vector<const query::Predicate*> children;
            std::function<void(const query::Predicate*)> collect = [&](const query::Predicate* node){
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
                PrintPredicate(children[i], depth + 1, i == children.size() - 1);
            }
            break;
        }

        case query::PredicateType::kNegate: {
            const auto* neg = dynamic_cast<const query::NegatePredicate*>(pred);
            VMSDK_LOG(WARNING, nullptr) << prefix << "NOT\n";
            PrintPredicate(neg->GetPredicate(), depth + 1, true);
            break;
        }

        case query::PredicateType::kText: {
            if (auto prox = dynamic_cast<const query::ProximityPredicate*>(pred)) {
                VMSDK_LOG(WARNING, nullptr) << prefix
                                            << "PROXIMITY(slop=" << prox->GetSlop()
                                            << ", inorder=" << prox->IsInOrder() << ")\n";
                const auto& terms = prox->GetTerms();
                for (size_t i = 0; i < terms.size(); ++i)
                    PrintPredicate(terms[i].get(), depth + 1, i == terms.size() - 1);
            } else if (auto term = dynamic_cast<const query::TermPredicate*>(pred)) {
                VMSDK_LOG(WARNING, nullptr) << prefix << "TERM(" << term->GetTextString() << ")_"
                                            << term->GetIdentifier() << "\n";
            } else if (auto pre = dynamic_cast<const query::PrefixPredicate*>(pred)) {
                VMSDK_LOG(WARNING, nullptr) << prefix << "PREFIX(" << pre->GetTextString() << ")_"
                                            << pre->GetIdentifier() << "\n";
            } else if (auto fuzzy = dynamic_cast<const query::FuzzyPredicate*>(pred)) {
                VMSDK_LOG(WARNING, nullptr) << prefix << "FUZZY(" << fuzzy->GetTextString()
                                            << ", distance=" << fuzzy->GetDistance() << ")_"
                                            << fuzzy->GetIdentifier() << "\n";
            } else {
                VMSDK_LOG(WARNING, nullptr) << prefix << "UNKNOWN TEXT\n";
            }
            break;
        }

        case query::PredicateType::kNumeric: {
            const auto* np = dynamic_cast<const query::NumericPredicate*>(pred);
            VMSDK_LOG(WARNING, nullptr) << prefix
                                        << "NUMERIC(" << np->GetStart()
                                        << (np->IsStartInclusive() ? "≤" : "<")
                                        << " .. " << np->GetEnd()
                                        << (np->IsEndInclusive() ? "≤" : "<") << ")_" << np->GetIdentifier() << "\n";
            break;
        }

        case query::PredicateType::kTag: {
            const auto* tp = dynamic_cast<const query::TagPredicate*>(pred);
            VMSDK_LOG(WARNING, nullptr) << prefix << "TAG(" << tp->GetTagString() << ")_"
                                        << tp->GetIdentifier() << "\n";
            break;
        }

        default:
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

absl::StatusOr<std::unique_ptr<query::TextPredicate>> FilterParser::ParseTextPredicate(
    const std::string& field_name) {
  auto index = index_schema_.GetIndex(field_name);
  if (!index.ok() ||
      index.value()->GetIndexerType() != indexes::IndexerType::kText) {
    return absl::InvalidArgumentError(absl::StrCat(
        "`", field_name, "` is not indexed as a text field"));
  }
  auto identifier = index_schema_.GetIdentifier(field_name).value();
  filter_identifiers_.insert(identifier);
  // Currently, we do not support default field text predicates (ie - without a field specified).
  std::string text_value;
  bool in_quotes = Match('"');
  while (!IsEnd()) {
    char c = Peek();
    if (in_quotes && c == '"') {
      pos_++;
      break;
    } else if (c == ' ' || c == ')' || c == '|') {
      break;
    }
    text_value.push_back(c);
    pos_++;
  }
  if (in_quotes && text_value.empty()) {
    return absl::InvalidArgumentError("Empty quoted string");
  }
  if (text_value.empty()) {
    return absl::InvalidArgumentError("Empty text predicate");
  }
  auto text_index = dynamic_cast<const indexes::Text*>(index.value().get());
  // If in quotes, it is an exact match.
  // return std::make_unique<query::TextPredicate>(text_index, field_name, identifier, text_value, query::TextPredicate::Operation::kExact, 0);
  return std::make_unique<query::TermPredicate>(
      text_index, identifier,
      field_name, text_value);
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
  // print the accumulated AND chain by calling the PrintPredicate function
  VMSDK_LOG(WARNING, nullptr) << "Parsed Result";
  PrintPredicate(results.root_predicate.get());
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

// classify wildcard kind
enum class WildcardKind { kNone, kPrefix, kSuffix, kInfix };

// detect wildcard kind based on '*'
// I reviewed this fn. Looks good to me.
WildcardKind DetectWildcard(absl::string_view tok) {
  const bool starts = !tok.empty() && tok.front() == '*';
  const bool ends   = !tok.empty() && tok.back()  == '*';
  if (!starts && !ends) return WildcardKind::kNone;
  if (starts && ends)   return WildcardKind::kInfix;
  if (starts)           return WildcardKind::kSuffix; // "*x"
  return WildcardKind::kPrefix;                       // "x*"
}

// strip one leading and/or trailing '*' for prefix/suffix/infix
absl::string_view StripWildcardMarkers(absl::string_view tok) {
  if (!tok.empty() && tok.front() == '*') tok.remove_prefix(1);
  if (!tok.empty() && tok.back()  == '*') tok.remove_suffix(1);
  return tok;
}

// Fuzzy: allow 1..3 '%' on both sides: %x%, %%x%%, %%%x%%%
// TODO: Handle errors. Also detect escaped '%' in the future.
// I reviewed this fn. Looks good to me aside from the TODO comment.
inline size_t GetFuzzyDistance(absl::string_view tok) {
  if (tok.size() < 3) return 0;
  auto count_leading = [](absl::string_view s) {
    size_t n = 0; while (n < s.size() && s[n] == '%') ++n; return n;
  };
  auto count_trailing = [](absl::string_view s) {
    size_t n = 0; while (n < s.size() && s[s.size()-1-n] == '%') ++n; return n;
  };
  size_t lead = count_leading(tok);
  size_t tail = count_trailing(tok);
  if (lead == 0 || tail == 0 || lead != tail) return 0;
  if (lead > 3 || tail > 3) return 0;
  if ((lead + tail) < tok.size()) {
    return lead;
  }
  return 0;
}

absl::string_view StripFuzzyMarkers(absl::string_view tok, size_t& distance) {
  // remove up to distance (max 3 for now) leading/trailing '%'
  size_t i = 0; while (i < tok.size() && tok[i] == '%' && i < distance) ++i;
  size_t j = 0; while (j < tok.size() && tok[tok.size()-1-j] == '%' && j < distance) ++j;
  tok.remove_prefix(i);
  tok.remove_suffix(j);
  return tok;
}

absl::StatusOr<std::string> FilterParser::ResolveTextFieldOrDefault(const std::optional<std::string>& maybe_field) {
  if (maybe_field.has_value()) return *maybe_field;

  // We will need to track the "default" text field as NULL.
  // NULL means, we search across all fields.
  
  // Replace with your own API if different.
  // auto def = index_schema_.GetDefaultTextFieldAlias();
  // if (!def.ok()) {
  //   return absl::FailedPreconditionError(
  //       "No default text field available and no @field specified.");
  // }
  // return *def;
  return absl::FailedPreconditionError(
        "No default text field available and no @field specified.");
}

absl::StatusOr<std::unique_ptr<query::TextPredicate>>
FilterParser::BuildSingleTextPredicate(const std::string& field_name, absl::string_view raw_token) {
  // Validate the field is a text index
  auto index = index_schema_.GetIndex(field_name);
  if (!index.ok() || index.value()->GetIndexerType() != indexes::IndexerType::kText) {
    return absl::InvalidArgumentError(absl::StrCat("`", field_name, "` is not indexed as a text field"));
  }
  auto identifier = index_schema_.GetIdentifier(field_name).value();
  filter_identifiers_.insert(identifier);

  auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());

  absl::string_view token = absl::StripAsciiWhitespace(raw_token);
  // Wildcards
  if (auto kind = DetectWildcard(token); kind != WildcardKind::kNone) {
    absl::string_view stem = StripWildcardMarkers(token);
    if (stem.empty()) {
      return absl::InvalidArgumentError("Wildcard token must contain at least one character besides '*'");
    }
    switch (kind) {
      case WildcardKind::kPrefix:
        return std::make_unique<query::PrefixPredicate>(text_index, identifier, field_name, std::string(stem));
      // TODO: Implement Suffix and Infix predicates
      // case WildcardKind::kSuffix:
      //   return std::make_unique<query::SuffixPredicate>(text_index, identifier, field_name, std::string(stem));
      // case WildcardKind::kInfix:
      //   return std::make_unique<query::InfixPredicate>(text_index, identifier, field_name, std::string(stem));
      default: break;
    }
  }

  // Fuzzy
  size_t fuzzy_distance = GetFuzzyDistance(token);
  if (fuzzy_distance > 0) {
    absl::string_view core = StripFuzzyMarkers(token, fuzzy_distance);
    if (core.empty()) {
      return absl::InvalidArgumentError("Empty fuzzy token");
    }
    return std::make_unique<query::FuzzyPredicate>(text_index, identifier, field_name, std::string(core),
                                                   fuzzy_distance);
  }

  // Plain term
  if (token.empty()) {
    return absl::InvalidArgumentError("Empty text token");
  }
  return std::make_unique<query::TermPredicate>(text_index, identifier, field_name, std::string(token));
}

// I reviewed this fn. Looks good to me.
absl::StatusOr<std::vector<std::unique_ptr<query::TextPredicate>>>
FilterParser::ParseOneTextAtomIntoTerms(const std::string& field_for_default) {
  std::vector<std::unique_ptr<query::TextPredicate>> terms;
  SkipWhitespace();

  auto push_token = [&](std::string& tok) -> absl::Status {
    if (tok.empty()) return absl::OkStatus();
    VMSDK_ASSIGN_OR_RETURN(auto t, BuildSingleTextPredicate(field_for_default, tok));
    terms.push_back(std::move(t));
    tok.clear();
    return absl::OkStatus();
  };

  if (Match('"')) {
    std::string curr;
    while (!IsEnd()) {
      char c = Peek();
      if (c == '"') { ++pos_; break; }
      if (std::isspace(static_cast<unsigned char>(c))) { 
        VMSDK_RETURN_IF_ERROR(push_token(curr)); 
        ++pos_; 
      } else { 
        curr.push_back(c); 
        ++pos_; 
      }
    }
    VMSDK_RETURN_IF_ERROR(push_token(curr));
    if (terms.empty()) return absl::InvalidArgumentError("Empty quoted string");
    return terms; // exact phrase realized later by proximity (slop=0, inorder=true)
  }

  // Reads one raw token (unquoted) stopping on space, ')', '|', '{', '[', or start of '@field'
  std::string tok;
  while (pos_ < expression_.size()) {
      char c = expression_[pos_];
      if (std::isspace(static_cast<unsigned char>(c)) || c == ')' || c == '|' ||
          c == '{' || c == '[' || c == '@') break;
      tok.push_back(c);
      ++pos_;
  }
  if (tok.empty()) return absl::InvalidArgumentError("Empty text token");
  VMSDK_ASSIGN_OR_RETURN(auto t, BuildSingleTextPredicate(field_for_default, tok));
  terms.push_back(std::move(t));
  return terms;
}

// I did NOT review / test this function fully. Need to compare with previous implementation.
absl::StatusOr<std::unique_ptr<query::Predicate>>
FilterParser::ParseTextGroup(const std::string& initial_field) {
    std::vector<std::unique_ptr<query::TextPredicate>> all_terms;
    std::vector<std::unique_ptr<query::Predicate>> extra_terms;  // numeric/tag for later
    std::string current_field = initial_field;

    // Require at least one text atom
    if (!IsEnd() && Peek() == '@') {
        VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
        current_field = f;
    }

    VMSDK_ASSIGN_OR_RETURN(auto resolved, ResolveTextFieldOrDefault(current_field));
    VMSDK_ASSIGN_OR_RETURN(auto first_terms, ParseOneTextAtomIntoTerms(resolved));
    for (auto& t : first_terms) all_terms.push_back(std::move(t));

    while (!IsEnd()) {
        SkipWhitespace();
        if (IsEnd()) break;

        char c = Peek();

        // Stop text group if next is OR, parentheses
        if (c == '|' || c == ')' || c == '(') break;

        // Field override or numeric/tag
        if (c == '@') {
            VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
            current_field = f;
            SkipWhitespace();
            if (!IsEnd()) {
                char next = Peek();
                if (next == '[') {
                    ++pos_; // consume '[' for numeric
                    VMSDK_ASSIGN_OR_RETURN(auto numeric, ParseNumericPredicate(current_field));
                    extra_terms.push_back(std::move(numeric));
                    continue;
                } else if (next == '{') {
                    ++pos_; // consume '{' for tag
                    VMSDK_ASSIGN_OR_RETURN(auto tag, ParseTagPredicate(current_field));
                    extra_terms.push_back(std::move(tag));
                    continue;
                }
            }
        }

        VMSDK_ASSIGN_OR_RETURN(resolved, ResolveTextFieldOrDefault(current_field));
        VMSDK_ASSIGN_OR_RETURN(auto terms, ParseOneTextAtomIntoTerms(resolved));
        for (auto& t : terms) all_terms.push_back(std::move(t));
    }

    // Build main predicate from text terms
    std::unique_ptr<query::Predicate> prox;
    if (all_terms.size() == 1) {
        prox = std::move(all_terms[0]);
    } else if (!all_terms.empty()) {
        prox = std::make_unique<query::ProximityPredicate>(
            std::move(all_terms), /*slop=*/0, /*inorder=*/true);
    }

    // Append numeric/tag predicates
    for (auto &extra : extra_terms) {
        bool neg = false;
        prox = WrapPredicate(std::move(prox), std::move(extra), neg, query::LogicalOperator::kAnd);
    }

    return prox;
}


// absl::StatusOr<std::unique_ptr<query::Predicate>>
// FilterParser::ParseTextGroup(const std::string& initial_field) {
//     std::vector<std::unique_ptr<query::TextPredicate>> all_terms;
//     std::vector<std::unique_ptr<query::Predicate>> extra_terms;  // numeric/tag for later
//     std::string current_field = initial_field;

//     // Require at least one text atom
//     {
//         if (!IsEnd() && Peek() == '@') {
//             VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
//             current_field = f;
//         }

//         VMSDK_ASSIGN_OR_RETURN(auto resolved, ResolveTextFieldOrDefault(current_field));
//         VMSDK_ASSIGN_OR_RETURN(auto first_terms, ParseOneTextAtomIntoTerms(resolved));
//         for (auto& t : first_terms) all_terms.push_back(std::move(t));
//     }

//     while (!IsEnd()) {
//         SkipWhitespace();
//         if (IsEnd()) break;

//         char c = Peek();

//         // Stop text group if next is OR, parentheses
//         if (c == '|' || c == ')' || c == '(') break;

//         // Stop/consume if next is @field:[...] or @field:{...}
//         if (c == '@') {
//             size_t saved_pos = pos_;
//             VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
//             SkipWhitespace();
//             if (!IsEnd()) {
//                 char next = Peek();
//                 if (next == '[') {
//                     ++pos_; // consume '[' for numeric
//                     VMSDK_ASSIGN_OR_RETURN(auto numeric, ParseNumericPredicate(f));
//                     extra_terms.push_back(std::move(numeric));
//                     continue;  // keep parsing text group after numeric
//                 } else if (next == '{') {
//                     ++pos_; // consume '{' for tag
//                     VMSDK_ASSIGN_OR_RETURN(auto tag, ParseTagPredicate(f));
//                     extra_terms.push_back(std::move(tag));
//                     continue;
//                 }
//             }
//             // Otherwise, text field override
//             current_field = f;
//         }

//         VMSDK_ASSIGN_OR_RETURN(auto resolved, ResolveTextFieldOrDefault(current_field));
//         VMSDK_ASSIGN_OR_RETURN(auto terms, ParseOneTextAtomIntoTerms(resolved));
//         for (auto& t : terms) all_terms.push_back(std::move(t));
//     }

//     // Build result:
//     std::unique_ptr<query::Predicate> prox;
//     if (all_terms.size() == 1) {
//         prox = std::move(all_terms[0]);
//     } else if (!all_terms.empty()) {
//         prox = std::make_unique<query::ProximityPredicate>(
//             std::move(all_terms), /*slop=*/0, /*inorder=*/true);
//     }

//     // Optionally, append numeric/tag predicates (or drop them for now)
//     for (auto &extra : extra_terms) {
//         bool neg = false;
//         prox = WrapPredicate(std::move(prox), std::move(extra),
//                              /*negate=*/neg, query::LogicalOperator::kAnd);
//     }

//     return prox;
// }

absl::StatusOr<std::unique_ptr<query::Predicate>>
FilterParser::ParseExpression(uint32_t level) {
    if (level++ >= options::GetQueryStringDepth().GetValue()) {
        return absl::InvalidArgumentError("Query string is too complex");
    }

    std::unique_ptr<query::Predicate> and_accum;
    std::vector<std::unique_ptr<query::Predicate>> or_groups;
    auto flush_node_count = [&]() -> absl::Status {
        auto max_node_count = options::GetQueryStringTermsCount().GetValue();
        return vmsdk::VerifyRange(node_count_, std::nullopt, max_node_count);
    };

    std::optional<std::string> last_text_field;

    while (!IsEnd()) {
        SkipWhitespace();
        if (IsEnd() || Peek() == ')') break;

        if (Peek() == '|') {
            ++pos_;
            if (!and_accum) return absl::InvalidArgumentError("Empty left side of OR '|'");
            or_groups.push_back(std::move(and_accum));
            continue;
        }

        bool negate = Match('-');
        std::unique_ptr<query::Predicate> node;

        if (Match('(')) {
            VMSDK_ASSIGN_OR_RETURN(node, ParseExpression(level));
            if (!Match(')')) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Expected ')' after expression got '",
                                 expression_.substr(pos_, 1), "' at pos ", pos_));
            }
            ++node_count_;
            VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
        } else {
            std::optional<std::string> field_for_node;

            // Optional field token
            if (!IsEnd() && Peek() == '@') {
                VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
                field_for_node = f;
            }

            SkipWhitespace();

            // Numeric predicate
            if (!IsEnd() && Peek() == '[') {
                ++pos_; // consume '['
                ++node_count_;
                if (!field_for_node.has_value()) {
                    return absl::InvalidArgumentError("Numeric predicate must have explicit field");
                }
                VMSDK_ASSIGN_OR_RETURN(node, ParseNumericPredicate(*field_for_node));
                VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
            }
            // Tag predicate
            else if (!IsEnd() && Peek() == '{') {
                ++pos_; // consume '{'
                ++node_count_;
                if (!field_for_node.has_value()) {
                    return absl::InvalidArgumentError("Tag predicate must have explicit field");
                }
                VMSDK_ASSIGN_OR_RETURN(node, ParseTagPredicate(*field_for_node));
                VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
            }
            // Text group
            else {
                std::string text_field;
                if (field_for_node.has_value()) {
                    text_field = *field_for_node;
                } else if (last_text_field.has_value()) {
                    text_field = *last_text_field;
                } else {
                    VMSDK_ASSIGN_OR_RETURN(text_field, ResolveTextFieldOrDefault({}));
                }
                last_text_field = text_field;

                ++node_count_;
                VMSDK_ASSIGN_OR_RETURN(node, ParseTextGroup(text_field));
                VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
            }
        }

        and_accum = WrapPredicate(std::move(and_accum), std::move(node), negate,
                                  query::LogicalOperator::kAnd);
    }

    if (!or_groups.empty()) {
        if (and_accum) or_groups.push_back(std::move(and_accum));
        std::unique_ptr<query::Predicate> accum_or;
        bool dummy_neg = false;
        for (auto& part : or_groups) {
            ++node_count_;
            VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
                node_count_, std::nullopt, options::GetQueryStringTermsCount().GetValue()))
                << "Query string is too complex";
            accum_or = WrapPredicate(std::move(accum_or), std::move(part), dummy_neg,
                                     query::LogicalOperator::kOr);
        }
        return accum_or;
    }

    return and_accum;
}


// Builds "proximity members" into a AND group including all terms (non text predicates too).
// absl::StatusOr<std::unique_ptr<query::Predicate>>
// FilterParser::ParseTextGroup(const std::string& field) {
//     std::vector<std::unique_ptr<query::TextPredicate>> all_terms;

//     // Require at least one text atom
//     VMSDK_ASSIGN_OR_RETURN(auto resolved, ResolveTextFieldOrDefault(field));
//     VMSDK_ASSIGN_OR_RETURN(auto first_terms, ParseOneTextAtomIntoTerms(resolved));
//     for (auto& t : first_terms) all_terms.push_back(std::move(t));

//     while (!IsEnd()) {
//         SkipWhitespace();
//         if (IsEnd()) break;

//         char c = Peek();

//         // Stop text group on OR, parentheses, or a new field (numeric/tag will be handled outside)
//         if (c == '|' || c == '(' || c == ')' || c == '@') break;

//         // Parse next text atom
//         VMSDK_ASSIGN_OR_RETURN(auto terms, ParseOneTextAtomIntoTerms(resolved));
//         for (auto& t : terms) all_terms.push_back(std::move(t));
//     }

//     std::unique_ptr<query::Predicate> prox;
//     if (all_terms.size() == 1) {
//         prox = std::move(all_terms[0]);
//     } else if (!all_terms.empty()) {
//         prox = std::make_unique<query::ProximityPredicate>(
//             std::move(all_terms), /*slop=*/0, /*inorder=*/true);
//     }

//     return prox;
// }

// absl::StatusOr<std::unique_ptr<query::Predicate>>
// FilterParser::ParseExpression(uint32_t level) {
//     if (level++ >= options::GetQueryStringDepth().GetValue()) {
//         return absl::InvalidArgumentError("Query string is too complex");
//     }

//     std::unique_ptr<query::Predicate> and_accum;
//     std::vector<std::unique_ptr<query::Predicate>> or_groups;
//     auto flush_node_count = [&]() -> absl::Status {
//         auto max_node_count = options::GetQueryStringTermsCount().GetValue();
//         return vmsdk::VerifyRange(node_count_, std::nullopt, max_node_count);
//     };

//     std::optional<std::string> last_text_field;

//     while (!IsEnd()) {
//         SkipWhitespace();
//         if (IsEnd() || Peek() == ')') break;

//         if (Peek() == '|') {
//             ++pos_;
//             if (!and_accum) return absl::InvalidArgumentError("Empty left side of OR '|'");
//             or_groups.push_back(std::move(and_accum));
//             continue;
//         }

//         bool negate = Match('-');
//         std::unique_ptr<query::Predicate> node;

//         if (Match('(')) {
//             VMSDK_ASSIGN_OR_RETURN(node, ParseExpression(level));
//             if (!Match(')')) {
//                 return absl::InvalidArgumentError(
//                     absl::StrCat("Expected ')' after expression got '",
//                                  expression_.substr(pos_, 1), "' at pos ", pos_));
//             }
//             ++node_count_;
//             VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//         } else {
//             std::optional<std::string> field_for_node;

//             // Optional field token
//             if (!IsEnd() && Peek() == '@') {
//                 VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
//                 field_for_node = f;
//             }

//             SkipWhitespace();

//             // Numeric predicate
//             if (!IsEnd() && Peek() == '[') {
//                 ++pos_;
//                 ++node_count_;
//                 if (!field_for_node.has_value()) {
//                     return absl::InvalidArgumentError("Numeric predicate must have explicit field");
//                 }
//                 VMSDK_ASSIGN_OR_RETURN(node, ParseNumericPredicate(*field_for_node));
//                 VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//             }
//             // Tag predicate
//             else if (!IsEnd() && Peek() == '{') {
//                 ++pos_;
//                 ++node_count_;
//                 if (!field_for_node.has_value()) {
//                     return absl::InvalidArgumentError("Tag predicate must have explicit field");
//                 }
//                 VMSDK_ASSIGN_OR_RETURN(node, ParseTagPredicate(*field_for_node));
//                 VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//             }
//             // Text group
//             else {
//                 std::string text_field;
//                 if (field_for_node.has_value()) {
//                     text_field = *field_for_node;
//                 } else if (last_text_field.has_value()) {
//                     text_field = *last_text_field;
//                 } else {
//                     VMSDK_ASSIGN_OR_RETURN(text_field, ResolveTextFieldOrDefault({}));
//                 }
//                 last_text_field = text_field;

//                 ++node_count_;
//                 VMSDK_ASSIGN_OR_RETURN(node, ParseTextGroup(text_field));
//                 VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//             }
//         }

//         and_accum = WrapPredicate(std::move(and_accum), std::move(node), negate,
//                                   query::LogicalOperator::kAnd);
//     }

//     if (!or_groups.empty()) {
//         if (and_accum) or_groups.push_back(std::move(and_accum));
//         std::unique_ptr<query::Predicate> accum_or;
//         bool dummy_neg = false;
//         for (auto& part : or_groups) {
//             ++node_count_;
//             VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
//                 node_count_, std::nullopt, options::GetQueryStringTermsCount().GetValue()))
//                 << "Query string is too complex";
//             accum_or = WrapPredicate(std::move(accum_or), std::move(part), dummy_neg,
//                                      query::LogicalOperator::kOr);
//         }
//         return accum_or;
//     }

//     return and_accum;
// }


// This works well

// absl::StatusOr<std::unique_ptr<query::Predicate>>
// FilterParser::ParseTextGroup(const std::string& initial_field) {
//     std::vector<std::unique_ptr<query::TextPredicate>> all_terms;
//     std::vector<std::unique_ptr<query::Predicate>> extra_terms;  // numeric/tag for later
//     std::string current_field = initial_field;

//     // Require at least one text atom
//     {
//         if (!IsEnd() && Peek() == '@') {
//             VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
//             current_field = f;
//         }

//         VMSDK_ASSIGN_OR_RETURN(auto resolved, ResolveTextFieldOrDefault(current_field));
//         VMSDK_ASSIGN_OR_RETURN(auto first_terms, ParseOneTextAtomIntoTerms(resolved));
//         for (auto& t : first_terms) all_terms.push_back(std::move(t));
//     }

//     while (!IsEnd()) {
//         SkipWhitespace();
//         if (IsEnd()) break;

//         char c = Peek();

//         // Stop text group if next is OR, parentheses
//         if (c == '|' || c == ')' || c == '(') break;

//         // Stop/consume if next is @field:[...] or @field:{...}
//         if (c == '@') {
//             size_t saved_pos = pos_;
//             VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
//             SkipWhitespace();
//             if (!IsEnd()) {
//                 char next = Peek();
//                 if (next == '[') {
//                     ++pos_; // consume '[' for numeric
//                     VMSDK_ASSIGN_OR_RETURN(auto numeric, ParseNumericPredicate(f));
//                     extra_terms.push_back(std::move(numeric));
//                     continue;  // keep parsing text group after numeric
//                 } else if (next == '{') {
//                     ++pos_; // consume '{' for tag
//                     VMSDK_ASSIGN_OR_RETURN(auto tag, ParseTagPredicate(f));
//                     extra_terms.push_back(std::move(tag));
//                     continue;
//                 }
//             }
//             // Otherwise, text field override
//             current_field = f;
//         }

//         VMSDK_ASSIGN_OR_RETURN(auto resolved, ResolveTextFieldOrDefault(current_field));
//         VMSDK_ASSIGN_OR_RETURN(auto terms, ParseOneTextAtomIntoTerms(resolved));
//         for (auto& t : terms) all_terms.push_back(std::move(t));
//     }

//     // Build result:
//     std::unique_ptr<query::Predicate> prox;
//     if (all_terms.size() == 1) {
//         prox = std::move(all_terms[0]);
//     } else if (!all_terms.empty()) {
//         prox = std::make_unique<query::ProximityPredicate>(
//             std::move(all_terms), /*slop=*/0, /*inorder=*/true);
//     }

//     // Optionally, append numeric/tag predicates (or drop them for now)
//     for (auto &extra : extra_terms) {
//         bool neg = false;
//         prox = WrapPredicate(std::move(prox), std::move(extra),
//                              /*negate=*/neg, query::LogicalOperator::kAnd);
//     }

//     return prox;
// }

// absl::StatusOr<std::unique_ptr<query::Predicate>>
// FilterParser::ParseExpression(uint32_t level) {
//     if (level++ >= options::GetQueryStringDepth().GetValue()) {
//         return absl::InvalidArgumentError("Query string is too complex");
//     }

//     std::unique_ptr<query::Predicate> and_accum;
//     std::vector<std::unique_ptr<query::Predicate>> or_groups;
//     auto flush_node_count = [&]() -> absl::Status {
//         auto max_node_count = options::GetQueryStringTermsCount().GetValue();
//         return vmsdk::VerifyRange(node_count_, std::nullopt, max_node_count);
//     };

//     std::optional<std::string> last_text_field;

//     while (!IsEnd()) {
//         SkipWhitespace();
//         if (IsEnd() || Peek() == ')') break;

//         if (Peek() == '|') {
//             ++pos_;
//             if (!and_accum) return absl::InvalidArgumentError("Empty left side of OR '|'");
//             or_groups.push_back(std::move(and_accum));
//             continue;
//         }

//         bool negate = Match('-');
//         std::unique_ptr<query::Predicate> node;

//         if (Match('(')) {
//             VMSDK_ASSIGN_OR_RETURN(node, ParseExpression(level));
//             if (!Match(')')) {
//                 return absl::InvalidArgumentError(
//                     absl::StrCat("Expected ')' after expression got '",
//                                  expression_.substr(pos_, 1), "' at pos ", pos_));
//             }
//             ++node_count_;
//             VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//         } else {
//             std::optional<std::string> field_for_node;

//             // Optional field token
//             if (!IsEnd() && Peek() == '@') {
//                 VMSDK_ASSIGN_OR_RETURN(auto f, ParseFieldName());
//                 field_for_node = f;
//             }

//             SkipWhitespace();

//             // Numeric predicate
//             if (!IsEnd() && Peek() == '[') {
//                 ++pos_; // consume '['
//                 ++node_count_;
//                 if (!field_for_node.has_value()) {
//                     return absl::InvalidArgumentError("Numeric predicate must have explicit field");
//                 }
//                 VMSDK_ASSIGN_OR_RETURN(node, ParseNumericPredicate(*field_for_node));
//                 VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//             }
//             // Tag predicate
//             else if (!IsEnd() && Peek() == '{') {
//                 ++node_count_;
//                 if (!field_for_node.has_value()) {
//                     return absl::InvalidArgumentError("Tag predicate must have explicit field");
//                 }
//                 VMSDK_ASSIGN_OR_RETURN(node, ParseTagPredicate(*field_for_node));
//                 VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//             }
//             // Text group
//             else {
//                 std::string text_field;
//                 if (field_for_node.has_value()) {
//                     text_field = *field_for_node;
//                 } else if (last_text_field.has_value()) {
//                     text_field = *last_text_field;
//                 } else {
//                     VMSDK_ASSIGN_OR_RETURN(text_field, ResolveTextFieldOrDefault({}));
//                 }
//                 last_text_field = text_field;

//                 ++node_count_;
//                 VMSDK_ASSIGN_OR_RETURN(node, ParseTextGroup(text_field));
//                 VMSDK_RETURN_IF_ERROR(flush_node_count()) << "Query string is too complex";
//             }
//         }

//         and_accum = WrapPredicate(std::move(and_accum), std::move(node), negate,
//                                   query::LogicalOperator::kAnd);
//     }

//     if (!or_groups.empty()) {
//         if (and_accum) or_groups.push_back(std::move(and_accum));
//         std::unique_ptr<query::Predicate> accum_or;
//         bool dummy_neg = false;
//         for (auto& part : or_groups) {
//             ++node_count_;
//             VMSDK_RETURN_IF_ERROR(vmsdk::VerifyRange(
//                 node_count_, std::nullopt, options::GetQueryStringTermsCount().GetValue()))
//                 << "Query string is too complex";
//             accum_or = WrapPredicate(std::move(accum_or), std::move(part), dummy_neg,
//                                      query::LogicalOperator::kOr);
//         }
//         return accum_or;
//     }

//     return and_accum;
// } 
// namespace valkey_search
}