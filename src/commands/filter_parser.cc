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
        valid = false;
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
  // return UnexpectedChar(expression_, pos_);
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

// absl::StatusOr<std::unique_ptr<query::TextPredicate>>
// FilterParser::BuildSingleTextPredicate(const indexes::Text* text_index,
//                                        const indexes::text::Lexer& lexer,
//                                        const std::optional<std::string>& field_name,
//                                        absl::string_view raw_token) {
//   absl::string_view token = absl::StripAsciiWhitespace(raw_token);
//   if (token.empty()) {
//     return absl::InvalidArgumentError("Empty text token");
//   }
//   VMSDK_LOG(WARNING, nullptr) << "BuildSingleTextPredicate: " << token;
//   VMSDK_LOG(WARNING, nullptr) << "Processed BuildSingleTextPredicate: " << token;
//   uint64_t field_mask;
//   if (field_name.has_value()) {
//     auto identifier = index_schema_.GetIdentifier(field_name.value()).value();
//     filter_identifiers_.insert(identifier);
//     field_mask = 1ULL << text_index->GetTextFieldNumber();
//   } else {
//     field_mask = ~0ULL;
//     auto text_identifiers = index_schema_.GetAllTextIdentifiers();
//     for (const auto& identifier : text_identifiers) {
//       filter_identifiers_.insert(identifier);
//     }
//   }
//   // Helper function to check if character at position is escaped
//   auto is_escaped = [&](size_t pos) -> bool {
//     return pos > 0 && token[pos - 1] == '\\';
//   };
//   // // Helper function to process escaped characters in a string
//   // auto process_escapes = [](absl::string_view str) -> std::string {
//   //   std::string result;
//   //   for (size_t i = 0; i < str.size(); ++i) {
//   //     if (str[i] != '\\') {
//   //       result += str[i];
//   //     }
//   //   }
//   //   return result;
//   // };
//   // --- Fuzzy ---
//   bool starts_percent = !token.empty() && token.front() == '%' && !is_escaped(0);
//   bool ends_percent = !token.empty() && token.back() == '%' && !is_escaped(token.size() - 1);
//   if (starts_percent || ends_percent) {
//     size_t lead_pct = 0;
//     while (lead_pct < token.size() && token[lead_pct] == '%' && !is_escaped(lead_pct)) {
//       ++lead_pct;
//       if (lead_pct > FUZZY_MAX_DISTANCE) {
//         return absl::InvalidArgumentError("Too many leading '%' markers");
//       }
//     }
//     size_t tail_pct = 0;
//     while (tail_pct < token.size() && token[token.size() - 1 - tail_pct] == '%' && 
//            !is_escaped(token.size() - 1 - tail_pct)) {
//       ++tail_pct;
//       if (tail_pct > FUZZY_MAX_DISTANCE) {
//         return absl::InvalidArgumentError("Too many trailing '%' markers");
//       }
//     }
//     if (lead_pct || tail_pct) {
//       if (lead_pct != tail_pct) {
//         return absl::InvalidArgumentError("Mismatched fuzzy '%' markers");
//       }
//       absl::string_view core = token;
//       core.remove_prefix(lead_pct);
//       core.remove_suffix(tail_pct);
      // if (core.empty()) {
      //   return absl::InvalidArgumentError("Empty fuzzy token");
      // }
//       return std::make_unique<query::FuzzyPredicate>(
//           text_index, field_mask, std::string(core), lead_pct);
//     }
//   }
//   // --- Wildcard ---
//   bool starts_star = !token.empty() && token.front() == '*' && !is_escaped(0);
//   bool ends_star = !token.empty() && token.back() == '*' && !is_escaped(token.size() - 1);
//   if (starts_star || ends_star) {
//     absl::string_view core = token;
//     if (starts_star) core.remove_prefix(1);
//     if (ends_star && !core.empty()) core.remove_suffix(1);
//     if (core.empty()) {
//       return absl::InvalidArgumentError(
//           "Wildcard token must contain at least one character besides '*'");
//     }
//     // std::string processed_core = process_escapes(core);
//     if (starts_star && ends_star) {
//       return std::make_unique<query::InfixPredicate>(
//           text_index, field_mask, std::string(core));
//     }
//     if (starts_star) {
//       return std::make_unique<query::SuffixPredicate>(text_index, field_mask, std::string(core));
//     }
//     return std::make_unique<query::PrefixPredicate>(text_index, field_mask, std::string(core));
//   }
//   // --- Term ---
//   auto text_index_schema = text_index->GetTextIndexSchema();
//   bool should_stem = true;
//   std::string word(token);
//   auto stemmed_token = lexer.StemWord(word, text_index_schema->GetStemmer(), should_stem, text_index->GetMinStemSize());
//   return std::make_unique<query::TermPredicate>(text_index, field_mask, stemmed_token);
// }

// absl::StatusOr<std::vector<std::unique_ptr<query::TextPredicate>>>
// FilterParser::ParseOneTextAtomIntoTerms(const std::optional<std::string>& field_for_default) {
//   // Get text index for punctuation and stop word configuration
//   auto index = field_for_default.has_value() 
//       ? index_schema_.GetIndex(field_for_default.value())
//       : index_schema_.GetFirstTextIndex();
//   if (!index.ok() || index.value()->GetIndexerType() != indexes::IndexerType::kText) {
//     return absl::InvalidArgumentError(
//         absl::StrCat("Index does not have any text field"));
//   }
//   auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
//   auto text_index_schema = text_index->GetTextIndexSchema();
//   std::vector<std::unique_ptr<query::TextPredicate>> terms;
//   indexes::text::Lexer lexer;
//   auto push_token = [&](std::string& tok) -> absl::Status {
//     if (tok.empty()) return absl::OkStatus();
//     std::string lower = absl::AsciiStrToLower(tok);
//     if (lexer.IsStopWord(lower, text_index_schema->GetStopWordsSet())) {
//       tok.clear();
//       return absl::OkStatus();
//     }
//     VMSDK_ASSIGN_OR_RETURN(auto term, BuildSingleTextPredicate(text_index, lexer, field_for_default, lower));
//     terms.push_back(std::move(term));
//     tok.clear();
//     return absl::OkStatus();
//   };
//   size_t backslash_count = 0;
//   std::string curr;
//   bool escaped = false;
//   bool in_quotes = false;
//   while (!IsEnd()) {
//     char c = Peek();
//     // Handle quote termination
//     if (c == '"' && !escaped) {
//       in_quotes = !in_quotes;
//       bool first_term = curr.empty() && terms.empty();
//       ++pos_;
//       if (in_quotes && first_term) continue;
//       break;
//     }
//     // Count backslashes
//     if (c == '\\') {
//       backslash_count++;
//       ++pos_;
//       continue;
//     }
//     // Process accumulated backslashes
//     if (backslash_count > 0) {
//       if (in_quotes) {
        // if (backslash_count % 2 == 0 || !lexer.IsPunctuation(c, text_index_schema->GetPunctuationBitmap())) {
        //     curr.push_back('\\');
        // } else {
        //     escaped = true;
        // }
//       } else {
        // if (backslash_count % 2 == 0) {
        //     curr.push_back('\\');
        // } else if (!lexer.IsPunctuation(c, text_index_schema->GetPunctuationBitmap())) {
        //     if (backslash_count > 1) curr.push_back('\\');
        //     break;
        // } else {
        //     escaped = true;
        // }
//       }
//       backslash_count = 0;
//     }
//     // Option 1 - We could potentially delete this block since we have careful handling in the code below it.
//     // We can set escape to false after pushing the char at the end.
//     // Option 2 - (Recommended) We can keep this block and delete the escaped handling in the code below it.
//     // Therefore, if we encounter * or % when we are not in quotes, handle the wildcard / fuzzy logic.
//     if (escaped) {
//       curr.push_back(c);
//       escaped = false;
//       ++pos_;
//       continue;
//     }
//     // These are query syntax which are handled in the higher level parsing fns.
//     // Break to yield back.
//     if (!in_quotes && !escaped && (c == ')' || c == '|' || c == '(' || c == '@' || c == '-')) {
//       break; 
//     }
//     // These are unhandled characters which we need to skip over.
//     // This is done by advancing and breaking to parse as a new token.
//     if (!in_quotes && !escaped && c != '%' && c != '*' && lexer.IsPunctuation(c, text_index_schema->GetPunctuationBitmap())) {
//       ++pos_; 
//       break;
//     }
//     // TODO: Test that we don't strip out valid characters in the search query.
//     // What we use in ingestion: ",.<>{}[]\"':;!@#$%^&*()-+=~/\\|"
//     // IMPORTANT Note: They do not skip $ _ : characters when in quotes.
//     if (in_quotes && !escaped && lexer.IsPunctuation(c, text_index_schema->GetPunctuationBitmap())) {
//       VMSDK_RETURN_IF_ERROR(push_token(curr));
//       ++pos_;
//       continue;
//     }
//     // Regular character
//     curr.push_back(c);
//     ++pos_;
//     // VERY IMPORTANT NOTE: This is an easy entry point to perform left to right parsing.
//     // It might simplify escaped char handling. Especially, when implementing code to handle escaped query syntax itself.
//     // Rules to achieve this:
//     // 1. Identify the boundary
//     // 2. Validate any syntax specifications. For example, fuzzy needs ensuring the distance matches on left and right.
//     // 3. Take start and end and then pass it to a function which can build the predicate (you can decide if you want a single method,
//     // or a specific one for each text preficate).

//     // Parse Infix OR Suffix
//     if (c == '*') {
    
//     }
//     // Parse Fuzzy
//     else if (c == '%') {

//     }
//     // Parse Term OR Prefix
//     else {

//     }
//   }
//   VMSDK_RETURN_IF_ERROR(push_token(curr));
//   // TODO: In redis-search, they do not allow stop words in exact phrase
//   return terms;
// }






// size_t FilterParser::FindTokenEndWithEscapes(bool in_quotes, const indexes::text::TextIndexSchema* text_index_schema) {
//   indexes::text::Lexer lexer;
//   size_t current_pos = pos_;
//   size_t backslash_count = 0;
//   bool escaped = false;
//   size_t pct_count = 0;
//   bool is_blackslash_punct = lexer.IsPunctuation('\\', text_index_schema->GetPunctuationBitmap());
//   bool starts_with_star = false;
//   while (current_pos < expression_.size()) {
//     char ch = expression_[current_pos];
//     if (ch == '\\') {
//       backslash_count++;
//       ++current_pos;
//       continue;
//     }
//     if (backslash_count > 0) {
//       if (in_quotes) {
//         if (backslash_count % 2 == 0 || !lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
//           // Keep backslash, continue
//         } else {
//           escaped = true;
//         }
//       } else {
//         if (backslash_count % 2 == 0) {
//           // Keep backslash, continue
//         } else if (!lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
//           break; // End token
//         } else {
//           escaped = true;
//         }
//       }
//       backslash_count = 0;
//     }
//     if (escaped) {
//       escaped = false;
//       ++current_pos;
//       continue;
//     }
//     if (ch == '"') break;
//     if (!in_quotes && (ch == ')' || ch == '|' || ch == '(' || ch == '@' || ch == '-')) break;
//     if (!in_quotes && ch != '%' && ch != '*' && lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) break;
//     if (in_quotes && lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) break;
//     // Break at fuzzy pattern boundaries
//     if (!in_quotes && ch == '%') {
//       // Check if we're at the end of a complete fuzzy pattern
//       if (current_pos == pos_) {
//         while (current_pos < expression_.size() && expression_[current_pos] == '%') {
//           pct_count++;
//           current_pos++;
//           if (pct_count > FUZZY_MAX_DISTANCE) {
//             // This is an error case.
//             break;
//           }
//         }
//         continue;
//       }
//       // We have a valid fuzzy start, check if current position could start another
//       while (pct_count > 0 && current_pos < expression_.size() && expression_[current_pos] == '%') {
//         pct_count--;
//         current_pos++;
//       }
//       break;
//     }
//     // Can be condensed a lot.
//     if (!in_quotes && ch == '*') {
//       if (current_pos == pos_) {
//         starts_with_star = true;
//       } else {
//         if (starts_with_star) {
//           // Completed Infix
//           ++current_pos;
//           break;
//         } else {
//           // Completed Prefix
//           ++current_pos;
//           break;
//         }
//       }
//     }
//     ++current_pos;
//   }
//   return current_pos;
// }

// std::string FilterParser::ProcessEscapesInRange(size_t start, size_t end, bool in_quotes, const indexes::text::TextIndexSchema* text_index_schema) {
//   indexes::text::Lexer lexer;
//   std::string result;
//   size_t pos = start;
//   size_t backslash_count = 0;
//   while (pos < end) {
//     char ch = expression_[pos];
//     if (ch == '\\') {
//       backslash_count++;
//       ++pos;
//       continue;
//     }
//     if (backslash_count > 0) {
//       if (in_quotes) {
//         if (backslash_count % 2 == 0 || !lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
//           result.push_back('\\');
//         }
//       } else {
//         if (backslash_count % 2 == 0) {
//           result.push_back('\\');
//         }
//       }
//       backslash_count = 0;
//     }
//     result.push_back(ch);
//     ++pos;
//   }
//   return result;
// }

// absl::StatusOr<std::vector<std::unique_ptr<query::TextPredicate>>>
// FilterParser::ParseOneTextAtomIntoTerms(const std::optional<std::string>& field_for_default) {
//   auto index = field_for_default.has_value() 
//       ? index_schema_.GetIndex(field_for_default.value())
//       : index_schema_.GetFirstTextIndex();
//   if (!index.ok() || index.value()->GetIndexerType() != indexes::IndexerType::kText) {
//     return absl::InvalidArgumentError(
//         absl::StrCat("Index does not have any text field"));
//   }
//   auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
//   auto text_index_schema = text_index->GetTextIndexSchema();
//   std::vector<std::unique_ptr<query::TextPredicate>> terms;
//   indexes::text::Lexer lexer;
//   uint64_t field_mask;
//   if (field_for_default.has_value()) {
//     auto identifier = index_schema_.GetIdentifier(field_for_default.value()).value();
//     filter_identifiers_.insert(identifier);
//     field_mask = 1ULL << text_index->GetTextFieldNumber();
//   } else {
//     field_mask = ~0ULL;
//     auto text_identifiers = index_schema_.GetAllTextIdentifiers();
//     for (const auto& identifier : text_identifiers) {
//       filter_identifiers_.insert(identifier);
//     }
//   }
//   bool in_quotes = false;
//   while (!IsEnd()) {
//     char c = Peek();
//     if (c == '"') {
//       in_quotes = !in_quotes;
//       ++pos_;
//       if (in_quotes && terms.empty()) continue;
//       break;
//     }
//     if (!in_quotes && (c == ')' || c == '|' || c == '(' || c == '@' || c == '-')) {
//       break;
//     }
//     // Find token boundaries
//     size_t token_start = pos_;
//     size_t token_end = FindTokenEndWithEscapes(in_quotes, text_index_schema.get());
//     if (token_start == token_end) {
//       if (!IsEnd()) ++pos_;
//       continue;
//     }
//     // Analyze RAW token to determine predicate type
//     absl::string_view raw_token = expression_.substr(token_start, token_end - token_start);
//     auto is_escaped_in_raw = [&](size_t pos) -> bool {
//       return pos > 0 && raw_token[pos - 1] == '\\';
//     };
//     // Fuzzy logic - check RAW token
//     bool starts_percent = !raw_token.empty() && raw_token.front() == '%' && !is_escaped_in_raw(0);
//     bool ends_percent = !raw_token.empty() && raw_token.back() == '%' && !is_escaped_in_raw(raw_token.size() - 1);
//     if (!in_quotes && (starts_percent || ends_percent)) {
//       size_t lead_pct = 0;
//       while (lead_pct < raw_token.size() && raw_token[lead_pct] == '%' && !is_escaped_in_raw(lead_pct)) {
//         ++lead_pct;
//         if (lead_pct > FUZZY_MAX_DISTANCE) break;
//       }
//       size_t tail_pct = 0;
//       while (tail_pct < raw_token.size() && raw_token[raw_token.size() - 1 - tail_pct] == '%' && 
//              !is_escaped_in_raw(raw_token.size() - 1 - tail_pct)) {
//         ++tail_pct;
//         if (tail_pct > FUZZY_MAX_DISTANCE) break;
//       }
//       // Need to handle mismatched distance.
//       if (lead_pct && tail_pct && lead_pct == tail_pct && lead_pct <= FUZZY_MAX_DISTANCE) {
//         // Process escapes only for core content
//         std::string core = ProcessEscapesInRange(token_start + lead_pct, token_end - tail_pct, in_quotes, text_index_schema.get());
//         if (core.empty()) {
//           return absl::InvalidArgumentError("Empty fuzzy token");
//         }
//         std::string lower_core = absl::AsciiStrToLower(core);
//         terms.push_back(std::make_unique<query::FuzzyPredicate>(text_index, field_mask, lower_core, lead_pct));
//         pos_ = token_end;
//         break;
//       } else {
//         return absl::InvalidArgumentError("Invalid fuzzy '%' markers");
//       }
//     }
//     // Wildcard logic - check RAW token
//     bool starts_star = !raw_token.empty() && raw_token.front() == '*' && !is_escaped_in_raw(0);
//     bool ends_star = !raw_token.empty() && raw_token.back() == '*' && !is_escaped_in_raw(raw_token.size() - 1);
//     if (!in_quotes && (starts_star || ends_star)) {
//       size_t prefix_len = starts_star ? 1 : 0;
//       size_t suffix_len = ends_star ? 1 : 0;
//       VMSDK_LOG(WARNING, nullptr) << "wildcard token: " << raw_token << " starts_star: " << starts_star << " ends_star: " << ends_star;
//       if (raw_token.size() > prefix_len + suffix_len) {
//         // Process escapes only for core content
//         std::string core = ProcessEscapesInRange(token_start + prefix_len, token_end - suffix_len, in_quotes, text_index_schema.get());
//         std::string lower_core = absl::AsciiStrToLower(core);
//         if (starts_star && ends_star) {
//           terms.push_back(std::make_unique<query::InfixPredicate>(text_index, field_mask, lower_core));
//         } else if (starts_star) {
//           terms.push_back(std::make_unique<query::SuffixPredicate>(text_index, field_mask, lower_core));
//         } else {
//           terms.push_back(std::make_unique<query::PrefixPredicate>(text_index, field_mask, lower_core));
//         }
//         pos_ = token_end;
//         break;
//       } else {
//         return absl::InvalidArgumentError("Invalid wildcard '*' markers");
//       }
//     }
//     // Term - process entire token
//     std::string processed_token = ProcessEscapesInRange(token_start, token_end, in_quotes, text_index_schema.get());
//     std::string lower = absl::AsciiStrToLower(processed_token);
//     if (!lexer.IsStopWord(lower, text_index_schema->GetStopWordsSet()) && !lower.empty()) {
//       bool should_stem = true;
//       auto stemmed_token = lexer.StemWord(lower, text_index_schema->GetStemmer(), should_stem, text_index->GetMinStemSize());
//       terms.push_back(std::make_unique<query::TermPredicate>(text_index, field_mask, stemmed_token));
//     }
//     pos_ = token_end;
//   }
//   return terms;
// }


absl::StatusOr<FilterParser::TokenResult> FilterParser::ParseTokenAndBuildPredicate(
    bool in_quotes, 
    const indexes::text::TextIndexSchema* text_index_schema,
    const indexes::Text* text_index,
    uint64_t field_mask) {
  indexes::text::Lexer lexer;
  size_t current_pos = pos_;
  size_t backslash_count = 0;
  std::string processed_content;
  // State tracking for predicate detection
  bool starts_with_star = false;
  bool starts_with_percent = false;
  size_t leading_percent_count = 0;
  size_t trailing_percent_count = 0;
  bool found_content = false;
  bool ends_with_star = false;
  while (current_pos < expression_.size()) {
    char ch = expression_[current_pos];
    // Handle backslashes
    if (ch == '\\') {
      backslash_count++;
      ++current_pos;
      continue;
    }
    // Process accumulated backslashes
    if (backslash_count > 0) {
      bool should_escape = false;
      if (in_quotes) {
        // if (backslash_count % 2 == 1 && lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
        //   should_escape = true;
        // } else if (backslash_count % 2 == 0 || !lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
        //   processed_content.append(backslash_count / 2, '\\');
        //   if (backslash_count % 2 == 1) processed_content.push_back('\\');
        // }
        if (backslash_count % 2 == 0 || !lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
            processed_content.push_back('\\');
        } else {
            should_escape = true;
        }
      } else {
        // if (backslash_count % 2 == 0) {
        //   processed_content.append(backslash_count / 2, '\\');
        // } else if (!lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
        //   processed_content.append(backslash_count / 2, '\\');
        //   if (backslash_count > 1) processed_content.push_back('\\');
        //   break; // End token
        // } else {
        //   processed_content.append(backslash_count / 2, '\\');
        //   should_escape = true;
        // }
        if (backslash_count % 2 == 0) {
            processed_content.push_back('\\');
        } else if (!lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) {
            if (backslash_count > 1) processed_content.push_back('\\');
            break;
        } else {
            should_escape = true;
        }
      }
      if (should_escape) {
        processed_content.push_back(ch);
        ++current_pos;
        backslash_count = 0;
        found_content = true;
        should_escape = false;
        continue;
      }
      backslash_count = 0;
    }
    // Check for token boundaries
    if (ch == '"') break;
    if (!in_quotes && (ch == ')' || ch == '|' || ch == '(' || ch == '@' || ch == '-')) break;
    if (!in_quotes && ch != '%' && ch != '*' && lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap())) break;
    // For comatibility, the $ : _ characters are not stripped out.
    if (in_quotes && lexer.IsPunctuation(ch, text_index_schema->GetPunctuationBitmap()) && 
      ch != '$' && ch != ':' && ch != '_') break;
    // Handle special characters for predicate detection
    if (!in_quotes && ch == '%') {
      if (current_pos == pos_) {
        // Leading percent
        while (current_pos < expression_.size() && expression_[current_pos] == '%') {
          leading_percent_count++;
          current_pos++;
          if (leading_percent_count > FUZZY_MAX_DISTANCE) break;
        }
        starts_with_percent = true;
        continue;
      }
      // else if (!found_content) {
      //   // Still in leading percents, continue counting
      //   leading_percent_count++;
      //   current_pos++;
      //   continue;
      // } 
      else {
        // NOOP IF statement. It is handled below.
        // if (!starts_with_percent) {
        //   break;
        // }
        // Trailing percent - count them
        while (current_pos < expression_.size() && expression_[current_pos] == '%' && trailing_percent_count < leading_percent_count) {
          trailing_percent_count++;
          current_pos++;
        }
        break;
      }
    }
    if (!in_quotes && ch == '*') {
      if (current_pos == pos_) {
        starts_with_star = true;
        current_pos++;
        continue;
      } else {
        // Trailing star
        ends_with_star = true;
        current_pos++;
        break;
      }
    }
    // Regular character
    processed_content.push_back(ch);
    found_content = true;
    ++current_pos;
  }
  // Build predicate directly based on detected pattern
  if (!in_quotes && starts_with_percent && leading_percent_count > 0) {
    if (trailing_percent_count == leading_percent_count && leading_percent_count <= FUZZY_MAX_DISTANCE) {
      if (processed_content.empty()) {
        return absl::InvalidArgumentError("Empty fuzzy token");
      }
      std::string lower_content = absl::AsciiStrToLower(processed_content);
      return FilterParser::TokenResult{current_pos, std::make_unique<query::FuzzyPredicate>(text_index, field_mask, lower_content, leading_percent_count)};
    } else {
      return absl::InvalidArgumentError("Invalid fuzzy '%' markers");
    }
  } else if (!in_quotes && starts_with_star) {
    // if (trailing_percent_count > 0) {
    //   return absl::InvalidArgumentError("Mixed wildcard and fuzzy markers");
    // }
    if (processed_content.empty()) {
      return absl::InvalidArgumentError("Invalid wildcard '*' markers");
    }
    std::string lower_content = absl::AsciiStrToLower(processed_content);
    if (ends_with_star) {
      return FilterParser::TokenResult{current_pos, std::make_unique<query::InfixPredicate>(text_index, field_mask, lower_content)};
    } else {
      return FilterParser::TokenResult{current_pos, std::make_unique<query::SuffixPredicate>(text_index, field_mask, lower_content)};
    }
  } else if (!in_quotes && ends_with_star) {
    if (processed_content.empty()) {
      return absl::InvalidArgumentError("Invalid wildcard '*' markers");
    }
    std::string lower_content = absl::AsciiStrToLower(processed_content);
    return FilterParser::TokenResult{current_pos, std::make_unique<query::PrefixPredicate>(text_index, field_mask, lower_content)};
  } else {
    // Term predicate (default case) - apply stopword check and stemming
    std::string lower_content = absl::AsciiStrToLower(processed_content);
    if (lexer.IsStopWord(lower_content, text_index_schema->GetStopWordsSet()) || lower_content.empty()) {
      return FilterParser::TokenResult{current_pos, nullptr}; // Skip stop words
    }
    bool should_stem = true;
    auto stemmed_token = lexer.StemWord(lower_content, text_index_schema->GetStemmer(), should_stem, text_index->GetMinStemSize());
    return FilterParser::TokenResult{current_pos, std::make_unique<query::TermPredicate>(text_index, field_mask, stemmed_token)};
  }
}

absl::StatusOr<std::vector<std::unique_ptr<query::TextPredicate>>>
FilterParser::ParseOneTextAtomIntoTerms(const std::optional<std::string>& field_for_default) {
  auto index = field_for_default.has_value() 
      ? index_schema_.GetIndex(field_for_default.value())
      : index_schema_.GetFirstTextIndex();
  if (!index.ok() || index.value()->GetIndexerType() != indexes::IndexerType::kText) {
    return absl::InvalidArgumentError("Index does not have any text field");
  }
  auto* text_index = dynamic_cast<const indexes::Text*>(index.value().get());
  auto text_index_schema = text_index->GetTextIndexSchema();
  std::vector<std::unique_ptr<query::TextPredicate>> terms;
  uint64_t field_mask;
  if (field_for_default.has_value()) {
    auto identifier = index_schema_.GetIdentifier(field_for_default.value()).value();
    filter_identifiers_.insert(identifier);
    field_mask = 1ULL << text_index->GetTextFieldNumber();
  } else {
    field_mask = ~0ULL;
    auto text_identifiers = index_schema_.GetAllTextIdentifiers();
    for (const auto& identifier : text_identifiers) {
      filter_identifiers_.insert(identifier);
    }
  }
  bool in_quotes = false;
  while (!IsEnd()) {
    char c = Peek();
    if (c == '"') {
      in_quotes = !in_quotes;
      ++pos_;
      if (in_quotes && terms.empty()) continue;
      break;
    }
    if (!in_quotes && (c == ')' || c == '|' || c == '(' || c == '@' || c == '-')) {
      break;
    } 
    size_t token_start = pos_;
    VMSDK_ASSIGN_OR_RETURN(auto result, ParseTokenAndBuildPredicate(in_quotes, text_index_schema.get(), text_index, field_mask));
    if (token_start == result.end_pos) {
      if (!IsEnd()) ++pos_;
      continue;
    }
    if (result.predicate) {
      terms.push_back(std::move(result.predicate));
    }
    pos_ = result.end_pos;
  }
  return terms;
}

// TODO:
// Remove this function once we flatten AND and OR, and delete ProximityAND.
absl::StatusOr<std::unique_ptr<query::Predicate>> FilterParser::ParseTextGroup(
    const std::string& initial_field) {
  std::vector<std::unique_ptr<query::TextPredicate>> all_terms;
  std::vector<std::unique_ptr<query::Predicate>> extra_terms;
  std::string current_field = initial_field;
  while (!IsEnd()) {
    SkipWhitespace();
    if (IsEnd()) break;
    char c = Peek();
    // Stop text group if next is OR/Negate
    if (c == '|' || c == '-') break;
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
    VMSDK_ASSIGN_OR_RETURN(auto terms, ParseOneTextAtomIntoTerms(field_for_atom));
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
      std::string field_name;
      bool non_text = false;
      if (Peek() == '@') {
        VMSDK_ASSIGN_OR_RETURN(field_name, ParseFieldName());
        if (Match('[')) {
          node_count_++;
          VMSDK_ASSIGN_OR_RETURN(predicate, ParseNumericPredicate(field_name));
          non_text = true;
        } else if (Match('{')) {
          node_count_++;
          VMSDK_ASSIGN_OR_RETURN(predicate, ParseTagPredicate(field_name));
          non_text = true;
        }
      }
      if (!non_text) {
        node_count_++;
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
