/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/predicate.h"

#include <memory>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

bool NegatePredicate::Evaluate(Evaluator& evaluator) const {
  return !predicate_->Evaluate(evaluator);
}

TermPredicate::TermPredicate(const indexes::Text* index,
                            FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      index_(index),
      // identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      field_mask_(field_mask),
      term_(term) {}

bool TermPredicate::Evaluate(Evaluator& evaluator) const {
  // call dynamic dispatch on the evaluator
  return evaluator.EvaluateText(*this);
}

bool TermPredicate::Evaluate(const std::string_view& text) const {
  if (text.empty()) return false;
  return text == term_;  // exact match
}

PrefixPredicate::PrefixPredicate(const indexes::Text* index,
                            FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      index_(index),
      // identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      // alias_(alias),
      field_mask_(field_mask),
      term_(term) {}

bool PrefixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

bool PrefixPredicate::Evaluate(const std::string_view& text) const {
  if (text.empty()) return false;
  return absl::StartsWith(text, term_);
}

SuffixPredicate::SuffixPredicate(const indexes::Text* index,
                            FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      index_(index),
      // identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      // alias_(alias),
      field_mask_(field_mask),
      term_(term) {}

bool SuffixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

bool SuffixPredicate::Evaluate(const std::string_view& text) const {
  if (text.empty()) return false;
  return absl::EndsWith(text, term_);
}

InfixPredicate::InfixPredicate(const indexes::Text* index,
                            FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      index_(index),
      // identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      // alias_(alias),
      field_mask_(field_mask),
      term_(term) {}

bool InfixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

bool InfixPredicate::Evaluate(const std::string_view& text) const {
  if (text.empty()) return false;
  return absl::StrContains(text, term_);
}

FuzzyPredicate::FuzzyPredicate(const indexes::Text* index,
                               FieldMaskPredicate field_mask, std::string term,
                               uint32_t distance)
    : TextPredicate(),
      index_(index),
      // identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      // alias_(alias),
      field_mask_(field_mask),
      term_(term),
      distance_(distance) {}

bool FuzzyPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

bool FuzzyPredicate::Evaluate(const std::string_view& text) const {
  if (text.empty()) return false;

  // Implement fuzzy matching logic here
  // For simplicity, we can use a placeholder implementation
  // In a real implementation, you would use a library or algorithm for fuzzy
  // matching
  return absl::StrContains(text, term_);  // Placeholder for actual fuzzy logic
}

ProximityPredicate::ProximityPredicate(
    std::vector<std::unique_ptr<TextPredicate>> terms, uint32_t slop,
    bool inorder)
    : TextPredicate(),
      terms_(std::move(terms)),
      inorder_(inorder),
      slop_(slop) {}

bool ProximityPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

NumericPredicate::NumericPredicate(const indexes::Numeric* index,
                                   absl::string_view alias,
                                   absl::string_view identifier, double start,
                                   bool is_inclusive_start, double end,
                                   bool is_inclusive_end)
    : Predicate(PredicateType::kNumeric),
      index_(index),
      alias_(alias),
      identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      start_(start),
      is_inclusive_start_(is_inclusive_start),
      end_(end),
      is_inclusive_end_(is_inclusive_end) {}

bool NumericPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateNumeric(*this);
}

bool NumericPredicate::Evaluate(const double* value) const {
  if (!value) {
    return false;
  }
  return (((*value > start_ || (is_inclusive_start_ && *value == start_)) &&
           (*value < end_)) ||
          (is_inclusive_end_ && *value == end_));
}

TagPredicate::TagPredicate(const indexes::Tag* index, absl::string_view alias,
                           absl::string_view identifier,
                           absl::string_view raw_tag_string,
                           const absl::flat_hash_set<absl::string_view>& tags)
    : Predicate(PredicateType::kTag),
      index_(index),
      alias_(alias),
      identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      raw_tag_string_(raw_tag_string),
      tags_(tags.begin(), tags.end()) {}

bool TagPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateTags(*this);
}

bool TagPredicate::Evaluate(
    const absl::flat_hash_set<absl::string_view>* in_tags,
    bool case_sensitive) const {
  if (!in_tags) {
    return false;
  }

  for (const auto& in_tag : *in_tags) {
    for (const auto& tag : tags_) {
      absl::string_view left_hand_side = in_tag;
      absl::string_view right_hand_side = tag;
      if (right_hand_side.back() == '*') {
        if (left_hand_side.length() < right_hand_side.length() - 1) {
          continue;
        }
        left_hand_side = left_hand_side.substr(0, right_hand_side.length() - 1);
        right_hand_side =
            right_hand_side.substr(0, right_hand_side.length() - 1);
      }
      if (case_sensitive) {
        if (left_hand_side == right_hand_side) {
          return true;
        }
      } else {
        if (absl::EqualsIgnoreCase(left_hand_side, right_hand_side)) {
          return true;
        }
      }
    }
  }
  return false;
}

ComposedPredicate::ComposedPredicate(std::unique_ptr<Predicate> lhs_predicate,
                                     std::unique_ptr<Predicate> rhs_predicate,
                                     LogicalOperator logical_op)
    : Predicate(logical_op == LogicalOperator::kAnd
                    ? PredicateType::kComposedAnd
                    : PredicateType::kComposedOr),
      lhs_predicate_(std::move(lhs_predicate)),
      rhs_predicate_(std::move(rhs_predicate)) {}

bool ComposedPredicate::Evaluate(Evaluator& evaluator) const {
  if (GetType() == PredicateType::kComposedAnd) {
    auto lhs = lhs_predicate_->Evaluate(evaluator);
    VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate AND predicate lhs: " << lhs;
    auto rhs = rhs_predicate_->Evaluate(evaluator);
    VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate AND predicate rhs: " << rhs;
    return lhs && rhs;
  }

  auto lhs = lhs_predicate_->Evaluate(evaluator);
  VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate OR predicate lhs: " << lhs;
  auto rhs = rhs_predicate_->Evaluate(evaluator);
  VMSDK_LOG(DEBUG, nullptr) << "Inline evaluate OR predicate rhs: " << rhs;
  return lhs || rhs;
}

}  // namespace valkey_search::query
