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
#include "src/indexes/text.h"
#include "src/indexes/text/proximity.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/text/text_iterator.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

EvaluationResult NegatePredicate::Evaluate(Evaluator& evaluator) const {
  EvaluationResult result = predicate_->Evaluate(evaluator);
  return EvaluationResult(!result.matches);
}

TermPredicate::TermPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term, bool exact_)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term),
      exact_(exact_) {}

EvaluationResult TermPredicate::Evaluate(Evaluator& evaluator) const {
  // call dynamic dispatch on the evaluator
  return evaluator.EvaluateText(*this);
}

// TermPredicate: Exact term match in the text index.
EvaluationResult TermPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key) const {
  uint64_t field_mask = field_mask_;
  auto word_iter = text_index.prefix_.GetWordIterator(term_);
  if (word_iter.Done()) {
    return EvaluationResult(false);
  }

  auto postings = word_iter.GetTarget();
  if (!postings) {
    return EvaluationResult(false);
  }

  auto key_iter = postings->GetKeyIterator();
  // Skip to target key and verify it contains the required fields
  if (!key_iter.SkipForwardKey(target_key) ||
      !key_iter.ContainsFields(field_mask)) {
    return EvaluationResult(false);
  }

  std::vector<indexes::text::Postings::KeyIterator> key_iterators;
  key_iterators.emplace_back(std::move(key_iter));
  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, nullptr);

  if (iterator->DonePositions()) {
    return EvaluationResult(false);
  }
  return EvaluationResult(true, std::move(iterator));
}

PrefixPredicate::PrefixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult PrefixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

// PrefixPredicate: Matches all terms that start with the given prefix.
EvaluationResult PrefixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key) const {
  uint64_t field_mask = field_mask_;

  auto word_iter = text_index.prefix_.GetWordIterator(term_);
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;

  while (!word_iter.Done()) {
    std::string_view word = word_iter.GetWord();
    if (!word.starts_with(term_)) break;

    auto postings = word_iter.GetTarget();
    if (postings) {
      auto key_iter = postings->GetKeyIterator();
      // Skip to target key and verify it contains the required fields
      if (key_iter.SkipForwardKey(target_key) &&
          key_iter.ContainsFields(field_mask)) {
        key_iterators.emplace_back(std::move(key_iter));
      }
    }
    word_iter.Next();
  }

  if (key_iterators.empty()) {
    return EvaluationResult(false);
  }

  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, nullptr);

  if (iterator->DonePositions()) {
    return EvaluationResult(false);
  }

  return EvaluationResult(true, std::move(iterator));
}

SuffixPredicate::SuffixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult SuffixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

// SuffixPredicate: Matches terms that end with the given suffix
EvaluationResult SuffixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key) const {
  uint64_t field_mask = field_mask_;

  if (!text_index.suffix_.has_value()) {
    return EvaluationResult(false);
  }

  std::string reversed_term(term_.rbegin(), term_.rend());
  auto word_iter = text_index.suffix_->GetWordIterator(reversed_term);
  std::vector<indexes::text::Postings::KeyIterator> key_iterators;

  while (!word_iter.Done()) {
    std::string_view word = word_iter.GetWord();
    if (!word.starts_with(reversed_term)) break;

    auto postings = word_iter.GetTarget();
    if (postings) {
      auto key_iter = postings->GetKeyIterator();
      // Skip to target key and verify it contains the required fields
      if (key_iter.SkipForwardKey(target_key) &&
          key_iter.ContainsFields(field_mask)) {
        key_iterators.emplace_back(std::move(key_iter));
      }
    }
    word_iter.Next();
  }

  if (key_iterators.empty()) {
    return EvaluationResult(false);
  }

  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, nullptr);
  if (iterator->DonePositions()) {
    return EvaluationResult(false);
  }

  return EvaluationResult(true, std::move(iterator));
}

InfixPredicate::InfixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult InfixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

EvaluationResult InfixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key) const {
  // TODO: Implement infix evaluation
  return EvaluationResult(false);
}

FuzzyPredicate::FuzzyPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term, uint32_t distance)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term),
      distance_(distance) {}

EvaluationResult FuzzyPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

EvaluationResult FuzzyPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key) const {
  // TODO: Implement fuzzy evaluation
  return EvaluationResult(false);
}

// TODO: Remove proximity evaluator
ProximityPredicate::ProximityPredicate(
    std::vector<std::unique_ptr<TextPredicate>> terms, uint32_t slop,
    bool inorder)
    : TextPredicate(),
      terms_(std::move(terms)),
      inorder_(inorder),
      slop_(slop) {}

EvaluationResult ProximityPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this);
}

EvaluationResult ProximityPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key) const {
  return EvaluationResult(false);
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

EvaluationResult NumericPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateNumeric(*this);
}

EvaluationResult NumericPredicate::Evaluate(const double* value) const {
  if (!value) {
    return EvaluationResult(false);
  }
  bool matches =
      (((*value > start_ || (is_inclusive_start_ && *value == start_)) &&
        (*value < end_)) ||
       (is_inclusive_end_ && *value == end_));
  return EvaluationResult(matches);
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

EvaluationResult TagPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateTags(*this);
}

EvaluationResult TagPredicate::Evaluate(
    const absl::flat_hash_set<absl::string_view>* in_tags,
    bool case_sensitive) const {
  if (!in_tags) {
    return EvaluationResult(false);
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
          return EvaluationResult(true);
        }
      } else {
        if (absl::EqualsIgnoreCase(left_hand_side, right_hand_side)) {
          return EvaluationResult(true);
        }
      }
    }
  }
  return EvaluationResult(false);
}

ComposedPredicate::ComposedPredicate(std::unique_ptr<Predicate> lhs_predicate,
                                     std::unique_ptr<Predicate> rhs_predicate,
                                     LogicalOperator logical_op,
                                     std::optional<uint32_t> slop, bool inorder)
    : Predicate(logical_op == LogicalOperator::kAnd
                    ? PredicateType::kComposedAnd
                    : PredicateType::kComposedOr),
      lhs_predicate_(std::move(lhs_predicate)),
      rhs_predicate_(std::move(rhs_predicate)),
      slop_(slop),
      inorder_(inorder) {}

// ComposedPredicate: Combines two predicates with AND/OR logic.
// For text predicates with proximity constraints (slop/inorder), creates
// ProximityIterator to validate term positions meet distance and order
// requirements.
EvaluationResult ComposedPredicate::Evaluate(Evaluator& evaluator) const {
  if (GetType() == PredicateType::kComposedAnd) {
    EvaluationResult lhs = lhs_predicate_->Evaluate(evaluator);
    VMSDK_LOG(DEBUG, nullptr)
        << "Inline evaluate AND predicate lhs: " << lhs.matches;

    // Short-circuit for AND
    if (!lhs.matches) {
      return EvaluationResult(false);
    }

    EvaluationResult rhs = rhs_predicate_->Evaluate(evaluator);
    VMSDK_LOG(DEBUG, nullptr)
        << "Inline evaluate AND predicate rhs: " << rhs.matches;
    // Short-circuit for AND
    if (!rhs.matches) {
      return EvaluationResult(false);
    }

    // Proximity check: Only if slop/inorder set and both sides have
    // iterators. This ensures we only check proximity for text predicates,
    // not numeric/tag.
    if ((slop_.has_value() || inorder_) && lhs.filter_iterator &&
        rhs.filter_iterator) {
      // Get field_mask from lhs iterator
      uint64_t field_mask = lhs.filter_iterator->FieldMask();

      // Create vector of iterators for ProximityIterator
      std::vector<std::unique_ptr<indexes::text::TextIterator>> iterators;
      iterators.push_back(std::move(lhs.filter_iterator));
      iterators.push_back(std::move(rhs.filter_iterator));

      // Create ProximityIterator to check proximity
      auto proximity_iterator =
          std::make_unique<indexes::text::ProximityIterator>(
              std::move(iterators), slop_, inorder_, field_mask, nullptr);

      // Check if any valid proximity matches exist
      if (proximity_iterator->DonePositions()) {
        return EvaluationResult(false);
      }
      // Validate against original target key from evaluator
      auto target_key = evaluator.GetTargetKey();
      if (target_key && proximity_iterator->CurrentKey() != target_key) {
        return EvaluationResult(false);
      }

      // Return the proximity iterator for potential nested use.
      return EvaluationResult(true, std::move(proximity_iterator));
    }

    // Propagate the filter iterator from the LHS if it exists
    if (lhs.filter_iterator) {
      return EvaluationResult(true, std::move(lhs.filter_iterator));
    }
    // Propagate the filter iterator from the RHS if it exists
    if (rhs.filter_iterator) {
      return EvaluationResult(true, std::move(rhs.filter_iterator));
    }
    // Both matched, non-proximity case
    return EvaluationResult(true);
  }

  // OR logic
  EvaluationResult lhs = lhs_predicate_->Evaluate(evaluator);
  VMSDK_LOG(DEBUG, nullptr)
      << "Inline evaluate OR predicate lhs: " << lhs.matches;
  EvaluationResult rhs = rhs_predicate_->Evaluate(evaluator);
  VMSDK_LOG(DEBUG, nullptr)
      << "Inline evaluate OR predicate rhs: " << rhs.matches;

  // TODO: Implement position-aware OR logic for nested proximity queries.
  return EvaluationResult(lhs.matches || rhs.matches);
}

}  // namespace valkey_search::query
