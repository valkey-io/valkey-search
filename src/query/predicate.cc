/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/query/predicate.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/container/inlined_vector.h"
#include "absl/log/check.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "src/commands/filter_parser.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/orproximity.h"
#include "src/indexes/text/proximity.h"
#include "src/indexes/text/term.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/text/text_iterator.h"
#include "src/indexes/vector_base.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

EvaluationResult NegatePredicate::Evaluate(Evaluator &evaluator) const {
  EvaluationResult result = predicate_->Evaluate(evaluator);
  return EvaluationResult(!result.matches);
}

TermPredicate::TermPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term, bool exact)
    : text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term),
      exact_(exact) {}

EvaluationResult TermPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

PrefixPredicate::PrefixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult PrefixPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

void PrefixPredicate::ForEachMatch(
    const valkey_search::indexes::text::TextIndex &text_index,
    MatchSink sink) const {
  const uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  auto it = text_index.GetPrefix().GetWordIterator(term_);
  for (uint32_t n = 0; !it.Done() && n < max_words; ++n, it.Next()) {
    BACKGROUND_PAUSEPOINT("search_prefix_predicate");
    auto postings = it.GetPostingsTarget();
    if (postings && !sink(it.GetWord(), postings)) return;
  }
}

SuffixPredicate::SuffixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult SuffixPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

void SuffixPredicate::ForEachMatch(
    const valkey_search::indexes::text::TextIndex &text_index,
    MatchSink sink) const {
  // The suffix tree stores reversed words. The parser only builds a suffix
  // predicate for a schema WITHSUFFIXTRIE, and every tree of such a schema has
  // one.
  auto suffix = text_index.GetSuffix();
  CHECK(suffix.has_value()) << "Text index does not have suffix trie enabled.";
  const uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  auto it =
      suffix->get().GetWordIterator(std::string(term_.rbegin(), term_.rend()));
  for (uint32_t n = 0; !it.Done() && n < max_words; ++n, it.Next()) {
    BACKGROUND_PAUSEPOINT("search_suffix_expansion");
    auto postings = it.GetPostingsTarget();
    if (!postings) continue;
    // Buckets and callers want the forward word.
    const absl::string_view reversed = it.GetWord();
    if (!sink(std::string(reversed.rbegin(), reversed.rend()), postings))
      return;
  }
}

InfixPredicate::InfixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult InfixPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

void InfixPredicate::ForEachMatch(
    const valkey_search::indexes::text::TextIndex &text_index,
    MatchSink sink) const {
  // TODO: Implement infix evaluation
  CHECK(false) << "Infix Search - Not implemented";
}

// Counts every fuzzy term that uses byte distance for the fuzzy code-point
// distance compatibility defect (see COMPATIBILITY.md): whether the result
// differs depends on the indexed words, which a cheap check on the term cannot
// tell. Counted here, once per term, because Search runs per key in Evaluate.
// `unit` already selected the legacy path (GetLengthUnit); the macro only
// records it.
static indexes::text::LengthUnit FuzzyLengthUnit(
    const indexes::text::TextIndexSchema &schema) {
  auto unit = schema.GetLanguage().GetLengthUnit();
  if (unit == indexes::text::LengthUnit::kBytes) {
    VALKEY_SEARCH_COMPATIBILITY_FIX(
        1, 3, 0, "fuzzy_code_point_distance", [] {}, [] {});
  }
  return unit;
}

FuzzyPredicate::FuzzyPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term, uint32_t distance)
    : text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term),
      distance_(distance),
      length_unit_(FuzzyLengthUnit(*text_index_schema_)) {}

EvaluationResult FuzzyPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

void FuzzyPredicate::ForEachMatch(
    const valkey_search::indexes::text::TextIndex &text_index,
    MatchSink sink) const {
  const uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  indexes::text::FuzzySearch::Search(
      text_index.GetPrefix(), term_, distance_, max_words, length_unit_,
      [&](absl::string_view word,
          const indexes::text::InvasivePtr<indexes::text::Postings> &postings) {
        BACKGROUND_PAUSEPOINT("search_fuzzy_search");
        return !postings || sink(word, postings);
      });
}

NumericPredicate::NumericPredicate(const indexes::Numeric *index,
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

EvaluationResult NumericPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateNumeric(*this);
}

EvaluationResult NumericPredicate::Evaluate(const double *value) const {
  if (!value) {
    return EvaluationResult(false);
  }
  bool matches =
      (((*value > start_ || (is_inclusive_start_ && *value == start_)) &&
        (*value < end_)) ||
       (is_inclusive_end_ && *value == end_));
  return EvaluationResult(matches);
}

TagPredicate::TagPredicate(const indexes::Tag *index, absl::string_view alias,
                           absl::string_view identifier,
                           absl::string_view raw_tag_string,
                           const absl::flat_hash_set<absl::string_view> &tags)
    : Predicate(PredicateType::kTag),
      index_(index),
      alias_(alias),
      identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      raw_tag_string_(raw_tag_string) {
  // Unescape each tag (e.g., \| -> |, \\ -> \)
  for (const auto &tag : tags) {
    tags_.insert(indexes::Tag::UnescapeTag(tag));
  }
}

EvaluationResult TagPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateTags(*this);
}

VectorRangePredicate::VectorRangePredicate(absl::string_view attribute_alias,
                                           absl::string_view identifier,
                                           double radius,
                                           absl::string_view vector_param_name,
                                           std::optional<std::string> score_as,
                                           std::optional<double> epsilon)
    : Predicate(PredicateType::kVectorRange),
      alias_(attribute_alias),
      identifier_(vmsdk::MakeUniqueValkeyString(identifier)),
      vector_param_name_(vector_param_name),
      score_as_(std::move(score_as)),
      epsilon_(epsilon) {
  SetRadius(radius);
}

EvaluationResult VectorRangePredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateVectorRange(*this);
}

void VectorRangePredicate::SetQueryVector(std::string query) {
  query_vector_ = std::move(query);
}

EvaluationResult TagPredicate::Evaluate(
    const absl::flat_hash_set<absl::string_view> *in_tags,
    bool case_sensitive) const {
  if (!in_tags) {
    return EvaluationResult(false);
  }

  for (const auto &in_tag : *in_tags) {
    for (const auto &tag : tags_) {
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

ComposedPredicate::ComposedPredicate(
    LogicalOperator logical_op,
    std::vector<std::unique_ptr<Predicate>> children,
    std::optional<uint32_t> slop, bool inorder)
    : Predicate(logical_op == LogicalOperator::kAnd
                    ? PredicateType::kComposedAnd
                    : PredicateType::kComposedOr),
      children_(std::move(children)),
      slop_(slop),
      inorder_(inorder) {}

void ComposedPredicate::AddChild(std::unique_ptr<Predicate> child) {
  children_.push_back(std::move(child));
}
// Helper to evaluate text predicates with conditional position requirements
EvaluationResult EvaluatePredicate(const Predicate *predicate,
                                   Evaluator &evaluator, bool require_positions,
                                   bool from_or = false) {
  if (predicate->GetType() == PredicateType::kText) {
    return evaluator.EvaluateText(
        *static_cast<const TextPredicate *>(predicate), require_positions);
  }
  if (predicate->GetType() == PredicateType::kComposedAnd) {
    // Pass down the from_or flag to nested AND
    return static_cast<const ComposedPredicate *>(predicate)
        ->EvaluateWithContext(evaluator, from_or);
  }
  return predicate->Evaluate(evaluator);
}

// ComposedPredicate: Combines two predicates with AND/OR logic.
// For text predicates with proximity constraints (slop/inorder), creates
// ProximityIterator to validate term positions meet distance and order
// requirements.
EvaluationResult ComposedPredicate::Evaluate(Evaluator &evaluator) const {
  return EvaluateWithContext(evaluator, false);
}

EvaluationResult ComposedPredicate::EvaluateWithContext(Evaluator &evaluator,
                                                        bool from_or) const {
  // Determine if children need to return positions for proximity checks.
  bool require_positions = slop_.has_value() || inorder_;
  // Scoring mode: the sum of the matched children's scores.
  float sum = 0.0f;
  // Single-VR model: carry the matched VectorRange distance up the tree so the
  // top-level EvaluationResult exposes it (via HasVrScore()/vr_distance) for
  // the caller to write into Neighbor::distance. Only one VR predicate can be
  // present per query (enforced at parse time), so at most one child sets this.
  bool has_vr_distance = false;
  float vr_distance = 0.0f;
  auto carry_vr = [&](const EvaluationResult &r) {
    if (r.has_vr_distance) {
      has_vr_distance = true;
      vr_distance = r.vr_distance;
    }
  };
  auto matched = [&](EvaluationResult &&r) -> EvaluationResult {
    if (has_vr_distance) {
      r.has_vr_distance = true;
      r.vr_distance = vr_distance;
    }
    r.score = GetWeight() * sum;
    return std::move(r);
  };
  // Handle AND logic
  if (GetType() == PredicateType::kComposedAnd) {
    uint32_t childrenWithPositions = 0;
    uint64_t query_field_mask = ~0ULL;
    absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                        indexes::text::kProximityTermsInlineCapacity>
        iterators;
    for (const auto &child : children_) {
      // In AND: skip text children when the entries fetcher layer already fully
      // (recursively) resolved them. The only cases where this is not true are:
      // 1) when an AND predicate contains an OR which has some other non text
      // children and an AND child containing text. This is not solved in
      // entries fetcher yet.
      // 2) when the query has negation on text. Currently, a universal set is
      // used in the entries fetcher layer for text+negate queries.
      if (evaluator.SourceResolvesText() &&
          child->GetType() == PredicateType::kText && !from_or &&
          !(evaluator.GetQueryOperations() &
            QueryOperations::kContainsNegate)) {
        continue;
      }
      BACKGROUND_PAUSEPOINT("search_composed_predicate");
      EvaluationResult result =
          EvaluatePredicate(child.get(), evaluator, require_positions, from_or);
      // Short-circuit on first false
      if (!result.matches) {
        return EvaluationResult(false);
      }
      sum += result.score;
      carry_vr(result);
      if (result.filter_iterator) {
        childrenWithPositions++;
        query_field_mask &= result.filter_iterator->QueryFieldMask();
        iterators.push_back(std::move(result.filter_iterator));
      }
      VMSDK_LOG(DEBUG, nullptr)
          << "Inline evaluate AND predicate child: " << result.matches;
    }
    // Proximity check: Only if slop/inorder set and both sides have
    // iterators. This ensures we only check proximity for text predicates,
    // not numeric/tag.
    if (require_positions && (childrenWithPositions >= 2)) {
      // Short circuit if no common fields across all children.
      if (query_field_mask == 0) {
        return EvaluationResult(false);
      }
      // Create ProximityIterator to check proximity
      auto proximity_iterator =
          std::make_unique<indexes::text::ProximityIterator>(
              std::move(iterators), slop_, inorder_, false);
      // Check if any valid proximity matches exist
      if (!proximity_iterator->IsIteratorValid()) {
        return EvaluationResult(false);
      }
      // Validate against original target key from evaluator
      const auto &target_key = evaluator.GetTargetKey();
      if (target_key && proximity_iterator->CurrentKey() != target_key) {
        return EvaluationResult(false);
      }
      // Return the proximity iterator for potential nested use.
      return matched({true, std::move(proximity_iterator)});
    }
    // Propagate the filter iterator from the one child exists
    else if (childrenWithPositions == 1) {
      return matched({true, std::move(iterators[0])});
    }
    // All matched, but none have position. non-proximity case
    return matched(EvaluationResult(true));
  }
  // Handle OR logic
  auto filter_iterators =
      absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                          indexes::text::kProximityTermsInlineCapacity>();
  // A match with no positions to pass up settles the OR.
  bool settled = false;
  for (const auto &child : children_) {
    EvaluationResult result =
        EvaluatePredicate(child.get(), evaluator, require_positions, true);
    if (!result.matches) continue;
    sum += result.score;
    carry_vr(result);
    if (require_positions && result.filter_iterator) {
      filter_iterators.push_back(std::move(result.filter_iterator));
    } else {
      settled = true;
    }
    // Scoring visits every child to sum their scores.
    if (settled && !evaluator.Scoring()) break;
  }
  if (settled) {
    return matched(EvaluationResult(true));
  }
  // No matches found.
  if (filter_iterators.empty()) {
    return EvaluationResult(false);
  }
  // In case positional awareness is required, use a OrProximityIterator.
  auto or_proximity_iterator =
      std::make_unique<indexes::text::OrProximityIterator>(
          std::move(filter_iterators));
  // Check if any valid matches exist
  if (!or_proximity_iterator->IsIteratorValid()) {
    return EvaluationResult(false);
  }
  // Validate against original target key from evaluator
  auto target_key = evaluator.GetTargetKey();
  if (target_key && or_proximity_iterator->CurrentKey() != target_key) {
    return EvaluationResult(false);
  }
  // Return the OR proximity iterator for potential nested scenarios.
  return matched({true, std::move(or_proximity_iterator)});
}

}  // namespace valkey_search::query
