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

// Helper function to build EvaluationResult for text predicates.
EvaluationResult BuildTextEvaluationResult(
    std::unique_ptr<indexes::text::TextIterator> iterator) {
  if (!iterator->IsIteratorValid()) {
    return EvaluationResult(false);
  }
  return {true, std::move(iterator)};
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

namespace {

using PositionMaps = indexes::text::KeyTermIterator::PositionMaps;

// Probes `postings` for `target_key` in `field_mask`; the key's position map
// is read under the word's bucket and retained for `require_positions`. It
// stays valid after the bucket is released: only removing the key frees it,
// which cannot be in flight for a key under evaluation.
bool ProbePostings(const indexes::text::TextIndexSchema &schema,
                   const indexes::text::Postings &postings,
                   absl::string_view word, const InternedStringPtr &target_key,
                   uint64_t field_mask, bool require_positions, bool lock,
                   PositionMaps &maps) {
  auto value = schema.WithWordLock(word, lock, [&] {
    return postings.LookupKey(BorrowedInternedStringPtr(target_key),
                              field_mask);
  });
  if (!value) return false;
  if (require_positions) maps.push_back(value->map);
  return true;
}

bool ProbeWord(const indexes::text::TextIndexSchema &schema,
               const valkey_search::indexes::text::TextIndex &text_index,
               absl::string_view word, const InternedStringPtr &target_key,
               uint64_t field_mask, bool require_positions, bool lock,
               PositionMaps &maps) {
  auto word_iter = text_index.GetPrefix().GetWordIterator(word);
  if (word_iter.Done() || word_iter.GetWord() != word) return false;
  auto postings = word_iter.GetPostingsTarget();
  return postings && ProbePostings(schema, *postings, word, target_key,
                                   field_mask, require_positions, lock, maps);
}

}  // namespace

// TermPredicate: Exact term match in the text index.
EvaluationResult TermPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex &text_index,
    const InternedStringPtr &target_key, bool require_positions,
    bool lock) const {
  uint64_t field_mask = field_mask_;
  PositionMaps maps;
  // Search for the original word - may or may not exist in corpus
  BACKGROUND_PAUSEPOINT("search_term_predicate");
  bool found_original =
      ProbeWord(*text_index_schema_, text_index, term_, target_key, field_mask,
                require_positions, lock, maps);
  if (found_original && !require_positions) {
    return EvaluationResult(true);
  }
  // Get stem variants if not exact term search
  uint64_t stem_field_mask =
      field_mask & text_index_schema_->GetStemTextFieldMask();
  if (!exact_ && stem_field_mask != 0) {
    bool matched = false;
    text_index_schema_->WithStemParents(
        term_, lock,
        [&](const std::string &stemmed, absl::Span<const std::string> parents,
            uint32_t) {
          // Search for the stemmed word itself - may or may not exist in corpus
          if (stemmed != term_) {
            matched |=
                ProbeWord(*text_index_schema_, text_index, stemmed, target_key,
                          stem_field_mask, require_positions, lock, maps);
          }
          // Search for stem variants - these should all exist from ingestion
          for (const auto &parent : parents) {
            matched |=
                ProbeWord(*text_index_schema_, text_index, parent, target_key,
                          stem_field_mask, require_positions, lock, maps);
          }
        });
    if (matched && !require_positions) {
      return EvaluationResult(true);
    }
  }
  if (maps.empty()) {
    return EvaluationResult(false);
  }
  auto iterator = std::make_unique<indexes::text::KeyTermIterator>(
      target_key, maps, field_mask);
  return BuildTextEvaluationResult(std::move(iterator));
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

// PrefixPredicate: Matches all terms that start with the given prefix.
EvaluationResult PrefixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex &text_index,
    const InternedStringPtr &target_key, bool require_positions,
    bool lock) const {
  uint64_t field_mask = field_mask_;
  auto word_iter = text_index.GetPrefix().GetWordIterator(term_);
  PositionMaps maps;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  bool matched = false;
  while (!word_iter.Done() && word_count < max_words) {
    BACKGROUND_PAUSEPOINT("search_prefix_predicate");
    auto postings = word_iter.GetPostingsTarget();
    if (postings) {
      matched |=
          ProbePostings(*text_index_schema_, *postings, word_iter.GetWord(),
                        target_key, field_mask, require_positions, lock, maps);
    }
    word_iter.Next();
    ++word_count;
  }
  if (!matched) {
    return EvaluationResult(false);
  }
  if (!require_positions) {
    return EvaluationResult(true);
  }
  auto iterator = std::make_unique<indexes::text::KeyTermIterator>(
      target_key, maps, field_mask);
  return BuildTextEvaluationResult(std::move(iterator));
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

// SuffixPredicate: Matches terms that end with the given suffix
EvaluationResult SuffixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex &text_index,
    const InternedStringPtr &target_key, bool require_positions,
    bool lock) const {
  uint64_t field_mask = field_mask_;
  auto suffix_opt = text_index.GetSuffix();
  if (!suffix_opt.has_value()) {
    return EvaluationResult(false);
  }
  std::string reversed_term(term_.rbegin(), term_.rend());
  auto word_iter = suffix_opt.value().get().GetWordIterator(reversed_term);
  PositionMaps maps;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  bool matched = false;
  while (!word_iter.Done() && word_count < max_words) {
    BACKGROUND_PAUSEPOINT("search_suffix_expansion");
    std::string_view reversed = word_iter.GetWord();
    if (!reversed.starts_with(reversed_term)) {
      break;
    }
    auto postings = word_iter.GetPostingsTarget();
    if (postings) {
      // Buckets are keyed on the forward word, as CommitKeyData locks them.
      const std::string word(reversed.rbegin(), reversed.rend());
      matched |= ProbePostings(*text_index_schema_, *postings, word, target_key,
                               field_mask, require_positions, lock, maps);
    }
    word_iter.Next();
    ++word_count;
  }
  if (!matched) {
    return EvaluationResult(false);
  }
  if (!require_positions) {
    return EvaluationResult(true);
  }
  auto iterator = std::make_unique<indexes::text::KeyTermIterator>(
      target_key, maps, field_mask);
  return BuildTextEvaluationResult(std::move(iterator));
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

EvaluationResult InfixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex &text_index,
    const InternedStringPtr &target_key, bool require_positions,
    bool lock) const {
  // TODO: Implement infix evaluation
  CHECK(false) << "Infix Search - Not implemented";
  return EvaluationResult(false);
}

FuzzyPredicate::FuzzyPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term, uint32_t distance)
    : text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term),
      distance_(distance) {}

EvaluationResult FuzzyPredicate::Evaluate(Evaluator &evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

EvaluationResult FuzzyPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex &text_index,
    const InternedStringPtr &target_key, bool require_positions,
    bool lock) const {
  uint64_t field_mask = field_mask_;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  PositionMaps maps;
  bool matched = false;
  indexes::text::FuzzySearch::Search(
      text_index.GetPrefix(), term_, distance_, max_words,
      [&](absl::string_view word,
          const indexes::text::InvasivePtr<indexes::text::Postings> &postings) {
        BACKGROUND_PAUSEPOINT("search_fuzzy_search");
        matched |=
            ProbePostings(*text_index_schema_, *postings, word, target_key,
                          field_mask, require_positions, lock, maps);
        return true;
      });
  if (!matched) {
    return EvaluationResult(false);
  }
  if (!require_positions) {
    return EvaluationResult(true);
  }
  auto iterator = std::make_unique<indexes::text::KeyTermIterator>(
      target_key, maps, field_mask);
  return BuildTextEvaluationResult(std::move(iterator));
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
  // Handle AND logic
  if (GetType() == PredicateType::kComposedAnd) {
    uint32_t childrenWithPositions = 0;
    uint64_t query_field_mask = ~0ULL;
    absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                        indexes::text::kProximityTermsInlineCapacity>
        iterators;
    for (const auto &child : children_) {
      // In AND: skip text children when in prefilter evaluation because text in
      // AND is fully (recursively) resolved in the entries fetcher layer
      // already. The only cases where this is not true are:
      // 1) when an AND predicate contains an OR which has some other non text
      // children and an AND child containing text. This is not solved in
      // entries fetcher yet.
      // 2) when the query has negation on text. Currently, a universal set is
      // used in the entries fetcher layer for text+negate queries.
      if (evaluator.IsPrefilterEvaluator() &&
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
      return {true, std::move(proximity_iterator)};
    }
    // Propagate the filter iterator from the one child exists
    else if (childrenWithPositions == 1) {
      return {true, std::move(iterators[0])};
    }
    // All matched, but none have position. non-proximity case
    return EvaluationResult(true);
  }
  // Handle OR logic
  auto filter_iterators =
      absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                          indexes::text::kProximityTermsInlineCapacity>();
  for (const auto &child : children_) {
    EvaluationResult result =
        EvaluatePredicate(child.get(), evaluator, require_positions, true);
    // Short-circuit if any matches and positions not required.
    if (result.matches && !require_positions) {
      return EvaluationResult(true);
    } else if (result.matches) {
      if (result.filter_iterator == nullptr) {
        return EvaluationResult(true);
      }
      filter_iterators.push_back(std::move(result.filter_iterator));
    }
  }
  // No matches found.
  if (!require_positions || filter_iterators.empty()) {
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
  return {true, std::move(or_proximity_iterator)};
}

}  // namespace valkey_search::query
