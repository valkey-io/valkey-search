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
#include "absl/container/inlined_vector.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/text.h"
#include "src/indexes/text/fuzzy.h"
#include "src/indexes/text/orproximity.h"
#include "src/indexes/text/proximity.h"
#include "src/indexes/text/text_index.h"
#include "src/indexes/text/text_iterator.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"

namespace valkey_search::query {

EvaluationResult NegatePredicate::Evaluate(Evaluator& evaluator) const {
  EvaluationResult result = predicate_->Evaluate(evaluator);
  return EvaluationResult(!result.matches);
}

// Helper function to build EvaluationResult for text predicates.
EvaluationResult BuildTextEvaluationResult(
    std::unique_ptr<indexes::text::TextIterator> iterator) {
  if (!iterator->IsIteratorValid()) {
    return EvaluationResult(false);
  }
  return EvaluationResult(true, std::move(iterator));
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
  return evaluator.EvaluateText(*this, false);
}

namespace {

// Helper to search for a word in the text index and add matching key iterator
// for prefilter Returns true if the word was found and a valid key iterator was
// added
bool TryAddWordKeyIteratorForPrefilter(
    const valkey_search::indexes::text::TextIndex& text_index,
    absl::string_view word, const InternedStringPtr& target_key,
    uint64_t field_mask, bool require_positions,
    absl::InlinedVector<
        valkey_search::indexes::text::Postings::KeyIterator,
        valkey_search::indexes::text::kWordExpansionInlineCapacity>&
        key_iterators) {
  auto word_iter = text_index.GetPrefix().GetWordIterator(word);
  if (!word_iter.Done() && word_iter.GetWord() == word) {
    auto postings = word_iter.GetTarget();
    if (postings) {
      auto key_iter = postings->GetKeyIterator();
      if (key_iter.SkipForwardKey(target_key) &&
          key_iter.ContainsFields(field_mask)) {
        if (require_positions) {
          key_iterators.emplace_back(std::move(key_iter));
        }
        return true;
      }
    }
  }
  return false;
}

}  // namespace

// TermPredicate: Exact term match in the text index.
EvaluationResult TermPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key, bool require_positions) const {
  uint64_t field_mask = field_mask_;
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  
  if (exact_) {
    // For exact searches, only search for the original word
    TryAddWordKeyIteratorForPrefilter(text_index, term_, target_key,
                                     field_mask, require_positions,
                                     key_iterators);
  } else {
    // For non-exact searches, field_mask_ is already stem_field_mask from filter_parser.cc
    // Collect stem variant words (words that also stem to the same form)
    absl::InlinedVector<absl::string_view,
                        indexes::text::kStemVariantsInlineCapacity>
        stem_variants;
    std::string stemmed = text_index_schema_->GetAllStemVariants(
        term_, stem_variants, text_index_schema_->GetMinStemSize(),
        field_mask, true);
    // Search for the stemmed word itself - may or may not exist in corpus
    if (stemmed != term_) {
      if (TryAddWordKeyIteratorForPrefilter(text_index, stemmed, target_key,
                                            field_mask, require_positions,
                                            key_iterators)) {
        if (!require_positions) {
          return EvaluationResult(true);
        }
      }
    }
    // Search for stem variants - these should all exist from ingestion
    for (const auto& variant : stem_variants) {
      // Note: During prefilter, variants might not exist in this specific doc's per-key index
      TryAddWordKeyIteratorForPrefilter(
          text_index, variant, target_key, field_mask, require_positions,
          key_iterators);
    }
  }
  
  if (key_iterators.empty()) {
    return EvaluationResult(false);
  }
  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, nullptr, require_positions);
  return BuildTextEvaluationResult(std::move(iterator));
}

PrefixPredicate::PrefixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult PrefixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

// PrefixPredicate: Matches all terms that start with the given prefix.
EvaluationResult PrefixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key, bool require_positions) const {
  uint64_t field_mask = field_mask_;
  auto word_iter = text_index.GetPrefix().GetWordIterator(term_);
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  while (!word_iter.Done() && word_count < max_words) {
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
    ++word_count;
  }
  if (key_iterators.empty()) {
    return EvaluationResult(false);
  }
  if (!require_positions) {
    return EvaluationResult(true);
  }
  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, nullptr, require_positions);
  return BuildTextEvaluationResult(std::move(iterator));
}

SuffixPredicate::SuffixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult SuffixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

// SuffixPredicate: Matches terms that end with the given suffix
EvaluationResult SuffixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key, bool require_positions) const {
  uint64_t field_mask = field_mask_;
  auto suffix_opt = text_index.GetSuffix();
  if (!suffix_opt.has_value()) {
    return EvaluationResult(false);
  }
  std::string reversed_term(term_.rbegin(), term_.rend());
  auto word_iter = suffix_opt.value().get().GetWordIterator(reversed_term);
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      key_iterators;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  uint32_t word_count = 0;
  while (!word_iter.Done() && word_count < max_words) {
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
    ++word_count;
  }
  if (key_iterators.empty()) {
    return EvaluationResult(false);
  }
  if (!require_positions) {
    return EvaluationResult(true);
  }
  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(key_iterators), field_mask, nullptr, require_positions);
  return BuildTextEvaluationResult(std::move(iterator));
}

InfixPredicate::InfixPredicate(
    std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema,
    FieldMaskPredicate field_mask, std::string term)
    : TextPredicate(),
      text_index_schema_(text_index_schema),
      field_mask_(field_mask),
      term_(term) {}

EvaluationResult InfixPredicate::Evaluate(Evaluator& evaluator) const {
  return evaluator.EvaluateText(*this, false);
}

EvaluationResult InfixPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key, bool require_positions) const {
  // TODO: Implement infix evaluation
  CHECK(false) << "Infix Search - Not implemented";
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
  return evaluator.EvaluateText(*this, false);
}

EvaluationResult FuzzyPredicate::Evaluate(
    const valkey_search::indexes::text::TextIndex& text_index,
    const InternedStringPtr& target_key, bool require_positions) const {
  uint64_t field_mask = field_mask_;
  // Limit the number of term word expansions
  uint32_t max_words = options::GetMaxTermExpansions().GetValue();
  // Get all KeyIterators for words within edit distance
  auto key_iters = indexes::text::FuzzySearch::Search(
      text_index.GetPrefix(), term_, distance_, max_words);
  // Filter to only include KeyIterators that match target_key and field_mask
  absl::InlinedVector<indexes::text::Postings::KeyIterator,
                      indexes::text::kWordExpansionInlineCapacity>
      filtered_key_iterators;
  for (auto& key_iter : key_iters) {
    if (key_iter.SkipForwardKey(target_key) &&
        key_iter.ContainsFields(field_mask)) {
      filtered_key_iterators.emplace_back(std::move(key_iter));
    }
  }
  if (filtered_key_iterators.empty()) {
    return EvaluationResult(false);
  }
  if (!require_positions) {
    return EvaluationResult(true);
  }
  auto iterator = std::make_unique<indexes::text::TermIterator>(
      std::move(filtered_key_iterators), field_mask, nullptr,
      require_positions);
  return BuildTextEvaluationResult(std::move(iterator));
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
      raw_tag_string_(raw_tag_string) {
  // Unescape each tag (e.g., \| -> |, \\ -> \)
  for (const auto& tag : tags) {
    tags_.insert(indexes::Tag::UnescapeTag(tag));
  }
}

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
EvaluationResult EvaluatePredicate(const Predicate* predicate,
                                   Evaluator& evaluator,
                                   bool require_positions) {
  if (predicate->GetType() == PredicateType::kText) {
    return evaluator.EvaluateText(*static_cast<const TextPredicate*>(predicate),
                                  require_positions);
  }
  return predicate->Evaluate(evaluator);
}

// ComposedPredicate: Combines two predicates with AND/OR logic.
// For text predicates with proximity constraints (slop/inorder), creates
// ProximityIterator to validate term positions meet distance and order
// requirements.
EvaluationResult ComposedPredicate::Evaluate(Evaluator& evaluator) const {
  // Determine if children need to return positions for proximity checks.
  bool require_positions = slop_.has_value() || inorder_;
  // Handle AND logic
  if (GetType() == PredicateType::kComposedAnd) {
    uint32_t childrenWithPositions = 0;
    uint64_t query_field_mask = ~0ULL;
    absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                        indexes::text::kProximityTermsInlineCapacity>
        iterators;
    for (const auto& child : children_) {
      // In AND: skip text children when in prefilter evaluation because text in
      // AND is fully (recursively) resolved in the entries fetcher layer
      // already.
      if (evaluator.IsPrefilterEvaluator() &&
          child->GetType() == PredicateType::kText) {
        continue;
      }
      EvaluationResult result =
          EvaluatePredicate(child.get(), evaluator, require_positions);
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
              std::move(iterators), slop_, inorder_, nullptr, false);
      // Check if any valid proximity matches exist
      if (!proximity_iterator->IsIteratorValid()) {
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
    // Propagate the filter iterator from the one child exists
    else if (childrenWithPositions == 1) {
      return EvaluationResult(true, std::move(iterators[0]));
    }
    // All matched, but none have position. non-proximity case
    return EvaluationResult(true);
  }
  // Handle OR logic
  auto filter_iterators =
      absl::InlinedVector<std::unique_ptr<indexes::text::TextIterator>,
                          indexes::text::kProximityTermsInlineCapacity>();
  for (const auto& child : children_) {
    EvaluationResult result =
        EvaluatePredicate(child.get(), evaluator, require_positions);
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
          std::move(filter_iterators), nullptr);
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
  return EvaluationResult(true, std::move(or_proximity_iterator));
}

}  // namespace valkey_search::query
