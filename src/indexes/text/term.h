/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_

#include <cstdint>
#include <utility>

#include "absl/container/inlined_vector.h"
#include "absl/types/span.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/text.h"
#include "src/indexes/text/flat_position_map.h"
#include "src/indexes/text/text_iterator.h"
#include "src/utils/inlined_priority_queue.h"

namespace valkey_search::indexes::text {

/*

Top level iterator for a Term.
This is a union of the postings key iterators and derived position iterators
allowing a single lexically ordered iteration of keys and positions (where the
word/s, based on postings, exist in the key).

TermIterator Responsibilities:
- Manages a vector of posting (key) iterator/s, which operates in lexical order.
- Key iteration (of documents) takes place by advancing the posting iterator who
is on the smallest key until it is on a key whose field matches the field mask
of the search operation. Since multiple posting iterators can have the same key
amd same field, we create a vector of position iterators, one from each posting
iterator who are on the same key & field. Once no more keys are found, DoneKeys
returns true. Through this process, it "merges" multiple posting iterators.
- Position iteration happens across all the position iterators, allowing us to
search for positions in asc order across all the required words within the same
key and same field. Once no more positions are found, DonePositions returns
true. Thus, position iteration is a union of all position iterators obtained
from all the posting iterators that are on the current key and field mask. This
half lives in SingleKeyTermIterator, which TermIterator embeds and re-targets at
each key, and which per-key evaluation uses on its own over one copied-out key.

*/

// Inputs used only for scoring. The default (null schema/scorer) disables
// scoring, and GetScore() falls back to the constant stub.
struct TermScoringParams {
  float leaf_weight = 1.0f;
  uint32_t num_doc_contain_term = 0;
  // Stem scoring inputs; mutually exclusive with per_term_dt below, since an
  // expansion never stems.
  uint32_t stem_num_doc_contain_term = 0;
  uint32_t root_num_doc_contain_term = 0;
  bool has_root = false;
  const TextIndexSchema* text_index_schema = nullptr;
  const scoring::Scorer* scorer = nullptr;
  // Expansion (prefix/suffix/fuzzy) scoring input: one dt per matched term.
  absl::InlinedVector<uint32_t, kWordExpansionInlineCapacity> per_term_dt;
};

// A TermIterator fixed to one key: merges the positions of the term's words
// within that key. Per-key evaluation (filter revalidation, prefilter
// candidates) builds one from the position maps that Postings::GetPostingValue
// copied out and uses it as the leaf TextIterator; TermIterator embeds one and
// re-targets it at each key the merge lands on. Never scores: per-key callers
// score through ResolvedLeafCache::Probe.
class SingleKeyTermIterator final : public TextIterator {
 public:
  using PositionMaps =
      absl::InlinedVector<const FlatPositionMap*, kWordExpansionInlineCapacity>;

  // Primes at the first position of `key` that falls in `query_field_mask`.
  // `key` must outlive the iterator; `maps` is only read.
  SingleKeyTermIterator(const Key& key,
                        absl::Span<const FlatPositionMap* const> maps,
                        FieldMaskPredicate query_field_mask)
      : query_field_mask_(query_field_mask) {
    Reset(key, [maps](auto&& fn) {
      for (const FlatPositionMap* map : maps) fn(*map);
    });
  }
  // Empty iterator for embedding; Reset() before use.
  explicit SingleKeyTermIterator(FieldMaskPredicate query_field_mask);

  // Re-targets the iterator at `key`. `for_each_map(fn)` must call
  // `fn(const FlatPositionMap&)` once per word on `key`; a template so the
  // entries-fetcher hot path passes a lambda over its active cursors with no
  // intermediate array and no indirect call. Reuses the inlined storage, so a
  // reset allocates only when the word expansion exceeds the inline capacity.
  template <class ForEachMap>
  void Reset(const Key& key, ForEachMap&& for_each_map) {
    ClearPositionState();
    key_ = &key;
    done_ = false;
    for_each_map([this](const FlatPositionMap& map) {
      pos_iterators_.emplace_back(map);
      // Populate the position heap.
      InsertValidPositionIterator(pos_iterators_.size() - 1);
    });
    SingleKeyTermIterator::NextPosition();
  }

  /* Implementation of TextIterator APIs */
  FieldMaskPredicate QueryFieldMask() const override {
    return query_field_mask_;
  }
  // Key-level iteration: a single key.
  bool DoneKeys() const override { return done_; }
  const Key& CurrentKey() const override {
    CHECK(!done_);
    return *key_;
  }
  bool NextKey() override {
    ClearPositionState();
    done_ = true;
    return false;
  }
  bool SeekForwardKey(const Key& target_key) override {
    if (!done_ && *key_ < target_key) NextKey();
    return !done_;
  }
  // Position-level iteration. The accessors are defined here so TermIterator's
  // forwards inline them instead of compiling to a thunk plus a second jump on
  // every proximity step.
  bool DonePositions() const override { return !current_position_.has_value(); }
  const PositionRange& CurrentPosition() const override {
    CHECK(current_position_.has_value());
    return current_position_.value();
  }
  bool NextPosition() override;
  bool SeekForwardPosition(Position target_position) override;
  FieldMaskPredicate CurrentFieldMask() const override {
    CHECK(current_field_mask_ != 0ULL);
    return current_field_mask_;
  }
  bool IsIteratorValid() const override {
    return !done_ && current_position_.has_value() &&
           current_field_mask_ != 0ULL;
  }
  // Unreachable from current callers: TermIterator scores from its own
  // cursors and per-key evaluation scores through ResolvedLeafCache::Probe.
  // The constant stub keeps the contract if a composite ever asks.
  float GetScore() const override { return done_ ? 0.0f : 1.0f; }

 private:
  const FieldMaskPredicate query_field_mask_;
  const Key* key_{nullptr};
  bool done_{true};
  absl::InlinedVector<PositionIterator, kWordExpansionInlineCapacity>
      pos_iterators_;
  std::optional<PositionRange> current_position_;
  FieldMaskPredicate current_field_mask_{0ULL};
  // Pending queue: heap of valid iterators not currently being processed.
  // Provides O(1) access to the minimum position and O(log K) extraction.
  valkey_search::InlinedPriorityQueue<std::pair<uint32_t, size_t>,
                                      kWordExpansionInlineCapacity>
      pos_set_;
  // Indices of iterators at current_position_ (active, not in pos_set_)
  absl::InlinedVector<size_t, kWordExpansionInlineCapacity>
      current_pos_indices_;

  bool FindMinimumValidPosition();
  void InsertValidPositionIterator(size_t idx);
  void ClearPositionState();
};

// Merges the words' posting lists into one lexically ordered key stream and
// scores each key; the positions within the current key come from the embedded
// SingleKeyTermIterator. Entries-fetcher path only (multi-key).
class TermIterator : public TextIterator {
 public:
  using KeyIterators =
      absl::InlinedVector<Postings::KeyIterator, kWordExpansionInlineCapacity>;

  TermIterator(KeyIterators&& key_iterators,
               const FieldMaskPredicate query_field_mask,
               const bool require_positions,
               const FieldMaskPredicate stem_field_mask = 0,
               bool has_original = false,
               const TermScoringParams& scoring = {});
  /* Implementation of TextIterator APIs */
  FieldMaskPredicate QueryFieldMask() const override;
  // Key-level iteration
  bool DoneKeys() const override;
  const Key& CurrentKey() const override;
  bool NextKey() override;
  bool SeekForwardKey(const Key& target_key) override;
  // Position-level iteration, delegated to positions_ (statically bound).
  bool DonePositions() const override { return positions_.DonePositions(); }
  const PositionRange& CurrentPosition() const override {
    return positions_.CurrentPosition();
  }
  bool NextPosition() override { return positions_.NextPosition(); }
  bool SeekForwardPosition(Position target_position) override {
    return positions_.SeekForwardPosition(target_position);
  }
  FieldMaskPredicate CurrentFieldMask() const override {
    return positions_.CurrentFieldMask();
  }
  // Returns true if iterator is at a valid state with current key, position,
  // and field. positions_ is only reset when positions are required.
  bool IsIteratorValid() const override {
    if (require_positions_) {
      return current_key_ && positions_.IsIteratorValid();
    }
    return current_key_ != nullptr;
  }

  // Computes the leaf BM25 score for the current document via the active
  // scorer. Falls back to the constant stub (1.0 for any match) when no
  // scoring context is supplied.
  float GetScore() const override;

 private:
  const FieldMaskPredicate query_field_mask_;
  const FieldMaskPredicate stem_field_mask_;
  KeyIterators key_iterators_;
  // Raw pointer to the current key's btree_map entry, immutable while the
  // reader lock is held.
  const Key* current_key_{nullptr};
  SingleKeyTermIterator positions_;
  const bool require_positions_;
  const bool has_original_;
  // Whether a stem root literal iterator is present (index has_original_?1:0).
  const bool has_root_;

  // Scoring inputs. leaf_weight_ is the query-tree weight applied to this leaf;
  // num_doc_contain_term_ (dt) is the per-term document count captured at build
  // time; text_index_schema_ supplies the query-invariant corpus stats and the
  // per-document doc_len (null disables scoring).
  const float leaf_weight_;
  const uint32_t num_doc_contain_term_;
  const TextIndexSchema* const text_index_schema_;

  // Query-selected scorer and the query-invariant inputs cached at
  // construction: idf_ (per-term) and avg_doc_len_ (corpus-wide). GetScore()
  // combines them with the per-document term frequency and doc_len via
  // ScoreLeaf(). Null scorer_ means scoring is disabled (constant-stub
  // fallback).
  const scoring::Scorer* scorer_{nullptr};
  float idf_{0.0f};
  // Separate IDFs for the stem inflection group and the stem root literal leaf.
  float idf_stem_{0.0f};
  float idf_root_{0.0f};
  float avg_doc_len_{0.0f};

  // Per-matched-term IDF for prefix/suffix/fuzzy, index-aligned with
  // key_iterators_. Non-empty selects expansion mode in GetScore().
  absl::InlinedVector<float, kWordExpansionInlineCapacity> per_term_idf_;

  // Pending queue: heap of valid iterators not currently being processed.
  // Provides O(1) access to the minimum key and O(log K) extraction.
  // Uses PriorityQueueEntry (raw pointer) instead of copying Key to avoid
  // atomic ref counting. Safe because pointers reference btree_map entries
  // which are immutable during search (reader lock held).
  valkey_search::InlinedPriorityQueue<valkey_search::PriorityQueueEntry<Key>,
                                      kWordExpansionInlineCapacity>
      key_set_;
  // Indices of iterators at current_key_ (active, not in key_set_)
  absl::InlinedVector<size_t, kWordExpansionInlineCapacity>
      current_key_indices_;

  bool FindMinimumValidKey();
  void InsertValidKeyIterator(size_t idx);
  void ClearKeyState();
};

}  // namespace valkey_search::indexes::text

#endif
