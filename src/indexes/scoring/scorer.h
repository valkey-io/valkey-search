/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORER_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORER_H_

#include <string_view>

#include "absl/types/span.h"
#include "src/indexes/scoring/scoring_stats.h"

namespace valkey_search::indexes::scoring {

// Enumerates the concrete scoring algorithms. Returned by Scorer::Type()
// for cheap dispatch in switch statements (factory, EXPLAINSCORE
// formatting, telemetry) without string compares against Name().
//
// Add a new entry here when introducing a new concrete scorer.
enum class ScorerType {
  kBm25Std,
  kTfidf,
};

// Polymorphic base for query-result scoring algorithms. Initial concrete
// scorers cover full-text (BM25STD, TFIDF); tag and numeric scorers may
// be added as peers in the future without changing this interface.
//
// A Scorer is stateless and pure: the same instance can be safely shared
// across concurrent queries. Per-query state (accumulated per-doc partial
// scores, group weight stack, ranking) lives in ScoringSession.
//
// Three call sites during query evaluation:
//   - ScoreLeaf: contribution of one (query leaf, candidate document)
//     match. The "leaf" is whatever the query tree's terminal node
//     represents for a given index type (a term for text; later: a tag
//     value, a numeric range hit).
//   - CombineGroup: combine children of an AND/OR/group node, applying
//     the node's group weight. Same logic for every algorithm; the base
//     class provides a non-virtual default implementation.
//   - ComposeDocumentScore: produce one document's final score from its
//     accumulated leaf + group contributions.
//
// Concrete scorers expect a specific ScoringStats subtype (e.g. BM25STD
// expects Bm25StdStats). The caller must pass the matching subtype;
// scorers DCHECK this contract.
class Scorer {
 public:
  virtual ~Scorer() = default;

  // Identity string surfaced by FT.SEARCH SCORER and EXPLAINSCORE.
  virtual std::string_view Name() const = 0;

  // Concrete-class identity for dispatch.
  virtual ScorerType Type() const = 0;

  // Per-leaf contribution for one (query leaf, candidate document) match.
  // `leaf_weight` comes from the leaf's =>{$weight:N}; default 1.0.
  virtual float ScoreLeaf(const ScoringStats& stats,
                          float leaf_weight) const = 0;

  // Final composition: takes the accumulated sum of leaf + group
  // contributions for one document and applies algorithm-specific
  // post-processing (e.g. document_score multiplier; norm / slop
  // divisors for TFIDF).
  virtual float ComposeDocumentScore(float sum_of_terms,
                                     const ScoringStats& stats) const = 0;

  // Combine an internal node's children:
  //   group_weight * sum(child_scores)
  // Identical for every algorithm, so the base class implements this
  // non-virtually.
  float CombineGroup(absl::Span<const float> child_scores,
                     float group_weight) const;
};

}  // namespace valkey_search::indexes::scoring

#endif
