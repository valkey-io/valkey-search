/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORER_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORER_H_

#include <cstdint>
#include <string_view>

namespace valkey_search::indexes::scoring {

// std::isinf is unreliable under -ffast-math (which this code is built with);
// detect ±inf by IEEE 754 bit pattern instead.
bool IsInf(float f);

enum class ScorerType {
  kBm25Std,
  kTfidf,
};

// Additional inputs needed by future scorers (e.g. TFIDF's per-document norm)
// are added here when that scorer is implemented.
struct LeafInput {
  uint32_t total_docs = 0;            // N: documents in the index
  uint64_t total_doc_len = 0;         // summed length of all documents
  uint32_t num_doc_contain_term = 0;  // dt: documents containing the term
  uint32_t term_frequency = 0;        // F: term occurrences in this document
  uint32_t doc_len = 0;               // total indexed terms in this document
};

// Stateless, thread-safe scoring algorithm. Per-query state lives in
// ScoringSession. Each scorer reads the LeafInput fields its algorithm needs.
class Scorer {
 public:
  virtual ~Scorer() = default;

  virtual std::string_view Name() const = 0;
  virtual ScorerType Type() const = 0;

  // Scores one matching (term, document) leaf from scorer-agnostic raw inputs.
  virtual float ScoreLeaf(const LeafInput& input, float leaf_weight) const = 0;

  virtual float ComposeDocumentScore(float sum_of_terms,
                                     float document_score) const = 0;
};

const Scorer* GetScorer(ScorerType type);

}  // namespace valkey_search::indexes::scoring

#endif
