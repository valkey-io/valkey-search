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

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"

namespace valkey_search::indexes::scoring {

// std::isinf is unreliable under -ffast-math (which this code is built with);
// detect ±inf by IEEE 754 bit pattern instead.
bool IsInf(float f);

enum class ScorerType {
  kBm25Std,
  kTfidf,
};

inline absl::string_view ScorerToString(ScorerType scorer) {
  switch (scorer) {
    case ScorerType::kBm25Std:
      return "BM25STD";
    case ScorerType::kTfidf:
      return "TFIDF";
  }
  return "BM25STD";
}

const absl::NoDestructor<absl::flat_hash_map<absl::string_view, ScorerType>>
    kScorerByStr({
        {"BM25STD", ScorerType::kBm25Std},
        {"TFIDF", ScorerType::kTfidf},
    });

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

  // Query-invariant per-term weight (e.g. BM25 IDF). Depends only on corpus
  // size and the term's document count, so the search path computes it once per
  // distinct term and reuses it across every matching document.
  virtual float PrecomputeIDF(uint32_t total_docs,
                              uint32_t num_doc_contain_term) const = 0;

  // Scores one matching (term, document) leaf. Takes the term weight from
  // PrecomputeIDF so the per-document walk skips recomputing it (the
  // BM25 IDF involves a log).
  virtual float ScoreLeaf(float term_weight, const LeafInput& input,
                          float leaf_weight) const = 0;

  virtual float ComposeDocumentScore(float sum_of_terms,
                                     float document_score) const = 0;
};

const Scorer* GetScorer(ScorerType type);

}  // namespace valkey_search::indexes::scoring

#endif
