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

// Stateless, thread-safe scoring algorithm.
class Scorer {
 public:
  virtual ~Scorer() = default;

  virtual std::string_view Name() const = 0;
  virtual ScorerType Type() const = 0;

  // Query-invariant inverse document frequency. Depends only on the corpus
  // size and the term's document count, so callers precompute it once per term
  // and pass it to ScoreLeaf for every matching document.
  virtual float PrecomputeIDF(uint32_t total_docs,
                              uint32_t num_doc_contain_term) const = 0;

  // Scores one matching (term, document) leaf given a precomputed IDF. Returns
  // 0 for a degenerate corpus (avg_doc_len <= 0).
  virtual float ScoreLeaf(float idf, uint32_t term_frequency, uint32_t doc_len,
                          float avg_doc_len, float leaf_weight) const = 0;

  virtual float ComposeDocumentScore(float sum_of_terms,
                                     float document_score) const = 0;
};

const Scorer* GetScorer(ScorerType type);

}  // namespace valkey_search::indexes::scoring

#endif
