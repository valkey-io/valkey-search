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

// new scorers will be added here when implemented
const absl::NoDestructor<absl::flat_hash_map<absl::string_view, ScorerType>>
    kScorerByStr({{"BM25STD", ScorerType::kBm25Std}});

// Query-invariant, corpus-level inputs for PrecomputeIDF (once per term).
struct IdfInput {
  uint32_t total_docs = 0;
  uint32_t num_doc_contain_term = 0;
};

// Per-(term, document) inputs for ScoreLeaf, packed once at the scoring seam.
// `idf` is PrecomputeIDF's result, computed earlier. Callers supply every
// available leaf signal and each scorer reads the subset it needs, so
// swapping/adding a scorer requires no call-site change. Add a new signal as a
// trailing field (default member initializer keeps existing brace-init callers
// compiling), and gate expensive ones behind a capability method so they are
// produced only when a scorer needs them.
struct LeafScoreInput {
  float idf = 0.0f;
  uint32_t term_frequency = 0;
  uint32_t doc_len = 0;
  float avg_doc_len = 0.0f;
  float leaf_weight = 1.0f;
};

// Stateless, thread-safe scoring algorithm.
class Scorer {
 public:
  virtual ~Scorer() = default;

  virtual std::string_view Name() const = 0;
  virtual ScorerType Type() const = 0;

  // Whether the scorer uses document-length normalization. When false, callers
  // skip resolving corpus/per-document lengths (total_doc_len, avg_doc_len,
  // per-key doc_len) and pass 0 for those inputs. Lets the scoring paths stay
  // generic instead of switching on Type().
  virtual bool NeedsDocumentLength() const = 0;

  // Query-invariant inverse document frequency. Depends only on the corpus
  // size and the term's document count, so callers precompute it once per term
  // and pass it to ScoreLeaf for every matching document.
  virtual float PrecomputeIDF(const IdfInput& input) const = 0;

  // Scores one matching (term, document) leaf given a precomputed IDF. Returns
  // 0 for a degenerate corpus (avg_doc_len <= 0).
  virtual float ScoreLeaf(const LeafScoreInput& input) const = 0;

  virtual float ComposeDocumentScore(float sum_of_terms,
                                     float document_score) const = 0;
};

const Scorer* GetScorer(ScorerType type);

}  // namespace valkey_search::indexes::scoring

#endif
