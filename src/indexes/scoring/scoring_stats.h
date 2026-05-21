/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_STATS_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_STATS_H_

#include <cstdint>

#include "absl/strings/string_view.h"
#include "absl/types/span.h"

namespace valkey_search::indexes::scoring {

// Inputs common to every scoring algorithm. Populated for one
// (query term, candidate document) pair during result collection.
//
// Concrete algorithms derive from this and add their algorithm-specific
// fields (e.g. Bm25StdStats adds doc_len / avg_doc_len).
//
// Field semantics:
//   total_docs            - N, total documents in the index
//   doc_id                - candidate document id
//   document_score        - SCORE_FIELD | SCORE | 1.0
//   term                  - surface term from the query leaf
//   num_doc_contain_term  - dt, docs containing this term (index-wide)
//   term_frequency        - doc-wide term frequency
struct ScoringStats {
  virtual ~ScoringStats() = default;

  uint64_t total_docs = 0;
  uint64_t doc_id = 0;
  double document_score = 1.0;
  absl::string_view term;
  uint64_t num_doc_contain_term = 0;
  uint32_t term_frequency = 0;
};

// BM25STD-specific inputs. Adds only the fields BM25STD needs on top of
// the common stats.
//
// Field semantics:
//   avg_doc_len  - index-wide average indexed-terms per document
//   doc_len      - total indexed terms in this document
struct Bm25StdStats : ScoringStats {
  double avg_doc_len = 0.0;
  uint32_t doc_len = 0;
};

// TFIDF-specific inputs. Adds the fields TFIDF needs on top of the common
// stats: the per-document `norm` divisor and the term's positional
// offsets within the document (used for the SLOP penalty).
//
// `positions` is a non-owning view; the underlying storage must outlive
// the ScoringStats. Empty when SLOP is not being computed.
//
// Field semantics:
//   norm       - max term frequency within the document; 0 means the
//                document has no TEXT fields, which forces the final
//                score to 0
//   positions  - positional offsets of `term` within the document, sorted
//                ascending; empty when SLOP is not used
struct TfidfStats : ScoringStats {
  uint32_t norm = 0;
  absl::Span<const uint32_t> positions;
};

}  // namespace valkey_search::indexes::scoring

#endif
