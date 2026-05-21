/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_TESTING_SCORING_SCORING_TEST_DATA_H_
#define VALKEYSEARCH_TESTING_SCORING_SCORING_TEST_DATA_H_

#include <cstdint>

#include "src/indexes/scoring/scoring_stats.h"

// Shared test data for Bm25StdScorer and ScoringSession unit tests.
namespace valkey_search::indexes::scoring::test_data {

// Index-wide constants for the corpus below.
inline constexpr uint64_t kTotalDocs = 8;
// total_doc_len = 5+6+7+9+4+4+6+1 = 42; avg = 42 / 8 = 5.25.
inline constexpr double kAvgDocLen = 5.25;
inline constexpr uint64_t kDtHello = 6;
inline constexpr uint64_t kDtWorld = 6;
inline constexpr uint64_t kDtRare = 2;
inline constexpr uint64_t kDtUnique = 1;

// Per-document fixed inputs. f_<term> is the doc-wide term frequency
// for that term in this doc; 0 means the term is absent.
struct DocInfo {
  uint64_t doc_id;
  uint32_t doc_len;
  uint32_t f_hello;
  uint32_t f_world;
  uint32_t f_rare;
  uint32_t f_unique;
};

// 8-doc corpus. Each row is one document; columns match DocInfo above:
//   {doc_id, doc_len, f_hello, f_world, f_rare, f_unique}
// where f_<term> is the doc-wide term frequency (0 = term absent).
//
// Each doc earns its slot by isolating one scoring axis:
//   doc:1-4  F sweep at growing doc_len for "hello"; tests TF saturation
//            and length normalization together.
//   doc:4 vs doc:5
//            Different F (5 vs 4) and very different doc_len (9 vs 4);
//            doc:5 must outrank doc:4 on "hello" (length norm dominates).
//   doc:5    Missing "world"; supports OR-partial-match queries.
//   doc:6    Missing "hello"; introduces low-dt terms ("rare" dt=2,
//            "unique" dt=1) to exercise high IDF.
//   doc:7    Byte-identical scoring inputs to doc:2; tests tie-break
//            by doc_id ascending.
//   doc:8    Only "rare" with doc_len=1; extreme-short-doc shape for
//            length norm at dt=2.
inline constexpr DocInfo kDocs[] = {
    {1, 5, 1, 1, 0, 0}, {2, 6, 2, 1, 0, 0}, {3, 7, 3, 1, 0, 0},
    {4, 9, 5, 1, 0, 0}, {5, 4, 4, 0, 0, 0}, {6, 4, 0, 1, 1, 1},
    {7, 6, 2, 1, 0, 0}, {8, 1, 0, 0, 1, 0},
};

// Per-term Bm25StdStats builders. Each fills the index-wide constants
// above plus the per-doc fields from `doc`. Callers may overwrite
// individual fields after construction (e.g. document_score).
inline Bm25StdStats StatsForHello(const DocInfo& doc) {
  Bm25StdStats s;
  s.total_docs = kTotalDocs;
  s.doc_id = doc.doc_id;
  s.term = "hello";
  s.num_doc_contain_term = kDtHello;
  s.term_frequency = doc.f_hello;
  s.avg_doc_len = kAvgDocLen;
  s.doc_len = doc.doc_len;
  return s;
}

inline Bm25StdStats StatsForWorld(const DocInfo& doc) {
  Bm25StdStats s;
  s.total_docs = kTotalDocs;
  s.doc_id = doc.doc_id;
  s.term = "world";
  s.num_doc_contain_term = kDtWorld;
  s.term_frequency = doc.f_world;
  s.avg_doc_len = kAvgDocLen;
  s.doc_len = doc.doc_len;
  return s;
}

inline Bm25StdStats StatsForRare(const DocInfo& doc) {
  Bm25StdStats s;
  s.total_docs = kTotalDocs;
  s.doc_id = doc.doc_id;
  s.term = "rare";
  s.num_doc_contain_term = kDtRare;
  s.term_frequency = doc.f_rare;
  s.avg_doc_len = kAvgDocLen;
  s.doc_len = doc.doc_len;
  return s;
}

inline Bm25StdStats StatsForUnique(const DocInfo& doc) {
  Bm25StdStats s;
  s.total_docs = kTotalDocs;
  s.doc_id = doc.doc_id;
  s.term = "unique";
  s.num_doc_contain_term = kDtUnique;
  s.term_frequency = doc.f_unique;
  s.avg_doc_len = kAvgDocLen;
  s.doc_len = doc.doc_len;
  return s;
}

}  // namespace valkey_search::indexes::scoring::test_data

#endif  // VALKEYSEARCH_TESTING_SCORING_SCORING_TEST_DATA_H_
