/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_TESTING_SCORING_SCORING_TEST_DATA_H_
#define VALKEYSEARCH_TESTING_SCORING_SCORING_TEST_DATA_H_

#include <cstdint>
#include <string>

#include "src/indexes/scoring/scoring_stats.h"
#include "src/utils/string_interning.h"

// Shared test data for Bm25StdScorer and ScoringSession unit tests.
namespace valkey_search::indexes::scoring::test_data {

inline constexpr uint32_t kTotalDocs = 8;
// total_doc_len = 5+6+7+9+4+4+6+1 = 42; avg = 42 / 8 = 5.25.
inline constexpr float kAvgDocLen = 5.25f;
inline constexpr uint32_t kDtHello = 6;
inline constexpr uint32_t kDtWorld = 6;
inline constexpr uint32_t kDtRare = 2;
inline constexpr uint32_t kDtUnique = 1;

struct DocInfo {
  std::string key_name;
  uint32_t doc_len;
  uint32_t f_hello;
  uint32_t f_world;
  uint32_t f_rare;
  uint32_t f_unique;

  InternedStringPtr GetKey() const {
    return StringInternStore::Intern(key_name);
  }
};

// {key_name, doc_len, f_hello, f_world, f_rare, f_unique}
// doc:2 / doc:7 are byte-identical for the tie-break test.
inline const DocInfo kDocs[] = {
    {"doc:1", 5, 1, 1, 0, 0}, {"doc:2", 6, 2, 1, 0, 0},
    {"doc:3", 7, 3, 1, 0, 0}, {"doc:4", 9, 5, 1, 0, 0},
    {"doc:5", 4, 4, 0, 0, 0}, {"doc:6", 4, 0, 1, 1, 1},
    {"doc:7", 6, 2, 1, 0, 0}, {"doc:8", 1, 0, 0, 1, 0},
};

inline Bm25StdStats StatsForHello(const DocInfo& doc) {
  Bm25StdStats s;
  s.total_docs = kTotalDocs;
  s.key = doc.GetKey();
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
  s.key = doc.GetKey();
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
  s.key = doc.GetKey();
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
  s.key = doc.GetKey();
  s.term = "unique";
  s.num_doc_contain_term = kDtUnique;
  s.term_frequency = doc.f_unique;
  s.avg_doc_len = kAvgDocLen;
  s.doc_len = doc.doc_len;
  return s;
}

}  // namespace valkey_search::indexes::scoring::test_data

#endif  // VALKEYSEARCH_TESTING_SCORING_SCORING_TEST_DATA_H_
