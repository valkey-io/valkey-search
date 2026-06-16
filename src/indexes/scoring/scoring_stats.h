/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_STATS_H_
#define VALKEYSEARCH_SRC_INDEXES_SCORING_SCORING_STATS_H_

#include <cstdint>
#include <string>

#include "absl/types/span.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::scoring {

struct ScoringStats {
  virtual ~ScoringStats() = default;

  uint32_t total_docs = 0;
  InternedStringPtr key;
  float document_score = 1.0f;
  std::string term;
  uint32_t num_doc_contain_term = 0;
  uint32_t term_frequency = 0;
};

struct Bm25StdStats : ScoringStats {
  float avg_doc_len = 0.0f;
  uint32_t doc_len = 0;
};

// `positions` is a non-owning view; storage must outlive the stats.
// `norm` of 0 means the document has no TEXT fields and forces score to 0.
struct TfidfStats : ScoringStats {
  uint32_t norm = 0;
  absl::Span<const uint32_t> positions;
};

}  // namespace valkey_search::indexes::scoring

#endif
