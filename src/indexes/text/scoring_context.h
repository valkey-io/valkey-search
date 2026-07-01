/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_SCORING_CONTEXT_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_SCORING_CONTEXT_H_

#include <cstdint>

namespace valkey_search::indexes::scoring {
class Scorer;
}  // namespace valkey_search::indexes::scoring

namespace valkey_search::indexes::text {

class TextIndexSchema;

// Query-invariant scoring inputs shared by every leaf iterator in a single
// search. Built once under the reader lock in the search path and threaded to
// leaf iterators at construction. A null `scorer` disables scoring (leaves fall
// back to the constant stub).
struct ScoringContext {
  // Borrowed; must outlive the iterators. Null means scoring is disabled.
  const scoring::Scorer* scorer = nullptr;

  // Total number of indexed documents (N).
  uint32_t total_docs = 0;

  // Average document length across the corpus (total_doc_len / N).
  float avg_doc_len = 0.0f;

  // Per-key document length source. Borrowed; must outlive the iterators.
  // Queried directly (no type erasure) on the per-document hot path; valid only
  // while the reader lock is held.
  const TextIndexSchema* text_index_schema = nullptr;
};

}  // namespace valkey_search::indexes::text

#endif  // VALKEYSEARCH_SRC_INDEXES_TEXT_SCORING_CONTEXT_H_
