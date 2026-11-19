/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_QUERY_CONTEXT_H_
#define VALKEYSEARCH_SRC_QUERY_QUERY_CONTEXT_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "absl/container/node_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/string_view.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_index.h"
#include "src/query/predicate.h"
#include "src/query/resolved_leaf.h"
#include "src/utils/string_interning.h"

namespace valkey_search {
class IndexSchema;
namespace indexes {
class Tag;
}
}  // namespace valkey_search

namespace valkey_search::query {

// Which locks the context and its probes take. A caller that holds the
// time-sliced mutex in read mode needs none. The main-thread reply path must
// not wait on that mutex (a reader arriving during a write phase would stall
// for the whole phase), so it takes the short locks each writer takes instead.
enum class LockMode {
  kReadPhase,   // Caller holds the time-sliced mutex in read mode.
  kShortLocks,  // Tree lock, word bucket and tag mutex, each held briefly.
};

// Corpus-wide scoring inputs, read once per query.
struct CorpusStats {
  uint32_t total_docs = 0;
  uint64_t total_doc_len = 0;
  bool has_score_field = false;
  float default_document_score = 1.0f;
};

// Per-query state shared by filtering and scoring: the leaves resolved once
// per query, the lock mode, and the scoring inputs read once up front. Not
// thread-safe: a background Search() is one task, and the main-thread reply
// loop builds its own instance.
class QueryContext {
 public:
  // Reads the corpus stats up front under `mode`'s locks.
  QueryContext(const IndexSchema &index_schema,
               const indexes::scoring::Scorer *scorer,
               LockMode mode = LockMode::kReadPhase);
  QueryContext(const QueryContext &) = delete;
  QueryContext &operator=(const QueryContext &) = delete;

  // Resolves on the first visit. References stay valid for the context's
  // lifetime (node_hash_map), which positional evaluation relies on when it
  // holds one leaf while resolving a sibling.
  ResolvedLeaf &GetOrResolve(const Predicate *predicate);

  // The key's entry in the word's postings, under the word's bucket in
  // kShortLocks.
  std::optional<indexes::text::PostingValue> Probe(
      const WordPostings &word, BorrowedInternedStringPtr key,
      uint64_t field_mask) const;

  // Evaluates a text leaf for `key`; `per_key_index` is fetched only if needed.
  EvaluationResult EvaluateText(
      const TextPredicate &predicate, const InternedStringPtr &key,
      bool require_positions, bool scoring,
      absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index);
  // Evaluates a tag leaf for `key`; `carries` says if the key has a value.
  EvaluationResult EvaluateTag(
      const TagLeaf &leaf, const TagPredicate &predicate,
      const InternedStringPtr &key, bool scoring,
      absl::FunctionRef<bool(const TagLeaf::Value &)> carries) const;

  // IDF for a term carried by `dt` documents, clamped to the corpus size since
  // the two counts come from independently locked counters.
  float Idf(size_t dt) const;
  // One BM25 term for one document. Fills the query-wide fields; callers set
  // the per-leaf ones they have.
  float ScoreLeaf(indexes::scoring::LeafScoreInput input) const;

  // Per-document scoring inputs, each read under the locks `mode` requires.
  // Every lock choice by mode lives in this class; add new inputs here rather
  // than reading the index from the scorer.
  const indexes::text::TextIndex *PerKeyTextIndex(
      const InternedStringPtr &key) const;
  // 0 when the scorer does not use document length.
  uint32_t DocLen(BorrowedInternedStringPtr key) const;
  float DocScore(BorrowedInternedStringPtr key) const;

  bool ShortLocks() const { return mode_ == LockMode::kShortLocks; }
  const IndexSchema &Schema() const { return index_schema_; }

  const indexes::scoring::Scorer *Scorer() const { return scorer_; }
  const CorpusStats &Stats() const { return stats_; }
  size_t Size() const { return leaf_cache_.size(); }

 private:
  ResolvedLeaf Resolve(const Predicate *predicate) const;
  ResolvedLeaf ResolveText(const TextPredicate *predicate) const;
  ResolvedLeaf ResolveTag(const TagPredicate *predicate) const;
  EvaluationResult EvaluateTerm(const TermLeaf &leaf,
                                const TermPredicate &predicate,
                                const InternedStringPtr &key,
                                bool require_positions, bool scoring) const;
  EvaluationResult EvaluateExpansion(
      ExpansionLeaf &leaf, const ExpansionPredicate &predicate,
      const InternedStringPtr &key, bool require_positions, bool scoring,
      absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index)
      const;
  WordPostings Lookup(absl::string_view word) const;
  size_t KeyCount(const WordPostings &word) const;
  // The word only when probes will lock on it (kShortLocks).
  std::optional<std::string> LockWord(absl::string_view word) const;

  const IndexSchema &index_schema_;
  // Null on a schema with no TEXT fields; no text leaf can then reach us.
  const indexes::text::TextIndexSchema *text_index_schema_;
  CorpusStats stats_;
  const indexes::scoring::Scorer *scorer_;
  LockMode mode_;
  bool needs_doc_len_;
  float avg_doc_len_;
  absl::node_hash_map<const Predicate *, ResolvedLeaf> leaf_cache_;
};

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_QUERY_CONTEXT_H_
