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
#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_index.h"
#include "src/query/predicate.h"
#include "src/query/resolved_leaf.h"
#include "src/utils/string_interning.h"

namespace valkey_search {
class IndexSchema;
namespace indexes {
class Tag;
namespace scoring {
class Scorer;
}
}  // namespace indexes
}  // namespace valkey_search

namespace valkey_search::query {

// Which locks the context and its probes take. The main thread never waits on
// the time-sliced mutex (a reader arriving during a write phase would stall for
// the whole phase); it takes the short locks each writer takes instead.
enum class LockMode {
  kBackground,  // Caller holds the time-sliced mutex in read mode.
  kMainThread,  // Tree lock, word bucket and tag mutex, each held briefly.
};

// Corpus-wide scoring inputs, read once per query.
struct CorpusStats {
  uint32_t total_docs = 0;
  uint64_t total_doc_len = 0;
  bool has_score_field = false;
  float default_document_score = 1.0f;
};

// What scores an expansion leaf for one document: the document's entry in a
// matched word's postings and the idf to score it with.
struct ExpansionMatch {
  float idf = 0.0f;
  indexes::text::PostingValue entry;
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
               LockMode mode = LockMode::kBackground);
  QueryContext(const QueryContext &) = delete;
  QueryContext &operator=(const QueryContext &) = delete;

  // Resolves on the first visit. References stay valid for the context's
  // lifetime (node_hash_map), which positional evaluation relies on when it
  // holds one leaf while resolving a sibling.
  ResolvedLeaf &GetOrResolve(const Predicate *predicate);

  // The key's entry in the word's postings, under the word's bucket in
  // kMainThread.
  std::optional<indexes::text::PostingValue> Probe(
      const WordPostings &word, BorrowedInternedStringPtr key,
      uint64_t field_mask) const;

  // For a document that misses the leaf's representative: walks the document's
  // own tree (`per_key_index`, where the walk is bounded) for the first of the
  // expansion's matching words it carries in the predicate's fields, and makes
  // that word the representative if it is more common than the current one.
  std::optional<ExpansionMatch> FindExpansionMatch(
      ExpansionLeaf &leaf, const ExpansionPredicate &predicate,
      const indexes::text::TextIndex &per_key_index,
      const InternedStringPtr &key) const;

  // IDF for a term carried by `dt` documents, clamped to the corpus size since
  // the two counts come from independently locked counters.
  float Idf(size_t dt) const;
  // One BM25 term for one document.
  float ScoreLeaf(float idf, uint32_t tf, uint32_t doc_len, float weight) const;

  // Per-document index reads, each under the locks `mode` requires. Every
  // lock choice by mode lives in this class.
  const indexes::text::TextIndex *PerKeyTextIndex(
      const InternedStringPtr &key) const;
  // 0 when the scorer does not use document length.
  uint32_t DocLen(BorrowedInternedStringPtr key) const;
  float DocScore(BorrowedInternedStringPtr key) const;
  size_t TagPrefixDocCount(const indexes::Tag &index, absl::string_view prefix,
                           BorrowedInternedStringPtr key) const;

  bool MainThread() const { return mode_ == LockMode::kMainThread; }

  const indexes::scoring::Scorer *Scorer() const { return scorer_; }
  const CorpusStats &Stats() const { return stats_; }
  size_t Size() const { return leaf_cache_.size(); }

 private:
  ResolvedLeaf Resolve(const Predicate *predicate) const;
  ResolvedLeaf ResolveText(const TextPredicate *predicate) const;
  ResolvedLeaf ResolveTag(const TagPredicate *predicate) const;
  WordPostings Lookup(absl::string_view word) const;
  size_t KeyCount(const WordPostings &word) const;
  // The word only when probes will lock on it (main thread).
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

// The text dispatch both evaluators share: a term leaf is evaluated from the
// context, anything else walks the document's own tree, which `per_key_index`
// fetches only when reached.
EvaluationResult EvaluateText(
    QueryContext &query_ctx, const TextPredicate &predicate,
    const InternedStringPtr &key, bool require_positions,
    absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_QUERY_CONTEXT_H_
