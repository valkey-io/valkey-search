/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAVES_H_
#define VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAVES_H_

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

namespace valkey_search::indexes::scoring {
class Scorer;
}

namespace valkey_search::query {

// Which locks the cache and its probes take. The main thread never waits on
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

// One of an expansion's matching words that a document carries, with the
// document's entry and the word's df, read together under one lock.
struct ExpansionMatch {
  WordPostings term;
  indexes::text::PostingValue entry;
  size_t df = 0;
};

// Per-leaf resolutions, done once per query and shared by filtering and
// scoring. Not thread-safe: a background Search() is one task, and the
// main-thread reply loop builds its own instance.
class ResolvedLeafCache {
 public:
  // `text_index_schema` may be null when the index has no TEXT field; no text
  // leaf can then reach the cache.
  ResolvedLeafCache(const indexes::text::TextIndexSchema *text_index_schema,
                    CorpusStats stats, const indexes::scoring::Scorer *scorer,
                    LockMode mode = LockMode::kBackground);
  ResolvedLeafCache(const ResolvedLeafCache &) = delete;
  ResolvedLeafCache &operator=(const ResolvedLeafCache &) = delete;

  // Resolves on the first visit. References stay valid for the cache's
  // lifetime (node_hash_map), which positional evaluation relies on when it
  // holds one leaf while resolving a sibling.
  ResolvedLeaf &GetOrResolve(const Predicate *predicate);

  // Makes `match` the leaf's representative if it is more common than the
  // current one. Returns the IDF to score `match` with either way.
  float OfferExpansionTerm(ExpansionLeaf &leaf,
                           const ExpansionMatch &match) const;

  // Postings reads, under the word's bucket in kMainThread.
  size_t KeyCount(const WordPostings &word) const;
  std::optional<indexes::text::PostingValue> Probe(
      const WordPostings &word, BorrowedInternedStringPtr key,
      uint64_t field_mask) const;

  // Walks `per_key_index` for the first of the expansion's matching words that
  // `key` carries in the predicate's fields. Which match is unspecified (tree
  // order). `per_key_index` must be the document's own tree, where the walk is
  // bounded.
  std::optional<ExpansionMatch> FindExpansionMatch(
      const ExpansionPredicate &predicate,
      const indexes::text::TextIndex &per_key_index,
      const InternedStringPtr &key) const;

  // IDF for a term carried by `dt` documents, clamped to the corpus size since
  // the two counts come from independently locked counters.
  float Idf(size_t dt) const;

  bool MainThread() const { return mode_ == LockMode::kMainThread; }

  const indexes::scoring::Scorer *Scorer() const { return scorer_; }
  const CorpusStats &Stats() const { return stats_; }
  bool NeedsDocLen() const { return needs_doc_len_; }
  float AvgDocLen() const { return avg_doc_len_; }
  size_t Size() const { return leaves_.size(); }

 private:
  ResolvedLeaf Resolve(const Predicate *predicate) const;
  ResolvedLeaf ResolveText(const TextPredicate *predicate) const;
  ResolvedLeaf ResolveTag(const Predicate *predicate) const;
  WordPostings Lookup(absl::string_view word) const;
  // The word only when probes will lock on it (main thread).
  std::optional<std::string> LockWord(absl::string_view word) const;

  const indexes::text::TextIndexSchema *text_index_schema_;
  CorpusStats stats_;
  const indexes::scoring::Scorer *scorer_;
  LockMode mode_;
  bool needs_doc_len_;
  float avg_doc_len_;
  absl::node_hash_map<const Predicate *, ResolvedLeaf> leaves_;
};

// The text dispatch both evaluators share: a term leaf is evaluated from the
// cache, anything else walks the document's own tree, which `per_key_index`
// fetches only when reached.
EvaluationResult EvaluateText(
    ResolvedLeafCache &cache, const TextPredicate &predicate,
    const InternedStringPtr &key, bool require_positions,
    absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAVES_H_
