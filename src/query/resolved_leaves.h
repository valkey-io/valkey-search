/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAVES_H_
#define VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAVES_H_

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "absl/container/inlined_vector.h"
#include "absl/container/node_hash_map.h"
#include "absl/functional/function_ref.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/indexes/tag.h"
#include "src/indexes/text/invasive_ptr.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_index.h"
#include "src/query/predicate.h"

namespace valkey_search::indexes::scoring {
class Scorer;
}

namespace valkey_search::query {

// Collapses an all-fields mask (what the parser builds for an unscoped query)
// to the `~0ULL` sentinel so posting probes skip the per-position scan. Field
// numbers are dense from 0 (TextIndexSchema::AllocateTextFieldNumber).
uint64_t ScoringFieldMask(uint64_t field_mask, uint8_t num_fields);

// A word's shared posting list. Every tree (global and per-key) hands back the
// same Postings object for a word, so one lookup answers for every candidate.
// `lock` is the word's RaxTargetMutexPool bucket, set only when probes must be
// serialized against ingestion (LockMode::kMainThread); the bucket is what
// every writer of the postings holds.
struct WordPostings {
  std::string word;
  indexes::text::InvasivePtr<indexes::text::Postings> postings;
  absl::Mutex *lock = nullptr;
};

// One BM25 term: tf is summed across its postings, scored with one IDF.
struct TermGroup {
  // Which of the stemmed query's leaves this is; membership gates the original
  // word with the predicate's field mask and the rest with the stem mask.
  enum class Kind { kOriginal, kStemRoot, kInflections };
  Kind kind = Kind::kOriginal;
  absl::InlinedVector<WordPostings, indexes::text::kStemVariantsInlineCapacity>
      words;
  float idf = 0.0f;
  uint64_t field_mask = ~0ULL;
};

// Term leaf: sums up to 3 groups (exact word, stem root, stem inflections).
struct TermLeaf {
  absl::InlinedVector<TermGroup, 3> groups;
};

// Prefix/suffix/fuzzy leaf: scores ONE matched term per doc, never the sum.
// Nothing is walked globally for it. Scoring probes `representative` first and
// on a miss walks the document's own tree for a match, which replaces the
// representative if it is more common; every candidate that carries it is
// answered by one btree probe.
struct ExpansionLeaf {
  enum class Kind { kPrefix, kSuffix, kFuzzy };
  Kind kind = Kind::kPrefix;
  uint64_t field_mask = ~0ULL;  // Shared by all terms; expansions never stem.
  struct Term {
    WordPostings term;
    float idf = 0.0f;
  };
  std::optional<Term> representative;
};

// Tag leaf: each matched tag value is a BM25 term with tf = 1.
struct TagLeaf {
  const indexes::Tag *tag_index = nullptr;
  // Query values present in the index. The handle answers membership and dt
  // with one bag probe per candidate, replacing a per-candidate parse of the
  // document's tag string.
  struct Value {
    std::string value;
    // Borrowed from the rax slot, so only held while ingestion is excluded
    // (LockMode::kBackground); the main thread reads membership from the
    // fetched record instead.
    std::optional<indexes::Tag::ValueHandle> handle;
    float idf = 0.0f;
  };
  absl::InlinedVector<Value, 4> tag_values;
  // `foo*` values; dt depends on the doc's matching tag, so resolved per doc.
  absl::InlinedVector<absl::string_view, 2> tag_prefixes;
};

// monostate: a leaf that resolves to nothing scoreable (infix, a non-Tag kTag
// mock, any non-leaf type). Cached so the walk never re-asks.
using ResolvedLeaf =
    std::variant<std::monostate, TermLeaf, ExpansionLeaf, TagLeaf>;

// Which locks the cache and its probes take. The main thread never waits on
// the time-sliced mutex (a reader arriving during a write phase would stall for
// the whole phase); it takes the short locks each writer takes instead.
enum class LockMode {
  // The caller holds the time-sliced mutex in read mode, excluding ingestion.
  kBackground,
  // Global lookups under the tree lock, each posting probe under its word's
  // bucket, tag counts under the tag index's mutex.
  kMainThread,
  // As kMainThread, but the caller already holds every word bucket
  // (RaxTargetMutexPool::LockAll), which positional evaluation needs because it
  // keeps probes open across words that may share a bucket. Tree lookups still
  // take the tree lock: writers take bucket then tree, so the order is safe.
  kMainThreadWordLocksHeld,
};

// Corpus-wide scoring inputs, read once per query.
struct CorpusStats {
  uint32_t total_docs = 0;
  uint64_t total_doc_len = 0;
  bool has_score_field = false;
  float default_document_score = 1.0f;
};

// Query-scoped, lazily filled cache of per-leaf resolutions keyed on the base
// Predicate*, shared by filtering and scoring so each leaf's tree walk happens
// once per query rather than once per candidate per phase. Also carries the
// query-invariant scoring inputs, so a reply-scoped instance is everything
// main-thread rescoring needs. Unsynchronized: a background Search() runs as
// one task, and the main-thread reply loop builds its own.
class ResolvedLeafCache {
 public:
  ResolvedLeafCache(CorpusStats stats, const indexes::scoring::Scorer *scorer,
                    LockMode mode = LockMode::kBackground);
  ResolvedLeafCache(const ResolvedLeafCache &) = delete;
  ResolvedLeafCache &operator=(const ResolvedLeafCache &) = delete;

  // Resolves on the first visit. References stay valid for the cache's
  // lifetime (node_hash_map), which positional evaluation relies on when it
  // holds one leaf while resolving a sibling.
  ResolvedLeaf &GetOrResolve(const Predicate *predicate);

  // Makes `term` the leaf's representative if it is more common than the
  // current one. Returns the IDF to score `term` with either way.
  float OfferExpansionTerm(ExpansionLeaf &leaf, const WordPostings &term) const;

  // Buckets a per-key walk must lock around each probe; null when no per-probe
  // locking is needed.
  indexes::text::RaxTargetMutexPool *WalkLocks(
      const indexes::text::TextIndexSchema &schema) const {
    return mode_ == LockMode::kMainThread ? &schema.GetWordLocks() : nullptr;
  }
  bool MainThread() const { return mode_ != LockMode::kBackground; }

  const indexes::scoring::Scorer *Scorer() const { return scorer_; }
  const CorpusStats &Stats() const { return stats_; }
  bool NeedsDocLen() const { return needs_doc_len_; }
  float AvgDocLen() const { return avg_doc_len_; }
  size_t Size() const { return leaves_.size(); }

 private:
  ResolvedLeaf Resolve(const Predicate *predicate) const;
  ResolvedLeaf ResolveText(const TextPredicate *predicate) const;
  ResolvedLeaf ResolveTag(const Predicate *predicate) const;
  WordPostings Lookup(const indexes::text::TextIndexSchema &schema,
                      absl::string_view word) const;
  float Idf(size_t dt) const;

  CorpusStats stats_;
  const indexes::scoring::Scorer *scorer_;
  LockMode mode_;
  bool needs_doc_len_;
  float avg_doc_len_;
  absl::node_hash_map<const Predicate *, ResolvedLeaf> leaves_;
};

// The word's document count, under its bucket when one is set.
size_t KeyCount(const WordPostings &word);
// Probes `word` for `key`, under its bucket when one is set.
std::optional<indexes::text::PostingDocStats> ProbeDocStats(
    const WordPostings &word, BorrowedInternedStringPtr key,
    uint64_t field_mask);

// Walks `per_key_index` for the first of the expansion's matching words that
// `key` carries in the predicate's fields. Which match is unspecified (tree
// order). `per_key_index` must be the document's own tree, where the walk is
// bounded. `word_locks` are taken per probe when given, and the result carries
// its bucket so later probes take it too.
std::optional<WordPostings> FindExpansionMatch(
    const TextPredicate &predicate, ExpansionLeaf::Kind kind,
    const indexes::text::TextIndex &per_key_index, const InternedStringPtr &key,
    indexes::text::RaxTargetMutexPool *word_locks);

// Filters `key` against a cached term leaf, producing exactly what
// TermPredicate::Evaluate would, minus the tree lookups. Under
// `require_positions` that includes the TermIterator the enclosing AND/OR needs
// for its proximity check: a bare verdict there would silently drop the check
// and turn a phrase query into a conjunction.
EvaluationResult EvaluateTermLeaf(const TermPredicate &predicate,
                                  const TermLeaf &leaf,
                                  const InternedStringPtr &key,
                                  bool require_positions);

// Filters `key` against a cached tag leaf: one bag probe per exact value, and
// only if none hits and the predicate carries a prefix value does it fall back
// to parsing the document's own tags. The verdict equals
// TagPredicate::Evaluate(GetValue(key)): rax keys are stored with the same
// ASCII folding EqualsIgnoreCase applies, both sides are whitespace-stripped,
// and query values are already unescaped.
EvaluationResult EvaluateTagLeaf(const TagPredicate &predicate,
                                 const TagLeaf &leaf,
                                 const InternedStringPtr &key);

// Cache-first text evaluation shared by every Evaluator. Term leaves are
// answered from the cache; expansion leaves walk the document's own tree, which
// `per_key_index` fetches only when reached.
EvaluationResult EvaluateTextLeaf(
    ResolvedLeafCache &cache, const TextPredicate &predicate,
    const InternedStringPtr &key, bool require_positions,
    absl::FunctionRef<const indexes::text::TextIndex *()> per_key_index);

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAVES_H_
