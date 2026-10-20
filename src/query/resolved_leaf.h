/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_
#define VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_

// Per-leaf query data resolved once per query rather than once per key.
// Structs only, so predicate.h can take them as parameters without depending
// on the cache.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "absl/container/inlined_vector.h"
#include "absl/strings/string_view.h"
#include "src/indexes/text/invasive_ptr.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_index.h"
#include "src/utils/string_interning.h"

namespace valkey_search::query {

// A word's shared posting list. Every tree (global and per-key) hands back the
// same Postings object for a word, so one lookup answers for every candidate.
struct WordPostings {
  // Kept so main-thread probes can lock the postings via WithWordLock(word).
  std::string word;
  indexes::text::InvasivePtr<indexes::text::Postings> postings;
};

// One BM25 term: tf is summed across its postings, scored with one IDF.
struct TermGroup {
  // One word, except the inflection group, which holds every surface form of
  // a stem root.
  absl::InlinedVector<WordPostings, indexes::text::kStemVariantsInlineCapacity>
      words;
  float idf = 0.0f;
  // Fields an occurrence must be in to count: the predicate's mask for the
  // surface word, the stem mask for root and inflections. ~0 when that is
  // every text field, so probes skip the per-position scan.
  uint64_t field_mask = ~0ULL;
};

// Term leaf: sums up to 3 groups (exact word, stem root, stem inflections).
struct TermLeaf {
  absl::InlinedVector<TermGroup, 3> groups;
};

// Prefix/suffix/fuzzy leaf: scores ONE matched term per doc, never the sum.
struct ExpansionLeaf {
  uint64_t field_mask = ~0ULL;  // Shared by all terms; expansions never stem.
  struct Term {
    WordPostings term;
    size_t df = 0;
    float idf = 0.0f;
  };
  // The most common matched word seen so far. A doc that carries it is scored
  // with one probe instead of a walk of its own tree.
  std::optional<Term> representative;
};

// Tag leaf: each matched tag value is a BM25 term with tf = 1.
struct TagLeaf {
  // The values the query names (e.g. `{red|blue}`), minus any absent from the
  // index. Strings are borrowed from the predicate, which outlives the cache.
  struct Value {
    absl::string_view value;
    // Background only: the keys that have this value, borrowed from the index
    // and probed once per candidate. Unset on the main thread, which checks
    // membership against the fetched record's tags instead.
    std::optional<BorrowedBagOfInternedStringPtrs> bag;
    float idf = 0.0f;
  };
  absl::InlinedVector<Value, 4> tag_values;
  // The query's prefix values (`re*`). Nothing is resolved for them: a prefix
  // credits the doc's first matching tag, so dt is looked up while scoring.
  // They are kept here so the split from exact values and the case-dedup
  // (`{re*|Re*}` on a case-insensitive index) happen once per query, not once
  // per candidate.
  // Note: We could move this parsing into the predicate.
  absl::InlinedVector<absl::string_view, 2> tag_prefixes;
};

// monostate: a leaf that resolves to nothing scoreable (infix, a non-Tag kTag
// mock, any non-leaf type). Cached so the walk never re-asks.
using ResolvedLeaf =
    std::variant<std::monostate, TermLeaf, ExpansionLeaf, TagLeaf>;

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_
