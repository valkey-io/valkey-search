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

// Tag leaf: the predicate's values parsed once per query so each candidate
// costs a membership check and a BM25 term (tf = 1) per value it carries.
//
//   @color:{red|Red|blue|purple|gr*} on a case-insensitive index with no
//   "purple" documents resolves to:
//     tag_values   = [{red, idf, bag}, {blue, idf, bag}]
//     tag_prefixes = [gr*]
//   "Red" is a case-duplicate of "red", "purple" can never match, and the
//   prefix is kept as-is because its match depends on the document.
struct TagLeaf {
  // One query value and its idf. Whether a document carries the value is
  // checked in different ways:
  //   background: probe `bag`, the index's set of keys with this value. 
  //               Avoids looking up TagInfo and a string comparison.
  //   main:       compare `value` against the fetched record's own tags
  //               because the bag doesn't provide stable access on main
  //               thread.
  struct Value {
    // Borrowed from the predicate, which outlives the cache.
    absl::string_view value;
    // Background only, borrowed from the index.
    std::optional<BorrowedBagOfInternedStringPtrs> bag;
    float idf = 0.0f;
  };
  absl::InlinedVector<Value, 4> tag_values;
  // A prefix credits the document's first matching tag, so its dt and idf are
  // looked up while scoring.
  // Note: We could move this parsing into the predicate.
  absl::InlinedVector<absl::string_view, 2> tag_prefixes;
};

// monostate: a leaf that resolves to nothing scoreable (infix, a non-Tag kTag
// mock, any non-leaf type). Cached so the walk never re-asks.
using ResolvedLeaf =
    std::variant<std::monostate, TermLeaf, ExpansionLeaf, TagLeaf>;

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_
