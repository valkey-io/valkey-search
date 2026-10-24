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
  std::optional<std::string> word;  // Main thread only: used to lock postings.
  indexes::text::InvasivePtr<indexes::text::Postings> postings;
};

// One BM25 term: tf is summed across `words`, scored with one idf.
struct TermGroup {
  absl::InlinedVector<WordPostings, indexes::text::kStemVariantsInlineCapacity>
      words;
  float idf = 0.0f;
  // ~0 when it covers every text field, so probes skip the per-position scan.
  uint64_t field_mask = ~0ULL;
  // A term's own $weight scales its exact-word group only.
  float weight = 1.0f;
};

// Term leaf: the word, its stem root and the root's inflections looked up once
// per query so each candidate costs one probe per word and a BM25 term per
// group. The groups are summed.
//
//   @title:running on a stemmed field, with run/runs/running indexed,
//   resolves to:
//     groups = [{[running],       idf(df),             title},
//               {[run],           idf(df),             title & stem},
//               {[running, runs], idf(docs with any),  title & stem}]
//   "running" appears twice on purpose: the exact word scores on its own and
//   again as an inflection, the exact-match boost. A query word that is its own
//   root (`run`) has no root group; an unstemmed field or an exact term
//   (`"running"`) has only the first.
struct TermLeaf {
  absl::InlinedVector<TermGroup, 3> groups;
};

// Prefix/suffix/fuzzy leaf: scores ONE matched word per document, never the
// sum. Nothing is looked up per query; the match depends on the document.
//
//   @title:run* starts with no representative.
//     doc1 has "runner" (df 3):   walk doc1's tree, representative = runner
//     doc2 has "runner":          one probe, done
//     doc3 has "running" (df 90): probe misses, walk, representative = running
//   The representative is the most common matched word seen so far, so later
//   candidates that carry it skip the walk.
struct ExpansionLeaf {
  // The predicate's fields (~0 when all), the same for every matched word.
  uint64_t field_mask = ~0ULL;
  struct Term {
    WordPostings term;
    size_t df = 0;
    float idf = 0.0f;
  };
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

// monostate: a predicate with nothing to resolve (numeric, vector range, any
// composite). Cached so the walk never re-asks.
using ResolvedLeaf =
    std::variant<std::monostate, TermLeaf, ExpansionLeaf, TagLeaf>;

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_
