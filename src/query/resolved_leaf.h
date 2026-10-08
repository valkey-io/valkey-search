/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_
#define VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_

// What ResolvedLeafCache hands a predicate for one leaf of the query: the data
// the predicate evaluates a key against, resolved once per query instead of
// once per key. Structs only, so predicate.h can take them as parameters
// without depending on the cache.

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

namespace valkey_search::indexes {
class Tag;
}

namespace valkey_search::query {

// A word's shared posting list. Every tree (global and per-key) hands back the
// same Postings object for a word, so one lookup answers for every candidate.
// The word names the bucket a main-thread probe takes.
struct WordPostings {
  std::string word;
  indexes::text::InvasivePtr<indexes::text::Postings> postings;
};

// One BM25 term: tf is summed across its postings, scored with one IDF.
struct TermGroup {
  // Which part of the stemmed query this group is; membership gates the
  // original word with the predicate's field mask and the rest with the stem
  // mask.
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
  uint64_t field_mask = ~0ULL;  // Shared by all terms; expansions never stem.
  struct Term {
    WordPostings term;
    size_t key_count = 0;
    float idf = 0.0f;
  };
  std::optional<Term> representative;
};

// One of an expansion's matching words that a document carries, with the
// document's entry and the word's key count, read together under one lock.
struct ExpansionMatch {
  WordPostings term;
  indexes::text::PostingValue entry;
  size_t key_count = 0;
};

// Tag leaf: each matched tag value is a BM25 term with tf = 1.
struct TagLeaf {
  const indexes::Tag *tag_index = nullptr;
  // Query values present in the index. The bag answers membership and dt with
  // one probe per candidate, replacing a per-candidate parse of the document's
  // tag string.
  struct Value {
    std::string value;
    // Borrowed from the rax slot, so only held while ingestion is excluded
    // (LockMode::kBackground); the main thread reads membership from the
    // fetched record instead.
    std::optional<BorrowedBagOfInternedStringPtrs> bag;
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

}  // namespace valkey_search::query

#endif  // VALKEYSEARCH_SRC_QUERY_RESOLVED_LEAF_H_
