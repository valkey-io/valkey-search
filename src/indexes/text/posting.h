/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_TEXT_POSTING_H_
#define VALKEYSEARCH_SRC_INDEXES_TEXT_POSTING_H_

/*

For each entry in the inverted term index, there is an instance of
this structure which is used to contain the key/field/position information for
each word. It is expected that there will be a very large number of these
objects most of which will have only a small number of key/field/position
entries. However, there will be a small number of instances where the number of
key/field/position entries is quite large. Thus it's likely that the fully
optimized version of this object will have two or more encodings for its
contents. This optimization is hidden from external view.

This object is NOT multi-thread safe, it's expected that the caller performs
locking for mutation operations.

Conceptually, this object holds an ordered list of Keys and for each Key there
is an ordered list of Positions. Each position is tagged with a bitmask of
fields.

A KeyIterator is provided to iterate over the keys within this object.
A PositionIterator is provided to iterate over the positions of an individual
Key.

*/

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/types/span.h"
#include "src/indexes/text/flat_position_map.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

// Forward declaration
struct TextIndexMetadata;

using Key = InternedStringPtr;
using Position = uint32_t;
using FieldMaskPredicate = uint64_t;

struct FieldMask {
  // Constructors
  FieldMask() = default;
  explicit FieldMask(size_t num_fields);

  // FieldMask functions
  void SetField(size_t field_index);
  size_t CountSetFields() const;
  uint64_t GetMask() const;

 private:
  uint64_t mask_{0};
  uint8_t num_fields_{0};
};

static_assert(sizeof(FieldMask) == 16, "FieldMask should exactly be 16 bytes");

using PositionMap = absl::btree_map<Position, FieldMask>;

// Btree value: the position map pointer plus the key's (immutable) term
// frequency and document length, co-located so the scoring hot path reads them
// straight off the merge iterator instead of decoding the separately allocated
// FlatPositionMap block (tf) or probing the per-key scoring map (doc_len).
struct PostingValue {
  FlatPositionMap* map;
  uint32_t tf;
  uint32_t doc_len;
};
// doc_len fills the padding after tf, so PostingValue stays 16 bytes.
static_assert(sizeof(PostingValue) == 16,
              "doc_len should fill existing padding, not grow PostingValue");

struct Postings {
  struct KeyIterator;

  // Destructor: clean up all FlatPositionMaps
  ~Postings();

  // Are there any postings in this object?
  bool IsEmpty() const;

  // Insert the key with FlatPositionMap, the key's term frequency for this term
  // and its document length. tf must equal the total field occurrences in
  // flat_map; the caller already computes it while building the position map.
  void InsertKey(const Key& key, FlatPositionMap* flat_map, uint32_t tf,
                 uint32_t doc_len);

  // Remove a key and all positions for it
  void RemoveKey(const Key& key, TextIndexMetadata* metadata);

  // Total number of keys
  size_t GetKeyCount() const;

  // Total number of positions for all keys
  size_t GetPositionCount() const;

  // Total frequency of the term across all keys and positions
  size_t GetTotalTermFrequency() const;

  // Look up the posting entry (tf + doc_len) for a specific key in one find,
  // only used in extra-step scoring. Returns nullopt if the key is absent.
  std::optional<PostingValue> LookupKey(BorrowedInternedStringPtr key) const;

  // Defrag this contents of this object. Returns the updated "this" pointer.
  Postings* Defrag();

  // Get a Key iterator.
  KeyIterator GetKeyIterator() const;

  // The Key Iterator
  struct KeyIterator {
    // Is valid?
    bool IsValid() const;

    // Advance to next key
    void NextKey();

    // Skip forward to next key that is equal to or greater than.
    // return true if it lands on an equal key, false otherwise.
    bool SkipForwardKey(const Key& key);

    // Get Current key
    const Key& GetKey() const;

    // Check if word is present in any of the fields specified by field_mask for
    // current key
    bool ContainsFields(uint64_t field_mask) const;

    // Get Position Iterator
    PositionIterator GetPositionIterator() const;

    // get tf for the current key, only used in iterator scoring
    size_t GetTermFrequency() const;

    // get the document length for the current key (scoring hot path)
    uint32_t GetDocLen() const;

   private:
    friend struct Postings;

    // Iterator state - pointer to key_to_positions map
    const absl::btree_map<Key, PostingValue, InternedStringPtrLess>* key_map_;
    absl::btree_map<Key, PostingValue, InternedStringPtrLess>::const_iterator
        current_;
    absl::btree_map<Key, PostingValue, InternedStringPtrLess>::const_iterator
        end_;
  };

 private:
  // Cache tf in PostingValue to avoid a map lookup
  // PostValue should be removed and restored if no extra-step
  // Transparent comparator so LookupKey() can probe with a borrowed key.
  absl::btree_map<Key, PostingValue, InternedStringPtrLess> key_to_positions_;
};

// Distinct key (document) count across several key-sorted KeyIterators via a
// k-way merge, so a key present in more than one iterator is counted once (e.g.
// the stem-inflection leaf's df, where a doc holding several inflections must
// count as one). Advances the iterators to exhaustion. Shared by both scoring
// paths (in-iterator BuildTextIterator and extra-step ResolveLeaves) so they
// compute the same df.
uint32_t CountDistinctKeys(absl::Span<Postings::KeyIterator> iterators);

}  // namespace valkey_search::indexes::text

#endif
