/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_TEXT_H_
#define VALKEY_SEARCH_INDEXES_TEXT_TEST_H_

#include <concepts>
#include <memory>

#include "src/index_schema.pb.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

struct TextIndex {
  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be
  // maintained. But in any case for the same word the two trees must point to
  // the same Postings object, which is owned by this pair of trees. Plus,
  // updates to these two trees need to be atomic when viewed externally. The
  // locking provided by the RadixTree object is NOT quite sufficient to
  // guarantee that the two trees are always in lock step. thus this object
  // becomes responsible for cross-tree locking issues. Multiple locking
  // strategies are possible. TBD (a shared-ed word lock table should work well)
  //
  RadixTree<std::shared_ptr<Postings>, false> prefix_;
  std::optional<RadixTree<std::shared_ptr<Postings>, true>> suffix_;
};

struct IndexSchemaText {
  //
  // This is the main index of all Text fields in this index schema
  //
  std::shared_ptr<TextIndex> corpus_;
  //
  // To support the Delete record and the post-filtering case, there is a
  // separate table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread
  // safe.
  //
  absl::flat_hash_map<Key, TextIndex> by_key_;
};

}  // namespace valkey_search::indexes::text

#endif
