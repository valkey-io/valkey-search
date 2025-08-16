/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

#include <memory>
#include <optional>

#include "absl/strings/string_view.h"
#include "absl/functional/function_ref.h"
#include "absl/container/flat_hash_map.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/posting.h"
#include "src/index_schema.pb.h"

namespace valkey_search::indexes::text {

using Key = valkey_search::InternedStringPtr;
using Position = uint32_t;

struct TextIndex {
  TextIndex() = default;
  ~TextIndex() = default;
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

struct TextIndexSchema {
  TextIndexSchema() = default;
  TextIndexSchema(const data_model::IndexSchema &index_schema_proto) 
      : num_text_fields_(0), 
        text_index_(),
        language_(index_schema_proto.language()),
        punctuation_(index_schema_proto.punctuation()),
        with_offsets_(index_schema_proto.with_offsets()),
        stop_words_(index_schema_proto.stop_words().begin(), index_schema_proto.stop_words().end()) {}
  ~TextIndexSchema() = default;

  uint8_t num_text_fields_;
  //
  // This is the main index of all Text fields in this index schema
  //
  TextIndex text_index_;
  //
  // To support the Delete record and the post-filtering case, there is a
  // separate table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread
  // safe.
  //
  absl::flat_hash_map<Key, TextIndex> by_key_;

  // IndexSchema proto-derived configuration fields
  data_model::Language language_;
  std::string punctuation_;
  bool with_offsets_;
  std::vector<std::string> stop_words_;

  uint8_t AllocateTextFieldNumber() {
    return num_text_fields_++;
  }

  // TODO: Add ToProto() function here?
};

}  // namespace valkey_search::indexes::text

#endif
