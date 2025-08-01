#ifndef VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
#define VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "src/utils/string_interning.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"


namespace valkey_search {
namespace indexes {

using Key = InternedStringPtr;
using Position = uint32_t;



struct TextIndex {
  // Constructor
  TextIndex(const data_model::TextIndex& text_index_proto);

  //
  // The main query data structure maps Words into Postings objects. This
  // is always done with a prefix tree. Optionally, a suffix tree can also be maintained.
  // But in any case for the same word the two trees must point to the same Postings object,
  // which is owned by this pair of trees. Plus, updates to these two trees need
  // to be atomic when viewed externally. The locking provided by the RadixTree object
  // is NOT quite sufficient to guarantee that the two trees are always in lock step.
  // thus this object becomes responsible for cross-tree locking issues.
  // Multiple locking strategies are possible. TBD (a shared-ed word lock table should work well)
  //
  std::shared_ptr<RadixTree<std::unique_ptr<valkey_search::text::Postings>, false>> prefix_;
  std::optional<std::shared_ptr<RadixTree<std::unique_ptr<valkey_search::text::Postings>, true>>> suffix_;

  absl::flat_hash_set<Key> untracked_keys_;
};

struct IndexSchemaText {
  //
  // This is the main index of all Text fields in this index schema
  //
  TextIndex corpus_;
  //
  // To support the Delete record and the post-filtering case, there is a separate
  // table of postings that are indexed by Key.
  //
  // This object must also ensure that updates of this object are multi-thread safe.
  //
  absl::flat_hash_map<Key, TextIndex> by_key_;
};

}  // namespace indexes
}  // namespace valkey_search

#endif  // VALKEY_SEARCH_INDEXES_TEXT_INDEX_H_
