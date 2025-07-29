#ifndef VALKEY_SEARCH_INDEXES_TEXT_TEXT_H_
#define VALKEY_SEARCH_INDEXES_TEXT_TEST_H_


#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text/radix_tree.h" 

namespace valkey_search {
namespace indexes {

struct Text {

  // Constructor
  Text(const data_model& text_index_proto);

  text::RadixTree prefix_;
  std::optional<text::RadixTree> suffix_;

  absl::hashmap<Key, text::RadixTree> reverse_;

  absl::hashset<Key> untracked_keys_;
};

}  // namespace indexes
}  // namespace valkey_search

#endif
