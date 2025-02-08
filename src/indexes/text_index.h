#ifndef VALKEY_SEARCH_INDEXES_TEXT_TEXT_H_
#define VALKEY_SEARCH_INDEXES_TEXT_TEST_H_

namepsace valkey_search {
namespace indexes {

struct Text : public IndexBase {
  // Constructor
  Text(const data_model& text_index_proto);

  text::RadixTree prefix_;
  std::optional<text::RadixTree> suffix_;

  absl::hashmap<Key, text::RadixTree> reverse_;

  absl::hashset<Key> untracked_keys_;

}

}
}

#endif