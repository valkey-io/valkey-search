/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_

#include <vector>
#include <cstddef>
#include <optional>
#include "src/indexes/index_base.h"
#include "src/indexes/text/radix_tree.h"
#include "src/utils/string_interning.h"
#include "src/indexes/text/posting.h"


namespace valkey_search::indexes::text {

using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

/*


Top level iterator for a phrase.

A Phrase is defined as a sequence of words that can be separated by up to 'slop'
words. Optionally, the order of the words can be required or not.

This is implemented as a merge operation on the various Word Iterators passed in.
The code below is conceptual and is written like a Python generator

for (word0 : all words in word[0]) {
  for (word1 : all words in word[1]) {
    for (word2 : all words in word[2]) {
       match_one_word_combination([word0, word1, word2, ...]);
    }
  }
}

void match_one_word_combination(words) {
   KeyIterators[*] = words[*].GetKeyIterators();
   while (! Any KeyIterators.Done()) {
    if (KeyIterators[*] all point to same key) {
      process_one_key(KeyIterators[*])
    }
    Find lexically smallest KeyIterator and Advance it to the Next Smallest Key
    }
   }
}
// Need to handle the fields, this is a bit mask on the positions iterator
void process_one_key(KeyIterators[*]) {
  PositionIterators[*] = KeyIterators[*].GetPositionIterators();
  while (! Any PositionIterators.Done()) {
    if (PositionIterators[*] satisfy the Slop and In-order requirements) {
      Yield word;
    }
    Find smallest PositionIterator and advance it to the next Smallest Position Iterator.

  }
}
*/
class PhraseIterator : public indexes::EntriesFetcherIteratorBase {
 public:
  PhraseIterator(const std::vector<WordIterator>& words,
                 size_t slop,
                 bool in_order,
                 const InternedStringSet* untracked_keys = nullptr,
                 std::optional<size_t> text_field_number = std::nullopt);

  bool Done() const override;
  void Next() override;
  const InternedStringPtr& operator*() const override;

 private:
  std::vector<WordIterator> words_;
  std::shared_ptr<Postings> target_posting_;
  Postings::KeyIterator key_iter_;
  bool begin_ = true;  // Used to track if we are at the beginning of the iterator.
  size_t slop_;
  bool in_order_;
  const InternedStringSet* untracked_keys_;
  InternedStringPtr current_key_;
  std::optional<size_t> text_field_number_;
};


}  // namespace valkey_search::indexes::text

#endif