/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_

namespace valkey_search::indexes::text {

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
}
struct PhraseIterator : public indexes::EntriesFetcherIteratorBase {
  PhraseIterator(std::vector<WordIterator *> words, size_t slop, bool in_order);

  virtual bool Done() const override;
  virtual void Next() = override;
  // TODO: Should this return a Key or an InternedStringPtr?
  virtual const Key& operator*() const override;
  private:
    std::vector<WordIterator *> words_;
    size_t slop_{0};
    bool in_order_{true};
};


}  // namespace valkey_search::indexes::text

#endif