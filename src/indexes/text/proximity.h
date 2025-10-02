/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_PHRASE_H_

#include <cstddef>
#include <vector>

#include "src/indexes/index_base.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/text_iterator.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

using FieldMaskPredicate = uint64_t;
using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

/*


Top level iterator for a phrase.

Proximity is defined as a sequence of words that can be separated by up to
'slop' words. Optionally, the order of the words can be required or not.

This is implemented as a merge operation on the various Word Iterators passed
in. The code below is conceptual and is written like a Python generator

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
    Find smallest PositionIterator and advance it to the next Smallest Position
Iterator.

  }
}
*/
class ProximityIterator : public TextIterator {
 public:
  ProximityIterator(std::vector<std::unique_ptr<TextIterator>>&& iters,
                    size_t slop, bool in_order, FieldMaskPredicate field_mask,
                    const InternedStringSet* untracked_keys = nullptr);
  uint64_t FieldMask() const override;
  // Key-level iteration
  bool DoneKeys() const override;
  const InternedStringPtr& CurrentKey() const override;
  bool NextKey() override;
  bool SeekForwardKey(const InternedStringPtr& target_key) override;
  // Position-level iteration
  bool DonePositions() const override;
  std::pair<uint32_t, uint32_t> CurrentPosition() const override;
  bool NextPosition() override;

 private:
  // List of all the Text Predicates contained in the Proximity AND.
  std::vector<std::unique_ptr<TextIterator>> iters_;
  size_t slop_;
  bool in_order_;
  // This is from the query and is used in exact phrase
  uint64_t field_mask_;

  InternedStringPtr current_key_;
  std::optional<uint32_t> current_start_pos_;
  std::optional<uint32_t> current_end_pos_;

  // Used for Negate
  const InternedStringSet* untracked_keys_;

  bool FindCommonKey();
  bool IsValidPositionCombination(
      const std::vector<std::pair<uint32_t, uint32_t>>& positions) const;
  std::optional<size_t> FindViolatingIterator(
    const std::vector<std::pair<uint32_t, uint32_t>>& positions) const;
};
}  // namespace valkey_search::indexes::text

#endif
