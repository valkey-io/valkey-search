/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEY_SEARCH_INDEXES_TEXT_WILDCARD_ITERATOR_H_
#define VALKEY_SEARCH_INDEXES_TEXT_WILDCARD_ITERATOR_H_

/*

The WildCard iterator provides an iterator to words (and their postings) that
match any pattern with a single wildcard, i.e., pattern*, *pattern, or pat*tern.

Words are iterated in lexical order.

The Wildcard iterator has two underlying algorithms and it selects between the
two algorithms based on the constructor form used and/or runtime sizing
information.

Algorithm 1: Is used when there is no suffix tree OR the number of
prefix-matching words is small (exact algo here is TBD, probably some ratio
w.r.t. the size of the suffix tree).

This algorithm iterates over a candidate list defined only by the prefix. As
each candidate is visited, the suffix is compared and if not present, the
iterator advances until the next valid suffix is found. This algorithm operates
in time O(#PrefixMatches)

Algorithm 2: Is used when a suffix tree is present and the number of
suffix-matching words is a less than the number of prefix-matching
words.

This algorithm operates by constructing a temporary RadixTree. The suffix
RadixTree is used to generate suffix-matching candidates. These candidates are
filtered by their prefix with the survivors being inserted into the temporary
RadixTree which essentially serves to sort them since the suffix-matching
candidates won't be iterated in lexical order.

This algorithm operates in time O(#SuffixMatches)

*/

#include <cstddef>
#include <vector>

#include "src/indexes/index_base.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"
#include "src/utils/string_interning.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

enum WildCardOperation {
  kPrefix,
  kSuffix,
  kInfix,
};

class WildCardIterator : public TextIterator {
 public:
  WildCardIterator(const WordIterator& word_iter,
               const WildCardOperation operation,
               const absl::string_view data,
               const uint32_t field_mask,
               const InternedStringSet* untracked_keys = nullptr);

  // Key-level iteration
  bool DoneKeys() const override;
  bool NextKey() override;
  const InternedStringPtr& CurrentKey() override;

  // Position-level iteration
  bool DonePositions() const override;
  bool NextPosition() override;
  std::pair<uint32_t, uint32_t> CurrentPosition() override;
  uint64_t CurrentFieldMask() const override;

 private:
  const absl::string_view data_;
  const uint32_t field_mask_;

  WordIterator word_iter_;
  std::shared_ptr<Postings> target_posting_;
  Postings::KeyIterator key_iter_;
  Postings::PositionIterator pos_iter_;
  WildCardOperation operation_;

  InternedStringPtr current_key_;
  std::optional<uint32_t> current_position_;
  std::optional<uint64_t> current_field_mask_;
  const InternedStringSet* untracked_keys_;
  bool nomatch_;
};

// struct WildCardIterator : public indexes::EntriesFetcherIteratorBase {
//   WildCardIterator(const WordIterator& word, const WildCardOperation operation,
//                    const FieldMaskPredicate field_mask,
//                    const InternedStringSet* untracked_keys = nullptr);

//   // Points to valid Word?
//   bool Done() const override;

//   // Go to next word
//   void Next() override;

//   const InternedStringPtr& operator*() const override;

//  private:
//   WordIterator word_;
//   std::shared_ptr<Postings> target_posting_;
//   Postings::KeyIterator key_iter_;
//   bool begin_ =
//       true;  // Used to track if we are at the beginning of the iterator.
//   const InternedStringSet* untracked_keys_;
//   InternedStringPtr current_key_;
//   WildCardOperation operation_;
//   FieldMaskPredicate field_mask_;
//   bool nomatch_ = false;
// };

// struct WildCardIterator : public WordIterator {
//   using Posting = typename Postings::Posting;
//   // Use this form when there's no suffix tree available.
//   WildCardIterator(absl::string_view prefix, absl::string_view suffix,
//                    const RadixTree<Postings>& prefix_tree);

//   // Use this form when a suffix tree IS available.
//   WildCardIterator(absl::string_view prefix, absl::string_view suffix,
//                    const RadixTree<Postings>& prefix_tree,
//                    const RadixTree<Postings>& suffix_tree);

//   // Points to valid Word?
//   bool Done() const override;

//   // Go to next word
//   void Next() override;

//   // Seek forward to word that's equal or greater
//   // returns true => found equal word, false => didn't find equal word
//   bool SeekForward(absl::string_view word);

//   // Access the iterator, will assert if !IsValid()
//   absl::string_view GetWord() const override;
//   Posting& GetPosting() const;

//   absl::string_view GetPrefix() const { return prefix_; }
//   absl::string_view GetSuffix() const { return suffix_; }

//  private:
//   absl::string_view prefix_;
//   absl::string_view suffix_;
//   // the one to iterator over, could be temporary or not....
//   std::shared_ptr<RadixTree<Postings *>> radix_tree_;
// };

}  // namespace valkey_search::indexes::text

#endif
