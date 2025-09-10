/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_

#include <cstddef>
#include <vector>

#include "src/indexes/index_base.h"
#include "src/indexes/text/posting.h"
#include "src/indexes/text/radix_tree.h"
#include "src/utils/string_interning.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

using FieldMaskPredicate = uint64_t;
using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

/*


Top level iterator for a Term

*/
// class TermIterator : public indexes::EntriesFetcherIteratorBase {
//  public:
//   TermIterator(const WordIterator& word, const absl::string_view data,
//                const FieldMaskPredicate field_mask,
//                const InternedStringSet* untracked_keys = nullptr);

//   bool Done() const override;
//   void Next() override;
//   const InternedStringPtr& operator*() const override;

//  private:
//   WordIterator word_;
//   std::shared_ptr<Postings> target_posting_;
//   Postings::KeyIterator key_iter_;
//   const absl::string_view data_;
//   uint32_t current_idx_ = 0;
//   bool begin_ =
//       true;  // Used to track if we are at the beginning of the iterator.
//   bool nomatch_ = false;
//   const InternedStringSet* untracked_keys_;
//   InternedStringPtr current_key_;
//   FieldMaskPredicate field_mask_;
// };
class TermIterator : public TextIterator {
 public:
  TermIterator(const WordIterator& word_iter, 
               bool exact, 
               const absl::string_view data,
               const uint32_t field_mask,
               const InternedStringSet* untracked_keys = nullptr);

  // // Word-level iteration
  // bool NextWord() override;
  // absl::string_view CurrentWord() override;

  // Key-level iteration
  bool NextKey() override;
  const InternedStringPtr& CurrentKey() override;

  // Position-level iteration
  bool NextPosition() override;
  uint32_t CurrentPosition() override;

  // Optional unified iteration contract
  bool Done() const override;
  void Next() override;

 private:
  const bool exact_;
  const absl::string_view data_;
  const uint32_t field_mask_;

  WordIterator word_iter_;
  std::shared_ptr<Postings> target_posting_;
  Postings::KeyIterator key_iter_;
  Postings::PositionIterator pos_iter_;

  InternedStringPtr current_key_;
  uint32_t current_pos_;
  const InternedStringSet* untracked_keys_;
  bool nomatch_;
};

}  // namespace valkey_search::indexes::text

#endif
