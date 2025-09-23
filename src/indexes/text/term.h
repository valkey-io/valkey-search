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
class TermIterator : public TextIterator {
 public:
  TermIterator(const WordIterator& word_iter, 
               bool exact, 
               const absl::string_view data,
               const uint32_t field_mask,
               const InternedStringSet* untracked_keys = nullptr);
  uint64_t FieldMask() const override;
  // Key-level iteration
  bool DoneKeys() const override;
  bool NextKey() override;
  const InternedStringPtr& CurrentKey() const override;

  // Position-level iteration
  bool DonePositions() const override;
  bool NextPosition() override;
  std::pair<uint32_t, uint32_t> CurrentPosition() const override;

 private:
  const bool exact_;
  const absl::string_view data_;
  const uint32_t field_mask_;

  WordIterator word_iter_;
  std::shared_ptr<Postings> target_posting_;
  Postings::KeyIterator key_iter_;
  Postings::PositionIterator pos_iter_;

  InternedStringPtr current_key_;
  std::optional<uint32_t> current_position_;
  const InternedStringSet* untracked_keys_;
  bool nomatch_;
};

}  // namespace valkey_search::indexes::text

#endif
