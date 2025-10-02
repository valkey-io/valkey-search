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
#include "src/indexes/text/text_iterator.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

using FieldMaskPredicate = uint64_t;
using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

/*


Top level iterator for a Term

*/
class TermIterator : public TextIterator {
 public:
  TermIterator(const std::vector<Postings::KeyIterator>& key_iterators,
               const uint32_t field_mask,
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
  const uint32_t field_mask_;

  std::vector<Postings::KeyIterator> key_iterators_;
  std::vector<Postings::PositionIterator> pos_iterators_;

  InternedStringPtr current_key_;
  std::optional<uint32_t> current_position_;
  const InternedStringSet* untracked_keys_;
};

}  // namespace valkey_search::indexes::text

#endif
