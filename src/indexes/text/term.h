/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_TERM_H_

#include <vector>

#include "src/indexes/text/posting.h"
#include "src/indexes/text/text_iterator.h"

namespace valkey_search::indexes::text {

/*

Top level iterator for a Term.

TermIterator Responsibilities:
- Manages a vector of posting (key) iterator/s, which operates in lexical order.
- Key iteration (of documents) takes place by advancing the posting iterator who
is on the smallest key until it is on a key whose field matches the field mask
of the search operation. Since multiple posting iterators can have the same key
amd same field, we create a vector of position iterators, one from each posting
iterator who are on the same key & field. Once no more keys are found, DoneKeys
returns true. Through this process, it "merges" multiples posting iterators.
- Position iteration happens across all the position iterators, allowing us to
search for positions across all the required words within the same key and same
field. Once no more positions are found, DonePositions returns true.

*/
class TermIterator : public TextIterator {
 public:
  TermIterator(std::vector<Postings::KeyIterator>&& key_iterators,
               const uint64_t field_mask,
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
  const uint64_t field_mask_;
  std::vector<Postings::KeyIterator> key_iterators_;
  std::vector<Postings::PositionIterator> pos_iterators_;
  InternedStringPtr current_key_;
  std::optional<uint32_t> current_position_;
  const InternedStringSet* untracked_keys_;
  bool FindMinimumValidKey();
};

}  // namespace valkey_search::indexes::text

#endif
