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
#include "src/indexes/text/text_index.h"
#include "src/utils/string_interning.h"

namespace valkey_search::indexes::text {

using FieldMaskPredicate = uint64_t;
using WordIterator = RadixTree<std::shared_ptr<Postings>, false>::WordIterator;

/*


Top level iterator for a Term

*/
class TermIterator : public indexes::EntriesFetcherIteratorBase {
 public:
  TermIterator(const WordIterator& word, const absl::string_view data,
               const FieldMaskPredicate field_mask,
               const InternedStringSet* untracked_keys = nullptr,
               bool stemming_enabled = false,
               std::shared_ptr<TextIndexSchema> text_index_schema = nullptr,
               uint32_t min_stem_size = 0,
               std::shared_ptr<TextIndex> text_index = nullptr);

  bool Done() const override;
  void Next() override;
  const InternedStringPtr& operator*() const override;

 private:
  WordIterator word_;
  std::shared_ptr<Postings> target_posting_;
  Postings::KeyIterator key_iter_;
  const absl::string_view data_;
  uint32_t current_idx_ = 0;
  bool begin_ =
      true;  // Used to track if we are at the beginning of the iterator.
  bool nomatch_ = false;
  const InternedStringSet* untracked_keys_;
  InternedStringPtr current_key_;
  FieldMaskPredicate field_mask_;
  
  // Stemming support
  bool stemming_enabled_ = false;
  std::shared_ptr<TextIndexSchema> text_index_schema_;
  uint32_t min_stem_size_ = 0;
  std::shared_ptr<TextIndex> text_index_;
  std::shared_ptr<StemTarget> stem_target_;
  StemTarget::iterator stem_word_iter_;
};

}  // namespace valkey_search::indexes::text

#endif
