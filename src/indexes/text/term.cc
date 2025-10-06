/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/term.h"
#include "src/indexes/text/lexer.h"
#include "src/indexes/text/text_index.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(const WordIterator& word,
                           const absl::string_view data,
                           const FieldMaskPredicate field_mask,
                           const InternedStringSet* untracked_keys,
                           bool stemming_enabled,
                           std::shared_ptr<TextIndexSchema> text_index_schema,
                           uint32_t min_stem_size,
                           std::shared_ptr<TextIndex> text_index)
    : word_(word),
      data_(data),
      field_mask_(field_mask),
      untracked_keys_(untracked_keys),
      stemming_enabled_(stemming_enabled),
      text_index_schema_(text_index_schema),
      min_stem_size_(min_stem_size),
      text_index_(text_index) {}

bool TermIterator::Done() const {
  if (nomatch_) {
    return true;
  }
  
  if (stemming_enabled_) {
    if (key_iter_.IsValid()) {
      return false;
    }
    if (stem_target_ && stem_word_iter_ != stem_target_->end()) {
      return false;
    }
    if (word_.GetWord() != data_) {
      return false;
    }
    return true;
  }
  
  if (word_.GetWord() != data_) {
    return true;
  }
  return !key_iter_.IsValid();
}

void TermIterator::Next() {
  if (begin_) {
    if (stemming_enabled_ && text_index_schema_ && text_index_) {
      Lexer lexer;
      std::string search_term = std::string(data_);
      std::string stemmed_term = lexer.StemWord(
          search_term, text_index_schema_->GetStemmer(), true, min_stem_size_,
          text_index_schema_->GetStemmerMutex());
      
      auto stem_iter = text_index_->stem_.GetWordIterator(stemmed_term);
      if (!stem_iter.Done() && stem_iter.GetWord() == stemmed_term) {
        stem_target_ = stem_iter.GetTarget();
        if (stem_target_ && !stem_target_->empty()) {
          stem_word_iter_ = stem_target_->begin();
          word_ = text_index_->prefix_.GetWordIterator(*stem_word_iter_);
        }
      }
    }
    
    if (word_.Done()) {
      nomatch_ = true;
      return;
    }
    target_posting_ = word_.GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;
  } else if (key_iter_.IsValid()) {
    key_iter_.NextKey();
  }
  
  if (stemming_enabled_) {
    while (stem_target_ && stem_word_iter_ != stem_target_->end()) {
      while (key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
        key_iter_.NextKey();
      }
      if (key_iter_.IsValid()) {
        return;
      }
      
      ++stem_word_iter_;
      if (stem_word_iter_ != stem_target_->end()) {
        word_ = text_index_->prefix_.GetWordIterator(*stem_word_iter_);
        if (!word_.Done()) {
          target_posting_ = word_.GetTarget();
          if (target_posting_) {
            key_iter_ = target_posting_->GetKeyIterator();
          }
        }
      }
    }
    
    if ((!stem_target_ || stem_word_iter_ == stem_target_->end()) && 
        (word_.GetWord() != data_ || word_.Done())) {
      word_ = text_index_->prefix_.GetWordIterator(data_);
      if (!word_.Done() && word_.GetWord() == data_) {
        target_posting_ = word_.GetTarget();
        if (target_posting_) {
          key_iter_ = target_posting_->GetKeyIterator();
          while (key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
            key_iter_.NextKey();
          }
          if (key_iter_.IsValid()) {
            return;
          }
        }
      }
    }
  }
  
  while (!Done() && key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
    key_iter_.NextKey();
  }
}

const InternedStringPtr& TermIterator::operator*() const {
  return key_iter_.GetKey();
}

}  // namespace valkey_search::indexes::text
