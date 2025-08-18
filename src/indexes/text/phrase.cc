#include "src/indexes/text/phrase.h"

namespace valkey_search::indexes::text {

PhraseIterator::PhraseIterator(const std::vector<WordIterator>& words,
                              size_t slop,
                              bool in_order,
                              const InternedStringSet* untracked_keys,
                              std::optional<size_t> text_field_number)
    : words_(words),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      text_field_number_(text_field_number) {
}

bool PhraseIterator::Done() const {
  // Check if key iterator is valid
  return !key_iter_.IsValid();
}

void PhraseIterator::Next() {
  // On a Begin() call, we initialize the target_posting_ and key_iter_.
  if (begin_) {
    target_posting_ = words_[0].GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;  // Set to false after the first call to Next.
    
    // Check first key for field requirement
    if (text_field_number_.has_value() && !Done() && 
        !key_iter_.ContainsField(text_field_number_.value())) {
      Next();
    }
    return;
  }
  
  // Advance until we find a valid key or reach the end
  do {
    key_iter_.NextKey();
    if (Done()) {
      break;
    }
  } while (text_field_number_.has_value() && 
           !key_iter_.ContainsField(text_field_number_.value()));
}

const InternedStringPtr& PhraseIterator::operator*() const {
  // Return the current key from the key iterator of the posting object.
  return key_iter_.GetKey();
}

} // namespace valkey_search::indexes::text
