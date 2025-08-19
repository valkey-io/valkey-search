#include "src/indexes/text/phrase.h"

namespace valkey_search::indexes::text {

PhraseIterator::PhraseIterator(const std::vector<WordIterator>& words,
                              size_t slop,
                              bool in_order,
                              const InternedStringSet* untracked_keys,
                              std::unique_ptr<FieldMask> field_mask)
    : words_(words),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      field_mask_(std::move(field_mask)) {
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
    if (field_mask_ != nullptr && !Done() && 
        !key_iter_.ContainsFields(*field_mask_)) {
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
  } while (field_mask_ != nullptr && 
           !key_iter_.ContainsFields(*field_mask_));
}

const InternedStringPtr& PhraseIterator::operator*() const {
  // Return the current key from the key iterator of the posting object.
  return key_iter_.GetKey();
}

} // namespace valkey_search::indexes::text
