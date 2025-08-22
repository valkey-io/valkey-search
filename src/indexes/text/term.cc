#include "src/indexes/text/term.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(const WordIterator& word,
                              const FieldMaskPredicate field_mask,
                              const InternedStringSet* untracked_keys)
    : word_(word),
      field_mask_(field_mask),
      untracked_keys_(untracked_keys) {
}

bool TermIterator::Done() const {
  // Check if key iterator is valid
  return !key_iter_.IsValid();
}

void TermIterator::Next() {
  // On a Begin() call, we initialize the target_posting_ and key_iter_.
  if (begin_) {
    target_posting_ = word_.GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;  // Set to false after the first call to Next.
    
    // Check first key for field requirement
    if (!Done() && !key_iter_.ContainsFields(field_mask_)) {
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
  } while (!key_iter_.ContainsFields(field_mask_));
}

const InternedStringPtr& TermIterator::operator*() const {
  // Return the current key from the key iterator of the posting object.
  return key_iter_.GetKey();
}

} // namespace valkey_search::indexes::text