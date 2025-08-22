#include "src/indexes/text/wildcard.h"

namespace valkey_search::indexes::text {

WildCardIterator::WildCardIterator(const WordIterator& word,
                              const text::WildCardOperation op,
                              const FieldMaskPredicate field_mask,
                              const InternedStringSet* untracked_keys)
    : word_(word),
      operation_(op),
      field_mask_(field_mask),
      untracked_keys_(untracked_keys) {
}

bool WildCardIterator::Done() const {
  return word_.Done() && !key_iter_.IsValid();
}

void WildCardIterator::Next() {
  auto advance_to_valid_key = [&]() {
    while (key_iter_.IsValid() && !key_iter_.ContainsFields(field_mask_)) {
      key_iter_.NextKey();
    }
  };
  // On a Begin() call, we initialize the target_posting_ and key_iter_.
  if (begin_) {
    target_posting_ = word_.GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;  // Set to false after the first call to Next.
    advance_to_valid_key();
    return;
  }
  if (key_iter_.IsValid()) {
    key_iter_.NextKey();
    advance_to_valid_key();
    if (key_iter_.IsValid()) return;
  }
  while (!word_.Done()) {
    word_.Next();
    if (word_.Done()) return;
    target_posting_ = word_.GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    advance_to_valid_key();
    if (key_iter_.IsValid()) return;
  }
}

const InternedStringPtr& WildCardIterator::operator*() const {
  // Return the current key from the key iterator of the posting object.
  return key_iter_.GetKey();
}

} // namespace valkey_search::indexes::text
