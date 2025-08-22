#include "src/indexes/text/term.h"

namespace valkey_search::indexes::text {

TermIterator::TermIterator(const WordIterator& word,
                              const InternedStringSet* untracked_keys)
    : word_(word),
      untracked_keys_(untracked_keys),
      current_idx_(0) {
}

bool TermIterator::Done() const {
  return current_idx_ >= target_posting_->GetKeyCount();
}

void TermIterator::Next() {
  // On a Begin() call, we initialize the target_posting_ and key_iter_.
  if (begin_) {
    target_posting_ = word_.GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;  // Set to false after the first call to Next.
    return;
  }
  // On subsequent calls, we advance the key iterator.
  // Note: In the current implementation, we support an exact term match.
  // There is also no consideration into the attribute (field) to see that it matches
  // the query used. Currently, all matches are returned.
  while (current_idx_ < target_posting_->GetKeyCount()) {
    key_iter_.NextKey();
    current_idx_ += 1;
    break;
  }
}

const InternedStringPtr& TermIterator::operator*() const {
  // Return the current key from the key iterator of the posting object.
  return key_iter_.GetKey();
}

} // namespace valkey_search::indexes::text