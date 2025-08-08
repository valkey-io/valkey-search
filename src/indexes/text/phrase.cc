#include "src/indexes/text/phrase.h"
#include "src/indexes/text/posting.h"

namespace valkey_search::indexes::text {

PhraseIterator::PhraseIterator(const std::vector<WordIterator>& words,
                              size_t slop,
                              bool in_order,
                              const InternedStringSet* untracked_keys)
    : words_(words),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys),
      current_idx_(0) {
}

bool PhraseIterator::Done() const {
  // 1. Check if we are at the end of the Posting Interator.
  // return key_iter_.IsValid();
  // target_posting_ = words_[0].GetTarget();
  // key_iter_ = target_posting_->GetKeyIterator();
  VMSDK_LOG(NOTICE, nullptr) << "Inside PhraseIterator::Done()" << " current_idx_=" << current_idx_
                             << " target_posting_->GetKeyCount()="
                             << (target_posting_ ? target_posting_->GetKeyCount() : 0);
  return current_idx_ >= target_posting_->GetKeyCount();
}

void PhraseIterator::Next() {
  VMSDK_LOG(NOTICE, nullptr) << "Inside PhraseIterator::Next()";
  if (begin_) {
    target_posting_ = words_[0].GetTarget();
    key_iter_ = target_posting_->GetKeyIterator();
    begin_ = false;  // Set to false after the first call to Next.
    return;
  }
  // TODO: Implement
  // 1. Get the Posting from the WordIterator of the idx 1 (we only support single word).
  // 2. Get the Posting Iterator. 
  // 3. Store the ref to the Posting Iterator.
  // 4. Move to the next position in the Posting Iterator.
  // NOTE: We should skip the word if the attribute field mask does not match the required fields.
  VMSDK_LOG(NOTICE, nullptr) << "target_posting_.GetKeyCount(): " << target_posting_->GetKeyCount();
  // while (key_iter_.IsValid()) {
  while (current_idx_ < target_posting_->GetKeyCount()) {
    VMSDK_LOG(NOTICE, nullptr) << "Iterating over key: " << key_iter_.GetKey();
    key_iter_.NextKey();
    current_idx_ += 1;
    break;
  }
}

const InternedStringPtr& PhraseIterator::operator*() const {
  // TODO: Implement
  // 1. Return the current key at the current position of the Posting Iterator.
  return key_iter_.GetKey();
}

} // namespace valkey_search::indexes::text
