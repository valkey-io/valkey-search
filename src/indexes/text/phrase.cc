#include "src/indexes/text/phrase.h"

namespace valkey_search::indexes::text {

PhraseIterator::PhraseIterator(const std::vector<WordIterator>& words,
                              size_t slop,
                              bool in_order,
                              const InternedStringSet* untracked_keys)
    : words_(words),
      slop_(slop),
      in_order_(in_order),
      untracked_keys_(untracked_keys) {
}

bool PhraseIterator::Done() const {
  // 1. Check if we are at the end of the Posting Interator.
  return true;
}

void PhraseIterator::Next() {
  // TODO: Implement
  // 1. Get the Posting from the WordIterator of the idx 1 (we only support single word).
  // 2. Get the Posting Iterator. 
  // 3. Store the ref to the Posting Iterator.
  // 4. Move to the next position in the Posting Iterator.
  // NOTE: We should skip the word if the attribute field mask does not match the required fields.
}

const InternedStringPtr& PhraseIterator::operator*() const {
  // TODO: Implement
  // 1. Return the current key at the current position of the Posting Iterator.
  return current_key_;
}

} // namespace valkey_search::indexes::text
