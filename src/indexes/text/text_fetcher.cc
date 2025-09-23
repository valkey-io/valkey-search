#include "text_fetcher.h"

namespace valkey_search::indexes::text {

TextFetcher::TextFetcher(std::unique_ptr<TextIterator> iter)
    : iter_(std::move(iter)) {}

void TextFetcher::Next() {
  if (Done()) return;
  iter_->NextKey();
}

bool TextFetcher::Done() const { return iter_->DoneKeys(); }

const InternedStringPtr& TextFetcher::operator*() const {
  return iter_->CurrentKey();
}

}  // namespace valkey_search::indexes::text
