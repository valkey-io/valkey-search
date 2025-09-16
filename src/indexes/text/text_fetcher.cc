#include "text_fetcher.h"

namespace valkey_search::indexes::text {

TextFetcher::TextFetcher(std::unique_ptr<TextIterator> iter)
    : iter_(std::move(iter))
{
    VMSDK_LOG(WARNING, nullptr) << "TF::init";
}

void TextFetcher::Next() {
    VMSDK_LOG(WARNING, nullptr) << "TF::Next1";   
    if (Done()) return;
    VMSDK_LOG(WARNING, nullptr) << "TF::Next2";   
    iter_->NextKey();
}

bool TextFetcher::Done() const {
    VMSDK_LOG(WARNING, nullptr) << "TF::Done";   
    return iter_->DoneKeys();
}

const InternedStringPtr& TextFetcher::operator*() const {
    VMSDK_LOG(WARNING, nullptr) << "TF::operator";   
    return iter_->CurrentKey();
}

}  // namespace valkey_search::indexes::text
