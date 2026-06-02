/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "text_fetcher.h"

namespace valkey_search::indexes::text {

TextFetcher::TextFetcher(std::unique_ptr<TextIterator> iter)
    : iter_(std::move(iter)) {}

bool TextFetcher::Done() const { return iter_->DoneKeys(); }

const Key& TextFetcher::operator*() const { return iter_->CurrentKey(); }

void TextFetcher::Next() { iter_->NextKey(); }

}  // namespace valkey_search::indexes::text
