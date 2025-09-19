/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/text_index.h"

#include "absl/strings/ascii.h"
#include "libstemmer.h"

namespace valkey_search::indexes::text {

namespace {

bool IsWhitespace(unsigned char c) {
  return std::isspace(c) || std::iscntrl(c);
}

PunctuationBitmap BuildPunctuationBitmap(const std::string& punctuation) {
  PunctuationBitmap bitmap;
  bitmap.reset();

  for (int i = 0; i < 256; ++i) {
    if (IsWhitespace(static_cast<unsigned char>(i))) {
      bitmap.set(i);
    }
  }

  for (char c : punctuation) {
    bitmap.set(static_cast<unsigned char>(c));
  }

  return bitmap;
}

absl::flat_hash_set<std::string> BuildStopWordsSet(
    const std::vector<std::string>& stop_words) {
  absl::flat_hash_set<std::string> stop_words_set;
  for (const auto& word : stop_words) {
    stop_words_set.insert(absl::AsciiStrToLower(word));
  }
  return stop_words_set;
}

}  // namespace

TextIndexSchema::TextIndexSchema(data_model::Language language,
                                 const std::string& punctuation,
                                 bool with_offsets,
                                 const std::vector<std::string>& stop_words)
    : language_(language),
      punct_bitmap_(BuildPunctuationBitmap(punctuation)),
      stop_words_set_(BuildStopWordsSet(stop_words)),
      with_offsets_(with_offsets),
      stemmer_(sb_stemmer_new(GetLanguageString(), "UTF_8")) {}

TextIndexSchema::~TextIndexSchema() {
  if (stemmer_) {
    sb_stemmer_delete(stemmer_);
  }
}

const char* TextIndexSchema::GetLanguageString() const {
  switch (language_) {
    case data_model::LANGUAGE_ENGLISH:
      return "english";
    default:
      return "english";
  }
}

}  // namespace valkey_search::indexes::text
