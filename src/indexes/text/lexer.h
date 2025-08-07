/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef _VALKEY_SEARCH_INDEXES_TEXT_LEXER_H_
#define _VALKEY_SEARCH_INDEXES_TEXT_LEXER_H_

/*

The Lexer takes in a UTF-8 string and outputs a vector of words.
Each word output is decorated with metadata like it's physical location, etc.

1. The lexer treats configurable punctuation as whitespace.
2. The lexer supports escaping of punctuation to force its treatment as a
natural character that is part of a word.
3. Words are defined as sequences of utf-8 characters separated by whitespace.

*/

#include <string>
#include <vector>

#include "absl/status/statusor.h"

namespace valkey_search::indexes::text {

/*

Configuration for Lexer operation. Typically, there's one of these for each
index-field.

*/

struct Lexer {
  //
  // Set/Get current punctuation characters.
  //
  // Since every scanned input character is subject to this table, we want to
  // make the implementation efficient, so by restricting punctuation characters
  // to ASCII characters a bitmap implementation becomes feasible and is
  // preferred.
  //
  // Rejected if we find some non-ASCII characters
  absl::Status SetPunctuation(const std::string& new_punctuation);

  std::string GetPunctuation() const;

  // Instantiate with default Punctuation
  Lexer();

  //
  // Process an input string
  //
  // May fail if there are non-UTF-8 characters present.
  //
  absl::StatusOr<LexerOutput> ProcessString(absl::string_view s) const;
};

//
// The primary output of the lexer. most words are string-views of the original
// input string. But for escaped words, the actual storage will be within this
// object. Hence clients of this object don't have to worry about whether a word
// is escaped or not. The position of a word is the index into the word vector.
//
struct LexerOutput {
  struct Word {
    std::string_view word;
    struct Location {
      unsigned start;  // Byte Offset of start within original text
      unsigned end;    // Byte Offset of end+1
    } location;
  };

  const std::vector<Word>& GetWords() const;

 private:
  // Storage for escaped words
  std::vector<std::string> escaped_words_;
}

}  // namespace valkey_search::indexes::text

#endif
