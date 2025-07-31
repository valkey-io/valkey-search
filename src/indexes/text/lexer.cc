/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/lexer.h"

#include <bitset>
#include <cstddef>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace valkey_search::text {

namespace {
// Default punctuation characters from FT.CREATE parser
constexpr absl::string_view kDefaultPunctuation{" \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"};

// ASCII character bitmap for fast punctuation lookup
using PunctuationBitmap = std::bitset<256>;

// Check if a character is valid UTF-8 continuation byte
bool IsUtf8Continuation(char c) {
  return (static_cast<unsigned char>(c) & 0xC0) == 0x80;
}

// Get the length of a UTF-8 character from its first byte
int GetUtf8CharLength(char c) {
  unsigned char byte = static_cast<unsigned char>(c);
  if (byte < 0x80) return 1;  // ASCII
  if ((byte & 0xE0) == 0xC0) return 2;  // 110xxxxx
  if ((byte & 0xF0) == 0xE0) return 3;  // 1110xxxx
  if ((byte & 0xF8) == 0xF0) return 4;  // 11110xxx
  return -1;  // Invalid UTF-8
}

// Validate UTF-8 sequence starting at position
bool IsValidUtf8Sequence(absl::string_view text, size_t pos, int expected_len) {
  if (pos + expected_len > text.size()) return false;
  
  for (int i = 1; i < expected_len; ++i) {
    if (!IsUtf8Continuation(text[pos + i])) {
      return false;
    }
  }
  return true;
}

// Check if entire string is valid UTF-8
bool IsValidUtf8(absl::string_view text) {
  size_t pos = 0;
  while (pos < text.size()) {
    int char_len = GetUtf8CharLength(text[pos]);
    if (char_len < 0 || !IsValidUtf8Sequence(text, pos, char_len)) {
      return false;
    }
    pos += char_len;
  }
  return true;
}

// Create punctuation bitmap from string
PunctuationBitmap CreatePunctuationBitmap(const std::string& punctuation) {
  PunctuationBitmap bitmap;
  for (char c : punctuation) {
    bitmap.set(static_cast<unsigned char>(c));
  }
  return bitmap;
}

}  // namespace

Lexer::Lexer() : punctuation_(kDefaultPunctuation) {}

absl::Status Lexer::SetPunctuation(const std::string& new_punctuation) {
  // Validate that all characters are ASCII
  for (char c : new_punctuation) {
    if (static_cast<unsigned char>(c) >= 128) {
      return absl::InvalidArgumentError(
          "Punctuation characters must be ASCII");
    }
  }
  
  punctuation_ = new_punctuation;
  return absl::OkStatus();
}

std::string Lexer::GetPunctuation() const {
  return punctuation_;
}

absl::StatusOr<LexerOutput> Lexer::ProcessString(absl::string_view s) const {
  // Validate UTF-8
  if (!IsValidUtf8(s)) {
    return absl::InvalidArgumentError("Invalid UTF-8 sequence in input");
  }
  
  LexerOutput output;
  PunctuationBitmap punct_bitmap = CreatePunctuationBitmap(punctuation_);
  
  size_t pos = 0;
  while (pos < s.size()) {
    // Skip punctuation/whitespace, but handle escape sequences
    while (pos < s.size()) {
      char current = s[pos];
      
      // Handle escape sequences - don't skip them as punctuation
      if (current == '\\' && pos + 1 < s.size()) {
        break;  // This is the start of a word with escaped characters
      }
      
      // Skip regular punctuation
      if (punct_bitmap[static_cast<unsigned char>(current)]) {
        pos++;
        continue;
      }
      
      // Found non-punctuation character - start of word
      break;
    }
    
    if (pos >= s.size()) break;
    
    // Found start of a word
    size_t word_start = pos;
    std::string escaped_word;
    bool has_escaped_chars = false;
    
    // Process the word
    while (pos < s.size()) {
      char current = s[pos];
      
      // Handle escape sequences
      if (current == '\\' && pos + 1 < s.size()) {
        // Next character is escaped - treat as literal
        has_escaped_chars = true;
        if (escaped_word.empty()) {
          // First escaped char - copy everything so far
          escaped_word = std::string(s.substr(word_start, pos - word_start));
        }
        escaped_word += s[pos + 1];  // Add the escaped character
        pos += 2;  // Skip both '\' and the escaped character
        continue;
      }
      
      // Check if current character is punctuation
      if (punct_bitmap[static_cast<unsigned char>(current)]) {
        break;  // End of word
      }
      
      // Regular character - add to escaped word if we're building one
      if (has_escaped_chars) {
        escaped_word += current;
      }
      
      pos++;
    }
    
    // Create word entry
    LexerOutput::Word word;
    word.location.start = word_start;
    word.location.end = pos;
    
    if (has_escaped_chars) {
      // Store escaped word and reference it
      output.escaped_words_.push_back(std::move(escaped_word));
      word.word = output.escaped_words_.back();
    } else {
      // Use string_view of original text
      word.word = s.substr(word_start, pos - word_start);
    }
    
    output.words_.push_back(word);
  }
  
  return output;
}

const std::vector<LexerOutput::Word>& LexerOutput::GetWords() const {
  return words_;
}

}  // namespace valkey_search::text
