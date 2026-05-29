/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEY_SEARCH_UTILS_UTF8_ITERATOR_H_
#define VALKEY_SEARCH_UTILS_UTF8_ITERATOR_H_

#include <cstdint>

#include "absl/strings/string_view.h"

namespace valkey_search::utils {

// Forward-only UTF-8 iterator. Decodes one Unicode code point per Next() call.
// On invalid bytes returns the raw byte value with byte_len=1.
// Scanner::NextUtf8()/PeekUtf8() delegate to this class.
class Utf8Iterator {
 public:
  // Result of decoding one code point.
  struct Result {
    uint32_t codepoint;  // Unicode scalar value (e.g., 0x00E9 for 'é')
    uint8_t byte_len;    // Bytes consumed: 1 for ASCII or invalid, 2-4 for
                         // valid multi-byte sequences.
  };

  explicit Utf8Iterator(absl::string_view text) : text_(text), pos_(0) {}

  // Returns true when there are no more bytes to decode.
  bool Done() const { return pos_ >= text_.size(); }

  // Current byte offset into the underlying string_view.
  size_t pos() const { return pos_; }

  // Decode and advance past one code point. Must only be called when !Done().
  //
  // Rejects RFC 3629 violations: overlong encodings, surrogate code points
  // (U+D800..U+DFFF), and code points > U+10FFFF. Invalid sequences advance
  // one byte and return the raw lead byte with byte_len=1.
  //
  // UTF-8 byte patterns:
  //   0xxxxxxx              (0x00..0x7F)  → 1 byte,  U+0000..U+007F  (ASCII)
  //   110xxxxx 10xxxxxx     (0xC2..0xDF)  → 2 bytes, U+0080..U+07FF
  //   1110xxxx 10xxxxxx×2   (0xE0..0xEF)  → 3 bytes, U+0800..U+FFFF \ surrogates
  //   11110xxx 10xxxxxx×3   (0xF0..0xF4)  → 4 bytes, U+10000..U+10FFFF
  Result Next() {
    uint8_t b0 = static_cast<uint8_t>(text_[pos_]);

    if (b0 < 0x80) {
      ++pos_;
      return {b0, 1};
    }

    // 2-byte: 110xxxxx 10xxxxxx
    if ((b0 & 0xE0) == 0xC0 && pos_ + 1 < text_.size()) {
      uint8_t b1 = static_cast<uint8_t>(text_[pos_ + 1]);
      if ((b1 & 0xC0) == 0x80) {
        uint32_t cp = ((b0 & 0x1F) << 6) | (b1 & 0x3F);
        if (cp >= 0x80) {
          pos_ += 2;
          return {cp, 2};
        }
      }
    }

    // 3-byte: 1110xxxx 10xxxxxx 10xxxxxx
    if ((b0 & 0xF0) == 0xE0 && pos_ + 2 < text_.size()) {
      uint8_t b1 = static_cast<uint8_t>(text_[pos_ + 1]);
      uint8_t b2 = static_cast<uint8_t>(text_[pos_ + 2]);
      if ((b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80) {
        uint32_t cp = ((b0 & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F);
        if (cp >= 0x800 && (cp < 0xD800 || cp > 0xDFFF)) {
          pos_ += 3;
          return {cp, 3};
        }
      }
    }

    // 4-byte: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
    if ((b0 & 0xF8) == 0xF0 && pos_ + 3 < text_.size()) {
      uint8_t b1 = static_cast<uint8_t>(text_[pos_ + 1]);
      uint8_t b2 = static_cast<uint8_t>(text_[pos_ + 2]);
      uint8_t b3 = static_cast<uint8_t>(text_[pos_ + 3]);
      if ((b1 & 0xC0) == 0x80 && (b2 & 0xC0) == 0x80 && (b3 & 0xC0) == 0x80) {
        uint32_t cp = ((b0 & 0x07) << 18) | ((b1 & 0x3F) << 12) |
                      ((b2 & 0x3F) << 6) | (b3 & 0x3F);
        if (cp >= 0x10000 && cp <= 0x10FFFF) {
          pos_ += 4;
          return {cp, 4};
        }
      }
    }

    // Invalid byte: return raw value, advance 1.
    ++pos_;
    return {b0, 1};
  }

  // Count code points in a string. E.g. CodePointCount("été") == 3.
  static size_t CodePointCount(absl::string_view text) {
    Utf8Iterator it(text);
    size_t count = 0;
    while (!it.Done()) {
      it.Next();
      ++count;
    }
    return count;
  }

  // Bytes expected for a UTF-8 sequence given its lead byte. An invalid lead
  // returns 1 (matches Next()'s invalid-byte behavior).
  static uint8_t ExpectedLen(uint8_t b0) {
    if (b0 < 0x80) return 1;
    if ((b0 & 0xE0) == 0xC0) return 2;
    if ((b0 & 0xF0) == 0xE0) return 3;
    if ((b0 & 0xF8) == 0xF0) return 4;
    return 1;
  }

  // True if the code point fits in a single UTF-8 byte (U+0000..U+007F).
  static constexpr bool IsAscii(uint32_t cp) { return cp < 0x80; }

 private:
  absl::string_view text_;
  size_t pos_;
};

}  // namespace valkey_search::utils

#endif  // VALKEY_SEARCH_UTILS_UTF8_ITERATOR_H_
