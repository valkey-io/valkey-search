/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEY_SEARCH_UTILS_UTF8_ITERATOR_H_
#define VALKEY_SEARCH_UTILS_UTF8_ITERATOR_H_

#include <cstdint>

#include "absl/log/check.h"
#include "absl/strings/string_view.h"

namespace valkey_search::utils {

// Forward-only UTF-8 iterator. Decodes one Unicode code point per Next() call.
//
// Pre-condition: the input must be valid UTF-8. All callers in this codebase
// validate text via Lexer::IsValidUtf8() before constructing this iterator.
// Calling Next() on invalid UTF-8 is a programming error and fires a DCHECK
// in debug builds.
//
// Typical usage:
//   Utf8Iterator it(text);
//   while (it.Next()) {
//     use(it.codepoint(), it.byte_len());
//   }
class Utf8Iterator {
 public:
  explicit Utf8Iterator(absl::string_view text)
      : text_(text), pos_(0), codepoint_(0), byte_len_(0) {}

  // Advance to the next code point. Returns true if a code point was
  // successfully decoded, false when the input is exhausted. Must be called
  // before accessing codepoint() or byte_len().
  //
  // UTF-8 byte patterns:
  //   0xxxxxxx              (0x00..0x7F)  → 1 byte,  U+0000..U+007F  (ASCII)
  //   110xxxxx 10xxxxxx     (0xC2..0xDF)  → 2 bytes, U+0080..U+07FF
  //   1110xxxx 10xxxxxx×2   (0xE0..0xEF)  → 3 bytes, U+0800..U+FFFF
  //   11110xxx 10xxxxxx×3   (0xF0..0xF4)  → 4 bytes, U+10000..U+10FFFF
  bool Next() {
    if (pos_ >= text_.size()) return false;

    uint8_t b0 = static_cast<uint8_t>(text_[pos_]);

    if (b0 < 0x80) {
      codepoint_ = b0;
      byte_len_ = 1;
      ++pos_;
      return true;
    }

    // 2-byte: 110xxxxx 10xxxxxx
    if ((b0 & 0xE0) == 0xC0 && pos_ + 1 < text_.size()) {
      uint8_t b1 = static_cast<uint8_t>(text_[pos_ + 1]);
      if ((b1 & 0xC0) == 0x80) {
        uint32_t cp = ((b0 & 0x1F) << 6) | (b1 & 0x3F);
        if (cp >= 0x80) {
          codepoint_ = cp;
          byte_len_ = 2;
          pos_ += 2;
          return true;
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
          codepoint_ = cp;
          byte_len_ = 3;
          pos_ += 3;
          return true;
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
          codepoint_ = cp;
          byte_len_ = 4;
          pos_ += 4;
          return true;
        }
      }
    }

    // Invalid byte. Pre-condition violation: callers must validate UTF-8 first.
    DCHECK(false)
        << "Utf8Iterator::Next() encountered invalid UTF-8 at byte " << pos_
        << " (0x" << std::hex << static_cast<unsigned>(b0)
        << "). Callers must validate input with Lexer::IsValidUtf8().";
    // Advance 1 byte in release builds to avoid infinite loops.
    codepoint_ = b0;
    byte_len_ = 1;
    ++pos_;
    return true;
  }

  // Current Unicode code point. Valid only after Next() returns true.
  uint32_t codepoint() const { return codepoint_; }

  // Bytes consumed for the current code point (1 for ASCII, 2-4 for
  // multi-byte). Valid only after Next() returns true.
  uint8_t byte_len() const { return byte_len_; }

  // Current byte offset into the underlying string_view.
  size_t pos() const { return pos_; }

  // Count code points in a string. E.g. CodePointCount("été") == 3.
  static size_t CodePointCount(absl::string_view text) {
    Utf8Iterator it(text);
    size_t count = 0;
    while (it.Next()) {
      ++count;
    }
    return count;
  }

  // Returns true iff text contains at least n code points. Exits as soon as
  // n is reached, so much cheaper than `CodePointCount(text) >= n` when the
  // answer is "yes" — only the first n code points are examined.
  //
  // Uses the UTF-8 invariant: every code point contributes exactly one
  // non-continuation byte (an ASCII byte or a lead byte). Continuation bytes
  // match (b & 0xC0) == 0x80 and are skipped. For ASCII-only input this is a
  // simple byte count.
  //
  // Pre-condition (same as Next()): the input must be valid UTF-8.
  static bool AtLeastNCodepoints(absl::string_view text, size_t n) {
    if (n == 0) return true;
    size_t count = 0;
    for (size_t i = 0; i < text.size(); ++i) {
      uint8_t b = static_cast<uint8_t>(text[i]);
      if ((b & 0xC0) != 0x80) {
        if (++count >= n) return true;
      }
    }
    return false;
  }

  // Bytes expected for a UTF-8 sequence given its lead byte. An invalid lead
  // returns 1 (consistent with the advance-1 fallback behavior).
  // Note: only 0xF0..0xF4 are valid 4-byte leads (U+10000..U+10FFFF);
  // 0xF5..0xF7 would encode code points > U+10FFFF and are invalid.
  static uint8_t ExpectedLen(uint8_t b0) {
    if (b0 < 0x80) return 1;
    if ((b0 & 0xE0) == 0xC0) return 2;
    if ((b0 & 0xF0) == 0xE0) return 3;
    if ((b0 & 0xF8) == 0xF0 && b0 <= 0xF4) return 4;
    return 1;
  }

  // True if the code point fits in a single UTF-8 byte (U+0000..U+007F).
  static constexpr bool IsAscii(uint32_t cp) { return cp < 0x80; }

 private:
  absl::string_view text_;
  size_t pos_;
  uint32_t codepoint_;
  uint8_t byte_len_;
};

}  // namespace valkey_search::utils

#endif  // VALKEY_SEARCH_UTILS_UTF8_ITERATOR_H_
