/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEYSEARCH_UTILS_SCANNER_H
#define VALKEYSEARCH_UTILS_SCANNER_H

#include <absl/log/check.h>

#include <cctype>
#include <cstdint>
#include <optional>

#include "absl/strings/string_view.h"

namespace valkey_search {
namespace utils {

class Scanner {
 public:
  using Char = uint32_t;

  // Sentinel for malformed/overlong/surrogate/out-of-range sequences.
  // Distinct from kEOF, and outside the valid Unicode range (0..0x10FFFF) so
  // it can never collide with a real code point.
  static constexpr Char kInvalidCp = 0xFFFFFFFE;

 private:
  // UTF-8 lead/continuation bit patterns. The decoder derives length via
  // ExpectedLen and strips leader bits arithmetically, so only the encode-side
  // lead values and the continuation mask/value are needed here.
  enum : uint32_t {
    kStart2Value = 0b11000000,
    kStart3Value = 0b11100000,
    kStart4Value = 0b11110000,
    kMoreMask = 0b11000000,
    kMoreValue = 0b10000000,
  };

  Char GetByte(size_t pos) const { return sv_[pos] & 0xFF; }

  // Reject the sequence starting at `start`: advance exactly one byte, count
  // it, and return kInvalidCp. Advancing by one (rather than the malformed
  // length) ensures forward progress without skipping bytes that may begin a
  // valid sequence.
  Char RejectOne(size_t start) {
    invalid_utf_count_++;
    pos_ = start + 1;
    last_utf8_byte_len_ = 1;
    return kInvalidCp;
  }

 public:
  Scanner(absl::string_view sv) : sv_(sv) {}
  size_t GetPosition() const { return pos_; }

  static constexpr Char kEOF = (Char)-1;
  static constexpr Char kMaxCodepoint = 0x10FFFF;

  Char PeekByte() {
    if (pos_ >= sv_.size()) {
      return kEOF;
    } else {
      return static_cast<uint8_t>(sv_[pos_]);
    }
  }

  Char NextByte() {
    if (pos_ >= sv_.size()) {
      return kEOF;
    } else {
      return static_cast<uint8_t>(sv_[pos_++]);
    }
  }

  bool PopByte(Char c) {
    CHECK(c != kEOF);
    if (PeekByte() == c) {
      pos_++;
      return true;
    } else {
      return false;
    }
  }

  // Strict UTF-8 decode. Returns the code point, kInvalidCp for a malformed
  // sequence (advancing one byte, see RejectOne), or kEOF at end of input.
  // Length is derived from the leader byte; continuation bytes are then
  // accumulated and the result checked against these validity rules:
  //   2-byte cp must be >= 0x80     (rejects overlong)
  //   3-byte cp must be >= 0x800    (rejects overlong)
  //   3-byte cp must not be a surrogate (0xD800..0xDFFF)
  //   4-byte cp must be >= 0x10000  (rejects overlong)
  //   4-byte cp must be <= 0x10FFFF (rejects out-of-range)
  Char NextUtf8() {
    last_utf8_byte_len_ = 0;
    if (pos_ >= sv_.size()) {
      return kEOF;
    }

    size_t start = pos_;
    uint8_t b0 = static_cast<uint8_t>(GetByte(pos_));

    if (b0 < 0x80) {  // ASCII fast path.
      ++pos_;
      last_utf8_byte_len_ = 1;
      return b0;
    }

    // Derive length from the leader byte; ExpectedLen returns 1 for a
    // continuation byte or an out-of-range lead, both of which are invalid
    // here.
    uint8_t len = ExpectedLen(b0);
    if (len == 1) {
      return RejectOne(start);
    }
    if (pos_ + len > sv_.size()) {  // Truncated sequence.
      return RejectOne(start);
    }

    // Initial code-point bits: strip the leader's high bits (5/4/3 bits for a
    // 2/3/4-byte sequence), then fold in each continuation byte.
    Char cp = b0 & (0x7F >> len);
    for (uint8_t i = 1; i < len; ++i) {
      uint8_t b = static_cast<uint8_t>(GetByte(pos_ + i));
      if ((b & kMoreMask) != kMoreValue) {
        return RejectOne(start);
      }
      cp = (cp << 6) | (b & ~kMoreMask);
    }

    if ((len == 2 && cp < 0x80) ||
        (len == 3 && (cp < 0x800 || (cp >= 0xD800 && cp <= 0xDFFF))) ||
        (len == 4 && (cp < 0x10000 || cp > kMaxCodepoint))) {
      return RejectOne(start);
    }

    pos_ = start + len;
    last_utf8_byte_len_ = len;
    return cp;
  }

  // Bytes consumed by the most recent NextUtf8 call, including the kEOF and
  // kInvalidCp paths.
  uint8_t LastUtf8ByteLen() const { return last_utf8_byte_len_; }

  Char PeekUtf8() {
    size_t pos = pos_;
    uint8_t saved_len = last_utf8_byte_len_;
    Char result = NextUtf8();
    pos_ = pos;
    last_utf8_byte_len_ = saved_len;
    return result;
  }

  // ── Static UTF-8 utilities ────────────────────────────────────────────────

  // True iff cp fits in a single UTF-8 byte (U+0000..U+007F).
  static constexpr bool IsAscii(uint32_t cp) { return cp < 0x80; }

  // Bytes expected for a UTF-8 sequence given its lead byte. An invalid lead
  // (continuation byte, or > 0xF4) returns 1. Note: 0xF5..0xF7 would encode
  // > U+10FFFF and are therefore invalid leads.
  static uint8_t ExpectedLen(uint8_t b0) {
    if (b0 < 0x80) return 1;
    if ((b0 & 0xE0) == 0xC0) return 2;
    if ((b0 & 0xF0) == 0xE0) return 3;
    if ((b0 & 0xF8) == 0xF0 && b0 <= 0xF4) return 4;
    return 1;
  }

  // Counts code points in text. Malformed bytes count as one each (NextUtf8
  // returns kInvalidCp per rejected byte, not kEOF). Use AtLeastNCodepoints
  // when only a threshold matters — it short-circuits.
  static size_t CodePointCount(absl::string_view text) {
    Scanner s(text);
    size_t count = 0;
    while (s.NextUtf8() != kEOF) ++count;
    return count;
  }

  // True iff `text` is well-formed UTF-8 (no overlong, surrogate,
  // out-of-range, or truncated sequences). Used as the ingestion gate in
  // LanguageProcessor::Tokenize implementations.
  static bool IsValidUtf8(absl::string_view text) {
    Scanner s(text);
    Char cp;
    while ((cp = s.NextUtf8()) != kEOF) {
      if (cp == kInvalidCp) return false;
    }
    return true;
  }

  // Returns `text` with each malformed byte replaced by U+FFFD (the Unicode
  // replacement character). Well-formed input is returned unchanged. This
  // reproduces the legacy 1.2 "tolerate malformed input by substituting" policy
  // so the result is well-formed UTF-8 that matches nothing downstream, without
  // relying on an ICU side effect.
  static std::string ReplaceInvalidUtf8(absl::string_view text) {
    if (IsValidUtf8(text)) return std::string(text);
    std::string out;
    out.reserve(text.size());
    Scanner s(text);
    Char cp;
    while ((cp = s.NextUtf8()) != kEOF) {
      PushBackUtf8(out, cp == kInvalidCp ? 0xFFFD : cp);
    }
    return out;
  }

  // True iff text contains at least n code points. Short-circuits at n.
  // Uses the UTF-8 invariant: every code point contributes exactly one
  // non-continuation byte (ASCII or lead). Continuation bytes match
  // (b & 0xC0) == 0x80 and are skipped. For ASCII-only input this is a
  // simple byte count.
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

  void SkipWhiteSpace() {
    while (std::isspace(PeekByte())) {
      (void)NextByte();
    }
  }

  //
  // These routines transparently skip whitespace and automatically handle utf-8
  //
  int SkipWhiteSpacePeekByte() {
    size_t pos = pos_;
    SkipWhiteSpace();
    auto result = PeekByte();
    pos_ = pos;
    return result;
  }

  int SkipWhiteSpaceNextByte() {
    SkipWhiteSpace();
    return NextByte();
  }

  bool SkipWhiteSpacePopByte(Char c) {
    size_t pos = pos_;
    SkipWhiteSpace();
    if (PopByte(c)) {
      return true;
    } else {
      pos_ = pos;
      return false;
    }
  }

  bool SkipWhiteSpacePopWord(absl::string_view word) {
    size_t pos = pos_;
    SkipWhiteSpace();
    for (auto ch : word) {
      if (NextByte() != ch) {
        pos_ = pos;
        return false;
      }
    }
    return true;
  }

  std::optional<double> PopDouble() {
    if (pos_ >= sv_.size()) {
      return std::nullopt;
    }
    double d = 0.0;
#if 0
    // CLANG doesn't support from_chars for floating point types.
    auto [ptr, ec] = std::from_chars(&sv_[pos_], sv_.data() + sv_.size(), d);
    if (ec == std::errc::invalid_argument) {
      return std::nullopt;
    }
    CHECK(ec == std::errc());
    pos_ = ptr - sv_.data();
    CHECK(pos_ <= sv_.size());
#else
    absl::string_view s(sv_);
    s.remove_prefix(pos_);
    std::string null_terminated(s);
    char* scanned{nullptr};
    d = std::strtod(null_terminated.data(), &scanned);
    if (scanned == null_terminated.data()) {
      return std::nullopt;
    }
    pos_ += scanned - null_terminated.data();
    CHECK(pos_ <= sv_.size());
#endif
    return d;
  }

  absl::string_view GetUnscanned() const {
    auto copy = sv_;
    copy.remove_prefix(pos_);
    return copy;
  }

  absl::string_view GetScanned() const {
    auto copy = sv_;
    copy.remove_prefix(sv_.size() - pos_);
    return copy;
  }

  static std::string& PushBackUtf8(std::string& s, Scanner::Char codepoint) {
    if (codepoint <= 0x7F) {
      s += char(codepoint);
    } else if (codepoint <= 0x7FF) {
      s += char(kStart2Value | (codepoint >> 6));
      s += char(kMoreValue | (codepoint & ~kMoreMask));
    } else if (codepoint <= 0xFFFF) {
      s += char(kStart3Value | (codepoint >> 12));
      s += char(kMoreValue | ((codepoint >> 6) & ~kMoreMask));
      s += char(kMoreValue | (codepoint & ~kMoreMask));
    } else if (codepoint <= 0x10FFFF) {
      s += char(kStart4Value | (codepoint >> 18));
      s += char(kMoreValue | ((codepoint >> 12) & ~kMoreMask));
      s += char(kMoreValue | ((codepoint >> 6) & ~kMoreMask));
      s += char(kMoreValue | (codepoint & ~kMoreMask));
    } else {
      // std::cerr << "Found invalid codepoint " << codepoint << "(" << std::hex
      // << size_t(codepoint) << ")\n";
      CHECK(false);
    }
    return s;
  }

  size_t GetInvalidUtf8Count() const { return invalid_utf_count_; }

 private:
  absl::string_view sv_;
  size_t pos_{0};
  size_t invalid_utf_count_{0};
  // Bytes consumed by most recent NextUtf8. Exposed via LastUtf8ByteLen() to
  // callers that copy the raw bytes of the decoded code point.
  uint8_t last_utf8_byte_len_{0};
};

}  // namespace utils
}  // namespace valkey_search

#endif
