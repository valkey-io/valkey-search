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
#include "src/utils/utf8_iterator.h"

namespace valkey_search {
namespace utils {

class Scanner {
 public:
  using Char = uint32_t;

 private:
  // Encoding constants used only by PushBackUtf8 (encode path).
  // The decode path is now handled entirely by Utf8Iterator.
  enum : uint32_t {
    kStart2Value = 0b11000000,
    kStart3Value = 0b11100000,
    kStart4Value = 0b11110000,
    kMoreValue = 0b10000000,
    kMoreMask = 0b11000000,
  };

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

  // Decode and advance one UTF-8 code point. Delegates to Utf8Iterator which
  // is the single source of truth for the decode algorithm.
  // Utf8Iterator::Next() returns {cp, 1} for both valid ASCII and invalid
  // bytes; only the latter has cp outside the ASCII range.
  Char NextUtf8() {
    if (pos_ >= sv_.size()) {
      return kEOF;
    }
    Utf8Iterator it(sv_.substr(pos_));
    it.Next();
    pos_ += it.byte_len();
    if (it.byte_len() == 1 && !Utf8Iterator::IsAscii(it.codepoint())) {
      invalid_utf_count_++;
    }
    return it.codepoint();
  }

  Char PeekUtf8() {
    size_t pos = pos_;
    Char result = NextUtf8();
    pos_ = pos;
    return result;
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
};

}  // namespace utils
}  // namespace valkey_search

#endif
