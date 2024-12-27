#ifndef _VALKEYSEARCH_UTILS_SCANNER_H
#define _VALKEYSEARCH_UtILS_SCANNER_H

#include "absl/strings/string_view.h"

#include <cctype>
#include <charconv>
#include <cuchar>
#include <iostream>
#include <optional>

namespace valkey_search { namespace utils {

class Scanner {
  enum {
    START1_MASK  = 0b10000000,
    START1_VALUE = 0b00000000,
    START2_MASK  = 0b11100000,
    START2_VALUE = 0b11000000,
    START3_MASK  = 0b11110000,
    START3_VALUE = 0b11100000,
    START4_MASK  = 0b11111000,
    START4_VALUE = 0b11110000,
    MORE_MASK    = 0b11000000,
    MORE_VALUE   = 0b10000000,
  };

  bool is_start(size_t mask, size_t value) const {
    return (sv_[pos_] & mask) == value;
  }

  char32_t get_start(size_t mask) {
    return sv_[pos_++] & ~mask;
  }

  bool is_more(size_t pos) const {
    return pos < sv_.size() && ((sv_[pos] & MORE_MASK) == MORE_VALUE);
  }

  char32_t get_more(char32_t result) {
    return (result << 6) | (sv_[pos_++] & ~MORE_MASK);
  }

 public:
  Scanner(absl::string_view sv) : sv_(sv) {}
  size_t get_position() const { return pos_; }

  typedef char32_t Char;

  static constexpr Char kEOF = (Char)-1;
  static constexpr Char MAX_CODEPOINT = 0x10FFFF;

  Char peek_byte() {
    if (pos_ >= sv_.size()) {
      return kEOF;
    } else {
      return sv_[pos_] & 0xFF;
    }
  }

  Char next_byte() {
    if (pos_ >= sv_.size()) {
      return kEOF;
    } else {
      return sv_[pos_++] & 0xFF;
    }
  }

  bool pop_byte(Char c) {
    assert(c != kEOF);
    if (peek_byte() == c) {
      pos_++;
      return true;
    } else {
      return false;
    }
  }

  Char next_utf8() {
    if (pos_ >= sv_.size()) {
      return kEOF;
    }
    if (is_start(START1_MASK, START1_VALUE)) {
      return get_start(START1_MASK);
    } else if (is_start(START2_MASK, START2_VALUE) && is_more(pos_+1)) {
        return get_more(get_start(START2_MASK));
    } else if (is_start(START3_MASK, START3_VALUE) && is_more(pos_+1) && is_more(pos_+2)) {
        return get_more(get_more(get_start(START3_MASK)));
    } else if (is_start(START4_MASK, START4_VALUE) && is_more(pos_+1) && is_more(pos_+2) && is_more(pos_+3)) {
      return get_more(get_more(get_more(get_start(START4_MASK))));
    }
    invalid_utf_count++;
    return sv_[pos_++] & 0xFF;
  }

  Char peek_utf8() {
    size_t pos = pos_;
    Char result = next_utf8();
    pos_ = pos;
    return result;
  }

  void skip_whitespace() {
    while (std::isspace(peek_byte())) { (void)next_byte(); }
  }

  //
  // These routines transparently skip whitespace and automatically handle utf-8
  //
  int skip_whitespace_peek_byte() {
    size_t pos = pos_;
    skip_whitespace();
    auto result = peek_byte();
    pos_ = pos;
    return result;
  }

  int skip_whitespace_next_byte() {
    skip_whitespace();
    return next_byte();
  }

  bool skip_whitespace_pop_byte(Char c) {
    size_t pos = pos_;
    skip_whitespace();
    if (pop_byte(c)) {
      return true;
    } else {
      pos_ = pos;
      return false;
    }
  }

  bool skip_whitespace_pop_word(absl::string_view word) {
    size_t pos = pos_;
    skip_whitespace();
    for (auto ch : word) {
      if (next_byte() != ch) {
        pos_ = pos;
        return false;
      }
    }
    return true;
  }

  std::optional<double> pop_double() {
    if (pos_ >= sv_.size()) {
      return std::nullopt;
    }
    double d = 0.0;
    auto [ptr, ec] = std::from_chars(&sv_[pos_], sv_.data() + sv_.size(), d);
    if (ec == std::errc::invalid_argument) {
      return std::nullopt;
    }
    assert(ec == std::errc());
    pos_ = ptr - sv_.data();
    assert(pos_ <= sv_.size());
    return d;
  }

  absl::string_view get_unscanned() const {
    auto copy = sv_;
    copy.remove_prefix(pos_);
    return copy;
  }

  absl::string_view get_scanned() const {
    auto copy = sv_;
    copy.remove_prefix(sv_.size() - pos_);
    return copy;
  }

  static std::string& push_back_utf8(std::string& s, Scanner::Char codepoint) {
    if (codepoint <= 0x7F) {
      s += char(codepoint);
    } else if (codepoint <= 0x7FF) {
      s += char(START2_VALUE | (codepoint >> 6));
      s += char(MORE_VALUE | (codepoint & ~MORE_MASK));
    } else if (codepoint <= 0xFFFF) {
      s += char(START3_VALUE | (codepoint >> 12));
      s += char(MORE_VALUE | ((codepoint>>6) & ~MORE_MASK));
      s += char(MORE_VALUE | (codepoint & ~MORE_MASK));
    } else if (codepoint <= 0x10FFFF) {
      s += char(START4_VALUE | (codepoint >> 18));
      s += char(MORE_VALUE | ((codepoint>>12) & ~MORE_MASK));
      s += char(MORE_VALUE | ((codepoint>>6) & ~MORE_MASK));
      s += char(MORE_VALUE | (codepoint & ~MORE_MASK));
    } else {
      // DBG << "Found invalid codepoint " << std::hex << size_t(codepoint) << "\n";
      assert(false);
    }
    return s;
  }

  size_t get_invalid_utf_count() const { 
    return invalid_utf_count;
  }

 private:
  absl::string_view sv_;
  size_t pos_{0};
  size_t invalid_utf_count{0};

};

}}

#endif