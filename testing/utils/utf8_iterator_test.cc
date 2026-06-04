/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/utils/utf8_iterator.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {
namespace utils {

// Helper: decode entire string into vector of codepoints using Next().
static std::vector<uint32_t> Decode(std::string_view s) {
  std::vector<uint32_t> result;
  Utf8Iterator it(s);
  while (it.Next()) {
    result.push_back(it.codepoint());
  }
  return result;
}

// Helper: decode with byte lengths too.
struct Cp {
  uint32_t codepoint;
  uint8_t byte_len;
  bool operator==(const Cp& o) const {
    return codepoint == o.codepoint && byte_len == o.byte_len;
  }
};
static std::vector<Cp> DecodeWithLen(std::string_view s) {
  std::vector<Cp> result;
  Utf8Iterator it(s);
  while (it.Next()) {
    result.push_back({it.codepoint(), it.byte_len()});
  }
  return result;
}

class Utf8IteratorTest : public testing::Test {};

// ── Empty string
// ──────────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, EmptyStringReturnsNothingOnNext) {
  Utf8Iterator it("");
  EXPECT_FALSE(it.Next());
}

TEST_F(Utf8IteratorTest, CodePointCountEmptyIsZero) {
  EXPECT_EQ(0u, Utf8Iterator::CodePointCount(""));
}

// ── Pure ASCII
// ────────────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, AsciiNullByte) {
  // U+0000 is valid UTF-8 encoded as a single 0x00 byte.
  std::string s(1, '\0');
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

TEST_F(Utf8IteratorTest, AsciiAllPrintable) {
  // Verify all ASCII bytes 0x20..0x7E decode as single-byte codepoints.
  for (int i = 0x20; i <= 0x7E; ++i) {
    std::string s(1, static_cast<char>(i));
    Utf8Iterator it(s);
    ASSERT_TRUE(it.Next()) << "char 0x" << std::hex << i;
    EXPECT_EQ(static_cast<uint32_t>(i), it.codepoint()) << "char 0x" << i;
    EXPECT_EQ(1u, it.byte_len()) << "char 0x" << i;
    EXPECT_FALSE(it.Next());
  }
}

TEST_F(Utf8IteratorTest, AsciiMultipleChars) {
  auto v = Decode("hello");
  ASSERT_EQ(5u, v.size());
  EXPECT_EQ('h', v[0]);
  EXPECT_EQ('e', v[1]);
  EXPECT_EQ('l', v[2]);
  EXPECT_EQ('l', v[3]);
  EXPECT_EQ('o', v[4]);
}

TEST_F(Utf8IteratorTest, AsciiIsAsciiHelper) {
  EXPECT_TRUE(Utf8Iterator::IsAscii(0x00));
  EXPECT_TRUE(Utf8Iterator::IsAscii(0x7F));
  EXPECT_FALSE(Utf8Iterator::IsAscii(0x80));
  EXPECT_FALSE(Utf8Iterator::IsAscii(0xFF));
}

// ── 2-byte sequences
// ──────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, TwoByteEAcute) {
  // U+00E9 é = 0xC3 0xA9
  std::string s = "\xC3\xA9";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x00E9u, v[0].codepoint);
  EXPECT_EQ(2u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, TwoByteBoundaryU0080) {
  // U+0080 = 0xC2 0x80 — first non-ASCII 2-byte value
  std::string s = "\xC2\x80";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x0080u, v[0].codepoint);
  EXPECT_EQ(2u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, TwoByteBoundaryU07FF) {
  // U+07FF = 0xDF 0xBF — last 2-byte value
  std::string s = "\xDF\xBF";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x07FFu, v[0].codepoint);
  EXPECT_EQ(2u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, TwoByteOverlongU0000RejectedAsInvalid) {
  // Overlong: U+0000 encoded as 0xC0 0x80 (should be 0x00 for valid UTF-8).
  // The 2-byte branch requires cp >= 0x80, so this falls through to invalid.
  std::string s = "\xC0\x80";
  Utf8Iterator it(s);
  // First byte 0xC0 hits invalid path → advance 1, return raw byte
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xC0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  // Second byte 0x80 also invalid on its own
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0x80u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

TEST_F(Utf8IteratorTest, TwoByteOverlongU007FRejectedAsInvalid) {
  // 0xC1 0xBF → decodes to U+007F overlong. cp < 0x80 check rejects it.
  std::string s = "\xC1\xBF";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  // Falls through to invalid, returns raw 0xC1 with byte_len=1
  EXPECT_EQ(0xC1u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, TwoByteTruncatedAtEndOfString) {
  // Lead byte with no continuation — must not read past end.
  std::string s = "\xC3";  // Needs one more byte
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  // pos_+1 >= size, falls through to invalid, returns raw 0xC3
  EXPECT_EQ(0xC3u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

// ── 3-byte sequences
// ──────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, ThreeByteEuroSign) {
  // U+20AC € = 0xE2 0x82 0xAC
  std::string s = "\xE2\x82\xAC";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x20ACu, v[0].codepoint);
  EXPECT_EQ(3u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, ThreeByteBoundaryU0800) {
  // U+0800 = 0xE0 0xA0 0x80 — first 3-byte value
  std::string s = "\xE0\xA0\x80";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x0800u, v[0].codepoint);
  EXPECT_EQ(3u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, ThreeByteBoundaryUFFFF) {
  // U+FFFF = 0xEF 0xBF 0xBF — last 3-byte value before surrogates area
  std::string s = "\xEF\xBF\xBF";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0xFFFFu, v[0].codepoint);
  EXPECT_EQ(3u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, ThreeByteOverlongU007FRejectedAsInvalid) {
  // 0xE0 0x80 0xAF would decode to U+002F overlong. cp < 0x800 check rejects.
  std::string s = "\xE0\x80\xAF";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  // Falls to invalid, raw 0xE0, byte_len=1
  EXPECT_EQ(0xE0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, ThreeByteSurrogateD800Rejected) {
  // U+D800 = 0xED 0xA0 0x80 — surrogate, must be rejected
  std::string s = "\xED\xA0\x80";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xEDu, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, ThreeByteSurrogateDFFFRejected) {
  // U+DFFF = 0xED 0xBF 0xBF — last surrogate
  std::string s = "\xED\xBF\xBF";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xEDu, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, ThreeByteTruncatedTwoBytes) {
  // Lead + one continuation, missing third byte — must not read past end.
  std::string s = "\xE2\x82";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xE2u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, ThreeByteTruncatedOneByteAtEnd) {
  std::string s = "\xE2";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xE2u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

// ── 4-byte sequences
// ──────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, FourByteLinearBScriptU10000) {
  // U+10000 = 0xF0 0x90 0x80 0x80
  std::string s = "\xF0\x90\x80\x80";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x10000u, v[0].codepoint);
  EXPECT_EQ(4u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, FourByteMaxU10FFFF) {
  // U+10FFFF = 0xF4 0x8F 0xBF 0xBF
  std::string s = "\xF4\x8F\xBF\xBF";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(1u, v.size());
  EXPECT_EQ(0x10FFFFu, v[0].codepoint);
  EXPECT_EQ(4u, v[0].byte_len);
}

TEST_F(Utf8IteratorTest, FourByteExceedsU10FFFFRejected) {
  // 0xF4 0x90 0x80 0x80 would decode to U+110000 > 0x10FFFF — reject.
  std::string s = "\xF4\x90\x80\x80";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xF4u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, FourByteOverlongU0000Rejected) {
  // 0xF0 0x80 0x80 0x80 — overlong U+0000
  std::string s = "\xF0\x80\x80\x80";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xF0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, FourByteTruncatedThreeBytes) {
  std::string s = "\xF0\x90\x80";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xF0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, FourByteTruncatedTwoBytes) {
  std::string s = "\xF0\x90";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xF0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
}

TEST_F(Utf8IteratorTest, FourByteTruncatedOneByte) {
  std::string s = "\xF0";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xF0u, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

// ── Lone continuation bytes
// ───────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, LoneContinuationBytesAllRange) {
  // All continuation bytes 0x80..0xBF should each advance exactly 1 byte.
  for (int i = 0x80; i <= 0xBF; ++i) {
    std::string s(1, static_cast<char>(i));
    Utf8Iterator it(s);
    ASSERT_TRUE(it.Next()) << "byte 0x" << std::hex << i;
    EXPECT_EQ(static_cast<uint32_t>(i), it.codepoint()) << "byte 0x" << i;
    EXPECT_EQ(1u, it.byte_len()) << "byte 0x" << i;
    EXPECT_FALSE(it.Next());
  }
}

TEST_F(Utf8IteratorTest, ByteFF_IsInvalid) {
  // 0xFF is never valid UTF-8
  std::string s = "\xFF";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xFFu, it.codepoint());
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

// ── Mixed valid and invalid
// ───────────────────────────────────────────────────

// Helper macro: compare a DecodeWithLen entry without triggering the
// "macro passed 3 arguments" error from brace-init with commas.
#define EXPECT_CP(vec, idx, cp_val, len_val)        \
  do {                                              \
    EXPECT_EQ((cp_val), (vec)[(idx)].codepoint)     \
        << "codepoint mismatch at index " << (idx); \
    EXPECT_EQ((len_val), (vec)[(idx)].byte_len)     \
        << "byte_len mismatch at index " << (idx);  \
  } while (0)

TEST_F(Utf8IteratorTest, MixedAsciiAndTwoByte) {
  // "abc" + U+00E9 é — three ASCII bytes followed by a 2-byte code point
  std::string s = "abc\xC3\xA9";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(4u, v.size());
  EXPECT_CP(v, 0, 'a', 1);
  EXPECT_CP(v, 1, 'b', 1);
  EXPECT_CP(v, 2, 'c', 1);
  EXPECT_CP(v, 3, 0x00E9u, 2);
}

TEST_F(Utf8IteratorTest, InvalidByteAmidstValidDoesNotConsumeFollowingBytes) {
  // "a" + 0xFF + "b" — invalid byte in the middle, 'b' is still reachable.
  std::string s;
  s += 'a';
  s += static_cast<char>(0xFF);
  s += 'b';
  auto v = DecodeWithLen(s);
  ASSERT_EQ(3u, v.size());
  EXPECT_CP(v, 0, 'a', 1);
  EXPECT_CP(v, 1, 0xFFu, 1);  // raw invalid byte, advance 1
  EXPECT_CP(v, 2, 'b', 1);
}

TEST_F(Utf8IteratorTest, ContinuationByteThenValidSequence) {
  // 0x80 (lone continuation) then valid 2-byte é
  std::string s;
  s += static_cast<char>(0x80);
  s += "\xC3\xA9";
  auto v = DecodeWithLen(s);
  ASSERT_EQ(2u, v.size());
  EXPECT_CP(v, 0, 0x80u, 1);  // lone continuation, raw
  EXPECT_CP(v, 1, 0xE9u, 2);  // valid é
}

TEST_F(Utf8IteratorTest, BadContinuationInMiddleOf3ByteSequence) {
  // 0xE2 0x41 0xAC — valid 3-byte lead, then ASCII 'A' where continuation
  // expected. The (b1 & 0xC0) == 0x80 check fails → returns 0xE2 as raw
  // byte, then decodes 'A' and 0xAC separately.
  std::string s = "\xE2\x41\xAC";
  Utf8Iterator it(s);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xE2u, it.codepoint());  // lead byte rejected, advance 1
  EXPECT_EQ(1u, it.byte_len());
  ASSERT_TRUE(it.Next());
  EXPECT_EQ('A', it.codepoint());  // 0x41 decoded as ASCII
  EXPECT_EQ(1u, it.byte_len());
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(0xACu, it.codepoint());  // lone continuation, raw
  EXPECT_EQ(1u, it.byte_len());
  EXPECT_FALSE(it.Next());
}

TEST_F(Utf8IteratorTest, InvalidLeadBytesF8_to_FE_AdvanceOneEach) {
  // 0xF8..0xFE: not valid lead bytes for any UTF-8 sequence.
  // Each must advance exactly 1 byte and return the raw byte value.
  for (int i = 0xF8; i <= 0xFE; ++i) {
    std::string s(1, static_cast<char>(i));
    Utf8Iterator it(s);
    ASSERT_TRUE(it.Next()) << "byte 0x" << std::hex << i;
    EXPECT_EQ(static_cast<uint32_t>(i), it.codepoint()) << "byte 0x" << i;
    EXPECT_EQ(1u, it.byte_len()) << "byte 0x" << i;
    EXPECT_FALSE(it.Next());
  }
}

// ── CodePointCount
// ────────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, CodePointCountAscii) {
  EXPECT_EQ(5u, Utf8Iterator::CodePointCount("hello"));
}

TEST_F(Utf8IteratorTest, CodePointCountMixed) {
  // "été" = e U+00E9 t e = 3 code points, 5 bytes
  std::string s = "\xC3\xA9t\xC3\xA9";
  EXPECT_EQ(3u, Utf8Iterator::CodePointCount(s));
}

TEST_F(Utf8IteratorTest, CodePointCountThreeByte) {
  // U+20AC € = 1 code point, 3 bytes
  EXPECT_EQ(1u, Utf8Iterator::CodePointCount("\xE2\x82\xAC"));
}

TEST_F(Utf8IteratorTest, CodePointCountFourByte) {
  // U+10000 = 1 code point, 4 bytes
  EXPECT_EQ(1u, Utf8Iterator::CodePointCount("\xF0\x90\x80\x80"));
}

TEST_F(Utf8IteratorTest, CodePointCountInvalidBytesEachCountAsOne) {
  // Three invalid bytes each advance 1 = 3 code points
  EXPECT_EQ(3u, Utf8Iterator::CodePointCount("\x80\xFF\xFE"));
}

// ── AtLeastNCodepoints
// ────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, AtLeastNCodepointsZeroAlwaysTrue) {
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints("", 0));
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints("hello", 0));
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints("\xC3\xA9", 0));
}

TEST_F(Utf8IteratorTest, AtLeastNCodepointsEmptyStringFalseForPositiveN) {
  EXPECT_FALSE(Utf8Iterator::AtLeastNCodepoints("", 1));
  EXPECT_FALSE(Utf8Iterator::AtLeastNCodepoints("", 100));
}

TEST_F(Utf8IteratorTest, AtLeastNCodepointsAsciiExactBoundary) {
  // "abc" has exactly 3 code points
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints("abc", 3));
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints("abc", 2));
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints("abc", 1));
  EXPECT_FALSE(Utf8Iterator::AtLeastNCodepoints("abc", 4));
  EXPECT_FALSE(Utf8Iterator::AtLeastNCodepoints("abc", 1000));
}

TEST_F(Utf8IteratorTest, AtLeastNCodepointsMultiByteExactBoundary) {
  // "été" = 3 code points, 5 bytes. Byte count would say >=5, codepoint
  // count says >=3 only.
  std::string s = "\xC3\xA9t\xC3\xA9";
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints(s, 3));
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints(s, 2));
  EXPECT_TRUE(Utf8Iterator::AtLeastNCodepoints(s, 1));
  EXPECT_FALSE(Utf8Iterator::AtLeastNCodepoints(s, 4));
  EXPECT_FALSE(Utf8Iterator::AtLeastNCodepoints(s, 5));
}

TEST_F(Utf8IteratorTest, AtLeastNCodepointsAgreesWithCodePointCount) {
  // For valid UTF-8 (the precondition for both functions),
  // AtLeastNCodepoints(s, n) must equal (CodePointCount(s) >= n) for all n.
  const std::string cases[] = {
      "",
      "a",
      "hello world",
      "\xC3\xA9",                 // é, 2 bytes / 1 cp
      "\xC3\xA9t\xC3\xA9",        // été, 5 bytes / 3 cps
      "\xE2\x82\xAC",             // €, 3 bytes / 1 cp
      "\xF0\x90\x80\x80",         // U+10000, 4 bytes / 1 cp
      "abc\xC3\xA9\xE2\x82\xAC",  // mixed 1/2/3-byte
  };
  for (const auto& s : cases) {
    size_t cp_count = Utf8Iterator::CodePointCount(s);
    for (size_t n = 0; n <= cp_count + 2; ++n) {
      EXPECT_EQ(cp_count >= n, Utf8Iterator::AtLeastNCodepoints(s, n))
          << "input.size=" << s.size() << " cp_count=" << cp_count
          << " n=" << n;
    }
  }
}

// ── ExpectedLen
// ───────────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, ExpectedLenAscii) {
  for (int i = 0; i <= 0x7F; ++i) {
    EXPECT_EQ(1u, Utf8Iterator::ExpectedLen(static_cast<uint8_t>(i)));
  }
}

TEST_F(Utf8IteratorTest, ExpectedLenTwoByte) {
  // 0xC0..0xDF
  for (int i = 0xC0; i <= 0xDF; ++i) {
    EXPECT_EQ(2u, Utf8Iterator::ExpectedLen(static_cast<uint8_t>(i)));
  }
}

TEST_F(Utf8IteratorTest, ExpectedLenThreeByte) {
  // 0xE0..0xEF
  for (int i = 0xE0; i <= 0xEF; ++i) {
    EXPECT_EQ(3u, Utf8Iterator::ExpectedLen(static_cast<uint8_t>(i)));
  }
}

TEST_F(Utf8IteratorTest, ExpectedLenFourByte) {
  // Only 0xF0..0xF4 are valid 4-byte leads (U+10000..U+10FFFF).
  // 0xF5..0xF7 encode code points > U+10FFFF and are invalid.
  for (int i = 0xF0; i <= 0xF4; ++i) {
    EXPECT_EQ(4u, Utf8Iterator::ExpectedLen(static_cast<uint8_t>(i)));
  }
}

TEST_F(Utf8IteratorTest, ExpectedLenContinuationAndInvalidReturnOne) {
  // 0x80..0xBF (continuation bytes) → 1
  for (int i = 0x80; i <= 0xBF; ++i) {
    EXPECT_EQ(1u, Utf8Iterator::ExpectedLen(static_cast<uint8_t>(i)));
  }
  // 0xF5..0xFF: 0xF5..0xF7 encode > U+10FFFF (invalid), 0xF8..0xFF are
  // always invalid UTF-8 lead bytes. All return 1.
  for (int i = 0xF5; i <= 0xFF; ++i) {
    EXPECT_EQ(1u, Utf8Iterator::ExpectedLen(static_cast<uint8_t>(i)));
  }
}

// ── pos() tracking
// ────────────────────────────────────────────────────────────

TEST_F(Utf8IteratorTest, PosAdvancesCorrectly) {
  std::string s = "a\xC3\xA9z";  // a (1) + é (2) + z (1) = 4 bytes
  Utf8Iterator it(s);
  EXPECT_EQ(0u, it.pos());
  ASSERT_TRUE(it.Next());  // 'a'
  EXPECT_EQ(1u, it.pos());
  ASSERT_TRUE(it.Next());  // 'é'
  EXPECT_EQ(3u, it.pos());
  ASSERT_TRUE(it.Next());  // 'z'
  EXPECT_EQ(4u, it.pos());
  EXPECT_FALSE(it.Next());
  EXPECT_EQ(4u, it.pos());
}

// ── Boundary: single-byte string exhaustion ────────────────────────────────

TEST_F(Utf8IteratorTest, SingleByteStringExhaustsAfterOneNext) {
  Utf8Iterator it("x");
  ASSERT_TRUE(it.Next());
  EXPECT_EQ('x', it.codepoint());
  EXPECT_FALSE(it.Next());
  EXPECT_FALSE(it.Next());  // idempotent once exhausted
}

}  // namespace utils
}  // namespace valkey_search
