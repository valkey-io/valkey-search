/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/utils/scanner.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {

namespace utils {

class ScannerTest : public testing::Test {};

TEST_F(ScannerTest, ByteTest) {
  std::string str;
  for (int i = 7; i < 0x100;
       i += 8) {  // start with 7 which avoids whitespace chars.
    str.clear();
    str += char(i);
    Scanner s(str);
    EXPECT_EQ(i, s.PeekByte());
    EXPECT_EQ(i, s.NextByte());
    EXPECT_EQ(Scanner::kEOF, s.PeekByte());
    EXPECT_EQ(Scanner::kEOF, s.NextByte());

    str.clear();
    str += ' ';
    str += char(i);
    s = Scanner(str);
    EXPECT_EQ(i, s.SkipWhiteSpacePeekByte());
    EXPECT_EQ(i, s.SkipWhiteSpaceNextByte());

    for (int j = 7; j < 0x100; j += 8) {
      str.clear();
      str += char(i);
      str += char(j);
      s = Scanner(str);
      EXPECT_EQ(i, s.PeekByte());
      EXPECT_EQ(i, s.NextByte());
      EXPECT_EQ(j, s.PeekByte());
      EXPECT_EQ(j, s.NextByte());
      EXPECT_EQ(Scanner::kEOF, s.PeekByte());
      EXPECT_EQ(Scanner::kEOF, s.NextByte());
    }
  }
}

TEST_F(ScannerTest, utf_test) {
  std::string str;
  Scanner::PushBackUtf8(str, 0x20ac);
  EXPECT_EQ(str, "\xe2\x82\xac");

  for (Scanner::Char i = 0; i <= Scanner::kMaxCodepoint; ++i) {
    // Surrogates (U+D800..U+DFFF) are not valid Unicode scalar values. The
    // strict decoder rejects them, so they cannot round-trip through
    // PushBackUtf8 -> NextUtf8. Skip them here.
    if (i >= 0xD800 && i <= 0xDFFF) continue;
    str.clear();
    Scanner::PushBackUtf8(str, i);
    std::cout << "I: " << i << " ";
    for (char c : str) std::cout << std::hex << (c & 0xFF) << " ";
    std::cout << "\n";
    Scanner s(str);
    EXPECT_EQ(s.NextUtf8(), i);
    EXPECT_EQ(s.NextUtf8(), Scanner::kEOF);
    if (str.size() > 1) {
      str.pop_back();
      s = Scanner(str);
      // Strict decode: a truncated multi-byte sequence is always rejected
      // (returns kInvalidCp), so it never equals the original code point.
      EXPECT_NE(s.NextUtf8(), i);
      EXPECT_EQ(s.GetInvalidUtf8Count(), 1)
          << " For " << std::hex << size_t(i) << "\n";
      str.clear();
      Scanner::PushBackUtf8(str, i);
      str = str.substr(1);
      s = Scanner(str);
      // Strict decode: dropping the lead byte leaves a stray continuation
      // byte, which is rejected (returns kInvalidCp).
      EXPECT_NE(s.NextUtf8(), i);
      EXPECT_EQ(s.GetInvalidUtf8Count(), 1);
    }
  }
}

// ════════════════════════════════════════════════════════════════════════════
// UTF-8 decode coverage (ported from former testing/utils/utf8_iterator_test.cc
// with the Utf8Iterator → Scanner API rename, plus strict-reject cases).
//
// Mapping used:
//   Utf8Iterator it(s); while (it.Next()) { use it.codepoint(), it.byte_len();
//   } →  Scanner s(input);
//      while ((cp = s.NextUtf8()) != Scanner::kEOF) { use cp,
//      s.LastUtf8ByteLen(); }
//
//   it.pos()                 → s.GetPosition()
//   Utf8Iterator::IsAscii    → Scanner::IsAscii
//   Utf8Iterator::ExpectedLen→ Scanner::ExpectedLen
//   Utf8Iterator::CodePointCount  → Scanner::CodePointCount
//   Utf8Iterator::AtLeastNCodepoints → Scanner::AtLeastNCodepoints
// ════════════════════════════════════════════════════════════════════════════

namespace {

// Helper: decode entire string into a vector of code points via NextUtf8.
std::vector<uint32_t> Decode(absl::string_view s) {
  std::vector<uint32_t> result;
  Scanner sc(s);
  for (Scanner::Char cp = sc.NextUtf8(); cp != Scanner::kEOF;
       cp = sc.NextUtf8()) {
    result.push_back(cp);
  }
  return result;
}

}  // namespace

// ── Empty string ───────────────────────────────────────────────────────────

TEST(ScannerUtf8Test, EmptyStringYieldsEofOnFirstNext) {
  Scanner s("");
  EXPECT_EQ(Scanner::kEOF, s.NextUtf8());
}

TEST(ScannerUtf8Test, CodePointCountEmptyIsZero) {
  EXPECT_EQ(0u, Scanner::CodePointCount(""));
}

// ── ASCII ──────────────────────────────────────────────────────────────────

TEST(ScannerUtf8Test, AsciiNullByte) {
  // U+0000 is valid UTF-8 as a single 0x00 byte.
  std::string s(1, '\0');
  Scanner sc(s);
  EXPECT_EQ(0u, sc.NextUtf8());
  EXPECT_EQ(1u, sc.LastUtf8ByteLen());
  EXPECT_EQ(Scanner::kEOF, sc.NextUtf8());
}

// AsciiAllPrintable subsumed by ScannerByteRangeTest::DecodesAsSelfWhenAscii
// row "Ascii_20_7E" below. See § Byte-range coverage.

TEST(ScannerUtf8Test, AsciiMultipleChars) {
  auto v = Decode("hello");
  ASSERT_EQ(5u, v.size());
  EXPECT_EQ('h', v[0]);
  EXPECT_EQ('e', v[1]);
  EXPECT_EQ('l', v[2]);
  EXPECT_EQ('l', v[3]);
  EXPECT_EQ('o', v[4]);
}

TEST(ScannerUtf8Test, IsAsciiHelper) {
  EXPECT_TRUE(Scanner::IsAscii(0x00));
  EXPECT_TRUE(Scanner::IsAscii(0x7F));
  EXPECT_FALSE(Scanner::IsAscii(0x80));
  EXPECT_FALSE(Scanner::IsAscii(0xFF));
}

// ── Table-driven valid-sequence decode tests ───────────────────────────────

struct DecodeTestCase {
  std::string name;
  std::string input;
  std::vector<uint32_t> expected_codepoints;
  std::vector<uint8_t> expected_byte_lens;
};

class ScannerUtf8DecodeTest : public testing::TestWithParam<DecodeTestCase> {};

TEST_P(ScannerUtf8DecodeTest, DecodesCorrectly) {
  // Every case here is well-formed; the single strict decoder accepts each
  // sequence and reports the expected code point, byte length, and zero
  // invalid count.
  const auto& tc = GetParam();
  Scanner sc(tc.input);
  for (size_t i = 0; i < tc.expected_codepoints.size(); ++i) {
    Scanner::Char cp = sc.NextUtf8();
    EXPECT_NE(Scanner::kEOF, cp) << "stopped early at index " << i;
    EXPECT_NE(Scanner::kInvalidCp, cp)
        << "rejected valid sequence at index " << i;
    EXPECT_EQ(tc.expected_codepoints[i], cp) << "index " << i;
    EXPECT_EQ(tc.expected_byte_lens[i], sc.LastUtf8ByteLen()) << "index " << i;
  }
  EXPECT_EQ(Scanner::kEOF, sc.NextUtf8()) << "scanner not exhausted";
  EXPECT_EQ(0u, sc.GetInvalidUtf8Count());
}

INSTANTIATE_TEST_SUITE_P(
    ValidSequences, ScannerUtf8DecodeTest,
    testing::Values(
        // 2-byte sequences
        DecodeTestCase{"TwoByteEAcute", "\xC3\xA9", {0x00E9u}, {2}},
        DecodeTestCase{"TwoByteBoundaryU0080", "\xC2\x80", {0x0080u}, {2}},
        DecodeTestCase{"TwoByteBoundaryU07FF", "\xDF\xBF", {0x07FFu}, {2}},
        // 3-byte sequences
        DecodeTestCase{"ThreeByteEuroSign", "\xE2\x82\xAC", {0x20ACu}, {3}},
        DecodeTestCase{
            "ThreeByteBoundaryU0800", "\xE0\xA0\x80", {0x0800u}, {3}},
        DecodeTestCase{
            "ThreeByteBoundaryUFFFF", "\xEF\xBF\xBF", {0xFFFFu}, {3}},
        // 4-byte sequences
        DecodeTestCase{"FourByteU10000", "\xF0\x90\x80\x80", {0x10000u}, {4}},
        DecodeTestCase{
            "FourByteMaxU10FFFF", "\xF4\x8F\xBF\xBF", {0x10FFFFu}, {4}},
        // Mixed sequences
        DecodeTestCase{"MixedAsciiAndTwoByte",
                       "abc\xC3\xA9",
                       {'a', 'b', 'c', 0x00E9u},
                       {1, 1, 1, 2}},
        DecodeTestCase{"MixedAllWidths",
                       "a\xC3\xA9\xE2\x82\xAC\xF0\x90\x80\x80",
                       {'a', 0x00E9u, 0x20ACu, 0x10000u},
                       {1, 2, 3, 4}}),
    [](const testing::TestParamInfo<DecodeTestCase>& info) {
      return info.param.name;
    });

// ── GetPosition() tracking ─────────────────────────────────────────────────

TEST(ScannerUtf8Test, PositionAdvancesCorrectly) {
  std::string s = "a\xC3\xA9z";  // a (1) + é (2) + z (1) = 4 bytes
  Scanner sc(s);
  EXPECT_EQ(0u, sc.GetPosition());
  EXPECT_EQ('a', sc.NextUtf8());
  EXPECT_EQ(1u, sc.GetPosition());
  EXPECT_EQ(0x00E9u, sc.NextUtf8());
  EXPECT_EQ(3u, sc.GetPosition());
  EXPECT_EQ('z', sc.NextUtf8());
  EXPECT_EQ(4u, sc.GetPosition());
  EXPECT_EQ(Scanner::kEOF, sc.NextUtf8());
  EXPECT_EQ(4u, sc.GetPosition());
}

TEST(ScannerUtf8Test, SingleByteStringExhaustsAfterOneNext) {
  Scanner sc("x");
  EXPECT_EQ('x', sc.NextUtf8());
  EXPECT_EQ(Scanner::kEOF, sc.NextUtf8());
  EXPECT_EQ(Scanner::kEOF, sc.NextUtf8());  // idempotent once exhausted
}

// ── CodePointCount ─────────────────────────────────────────────────────────

struct CodePointCountCase {
  std::string name;
  std::string input;
  size_t expected_count;
};

class ScannerCodePointCountTest
    : public testing::TestWithParam<CodePointCountCase> {};

TEST_P(ScannerCodePointCountTest, CountsCodePoints) {
  const auto& tc = GetParam();
  EXPECT_EQ(tc.expected_count, Scanner::CodePointCount(tc.input)) << tc.name;
}

INSTANTIATE_TEST_SUITE_P(
    Counts, ScannerCodePointCountTest,
    testing::Values(CodePointCountCase{"Ascii", "hello", 5},
                    // "été" = U+00E9, t, U+00E9 = 3 code points, 5 bytes
                    CodePointCountCase{"Mixed", "\xC3\xA9t\xC3\xA9", 3},
                    // U+20AC € = 1 code point, 3 bytes
                    CodePointCountCase{"ThreeByte", "\xE2\x82\xAC", 1},
                    // U+10000 = 1 code point, 4 bytes
                    CodePointCountCase{"FourByte", "\xF0\x90\x80\x80", 1}),
    [](const testing::TestParamInfo<CodePointCountCase>& info) {
      return info.param.name;
    });

// ── AtLeastNCodepoints ─────────────────────────────────────────────────────

TEST(ScannerUtf8Test, AtLeastNCodepointsZeroAlwaysTrue) {
  EXPECT_TRUE(Scanner::AtLeastNCodepoints("", 0));
  EXPECT_TRUE(Scanner::AtLeastNCodepoints("hello", 0));
  EXPECT_TRUE(Scanner::AtLeastNCodepoints("\xC3\xA9", 0));
}

TEST(ScannerUtf8Test, AtLeastNCodepointsEmptyStringFalseForPositiveN) {
  EXPECT_FALSE(Scanner::AtLeastNCodepoints("", 1));
  EXPECT_FALSE(Scanner::AtLeastNCodepoints("", 100));
}

TEST(ScannerUtf8Test, AtLeastNCodepointsAsciiExactBoundary) {
  EXPECT_TRUE(Scanner::AtLeastNCodepoints("abc", 3));
  EXPECT_TRUE(Scanner::AtLeastNCodepoints("abc", 2));
  EXPECT_TRUE(Scanner::AtLeastNCodepoints("abc", 1));
  EXPECT_FALSE(Scanner::AtLeastNCodepoints("abc", 4));
  EXPECT_FALSE(Scanner::AtLeastNCodepoints("abc", 1000));
}

TEST(ScannerUtf8Test, AtLeastNCodepointsMultiByteExactBoundary) {
  // "été" = 3 code points, 5 bytes. Byte count says >=5; cp count says >=3.
  std::string s = "\xC3\xA9t\xC3\xA9";
  EXPECT_TRUE(Scanner::AtLeastNCodepoints(s, 3));
  EXPECT_TRUE(Scanner::AtLeastNCodepoints(s, 2));
  EXPECT_TRUE(Scanner::AtLeastNCodepoints(s, 1));
  EXPECT_FALSE(Scanner::AtLeastNCodepoints(s, 4));
  EXPECT_FALSE(Scanner::AtLeastNCodepoints(s, 5));
}

TEST(ScannerUtf8Test, AtLeastNCodepointsAgreesWithCodePointCount) {
  // Invariant for valid UTF-8:
  //   AtLeastNCodepoints(s, n) == (CodePointCount(s) >= n)
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
    size_t cp_count = Scanner::CodePointCount(s);
    for (size_t n = 0; n <= cp_count + 2; ++n) {
      EXPECT_EQ(cp_count >= n, Scanner::AtLeastNCodepoints(s, n))
          << "input.size=" << s.size() << " cp_count=" << cp_count
          << " n=" << n;
    }
  }
}

// ════════════════════════════════════════════════════════════════════════════
// § Byte-range coverage
// One table-driven suite spanning every byte value 0x00..0xFF, partitioned
// into the canonical UTF-8 lead/continuation/invalid classes. Each row asserts
// Scanner::ExpectedLen() for every byte in the range; rows flagged
// `decodes_as_self` additionally verify that single-byte ASCII inputs decode
// through NextUtf8() to the same code point with LastUtf8ByteLen()==1.
//
// Replaces the former TEST(AsciiAllPrintable) and TEST(ExpectedLen{Ascii,
// TwoByte, ThreeByte, FourByte, InvalidReturnsOne}) hand-rolled loops — same
// coverage, single source of truth for byte-class boundaries.
// ════════════════════════════════════════════════════════════════════════════

struct ByteRangeTestCase {
  std::string name;
  uint8_t lo;            // inclusive
  uint8_t hi;            // inclusive
  size_t expected_len;   // ExpectedLen() must return this for every byte
  bool decodes_as_self;  // single-byte b → NextUtf8() yields code point b
};

class ScannerByteRangeTest : public testing::TestWithParam<ByteRangeTestCase> {
};

TEST_P(ScannerByteRangeTest, ExpectedLenForEveryByteInRange) {
  const auto& tc = GetParam();
  for (int b = tc.lo; b <= tc.hi; ++b) {
    EXPECT_EQ(tc.expected_len, Scanner::ExpectedLen(static_cast<uint8_t>(b)))
        << tc.name << " byte=0x" << std::hex << b;
  }
}

TEST_P(ScannerByteRangeTest, DecodesAsSelfWhenAscii) {
  const auto& tc = GetParam();
  if (!tc.decodes_as_self) {
    GTEST_SKIP() << tc.name << " is not an ASCII self-decode range";
  }
  for (int b = tc.lo; b <= tc.hi; ++b) {
    std::string s(1, static_cast<char>(b));
    Scanner sc(s);
    EXPECT_EQ(static_cast<uint32_t>(b), sc.NextUtf8())
        << tc.name << " byte=0x" << std::hex << b;
    EXPECT_EQ(1u, sc.LastUtf8ByteLen())
        << tc.name << " byte=0x" << std::hex << b;
    EXPECT_EQ(Scanner::kEOF, sc.NextUtf8())
        << tc.name << " byte=0x" << std::hex << b;
  }
}

INSTANTIATE_TEST_SUITE_P(
    Ranges, ScannerByteRangeTest,
    testing::Values(
        // Printable-ASCII subrange that the former AsciiAllPrintable test
        // walked: every byte must round-trip through NextUtf8 unchanged.
        ByteRangeTestCase{"Ascii_20_7E", 0x20, 0x7E, 1, true},
        // Full ASCII range — ExpectedLen only (control bytes 0x00..0x1F
        // aren't asserted to decode-as-self because the original test omitted
        // them; AsciiNullByte stays as a dedicated TEST for U+0000).
        ByteRangeTestCase{"Ascii_00_7F", 0x00, 0x7F, 1, false},
        // Continuation bytes — not legal leads; ExpectedLen returns 1.
        ByteRangeTestCase{"ContinuationBytes_80_BF", 0x80, 0xBF, 1, false},
        // 2-byte lead range.
        ByteRangeTestCase{"TwoByteLead_C0_DF", 0xC0, 0xDF, 2, false},
        // 3-byte lead range.
        ByteRangeTestCase{"ThreeByteLead_E0_EF", 0xE0, 0xEF, 3, false},
        // 4-byte lead range that can encode within U+10FFFF.
        ByteRangeTestCase{"FourByteLead_F0_F4", 0xF0, 0xF4, 4, false},
        // 0xF5..0xF7 would encode > U+10FFFF; 0xF8..0xFF never legal leads.
        ByteRangeTestCase{"InvalidLeadBytes_F5_FF", 0xF5, 0xFF, 1, false}),
    [](const testing::TestParamInfo<ByteRangeTestCase>& info) {
      return info.param.name;
    });

// ════════════════════════════════════════════════════════════════════════════
// NextUtf8: invalid-sequence rejection (the decoder is the security boundary
// used by IsValidUtf8). Each malformed input must:
//   1. return Scanner::kInvalidCp (not kEOF, not the bit-pattern code point)
//   2. advance pos by exactly 1 byte (so the caller makes forward progress
//      and bytes that may be part of valid sequences aren't skipped)
//   3. set LastUtf8ByteLen() == 1
//   4. increment GetInvalidUtf8Count() by exactly 1
// ════════════════════════════════════════════════════════════════════════════

struct StrictRejectCase {
  std::string name;
  std::string input;            // bytes that strict must reject as a unit
  size_t expected_invalid_cnt;  // total invalid count after fully draining
};

class ScannerUtf8StrictRejectTest
    : public testing::TestWithParam<StrictRejectCase> {};

TEST_P(ScannerUtf8StrictRejectTest, FirstByteIsRejectedAndAdvancesOne) {
  const auto& tc = GetParam();
  Scanner sc(tc.input);
  Scanner::Char cp = sc.NextUtf8();
  EXPECT_EQ(Scanner::kInvalidCp, cp) << tc.name;
  EXPECT_EQ(1u, sc.LastUtf8ByteLen()) << tc.name;
  EXPECT_EQ(1u, sc.GetPosition()) << tc.name;
  EXPECT_EQ(1u, sc.GetInvalidUtf8Count()) << tc.name;
}

TEST_P(ScannerUtf8StrictRejectTest, FullDrainCountsAllInvalidBytes) {
  const auto& tc = GetParam();
  Scanner sc(tc.input);
  while (sc.NextUtf8() != Scanner::kEOF) {
    // drain
  }
  EXPECT_EQ(tc.expected_invalid_cnt, sc.GetInvalidUtf8Count()) << tc.name;
  EXPECT_EQ(tc.input.size(), sc.GetPosition()) << tc.name;
}

INSTANTIATE_TEST_SUITE_P(
    Rejects, ScannerUtf8StrictRejectTest,
    testing::Values(
        // Overlong 2-byte: 0xC0/0xC1 leads can only encode <0x80.
        // After rejecting lead, the trailing 0x80 is itself an invalid byte.
        StrictRejectCase{"OverlongU0000_C0_80", std::string("\xC0\x80", 2), 2},
        StrictRejectCase{"OverlongU007F_C1_BF", std::string("\xC1\xBF", 2), 2},
        // Overlong 3-byte: 0xE0 0x80..0x9F yields cp<0x800.
        StrictRejectCase{"Overlong3_E0_80_AF", std::string("\xE0\x80\xAF", 3),
                         3},
        StrictRejectCase{"Overlong3_E0_9F_BF", std::string("\xE0\x9F\xBF", 3),
                         3},
        // Overlong 4-byte: 0xF0 0x80..0x8F yields cp<0x10000.
        StrictRejectCase{"Overlong4_F0_80_80_80",
                         std::string("\xF0\x80\x80\x80", 4), 4},
        StrictRejectCase{"Overlong4_F0_8F_BF_BF",
                         std::string("\xF0\x8F\xBF\xBF", 4), 4},
        // Surrogates: U+D800..U+DFFF are forbidden in UTF-8.
        StrictRejectCase{"SurrogateD800_ED_A0_80",
                         std::string("\xED\xA0\x80", 3), 3},
        StrictRejectCase{"SurrogateDFFF_ED_BF_BF",
                         std::string("\xED\xBF\xBF", 3), 3},
        // Out-of-range: > U+10FFFF.
        StrictRejectCase{"OutOfRange_F4_90_80_80",
                         std::string("\xF4\x90\x80\x80", 4), 4},
        StrictRejectCase{"OutOfRange_F5_80_80_80",
                         std::string("\xF5\x80\x80\x80", 4), 4},
        // Truncated leads — no continuation bytes follow.
        StrictRejectCase{"Truncated2_C3", std::string("\xC3", 1), 1},
        StrictRejectCase{"Truncated3_E2_82", std::string("\xE2\x82", 2), 2},
        StrictRejectCase{"Truncated4_F0_90_80", std::string("\xF0\x90\x80", 3),
                         3},
        // Stray continuation byte at start.
        StrictRejectCase{"StrayCont_80", std::string("\x80", 1), 1},
        StrictRejectCase{"StrayCont_BF", std::string("\xBF", 1), 1}),
    [](const testing::TestParamInfo<StrictRejectCase>& info) {
      return info.param.name;
    });

// ════════════════════════════════════════════════════════════════════════════
// § Strict full-drain coverage
// Walks an arbitrary mixed-bytes input through NextUtf8() and asserts
// the (valid_cps, invalid_byte_count, final_position) tuple. Subsumes:
//   - MixedValidAndInvalidPreservesValidsAndCounts
//   - ValidU10FFFFAcceptedButOneAboveRejected (split into Ok/Bad rows)
//   - EmptyInputReturnsEofNotInvalid
//
// Property NOT covered here (intentionally separate): "1-byte advance on
// reject" — that's `ScannerUtf8StrictRejectTest::FirstByteIsRejectedAnd
// AdvancesOne`, which guards the lookahead-leak invariant on the *first*
// byte and would be lost if conflated with multi-byte-mixed cases.
// ════════════════════════════════════════════════════════════════════════════

struct StrictDrainCase {
  std::string name;
  std::string input;
  std::vector<uint32_t> expected_valid_cps;
  size_t expected_invalid_cnt;
};

class ScannerUtf8StrictDrainTest
    : public testing::TestWithParam<StrictDrainCase> {};

TEST_P(ScannerUtf8StrictDrainTest, ProducesExpectedValidAndInvalidPartition) {
  const auto& tc = GetParam();
  Scanner sc(tc.input);
  std::vector<uint32_t> valid_cps;
  size_t invalid_count = 0;
  for (Scanner::Char cp = sc.NextUtf8(); cp != Scanner::kEOF;
       cp = sc.NextUtf8()) {
    if (cp == Scanner::kInvalidCp) {
      ++invalid_count;
      // Each invalid is exactly one consumed byte (forward-progress invariant).
      EXPECT_EQ(1u, sc.LastUtf8ByteLen()) << tc.name;
    } else {
      valid_cps.push_back(cp);
    }
  }
  EXPECT_EQ(tc.expected_valid_cps, valid_cps) << tc.name;
  EXPECT_EQ(tc.expected_invalid_cnt, invalid_count) << tc.name;
  EXPECT_EQ(tc.expected_invalid_cnt, sc.GetInvalidUtf8Count()) << tc.name;
  EXPECT_EQ(tc.input.size(), sc.GetPosition()) << tc.name;
}

INSTANTIATE_TEST_SUITE_P(
    Drains, ScannerUtf8StrictDrainTest,
    testing::Values(
        // Empty input: zero valids, zero invalids, position stays at 0.
        // Also locks in that LastUtf8ByteLen() does not transiently observe
        // a non-zero value (the loop body never runs, so the implicit check
        // is "no EXPECT_EQ(1u, LastUtf8ByteLen()) fired").
        StrictDrainCase{"Empty", "", {}, 0},
        // Max valid code point — accepted, no invalid bytes.
        StrictDrainCase{"ValidMaxU10FFFF",
                        std::string("\xF4\x8F\xBF\xBF", 4),
                        {0x10FFFFu},
                        0},
        // One past max (U+110000): all 4 bytes consumed, all flagged invalid,
        // zero valid code points produced.
        StrictDrainCase{
            "InvalidU110000", std::string("\xF4\x90\x80\x80", 4), {}, 4},
        // Mixed: 'a' (valid 1B) + 0xC0 0x80 (overlong → 2 invalid bytes)
        //      + "é" (valid 2B) + 0xED 0xA0 0x80 (surrogate → 3 invalid)
        //      + 'z' (valid 1B).
        // Explicit length 9 prevents the literal NUL terminator from slipping
        // in as U+0000 (regression: original `MixedValid…` test had length 10
        // and silently emitted a phantom 4th valid code point).
        StrictDrainCase{
            "MixedValidAndInvalid",
            std::string("a\xC0\x80\xC3\xA9\xED\xA0\x80z", 9),
            {static_cast<uint32_t>('a'), 0x00E9u, static_cast<uint32_t>('z')},
            5},
        // Misplaced continuation bytes between ASCII: "ab" + 0xFF 0xFE + "cd".
        // 0xFF/0xFE are never valid lead bytes; each is rejected individually,
        // advancing exactly one byte so the following ASCII is preserved.
        StrictDrainCase{
            "MisplacedContinuationBytes",
            std::string("ab\xFF\xFE"
                        "cd",
                        6),
            {static_cast<uint32_t>('a'), static_cast<uint32_t>('b'),
             static_cast<uint32_t>('c'), static_cast<uint32_t>('d')},
            2}),
    [](const testing::TestParamInfo<StrictDrainCase>& info) {
      return info.param.name;
    });

}  // namespace utils
}  // namespace valkey_search
