/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/utils/scanner.h"

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
      if (i != 0xC3) {
        EXPECT_NE(s.NextUtf8(), i);
      } else {
        EXPECT_EQ(s.NextUtf8(), i);
      }
      EXPECT_EQ(s.GetInvalidUtf8Count(), 1)
          << " For " << std::hex << size_t(i) << "\n";
      str.clear();
      Scanner::PushBackUtf8(str, i);
      str = str.substr(1);
      s = Scanner(str);
      if (i >= 0x80 && i <= 0xBF) {
        EXPECT_EQ(s.NextUtf8(), i);
      } else {
        EXPECT_NE(s.NextUtf8(), i);
      }
      EXPECT_EQ(s.GetInvalidUtf8Count(), 1);
    }
  }
}

}  // namespace utils
}  // namespace valkey_search
