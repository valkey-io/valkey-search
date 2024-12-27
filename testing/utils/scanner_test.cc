#include "src/utils/scanner.h"

#include "gtest/gtest.h"

namespace valkey_search {

namespace utils {

class ScannerTest : public testing::Test {};

TEST_F(ScannerTest, ByteTest) {
    std::string str;
    for (int i = 7; i < 0x100; i += 8) { // start with 7 which avoids whitespace chars.
        str.clear();
        str += char(i);
        Scanner s(str);
        EXPECT_EQ(i, s.peek_byte());
        EXPECT_EQ(i, s.next_byte());
        EXPECT_EQ(Scanner::kEOF, s.peek_byte());
        EXPECT_EQ(Scanner::kEOF, s.next_byte());

        str.clear();
        str += ' ';
        str += char(i);
        s = Scanner(str);
        EXPECT_EQ(i, s.skip_whitespace_peek_byte());
        EXPECT_EQ(i, s.skip_whitespace_next_byte());


        for (int j = 7; j < 0x100; j += 8) {
            str.clear();
            str += char(i);
            str += char(j);
            s = Scanner(str);
            EXPECT_EQ(i, s.peek_byte());
            EXPECT_EQ(i, s.next_byte());
            EXPECT_EQ(j, s.peek_byte());
            EXPECT_EQ(j, s.next_byte());
            EXPECT_EQ(Scanner::kEOF, s.peek_byte());
            EXPECT_EQ(Scanner::kEOF, s.next_byte());
        }
    }
}

TEST_F(ScannerTest, utf_test) {
    std::string str;
    Scanner::push_back_utf8(str, 0x20ac);
    EXPECT_EQ(str, "\xe2\x82\xac");

    for (Scanner::Char i = 0; i <= Scanner::MAX_CODEPOINT; ++i) {
        str.clear();
        Scanner::push_back_utf8(str, i);
        Scanner s(str);
        EXPECT_EQ(s.next_utf8(), i);
        EXPECT_EQ(s.next_utf8(), Scanner::kEOF);
        if (str.size() > 1) {
            str.pop_back();
            s = Scanner(str);
            if (i != 0xC3) {
                EXPECT_NE(s.next_utf8(), i);
            } else {
                EXPECT_EQ(s.next_utf8(), i);
            }
            EXPECT_EQ(s.get_invalid_utf_count(), 1) << " For " << std::hex << size_t(i) << "\n";
            str.clear();
            Scanner::push_back_utf8(str, i);
            str = str.substr(1);
            s = Scanner(str);
            if (i >= 0x80 && i <= 0xBF) {
                EXPECT_EQ(s.next_utf8(), i);
            } else {
                EXPECT_NE(s.next_utf8(), i);
            }
            EXPECT_EQ(s.get_invalid_utf_count(), 1);
        }
    }
}

}}

