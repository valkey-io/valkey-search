#include "src/expr/value.h"
#include "src/utils/scanner.h"

#include "gtest/gtest.h"

#include <cmath>

namespace valkey_search::expr {

class ValueTest : public testing::Test {
 protected:
  void SetUp() override {  }
  void TearDown() override { }
  Value pos_inf = Value(std::numeric_limits<double>::infinity());
  Value neg_inf = Value(-std::numeric_limits<double>::infinity());
  Value pos_zero = Value(0.0);
  Value neg_zero = Value(-0.0);
  Value min_neg = Value(-std::numeric_limits<double>::max());
  Value max_neg = Value(-std::numeric_limits<double>::min());
  Value min_pos = Value(std::numeric_limits<double>::min());
  Value max_pos = Value(std::numeric_limits<double>::max());
};

TEST_F(ValueTest, TypesTest) {
  struct Testcase {
    Value v;
    bool is_nil;
    bool is_bool;
    bool is_double;
    bool is_string;
  };

  std::vector<Testcase> t {
    { Value(), true, false, false, false },
    { Value(false), false, true, false, false },
    { Value(true), false, true, false, false },
    { Value(0.0), false, false, true, false },
    { Value(1.0), false, false, true, false },
    { Value(std::numeric_limits<double>::infinity()), false, false, true, false},
    { Value(-std::numeric_limits<double>::infinity()), false, false, true, false},
    { Value(std::nan("a nan")), true, false, false, false},
    { Value(std::string("")), false, false, false, true },
    { Value(std::string("a")), false, false, false, true },
    { Value(std::string("nan")), false, false, false, true }
  };

  for (auto& c : t) {
    EXPECT_EQ(c.v.IsNil(), c.is_nil) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsBool(), c.is_bool) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsDouble(), c.is_double) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsString(), c.is_string) << "Value is " << c.v;
  };
}

TEST_F(ValueTest, SimpleAdd) {
  Value l(1.0);
  Value r(1.0);
  Value res = FuncAdd(l, r);
  ASSERT_TRUE(res.IsDouble());
  EXPECT_EQ(res.AsDouble().value(), 2.0);
}

TEST_F(ValueTest, Compare_test) {
  struct Testcase {
    Value l;
    Value r;
    Ordering result;
  };

  std::vector<Testcase> t{
    { Value(), Value(), Ordering::kEQUAL },

    { Value(), Value(false), Ordering::kUNORDERED },
    { Value(), Value(true), Ordering::kUNORDERED },
    { Value(), Value(0.0), Ordering::kUNORDERED },
    { Value(), Value(std::string("")), Ordering::kUNORDERED },

    { Value(false), Value(false), Ordering::kEQUAL },
    { Value(false), Value(true), Ordering::kLESS },
    { Value(true), Value(false), Ordering::kGREATER },
    { Value(true), Value(true), Ordering::kEQUAL },

    { Value(-1.0), Value(0.0), Ordering::kLESS },
    { Value(0.0), Value(0.0), Ordering::kEQUAL },
    { Value(1.0), Value(0.0), Ordering::kGREATER },

    { Value(0.0), Value(std::string("0.0")), Ordering::kEQUAL},
    { Value(0.0), Value(std::string("1.0")), Ordering::kLESS},
    { Value(0.0), Value(std::string("-1.0")), Ordering::kGREATER},

    { Value(true), Value(std::string("0.0")), Ordering::kGREATER}, 
    { Value(std::string("a")), Value(std::string("b")), Ordering::kLESS},
    { Value(std::string("a")), Value(std::string("a")), Ordering::kEQUAL},
    { Value(std::string("a")), Value(std::string("aa")), Ordering::kLESS},
    { Value(std::string("0.0")), Value(std::string("0.00")), Ordering::kLESS }
  };

  for (auto& c : t) {
    EXPECT_EQ(c.result, Compare(c.l, c.r)) << "l = " << c.l << " r = " << c.r;
    switch (c.result) {
      case Ordering::kUNORDERED: EXPECT_EQ(Compare(c.r, c.l), Ordering::kUNORDERED); break;
      case Ordering::kEQUAL:     EXPECT_EQ(Compare(c.r, c.l), Ordering::kEQUAL); break;
      case Ordering::kGREATER:   EXPECT_EQ(Compare(c.r, c.l), Ordering::kLESS); break;
      case Ordering::kLESS:      EXPECT_EQ(Compare(c.r, c.l), Ordering::kGREATER); break;
      default: assert(false);
    }
  }
}

TEST_F(ValueTest, Compare_floating_point) {
  EXPECT_EQ(Compare(pos_zero, neg_zero), Ordering::kEQUAL);
  EXPECT_EQ(Compare(neg_zero, pos_zero), Ordering::kEQUAL);

  std::vector<Value> number_lines[] = {
    {neg_inf, min_neg, max_neg, neg_zero, min_pos, max_pos, pos_inf },
    {neg_inf, min_neg, max_neg, pos_zero, min_pos, max_pos, pos_inf },
  };

  for (auto& number_line : number_lines) {
    for (auto i = 0; i < number_line.size(); ++i) {
      EXPECT_EQ(Compare(number_line[i], number_line[i]), Ordering::kEQUAL);
      EXPECT_EQ(number_line[i], number_line[i]);
      EXPECT_TRUE(number_line[i] == number_line[i]);
      EXPECT_FALSE(number_line[i] != number_line[i]);
      EXPECT_FALSE(number_line[i] < number_line[i]);
      EXPECT_TRUE(number_line[i] <= number_line[i]);
      EXPECT_FALSE(number_line[i] > number_line[i]);
      EXPECT_TRUE(number_line[i] >= number_line[i]);
      for (auto j = i+1; j < number_line.size(); ++j) {
        EXPECT_EQ(Compare(number_line[i], number_line[j]), Ordering::kLESS);
        EXPECT_FALSE(number_line[i] == number_line[j]);
        EXPECT_TRUE (number_line[i] != number_line[j]);
        EXPECT_TRUE(number_line[i] < number_line[j]);
        EXPECT_TRUE(number_line[i] <= number_line[j]);
        EXPECT_FALSE(number_line[i] > number_line[j]);
        EXPECT_FALSE(number_line[i] >= number_line[j]);

        EXPECT_EQ(Compare(number_line[j], number_line[i]), Ordering::kGREATER);
        EXPECT_FALSE(number_line[j] == number_line[i]);
        EXPECT_TRUE (number_line[j] != number_line[i]);
        EXPECT_FALSE(number_line[j] < number_line[i]);
        EXPECT_FALSE(number_line[j] <= number_line[i]);
        EXPECT_TRUE(number_line[j] > number_line[i]);
        EXPECT_TRUE(number_line[j] >= number_line[i]);
      }
    }
  }
}

TEST_F(ValueTest, add) {
  struct TestCase {
    Value l;
    Value r;
    Value result;
  };

  TestCase test_cases[] = {
    { neg_inf, neg_inf, neg_inf },
    { neg_inf, min_neg, neg_inf },
    { neg_inf, max_neg, neg_inf },
    { neg_inf, neg_zero, neg_inf },
    { neg_inf, pos_zero, neg_inf },
    { neg_inf, min_pos, neg_inf },
    { neg_inf, max_pos, neg_inf },
    { neg_inf, pos_inf, Value() },

    { pos_inf, min_neg, pos_inf },
    { pos_inf, max_neg, pos_inf },
    { pos_inf, neg_zero, pos_inf },
    { pos_inf, pos_zero, pos_inf },
    { pos_inf, min_pos, pos_inf },
    { pos_inf, max_pos, pos_inf },

    { pos_zero, neg_zero, pos_zero },

    { Value(0.0), Value(), Value() },
    { Value(0.0), Value(1.0), Value(1.0) },
    { Value(0.0), Value(std::string("0.0")), Value(0.0) },
    { Value(0.0), Value(std::string("1.0")), Value(1.0) },
    { Value(0.0), Value(std::string("inf")), pos_inf },
    { Value(0.0), Value(std::string("-inf")), neg_inf },
    { Value(0.0), Value(std::string("abc")), Value() },
    { Value(0.0), Value(std::string("12abc")), Value() },
    { Value(0.0), Value(true), Value(1.0) },

  };

  for (auto& tc : test_cases) {
    EXPECT_EQ(FuncAdd(tc.l, tc.r), tc.result) << tc.l << '+' << tc.r;
    EXPECT_EQ(FuncAdd(tc.r, tc.l), tc.result) << tc.r << '+' << tc.l;
  }
}

TEST_F(ValueTest, math) {
  EXPECT_EQ(FuncSub(Value(1.0), Value(0.0)), Value(1.0));
  EXPECT_EQ(FuncMul(Value(1.0), Value(0.0)), Value(0.0));
  EXPECT_EQ(FuncDiv(Value(1.0), Value(2.0)), Value(0.5));

  EXPECT_EQ(FuncDiv(Value(1.0), pos_zero), pos_inf);
  EXPECT_EQ(FuncDiv(Value(1.0), neg_zero), neg_inf);

  EXPECT_EQ(FuncDiv(Value(0.0), Value(0.0)), Value());
}

/*
// Too long to include in typical runs, here just to prove that Unicode strings Compare > and < correctly.
// This has been run.
TEST_F(ValueTest, utf8_Compare) {
  std::string lstr;
  std::string rstr;
  for (utils::Scanner::Char l = 0; l <= utils::Scanner::kMaxCodepoint; l ++) {
    for (utils::Scanner::Char r = l+1; r <= utils::Scanner::kMaxCodepoint; r ++) {
      lstr.clear();
      rstr.clear();
      utils::Scanner::PushBackUtf8(lstr, l);
      utils::Scanner::PushBackUtf8(rstr, r);
      EXPECT_EQ(FuncLt(Value(lstr), Value(rstr)), Value(true));
    }
  }
}
*/

TEST_F(ValueTest, case_test) {
  std::tuple<std::string, std::string, std::string> testcases[] = {
    {"", "", ""},
    {"a", "a", "A"},
    {"aBc", "abc", "ABC"},
    {"\xe2\x82\xac", "\xe2\x82\xac", "\xe2\x82\xac"},
  };
  for (auto& [in, lower, upper] : testcases) {
    EXPECT_EQ(Value(lower), FuncLower(Value(in)));
    EXPECT_EQ(Value(upper), FuncUpper(Value(in)));
  }
}

TEST_F(ValueTest, timetest) {
  // 1739565015 corresponds to Fri Feb 14 2025 20:30:15 (GMT)
  Value ts(double(1739565015));
  EXPECT_EQ(FuncYear(ts), Value(2025));
  EXPECT_EQ(FuncDayofmonth(ts), Value(14));
  EXPECT_EQ(FuncDayofweek(ts), Value(5));
  EXPECT_EQ(FuncDayofyear(ts), Value(31+14-1));
  EXPECT_EQ(FuncMonthofyear(ts), Value(1));

  EXPECT_EQ(FuncTimefmt(ts,Value("%c")), Value("Fri Feb 14 20:30:15 2025"));

  EXPECT_EQ(FuncParsetime(Value("Fri Feb 14 20:30:15 2025"), Value("%c")), ts);

  EXPECT_EQ(FuncMinute(ts), Value(1739565000));
  EXPECT_EQ(FuncHour(ts), Value(1739563200));
  EXPECT_EQ(FuncDay(ts), Value(1739491200));
  EXPECT_EQ(FuncMonth(ts), Value(1738281600));
}
}  // namespace valkey_search::expr 
