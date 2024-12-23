#include "src/expr/value.h"

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
    EXPECT_EQ(c.v.is_nil(), c.is_nil) << "Value is " << c.v;
    EXPECT_EQ(c.v.is_bool(), c.is_bool) << "Value is " << c.v;
    EXPECT_EQ(c.v.is_double(), c.is_double) << "Value is " << c.v;
    EXPECT_EQ(c.v.is_string(), c.is_string) << "Value is " << c.v;
  };
}

TEST_F(ValueTest, SimpleAdd) {
  Value l(1.0);
  Value r(1.0);
  Value res = add(l, r);
  ASSERT_TRUE(res.is_double());
  EXPECT_EQ(res.as_double().value(), 2.0);
}

TEST_F(ValueTest, compare_test) {
  struct Testcase {
    Value l;
    Value r;
    Ordering result;
  };

  std::vector<Testcase> t{
    { Value(), Value(), Ordering::EQUAL },

    { Value(), Value(false), Ordering::UNORDERED },
    { Value(), Value(true), Ordering::UNORDERED },
    { Value(), Value(0.0), Ordering::UNORDERED },
    { Value(), Value(std::string("")), Ordering::UNORDERED },

    { Value(false), Value(false), Ordering::EQUAL },
    { Value(false), Value(true), Ordering::LESS },
    { Value(true), Value(false), Ordering::GREATER },
    { Value(true), Value(true), Ordering::EQUAL },

    { Value(-1.0), Value(0.0), Ordering::LESS },
    { Value(0.0), Value(0.0), Ordering::EQUAL },
    { Value(1.0), Value(0.0), Ordering::GREATER },

    { Value(0.0), Value(std::string("0.0")), Ordering::EQUAL},
    { Value(0.0), Value(std::string("1.0")), Ordering::LESS},
    { Value(0.0), Value(std::string("-1.0")), Ordering::GREATER},

    { Value(true), Value(std::string("0.0")), Ordering::GREATER}, 
    { Value(std::string("a")), Value(std::string("b")), Ordering::LESS},
    { Value(std::string("a")), Value(std::string("a")), Ordering::EQUAL},
    { Value(std::string("a")), Value(std::string("aa")), Ordering::LESS},
    { Value(std::string("0.0")), Value(std::string("0.00")), Ordering::LESS }
  };

  for (auto& c : t) {
    EXPECT_EQ(c.result, compare(c.l, c.r)) << "l = " << c.l << " r = " << c.r;
    switch (c.result) {
      case Ordering::UNORDERED: EXPECT_EQ(compare(c.r, c.l), Ordering::UNORDERED); break;
      case Ordering::EQUAL:     EXPECT_EQ(compare(c.r, c.l), Ordering::EQUAL); break;
      case Ordering::GREATER:   EXPECT_EQ(compare(c.r, c.l), Ordering::LESS); break;
      case Ordering::LESS:      EXPECT_EQ(compare(c.r, c.l), Ordering::GREATER); break;
      default: assert(false);
    }
  }
}

TEST_F(ValueTest, compare_floating_point) {
  EXPECT_EQ(compare(pos_zero, neg_zero), Ordering::EQUAL);
  EXPECT_EQ(compare(neg_zero, pos_zero), Ordering::EQUAL);

  std::vector<Value> number_lines[] = {
    {neg_inf, min_neg, max_neg, neg_zero, min_pos, max_pos, pos_inf },
    {neg_inf, min_neg, max_neg, pos_zero, min_pos, max_pos, pos_inf },
  };

  for (auto& number_line : number_lines) {
    for (auto i = 0; i < number_line.size(); ++i) {
      EXPECT_EQ(compare(number_line[i], number_line[i]), Ordering::EQUAL);
      EXPECT_EQ(number_line[i], number_line[i]);
      EXPECT_TRUE(number_line[i] == number_line[i]);
      EXPECT_FALSE(number_line[i] != number_line[i]);
      EXPECT_FALSE(number_line[i] < number_line[i]);
      EXPECT_TRUE(number_line[i] <= number_line[i]);
      EXPECT_FALSE(number_line[i] > number_line[i]);
      EXPECT_TRUE(number_line[i] >= number_line[i]);
      for (auto j = i+1; j < number_line.size(); ++j) {
        EXPECT_EQ(compare(number_line[i], number_line[j]), Ordering::LESS);
        EXPECT_FALSE(number_line[i] == number_line[j]);
        EXPECT_TRUE (number_line[i] != number_line[j]);
        EXPECT_TRUE(number_line[i] < number_line[j]);
        EXPECT_TRUE(number_line[i] <= number_line[j]);
        EXPECT_FALSE(number_line[i] > number_line[j]);
        EXPECT_FALSE(number_line[i] >= number_line[j]);

        EXPECT_EQ(compare(number_line[j], number_line[i]), Ordering::GREATER);
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
    EXPECT_EQ(add(tc.l, tc.r), tc.result) << tc.l << '+' << tc.r;
    EXPECT_EQ(add(tc.r, tc.l), tc.result) << tc.r << '+' << tc.l;
  }
}

TEST_F(ValueTest, math) {
  EXPECT_EQ(sub(Value(1.0), Value(0.0)), Value(1.0));
  EXPECT_EQ(mul(Value(1.0), Value(0.0)), Value(0.0));
  EXPECT_EQ(div(Value(1.0), Value(2.0)), Value(0.5));

  EXPECT_EQ(div(Value(1.0), pos_zero), pos_inf);
  EXPECT_EQ(div(Value(1.0), neg_zero), neg_inf);

  EXPECT_EQ(div(Value(0.0), Value(0.0)), Value());
}

}  // namespace valkey_search::expr 
