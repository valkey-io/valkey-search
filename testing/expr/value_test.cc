/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/expr/value.h"

#include <cmath>
#include <random>

#include "gtest/gtest.h"

namespace valkey_search::expr {

class ValueTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
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
    bool is_vector;
  };

  std::vector<Testcase> t{
      {Value(), true, false, false, false, false},
      {Value(false), false, true, false, false, false},
      {Value(true), false, true, false, false, false},
      {Value(0.0), false, false, true, false, false},
      {Value(1.0), false, false, true, false, false},
      {Value(std::numeric_limits<double>::infinity()), false, false, true,
       false, false},
      {Value(-std::numeric_limits<double>::infinity()), false, false, true,
       false, false},
      {Value(std::nan("a nan")), false, false, true, false, false},
      {Value(std::string("")), false, false, false, true, false},
      {Value(std::string("a")), false, false, false, true, false},
      {Value(std::nan("nan")), false, false, true, false, false},
      {Value({Value(1.0), Value(2.0)}), false, false, false, false, true},
      {Value({}), false, false, false, false, true}};

  for (auto& c : t) {
    EXPECT_EQ(c.v.IsNil(), c.is_nil) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsBool(), c.is_bool) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsDouble(), c.is_double) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsString(), c.is_string) << "Value is " << c.v;
    EXPECT_EQ(c.v.IsVector(), c.is_vector) << "Value is " << c.v;
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
      {Value(), Value(), Ordering::kEQUAL},

      {Value(), Value(false), Ordering::kUNORDERED},
      {Value(), Value(true), Ordering::kUNORDERED},
      {Value(), Value(0.0), Ordering::kUNORDERED},
      {Value(), Value(std::string("")), Ordering::kUNORDERED},

      {Value(false), Value(false), Ordering::kEQUAL},
      {Value(false), Value(true), Ordering::kLESS},
      {Value(true), Value(false), Ordering::kGREATER},
      {Value(true), Value(true), Ordering::kEQUAL},

      {Value(-1.0), Value(0.0), Ordering::kLESS},
      {Value(0.0), Value(0.0), Ordering::kEQUAL},
      {Value(1.0), Value(0.0), Ordering::kGREATER},

      {Value(0.0), Value(std::string("0.0")), Ordering::kEQUAL},
      {Value(0.0), Value(std::string("1.0")), Ordering::kLESS},
      {Value(0.0), Value(std::string("-1.0")), Ordering::kGREATER},

      {Value(true), Value(std::string("0.0")), Ordering::kGREATER},
      {Value(std::string("a")), Value(std::string("b")), Ordering::kLESS},
      {Value(std::string("a")), Value(std::string("a")), Ordering::kEQUAL},
      {Value(std::string("a")), Value(std::string("aa")), Ordering::kLESS},
      {Value(std::string("0.0")), Value(std::string("0.00")), Ordering::kLESS}};

  for (auto& c : t) {
    EXPECT_EQ(c.result, Compare(c.l, c.r)) << "l = " << c.l << " r = " << c.r;
    switch (c.result) {
      case Ordering::kUNORDERED:
        EXPECT_EQ(Compare(c.r, c.l), Ordering::kUNORDERED);
        break;
      case Ordering::kEQUAL:
        EXPECT_EQ(Compare(c.r, c.l), Ordering::kEQUAL);
        break;
      case Ordering::kGREATER:
        EXPECT_EQ(Compare(c.r, c.l), Ordering::kLESS);
        break;
      case Ordering::kLESS:
        EXPECT_EQ(Compare(c.r, c.l), Ordering::kGREATER);
        break;
      default:
        assert(false);
    }
  }
}

TEST_F(ValueTest, Compare_floating_point) {
  EXPECT_EQ(Compare(pos_zero, neg_zero), Ordering::kEQUAL);
  EXPECT_EQ(Compare(neg_zero, pos_zero), Ordering::kEQUAL);

  std::vector<Value> number_lines[] = {
      {neg_inf, min_neg, max_neg, neg_zero, min_pos, max_pos, pos_inf},
      {neg_inf, min_neg, max_neg, pos_zero, min_pos, max_pos, pos_inf},
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
      for (auto j = i + 1; j < number_line.size(); ++j) {
        EXPECT_EQ(Compare(number_line[i], number_line[j]), Ordering::kLESS);
        EXPECT_FALSE(number_line[i] == number_line[j]);
        EXPECT_TRUE(number_line[i] != number_line[j]);
        EXPECT_TRUE(number_line[i] < number_line[j]);
        EXPECT_TRUE(number_line[i] <= number_line[j]);
        EXPECT_FALSE(number_line[i] > number_line[j]);
        EXPECT_FALSE(number_line[i] >= number_line[j]);

        EXPECT_EQ(Compare(number_line[j], number_line[i]), Ordering::kGREATER);
        EXPECT_FALSE(number_line[j] == number_line[i]);
        EXPECT_TRUE(number_line[j] != number_line[i]);
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
      {neg_inf, neg_inf, neg_inf},
      {neg_inf, min_neg, neg_inf},
      {neg_inf, max_neg, neg_inf},
      {neg_inf, neg_zero, neg_inf},
      {neg_inf, pos_zero, neg_inf},
      {neg_inf, min_pos, neg_inf},
      {neg_inf, max_pos, neg_inf},
      {neg_inf, pos_inf, Value(-std::nan("a nan"))},

      {pos_inf, min_neg, pos_inf},
      {pos_inf, max_neg, pos_inf},
      {pos_inf, neg_zero, pos_inf},
      {pos_inf, pos_zero, pos_inf},
      {pos_inf, min_pos, pos_inf},
      {pos_inf, max_pos, pos_inf},

      {pos_zero, neg_zero, pos_zero},

      {Value(0.0), Value(), Value()},
      {Value(0.0), Value(1.0), Value(1.0)},
      {Value(0.0), Value(std::string("0.0")), Value(0.0)},
      {Value(0.0), Value(std::string("1.0")), Value(1.0)},
      {Value(0.0), Value(std::string("inf")), pos_inf},
      {Value(0.0), Value(std::string("-inf")), neg_inf},
      {Value(0.0), Value(std::string("abc")), Value()},
      {Value(0.0), Value(std::string("12abc")), Value()},
      {Value(0.0), Value(true), Value(1.0)},

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

  EXPECT_EQ(FuncDiv(Value(1.0), pos_zero), Value(std::nan("nan")));
  EXPECT_EQ(FuncDiv(Value(1.0), neg_zero), Value(std::nan("nan")));

  EXPECT_EQ(FuncDiv(Value(0.0), Value(0.0)), Value(std::nan("nan")));
}

/*
// Too long to include in typical runs, here just to prove that Unicode strings
Compare > and < correctly.
// This has been run.
TEST_F(ValueTest, utf8_Compare) {
  std::string lstr;
  std::string rstr;
  for (utils::Scanner::Char l = 0; l <= utils::Scanner::kMaxCodepoint; l ++) {
    for (utils::Scanner::Char r = l+1; r <= utils::Scanner::kMaxCodepoint; r ++)
{ lstr.clear(); rstr.clear(); utils::Scanner::PushBackUtf8(lstr, l);
      utils::Scanner::PushBackUtf8(rstr, r);
      EXPECT_EQ(FuncLt(Value(lstr), Value(rstr)), Value(true));
    }
  }
}
*/

// todo write unit tests for substr and other string handling

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
  EXPECT_EQ(FuncDayofyear(ts), Value(31 + 14 - 1));
  EXPECT_EQ(FuncMonthofyear(ts), Value(1));

  EXPECT_EQ(FuncTimefmt(ts, Value("%c")), Value("Fri Feb 14 20:30:15 2025"));

  EXPECT_EQ(FuncParsetime(Value("Fri Feb 14 20:30:15 2025"), Value("%c")), ts);

  EXPECT_EQ(FuncMinute(ts), Value(1739565000));
  EXPECT_EQ(FuncHour(ts), Value(1739563200));
  EXPECT_EQ(FuncDay(ts), Value(1739491200));
  EXPECT_EQ(FuncMonth(ts), Value(1738281600));
}

TEST_F(ValueTest, VectorConstruction) {
  // Test construction from shared_ptr
  auto vec_ptr = std::make_shared<std::vector<Value>>();
  vec_ptr->push_back(Value(1.0));
  vec_ptr->push_back(Value(2.0));
  vec_ptr->push_back(Value(3.0));
  Value v1(vec_ptr);
  EXPECT_FALSE(v1.IsNil());
  EXPECT_FALSE(v1.IsBool());
  EXPECT_FALSE(v1.IsDouble());
  EXPECT_FALSE(v1.IsString());

  // Test construction from initializer_list
  Value v2({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_FALSE(v2.IsNil());
  EXPECT_FALSE(v2.IsBool());
  EXPECT_FALSE(v2.IsDouble());
  EXPECT_FALSE(v2.IsString());

  // Test construction from vector with move semantics
  std::vector<Value> vec;
  vec.push_back(Value(1.0));
  vec.push_back(Value(2.0));
  vec.push_back(Value(3.0));
  Value v3(std::move(vec));
  EXPECT_FALSE(v3.IsNil());
  EXPECT_FALSE(v3.IsBool());
  EXPECT_FALSE(v3.IsDouble());
  EXPECT_FALSE(v3.IsString());

  // Test nested vectors
  Value nested(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  EXPECT_FALSE(nested.IsNil());
  EXPECT_FALSE(nested.IsBool());
  EXPECT_FALSE(nested.IsDouble());
  EXPECT_FALSE(nested.IsString());
}

TEST_F(ValueTest, VectorTypeChecking) {
  // Test IsVector() on vector values
  Value vec({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_TRUE(vec.IsVector());
  EXPECT_FALSE(vec.IsNil());
  EXPECT_FALSE(vec.IsBool());
  EXPECT_FALSE(vec.IsDouble());
  EXPECT_FALSE(vec.IsString());

  // Test IsVector() on scalar values
  EXPECT_FALSE(Value().IsVector());
  EXPECT_FALSE(Value(true).IsVector());
  EXPECT_FALSE(Value(42.0).IsVector());
  EXPECT_FALSE(Value(std::string("test")).IsVector());

  // Test VectorSize() on vector values
  Value empty_vec({});
  EXPECT_EQ(empty_vec.VectorSize(), 0);

  Value single_elem({Value(1.0)});
  EXPECT_EQ(single_elem.VectorSize(), 1);

  Value three_elem({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_EQ(three_elem.VectorSize(), 3);

  // Test VectorSize() on scalar values (should return 0)
  EXPECT_EQ(Value().VectorSize(), 0);
  EXPECT_EQ(Value(true).VectorSize(), 0);
  EXPECT_EQ(Value(42.0).VectorSize(), 0);
  EXPECT_EQ(Value(std::string("test")).VectorSize(), 0);

  // Test IsEmptyVector()
  EXPECT_TRUE(empty_vec.IsEmptyVector());
  EXPECT_FALSE(single_elem.IsEmptyVector());
  EXPECT_FALSE(three_elem.IsEmptyVector());

  // Test IsEmptyVector() on scalar values (should return false)
  EXPECT_FALSE(Value().IsEmptyVector());
  EXPECT_FALSE(Value(true).IsEmptyVector());
  EXPECT_FALSE(Value(42.0).IsEmptyVector());
  EXPECT_FALSE(Value(std::string("test")).IsEmptyVector());

  // Test nested vectors
  Value nested({Value({Value(1.0), Value(2.0)}),
                Value({Value(3.0), Value(4.0), Value(5.0)})});
  EXPECT_TRUE(nested.IsVector());
  EXPECT_EQ(nested.VectorSize(), 2);
  EXPECT_FALSE(nested.IsEmptyVector());
}

TEST_F(ValueTest, VectorAccessors) {
  // Test GetVector() on vector values
  Value vec({Value(1.0), Value(2.0), Value(3.0)});
  auto vec_ptr = vec.GetVector();
  ASSERT_NE(vec_ptr, nullptr);
  EXPECT_EQ(vec_ptr->size(), 3);
  EXPECT_EQ((*vec_ptr)[0].GetDouble(), 1.0);
  EXPECT_EQ((*vec_ptr)[1].GetDouble(), 2.0);
  EXPECT_EQ((*vec_ptr)[2].GetDouble(), 3.0);

  // Test GetVectorElement() with valid indices
  EXPECT_EQ(vec.GetVectorElement(0).GetDouble(), 1.0);
  EXPECT_EQ(vec.GetVectorElement(1).GetDouble(), 2.0);
  EXPECT_EQ(vec.GetVectorElement(2).GetDouble(), 3.0);

  // Test GetVectorElement() with out of bounds index (should throw)
  EXPECT_THROW(vec.GetVectorElement(3), std::out_of_range);
  EXPECT_THROW(vec.GetVectorElement(100), std::out_of_range);

  // Test AsVector() on vector values
  auto opt_vec = vec.AsVector();
  ASSERT_TRUE(opt_vec.has_value());
  EXPECT_EQ(opt_vec.value()->size(), 3);

  // Test AsVector() on scalar values (should return nullopt)
  EXPECT_FALSE(Value().AsVector().has_value());
  EXPECT_FALSE(Value(true).AsVector().has_value());
  EXPECT_FALSE(Value(42.0).AsVector().has_value());
  EXPECT_FALSE(Value(std::string("test")).AsVector().has_value());

  // Test GetVector() returns shared_ptr (efficient copying)
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value vec2 = vec1;  // Copy - intentionally testing shared_ptr semantics
  auto ptr1 = vec1.GetVector();
  auto ptr2 = vec2.GetVector();
  EXPECT_EQ(ptr1, ptr2);  // Should point to same underlying vector
  // Verify the copy is valid by accessing elements
  EXPECT_EQ(vec2.GetVectorElement(0).GetDouble(), 1.0);

  // Test nested vector access
  Value nested(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  auto outer = nested.GetVector();
  EXPECT_EQ(outer->size(), 2);

  auto inner1 = (*outer)[0].GetVector();
  EXPECT_EQ(inner1->size(), 2);
  EXPECT_EQ((*inner1)[0].GetDouble(), 1.0);
  EXPECT_EQ((*inner1)[1].GetDouble(), 2.0);

  auto inner2 = (*outer)[1].GetVector();
  EXPECT_EQ(inner2->size(), 2);
  EXPECT_EQ((*inner2)[0].GetDouble(), 3.0);
  EXPECT_EQ((*inner2)[1].GetDouble(), 4.0);
}

TEST_F(ValueTest, vector_arithmetic) {
  // Test vector-scalar addition
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value scalar(5.0);
  Value result1 = FuncAdd(vec1, scalar);
  ASSERT_TRUE(result1.IsVector());
  EXPECT_EQ(result1.VectorSize(), 3);
  EXPECT_EQ(result1.GetVectorElement(0).GetDouble(), 6.0);
  EXPECT_EQ(result1.GetVectorElement(1).GetDouble(), 7.0);
  EXPECT_EQ(result1.GetVectorElement(2).GetDouble(), 8.0);

  // Test scalar-vector addition
  Value result2 = FuncAdd(scalar, vec1);
  ASSERT_TRUE(result2.IsVector());
  EXPECT_EQ(result2.VectorSize(), 3);
  EXPECT_EQ(result2.GetVectorElement(0).GetDouble(), 6.0);
  EXPECT_EQ(result2.GetVectorElement(1).GetDouble(), 7.0);
  EXPECT_EQ(result2.GetVectorElement(2).GetDouble(), 8.0);

  // Test vector-vector addition
  Value vec2({Value(10.0), Value(20.0), Value(30.0)});
  Value result3 = FuncAdd(vec1, vec2);
  ASSERT_TRUE(result3.IsVector());
  EXPECT_EQ(result3.VectorSize(), 3);
  EXPECT_EQ(result3.GetVectorElement(0).GetDouble(), 11.0);
  EXPECT_EQ(result3.GetVectorElement(1).GetDouble(), 22.0);
  EXPECT_EQ(result3.GetVectorElement(2).GetDouble(), 33.0);

  // Test vector-scalar subtraction
  Value result4 = FuncSub(vec1, Value(1.0));
  ASSERT_TRUE(result4.IsVector());
  EXPECT_EQ(result4.VectorSize(), 3);
  EXPECT_EQ(result4.GetVectorElement(0).GetDouble(), 0.0);
  EXPECT_EQ(result4.GetVectorElement(1).GetDouble(), 1.0);
  EXPECT_EQ(result4.GetVectorElement(2).GetDouble(), 2.0);

  // Test vector-scalar multiplication
  Value result5 = FuncMul(vec1, Value(2.0));
  ASSERT_TRUE(result5.IsVector());
  EXPECT_EQ(result5.VectorSize(), 3);
  EXPECT_EQ(result5.GetVectorElement(0).GetDouble(), 2.0);
  EXPECT_EQ(result5.GetVectorElement(1).GetDouble(), 4.0);
  EXPECT_EQ(result5.GetVectorElement(2).GetDouble(), 6.0);

  // Test vector-scalar division
  Value result6 = FuncDiv(vec1, Value(2.0));
  ASSERT_TRUE(result6.IsVector());
  EXPECT_EQ(result6.VectorSize(), 3);
  EXPECT_EQ(result6.GetVectorElement(0).GetDouble(), 0.5);
  EXPECT_EQ(result6.GetVectorElement(1).GetDouble(), 1.0);
  EXPECT_EQ(result6.GetVectorElement(2).GetDouble(), 1.5);

  // Test vector-scalar power
  Value result7 = FuncPower(vec1, Value(2.0));
  ASSERT_TRUE(result7.IsVector());
  EXPECT_EQ(result7.VectorSize(), 3);
  EXPECT_EQ(result7.GetVectorElement(0).GetDouble(), 1.0);
  EXPECT_EQ(result7.GetVectorElement(1).GetDouble(), 4.0);
  EXPECT_EQ(result7.GetVectorElement(2).GetDouble(), 9.0);

  // Test length mismatch error
  Value vec3({Value(1.0), Value(2.0)});
  Value result8 = FuncAdd(vec1, vec3);
  ASSERT_TRUE(result8.IsNil());
  std::string error_msg = result8.GetNil().GetReason();
  EXPECT_NE(error_msg.find("Length mismatch"), std::string::npos);
}

TEST_F(ValueTest, VectorComparison_EqualVectors) {
  // Test equal vectors with same elements
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value vec2({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_EQ(Compare(vec1, vec2), Ordering::kEQUAL);
  EXPECT_TRUE(vec1 == vec2);
  EXPECT_FALSE(vec1 != vec2);
  EXPECT_FALSE(vec1 < vec2);
  EXPECT_TRUE(vec1 <= vec2);
  EXPECT_FALSE(vec1 > vec2);
  EXPECT_TRUE(vec1 >= vec2);

  // Test equal empty vectors
  Value empty1({});
  Value empty2({});
  EXPECT_EQ(Compare(empty1, empty2), Ordering::kEQUAL);
  EXPECT_TRUE(empty1 == empty2);

  // Test equal single-element vectors
  Value single1({Value(42.0)});
  Value single2({Value(42.0)});
  EXPECT_EQ(Compare(single1, single2), Ordering::kEQUAL);
  EXPECT_TRUE(single1 == single2);

  // Test equal vectors with string elements
  Value str_vec1({Value(std::string("a")), Value(std::string("b"))});
  Value str_vec2({Value(std::string("a")), Value(std::string("b"))});
  EXPECT_EQ(Compare(str_vec1, str_vec2), Ordering::kEQUAL);
  EXPECT_TRUE(str_vec1 == str_vec2);

  // Test equal vectors with mixed types
  Value mixed1({Value(1.0), Value(std::string("test")), Value(true)});
  Value mixed2({Value(1.0), Value(std::string("test")), Value(true)});
  EXPECT_EQ(Compare(mixed1, mixed2), Ordering::kEQUAL);
  EXPECT_TRUE(mixed1 == mixed2);

  // Test equal nested vectors
  Value nested1(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  Value nested2(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  EXPECT_EQ(Compare(nested1, nested2), Ordering::kEQUAL);
  EXPECT_TRUE(nested1 == nested2);
}

TEST_F(ValueTest, VectorComparison_DifferingElements) {
  // Test vectors differing in first element
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value vec2({Value(2.0), Value(2.0), Value(3.0)});
  EXPECT_EQ(Compare(vec1, vec2), Ordering::kLESS);
  EXPECT_EQ(Compare(vec2, vec1), Ordering::kGREATER);
  EXPECT_TRUE(vec1 < vec2);
  EXPECT_TRUE(vec2 > vec1);
  EXPECT_FALSE(vec1 == vec2);
  EXPECT_TRUE(vec1 != vec2);

  // Test vectors differing in middle element
  Value vec3({Value(1.0), Value(2.0), Value(3.0)});
  Value vec4({Value(1.0), Value(5.0), Value(3.0)});
  EXPECT_EQ(Compare(vec3, vec4), Ordering::kLESS);
  EXPECT_EQ(Compare(vec4, vec3), Ordering::kGREATER);
  EXPECT_TRUE(vec3 < vec4);
  EXPECT_TRUE(vec4 > vec3);

  // Test vectors differing in last element
  Value vec5({Value(1.0), Value(2.0), Value(3.0)});
  Value vec6({Value(1.0), Value(2.0), Value(10.0)});
  EXPECT_EQ(Compare(vec5, vec6), Ordering::kLESS);
  EXPECT_EQ(Compare(vec6, vec5), Ordering::kGREATER);
  EXPECT_TRUE(vec5 < vec6);
  EXPECT_TRUE(vec6 > vec5);

  // Test vectors with string elements differing
  Value str_vec1({Value(std::string("a")), Value(std::string("b"))});
  Value str_vec2({Value(std::string("a")), Value(std::string("c"))});
  EXPECT_EQ(Compare(str_vec1, str_vec2), Ordering::kLESS);
  EXPECT_EQ(Compare(str_vec2, str_vec1), Ordering::kGREATER);
  EXPECT_TRUE(str_vec1 < str_vec2);

  // Test vectors with negative numbers
  Value neg_vec1({Value(-5.0), Value(2.0)});
  Value neg_vec2({Value(-3.0), Value(2.0)});
  EXPECT_EQ(Compare(neg_vec1, neg_vec2), Ordering::kLESS);
  EXPECT_EQ(Compare(neg_vec2, neg_vec1), Ordering::kGREATER);

  // Test nested vectors differing in inner elements
  Value nested1(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  Value nested2(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(5.0)})});
  EXPECT_EQ(Compare(nested1, nested2), Ordering::kLESS);
  EXPECT_EQ(Compare(nested2, nested1), Ordering::kGREATER);
}

TEST_F(ValueTest, VectorComparison_DifferingLength) {
  // Test shorter vector vs longer vector (same prefix)
  Value short_vec({Value(1.0), Value(2.0)});
  Value long_vec({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_EQ(Compare(short_vec, long_vec), Ordering::kLESS);
  EXPECT_EQ(Compare(long_vec, short_vec), Ordering::kGREATER);
  EXPECT_TRUE(short_vec < long_vec);
  EXPECT_TRUE(long_vec > short_vec);
  EXPECT_FALSE(short_vec == long_vec);
  EXPECT_TRUE(short_vec != long_vec);

  // Test empty vector vs non-empty vector
  Value empty({});
  Value non_empty({Value(1.0)});
  EXPECT_EQ(Compare(empty, non_empty), Ordering::kLESS);
  EXPECT_EQ(Compare(non_empty, empty), Ordering::kGREATER);
  EXPECT_TRUE(empty < non_empty);
  EXPECT_TRUE(non_empty > empty);

  // Test vectors of different lengths with different first elements
  Value vec1({Value(5.0)});
  Value vec2({Value(1.0), Value(2.0), Value(3.0)});
  // First element differs (5.0 > 1.0), so length doesn't matter
  EXPECT_EQ(Compare(vec1, vec2), Ordering::kGREATER);
  EXPECT_EQ(Compare(vec2, vec1), Ordering::kLESS);

  // Test vectors where shorter has larger elements
  Value short_large({Value(10.0), Value(20.0)});
  Value long_small({Value(10.0), Value(20.0), Value(1.0)});
  // All common elements equal, so shorter < longer
  EXPECT_EQ(Compare(short_large, long_small), Ordering::kLESS);
  EXPECT_EQ(Compare(long_small, short_large), Ordering::kGREATER);

  // Test nested vectors with different lengths
  Value nested_short({Value({Value(1.0)})});
  Value nested_long({Value({Value(1.0)}), Value({Value(2.0)})});
  EXPECT_EQ(Compare(nested_short, nested_long), Ordering::kLESS);
  EXPECT_EQ(Compare(nested_long, nested_short), Ordering::kGREATER);
}

TEST_F(ValueTest, VectorComparison_VectorVsScalar) {
  // Test vector vs scalar comparisons (should be UNORDERED)
  Value vec({Value(1.0), Value(2.0), Value(3.0)});
  Value scalar_double(1.0);
  Value scalar_string(std::string("test"));
  Value scalar_bool(true);
  Value scalar_nil;

  // Vector vs double
  EXPECT_EQ(Compare(vec, scalar_double), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_double, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_double);   // UNORDERED treated as equal by ==
  EXPECT_FALSE(vec != scalar_double);  // UNORDERED not treated as != by !=

  // Vector vs string
  EXPECT_EQ(Compare(vec, scalar_string), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_string, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_string);

  // Vector vs bool
  EXPECT_EQ(Compare(vec, scalar_bool), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_bool, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_bool);

  // Vector vs nil
  EXPECT_EQ(Compare(vec, scalar_nil), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_nil, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_nil);

  // Empty vector vs scalar
  Value empty_vec({});
  EXPECT_EQ(Compare(empty_vec, scalar_double), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_double, empty_vec), Ordering::kUNORDERED);

  // Single-element vector vs scalar
  Value single_vec({Value(42.0)});
  EXPECT_EQ(Compare(single_vec, Value(42.0)), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(Value(42.0), single_vec), Ordering::kUNORDERED);

  // Nested vector vs scalar
  Value nested({Value({Value(1.0)})});
  EXPECT_EQ(Compare(nested, scalar_double), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_double, nested), Ordering::kUNORDERED);
}

}  // namespace valkey_search::expr
