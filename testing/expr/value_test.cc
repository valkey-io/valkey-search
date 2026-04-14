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
    EXPECT_EQ(c.v.IsArray(), c.is_vector) << "Value is " << c.v;
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

TEST_F(ValueTest, ArrayConstruction) {
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

TEST_F(ValueTest, ArrayTypeChecking) {
  // Test IsArray() on vector values
  Value vec({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_TRUE(vec.IsArray());
  EXPECT_FALSE(vec.IsNil());
  EXPECT_FALSE(vec.IsBool());
  EXPECT_FALSE(vec.IsDouble());
  EXPECT_FALSE(vec.IsString());

  // Test IsArray() on scalar values
  EXPECT_FALSE(Value().IsArray());
  EXPECT_FALSE(Value(true).IsArray());
  EXPECT_FALSE(Value(42.0).IsArray());
  EXPECT_FALSE(Value(std::string("test")).IsArray());

  // Test ArraySize() on vector values
  Value empty_vec({});
  EXPECT_EQ(empty_vec.ArraySize(), 0);

  Value single_elem({Value(1.0)});
  EXPECT_EQ(single_elem.ArraySize(), 1);

  Value three_elem({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_EQ(three_elem.ArraySize(), 3);

  // Test ArraySize() on scalar values (should return 0)
  EXPECT_EQ(Value().ArraySize(), 0);
  EXPECT_EQ(Value(true).ArraySize(), 0);
  EXPECT_EQ(Value(42.0).ArraySize(), 0);
  EXPECT_EQ(Value(std::string("test")).ArraySize(), 0);

  // Test IsEmptyArray()
  EXPECT_TRUE(empty_vec.IsEmptyArray());
  EXPECT_FALSE(single_elem.IsEmptyArray());
  EXPECT_FALSE(three_elem.IsEmptyArray());

  // Test IsEmptyArray() on scalar values (should return false)
  EXPECT_FALSE(Value().IsEmptyArray());
  EXPECT_FALSE(Value(true).IsEmptyArray());
  EXPECT_FALSE(Value(42.0).IsEmptyArray());
  EXPECT_FALSE(Value(std::string("test")).IsEmptyArray());

  // Test nested vectors
  Value nested({Value({Value(1.0), Value(2.0)}),
                Value({Value(3.0), Value(4.0), Value(5.0)})});
  EXPECT_TRUE(nested.IsArray());
  EXPECT_EQ(nested.ArraySize(), 2);
  EXPECT_FALSE(nested.IsEmptyArray());
}

TEST_F(ValueTest, ArrayAccessors) {
  // Test GetArray() on vector values
  Value vec({Value(1.0), Value(2.0), Value(3.0)});
  auto vec_ptr = vec.GetArray();
  ASSERT_NE(vec_ptr, nullptr);
  EXPECT_EQ(vec_ptr->size(), 3);
  EXPECT_EQ((*vec_ptr)[0].GetDouble(), 1.0);
  EXPECT_EQ((*vec_ptr)[1].GetDouble(), 2.0);
  EXPECT_EQ((*vec_ptr)[2].GetDouble(), 3.0);

  // Test GetArrayElement() with valid indices
  EXPECT_EQ(vec.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(vec.GetArrayElement(1).GetDouble(), 2.0);
  EXPECT_EQ(vec.GetArrayElement(2).GetDouble(), 3.0);

  // Test GetArrayElement() with out of bounds index (should throw)
  EXPECT_THROW(vec.GetArrayElement(3), std::out_of_range);
  EXPECT_THROW(vec.GetArrayElement(100), std::out_of_range);

  // Test AsArray() on vector values
  auto opt_vec = vec.AsArray();
  ASSERT_TRUE(opt_vec.has_value());
  EXPECT_EQ(opt_vec.value()->size(), 3);

  // Test AsArray() on scalar values (should return nullopt)
  EXPECT_FALSE(Value().AsArray().has_value());
  EXPECT_FALSE(Value(true).AsArray().has_value());
  EXPECT_FALSE(Value(42.0).AsArray().has_value());
  EXPECT_FALSE(Value(std::string("test")).AsArray().has_value());

  // Test GetArray() returns shared_ptr (efficient copying)
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value vec2 = vec1;  // Copy - intentionally testing shared_ptr semantics
  auto ptr1 = vec1.GetArray();
  auto ptr2 = vec2.GetArray();
  EXPECT_EQ(ptr1, ptr2);  // Should point to same underlying vector
  // Verify the copy is valid by accessing elements
  EXPECT_EQ(vec2.GetArrayElement(0).GetDouble(), 1.0);

  // Test nested vector access
  Value nested(
      {Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  auto outer = nested.GetArray();
  EXPECT_EQ(outer->size(), 2);

  auto inner1 = (*outer)[0].GetArray();
  EXPECT_EQ(inner1->size(), 2);
  EXPECT_EQ((*inner1)[0].GetDouble(), 1.0);
  EXPECT_EQ((*inner1)[1].GetDouble(), 2.0);

  auto inner2 = (*outer)[1].GetArray();
  EXPECT_EQ(inner2->size(), 2);
  EXPECT_EQ((*inner2)[0].GetDouble(), 3.0);
  EXPECT_EQ((*inner2)[1].GetDouble(), 4.0);
}

TEST_F(ValueTest, vector_arithmetic) {
  // Test vector-scalar addition
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value scalar(5.0);
  Value result1 = FuncAdd(vec1, scalar);
  ASSERT_TRUE(result1.IsArray());
  EXPECT_EQ(result1.ArraySize(), 3);
  EXPECT_EQ(result1.GetArrayElement(0).GetDouble(), 6.0);
  EXPECT_EQ(result1.GetArrayElement(1).GetDouble(), 7.0);
  EXPECT_EQ(result1.GetArrayElement(2).GetDouble(), 8.0);

  // Test scalar-vector addition
  Value result2 = FuncAdd(scalar, vec1);
  ASSERT_TRUE(result2.IsArray());
  EXPECT_EQ(result2.ArraySize(), 3);
  EXPECT_EQ(result2.GetArrayElement(0).GetDouble(), 6.0);
  EXPECT_EQ(result2.GetArrayElement(1).GetDouble(), 7.0);
  EXPECT_EQ(result2.GetArrayElement(2).GetDouble(), 8.0);

  // Test vector-vector addition
  Value vec2({Value(10.0), Value(20.0), Value(30.0)});
  Value result3 = FuncAdd(vec1, vec2);
  ASSERT_TRUE(result3.IsArray());
  EXPECT_EQ(result3.ArraySize(), 3);
  EXPECT_EQ(result3.GetArrayElement(0).GetDouble(), 11.0);
  EXPECT_EQ(result3.GetArrayElement(1).GetDouble(), 22.0);
  EXPECT_EQ(result3.GetArrayElement(2).GetDouble(), 33.0);

  // Test vector-scalar subtraction
  Value result4 = FuncSub(vec1, Value(1.0));
  ASSERT_TRUE(result4.IsArray());
  EXPECT_EQ(result4.ArraySize(), 3);
  EXPECT_EQ(result4.GetArrayElement(0).GetDouble(), 0.0);
  EXPECT_EQ(result4.GetArrayElement(1).GetDouble(), 1.0);
  EXPECT_EQ(result4.GetArrayElement(2).GetDouble(), 2.0);

  // Test vector-scalar multiplication
  Value result5 = FuncMul(vec1, Value(2.0));
  ASSERT_TRUE(result5.IsArray());
  EXPECT_EQ(result5.ArraySize(), 3);
  EXPECT_EQ(result5.GetArrayElement(0).GetDouble(), 2.0);
  EXPECT_EQ(result5.GetArrayElement(1).GetDouble(), 4.0);
  EXPECT_EQ(result5.GetArrayElement(2).GetDouble(), 6.0);

  // Test vector-scalar division
  Value result6 = FuncDiv(vec1, Value(2.0));
  ASSERT_TRUE(result6.IsArray());
  EXPECT_EQ(result6.ArraySize(), 3);
  EXPECT_EQ(result6.GetArrayElement(0).GetDouble(), 0.5);
  EXPECT_EQ(result6.GetArrayElement(1).GetDouble(), 1.0);
  EXPECT_EQ(result6.GetArrayElement(2).GetDouble(), 1.5);

  // Test vector-scalar power
  Value result7 = FuncPower(vec1, Value(2.0));
  ASSERT_TRUE(result7.IsArray());
  EXPECT_EQ(result7.ArraySize(), 3);
  EXPECT_EQ(result7.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(result7.GetArrayElement(1).GetDouble(), 4.0);
  EXPECT_EQ(result7.GetArrayElement(2).GetDouble(), 9.0);

  // Test length mismatch error
  Value vec3({Value(1.0), Value(2.0)});
  Value result8 = FuncAdd(vec1, vec3);
  ASSERT_TRUE(result8.IsNil());
  std::string error_msg = result8.GetNil().GetReason();
  EXPECT_EQ(error_msg, "Length mismatch: vectors have lengths 3 and 2");
}

TEST_F(ValueTest, ArrayComparison_EqualArrays) {
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

TEST_F(ValueTest, ArrayComparison_DifferingElements) {
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

TEST_F(ValueTest, ArrayComparison_DifferingLength) {
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

TEST_F(ValueTest, ArrayComparison_ArrayVsScalar) {
  // Test vector vs scalar comparisons (should be UNORDERED)
  Value vec({Value(1.0), Value(2.0), Value(3.0)});
  Value scalar_double(1.0);
  Value scalar_string(std::string("test"));
  Value scalar_bool(true);
  Value scalar_nil;

  // Array vs double
  EXPECT_EQ(Compare(vec, scalar_double), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_double, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_double);   // UNORDERED treated as equal by ==
  EXPECT_FALSE(vec != scalar_double);  // UNORDERED not treated as != by !=

  // Array vs string
  EXPECT_EQ(Compare(vec, scalar_string), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_string, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_string);

  // Array vs bool
  EXPECT_EQ(Compare(vec, scalar_bool), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_bool, vec), Ordering::kUNORDERED);
  EXPECT_TRUE(vec == scalar_bool);

  // Array vs nil
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

// Test vector serialization to RESP format
TEST_F(ValueTest, ArraySerializationTest) {
  // Note: This test verifies the serialization logic exists and compiles.
  // Full integration testing with actual ValkeyModuleCtx would be done
  // in integration tests.

  // Test simple vector
  Value vec1 = Value({Value(1.0), Value(2.0), Value(3.0)});
  EXPECT_TRUE(vec1.IsArray());
  EXPECT_EQ(vec1.ArraySize(), 3);

  // Test nested vector
  Value vec2 =
      Value({Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  EXPECT_TRUE(vec2.IsArray());
  EXPECT_EQ(vec2.ArraySize(), 2);
  EXPECT_TRUE(vec2.GetArrayElement(0).IsArray());
  EXPECT_TRUE(vec2.GetArrayElement(1).IsArray());

  // Test mixed-type vector
  Value vec3 = Value({Value(1.0), Value("hello"), Value(true)});
  EXPECT_TRUE(vec3.IsArray());
  EXPECT_EQ(vec3.ArraySize(), 3);
  EXPECT_TRUE(vec3.GetArrayElement(0).IsDouble());
  EXPECT_TRUE(vec3.GetArrayElement(1).IsString());
  EXPECT_TRUE(vec3.GetArrayElement(2).IsBool());
}

// Test vector deserialization from RESP format
// Note: Full testing requires ValkeyModuleCallReply mocks, which would be
// done in integration tests. This test verifies the function signature exists.
TEST_F(ValueTest, ArrayDeserializationSignatureTest) {
  // Verify the deserialization function is declared and can be called
  // with nullptr (will return Nil)
  Value result = DeserializeValueFromResp(nullptr);
  EXPECT_TRUE(result.IsNil());
}

// Array-specific function tests

TEST_F(ValueTest, FuncArrayLen_ValidArray) {
  Value vec = Value({Value(1.0), Value(2.0), Value(3.0)});
  Value result = FuncArrayLen(vec);
  EXPECT_TRUE(result.IsDouble());
  EXPECT_EQ(result.GetDouble(), 3.0);
}

TEST_F(ValueTest, FuncArrayLen_EmptyArray) {
  Value vec = Value(std::vector<Value>{});
  Value result = FuncArrayLen(vec);
  EXPECT_TRUE(result.IsDouble());
  EXPECT_EQ(result.GetDouble(), 0.0);
}

TEST_F(ValueTest, FuncArrayLen_NotAArray) {
  Value scalar = Value(42.0);
  Value result = FuncArrayLen(scalar);
  EXPECT_TRUE(result.IsNil());
  EXPECT_STREQ(result.GetNil().GetReason(),
               "vectorlen: operand is not a vector");
}

TEST_F(ValueTest, FuncArrayAt_ValidIndex) {
  Value vec = Value({Value(10.0), Value(20.0), Value(30.0)});
  Value result = FuncArrayAt(vec, Value(1.0));
  EXPECT_TRUE(result.IsDouble());
  EXPECT_EQ(result.GetDouble(), 20.0);
}

TEST_F(ValueTest, FuncArrayAt_FirstElement) {
  Value vec = Value({Value("first"), Value("second"), Value("third")});
  Value result = FuncArrayAt(vec, Value(0.0));
  EXPECT_TRUE(result.IsString());
  EXPECT_EQ(result.AsString(), "first");
}

TEST_F(ValueTest, FuncArrayAt_LastElement) {
  Value vec = Value({Value(1.0), Value(2.0), Value(3.0)});
  Value result = FuncArrayAt(vec, Value(2.0));
  EXPECT_TRUE(result.IsDouble());
  EXPECT_EQ(result.GetDouble(), 3.0);
}

TEST_F(ValueTest, FuncArrayAt_IndexOutOfBounds) {
  Value vec = Value({Value(1.0), Value(2.0), Value(3.0)});
  Value result = FuncArrayAt(vec, Value(10.0));
  EXPECT_TRUE(result.IsNil());
  std::string reason = result.GetNil().GetReason();
  EXPECT_EQ(reason,
            std::string("Index out of bounds: index 10, vector length 3"));
}

TEST_F(ValueTest, FuncArrayAt_NegativeIndex) {
  Value vec = Value({Value(1.0), Value(2.0), Value(3.0)});
  Value result = FuncArrayAt(vec, Value(-1.0));
  EXPECT_TRUE(result.IsNil());
  std::string reason = result.GetNil().GetReason();
  EXPECT_EQ(reason, "Index out of bounds: index -1, vector length 3");
}

TEST_F(ValueTest, FuncArrayAt_NotAArray) {
  Value scalar = Value(42.0);
  Value result = FuncArrayAt(scalar, Value(0.0));
  EXPECT_TRUE(result.IsNil());
  EXPECT_STREQ(result.GetNil().GetReason(),
               "vectorat: first operand is not a vector");
}

TEST_F(ValueTest, FuncArrayAt_InvalidIndex) {
  Value vec = Value({Value(1.0), Value(2.0), Value(3.0)});
  Value result = FuncArrayAt(vec, Value("not a number"));
  EXPECT_TRUE(result.IsNil());
  EXPECT_STREQ(result.GetNil().GetReason(),
               "vectorat: index is not an integer");
}

TEST_F(ValueTest, FuncIsArray_Array) {
  Value vec = Value({Value(1.0), Value(2.0)});
  Value result = FuncIsArray(vec);
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.GetBool());
}

TEST_F(ValueTest, FuncIsArray_EmptyArray) {
  Value vec = Value(std::vector<Value>{});
  Value result = FuncIsArray(vec);
  EXPECT_TRUE(result.IsBool());
  EXPECT_TRUE(result.GetBool());
}

TEST_F(ValueTest, FuncIsArray_Scalar) {
  Value scalar = Value(42.0);
  Value result = FuncIsArray(scalar);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST_F(ValueTest, FuncIsArray_String) {
  Value str = Value("hello");
  Value result = FuncIsArray(str);
  EXPECT_TRUE(result.IsBool());
  EXPECT_FALSE(result.GetBool());
}

TEST_F(ValueTest, FuncFlatten_SingleLevel) {
  Value nested =
      Value({Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});
  Value result = FuncFlatten(nested, Value(1.0));
  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 4);
  EXPECT_EQ(result.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(result.GetArrayElement(1).GetDouble(), 2.0);
  EXPECT_EQ(result.GetArrayElement(2).GetDouble(), 3.0);
  EXPECT_EQ(result.GetArrayElement(3).GetDouble(), 4.0);
}

TEST_F(ValueTest, FuncFlatten_MultiLevel) {
  Value nested =
      Value({Value({Value({Value(1.0), Value(2.0)}), Value(3.0)}), Value(4.0)});
  Value result = FuncFlatten(nested, Value(2.0));
  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 4);
  EXPECT_EQ(result.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(result.GetArrayElement(1).GetDouble(), 2.0);
  EXPECT_EQ(result.GetArrayElement(2).GetDouble(), 3.0);
  EXPECT_EQ(result.GetArrayElement(3).GetDouble(), 4.0);
}

TEST_F(ValueTest, FuncFlatten_DepthZero) {
  Value nested = Value({Value({Value(1.0), Value(2.0)}), Value(3.0)});
  Value result = FuncFlatten(nested, Value(0.0));
  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 2);
  EXPECT_TRUE(result.GetArrayElement(0).IsArray());
  EXPECT_EQ(result.GetArrayElement(1).GetDouble(), 3.0);
}

TEST_F(ValueTest, FuncFlatten_MixedScalarsAndArrays) {
  Value mixed =
      Value({Value(1.0), Value({Value(2.0), Value(3.0)}), Value(4.0)});
  Value result = FuncFlatten(mixed, Value(1.0));
  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 4);
  EXPECT_EQ(result.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(result.GetArrayElement(1).GetDouble(), 2.0);
  EXPECT_EQ(result.GetArrayElement(2).GetDouble(), 3.0);
  EXPECT_EQ(result.GetArrayElement(3).GetDouble(), 4.0);
}

TEST_F(ValueTest, FuncFlatten_NotAArray) {
  Value scalar = Value(42.0);
  Value result = FuncFlatten(scalar, Value(1.0));
  EXPECT_TRUE(result.IsNil());
  EXPECT_STREQ(result.GetNil().GetReason(),
               "flatten: first operand is not a vector");
}

TEST_F(ValueTest, FuncFlatten_InvalidDepth) {
  Value vec = Value({Value(1.0), Value(2.0)});
  Value result = FuncFlatten(vec, Value("not a number"));
  EXPECT_TRUE(result.IsNil());
  EXPECT_STREQ(result.GetNil().GetReason(), "flatten: depth is not an integer");
}

// Nested vector construction tests

TEST_F(ValueTest, NestedArray_TwoLevels) {
  // Create a 2-level nested vector: [[1, 2], [3, 4], [5, 6]]
  Value nested =
      Value({Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)}),
             Value({Value(5.0), Value(6.0)})});

  EXPECT_TRUE(nested.IsArray());
  EXPECT_EQ(nested.ArraySize(), 3);

  // Verify first inner vector
  Value inner1 = nested.GetArrayElement(0);
  EXPECT_TRUE(inner1.IsArray());
  EXPECT_EQ(inner1.ArraySize(), 2);
  EXPECT_EQ(inner1.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(inner1.GetArrayElement(1).GetDouble(), 2.0);

  // Verify second inner vector
  Value inner2 = nested.GetArrayElement(1);
  EXPECT_TRUE(inner2.IsArray());
  EXPECT_EQ(inner2.ArraySize(), 2);
  EXPECT_EQ(inner2.GetArrayElement(0).GetDouble(), 3.0);
  EXPECT_EQ(inner2.GetArrayElement(1).GetDouble(), 4.0);

  // Verify third inner vector
  Value inner3 = nested.GetArrayElement(2);
  EXPECT_TRUE(inner3.IsArray());
  EXPECT_EQ(inner3.ArraySize(), 2);
  EXPECT_EQ(inner3.GetArrayElement(0).GetDouble(), 5.0);
  EXPECT_EQ(inner3.GetArrayElement(1).GetDouble(), 6.0);
}

TEST_F(ValueTest, NestedArray_ThreeLevels) {
  // Create a 3-level nested vector: [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
  Value nested = Value({Value({Value({Value(1.0), Value(2.0)}),
                               Value({Value(3.0), Value(4.0)})}),
                        Value({Value({Value(5.0), Value(6.0)}),
                               Value({Value(7.0), Value(8.0)})})});

  EXPECT_TRUE(nested.IsArray());
  EXPECT_EQ(nested.ArraySize(), 2);

  // Verify first level 2 vector
  Value level2_1 = nested.GetArrayElement(0);
  EXPECT_TRUE(level2_1.IsArray());
  EXPECT_EQ(level2_1.ArraySize(), 2);

  // Verify first level 3 vector
  Value level3_1 = level2_1.GetArrayElement(0);
  EXPECT_TRUE(level3_1.IsArray());
  EXPECT_EQ(level3_1.ArraySize(), 2);
  EXPECT_EQ(level3_1.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(level3_1.GetArrayElement(1).GetDouble(), 2.0);

  // Verify second level 3 vector
  Value level3_2 = level2_1.GetArrayElement(1);
  EXPECT_TRUE(level3_2.IsArray());
  EXPECT_EQ(level3_2.ArraySize(), 2);
  EXPECT_EQ(level3_2.GetArrayElement(0).GetDouble(), 3.0);
  EXPECT_EQ(level3_2.GetArrayElement(1).GetDouble(), 4.0);

  // Verify second level 2 vector
  Value level2_2 = nested.GetArrayElement(1);
  EXPECT_TRUE(level2_2.IsArray());
  EXPECT_EQ(level2_2.ArraySize(), 2);

  // Verify third level 3 vector
  Value level3_3 = level2_2.GetArrayElement(0);
  EXPECT_TRUE(level3_3.IsArray());
  EXPECT_EQ(level3_3.ArraySize(), 2);
  EXPECT_EQ(level3_3.GetArrayElement(0).GetDouble(), 5.0);
  EXPECT_EQ(level3_3.GetArrayElement(1).GetDouble(), 6.0);

  // Verify fourth level 3 vector
  Value level3_4 = level2_2.GetArrayElement(1);
  EXPECT_TRUE(level3_4.IsArray());
  EXPECT_EQ(level3_4.ArraySize(), 2);
  EXPECT_EQ(level3_4.GetArrayElement(0).GetDouble(), 7.0);
  EXPECT_EQ(level3_4.GetArrayElement(1).GetDouble(), 8.0);
}

TEST_F(ValueTest, NestedArray_MixedDepths) {
  // Create a vector with mixed nesting depths: [1, [2, 3], [[4, 5], 6]]
  Value nested = Value({Value(1.0), Value({Value(2.0), Value(3.0)}),
                        Value({Value({Value(4.0), Value(5.0)}), Value(6.0)})});

  EXPECT_TRUE(nested.IsArray());
  EXPECT_EQ(nested.ArraySize(), 3);

  // First element is scalar
  EXPECT_TRUE(nested.GetArrayElement(0).IsDouble());
  EXPECT_EQ(nested.GetArrayElement(0).GetDouble(), 1.0);

  // Second element is 1-level nested vector
  Value elem2 = nested.GetArrayElement(1);
  EXPECT_TRUE(elem2.IsArray());
  EXPECT_EQ(elem2.ArraySize(), 2);
  EXPECT_EQ(elem2.GetArrayElement(0).GetDouble(), 2.0);
  EXPECT_EQ(elem2.GetArrayElement(1).GetDouble(), 3.0);

  // Third element is 2-level nested vector
  Value elem3 = nested.GetArrayElement(2);
  EXPECT_TRUE(elem3.IsArray());
  EXPECT_EQ(elem3.ArraySize(), 2);

  Value elem3_inner = elem3.GetArrayElement(0);
  EXPECT_TRUE(elem3_inner.IsArray());
  EXPECT_EQ(elem3_inner.ArraySize(), 2);
  EXPECT_EQ(elem3_inner.GetArrayElement(0).GetDouble(), 4.0);
  EXPECT_EQ(elem3_inner.GetArrayElement(1).GetDouble(), 5.0);

  EXPECT_TRUE(elem3.GetArrayElement(1).IsDouble());
  EXPECT_EQ(elem3.GetArrayElement(1).GetDouble(), 6.0);
}

TEST_F(ValueTest, NestedArray_EmptyInnerArrays) {
  // Create a vector containing empty vectors: [[], [1, 2], []]
  Value nested =
      Value({Value(std::vector<Value>{}), Value({Value(1.0), Value(2.0)}),
             Value(std::vector<Value>{})});

  EXPECT_TRUE(nested.IsArray());
  EXPECT_EQ(nested.ArraySize(), 3);

  // First element is empty vector
  Value elem1 = nested.GetArrayElement(0);
  EXPECT_TRUE(elem1.IsArray());
  EXPECT_EQ(elem1.ArraySize(), 0);
  EXPECT_TRUE(elem1.IsEmptyArray());

  // Second element is non-empty vector
  Value elem2 = nested.GetArrayElement(1);
  EXPECT_TRUE(elem2.IsArray());
  EXPECT_EQ(elem2.ArraySize(), 2);

  // Third element is empty vector
  Value elem3 = nested.GetArrayElement(2);
  EXPECT_TRUE(elem3.IsArray());
  EXPECT_EQ(elem3.ArraySize(), 0);
  EXPECT_TRUE(elem3.IsEmptyArray());
}

// Operations on nested vectors tests

TEST_F(ValueTest, NestedArray_ScalarFunctionRecursiveApplication) {
  // Test that scalar functions recursively apply to nested elements
  // Create nested vector of strings: [["HELLO", "WORLD"], ["FOO", "BAR"]]
  Value nested =
      Value({Value({Value(std::string("HELLO")), Value(std::string("WORLD"))}),
             Value({Value(std::string("FOO")), Value(std::string("BAR"))})});

  // Apply FuncLower - should recursively lowercase all strings
  Value result = FuncLower(nested);

  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 2);

  // Verify first inner vector
  Value inner1 = result.GetArrayElement(0);
  EXPECT_TRUE(inner1.IsArray());
  EXPECT_EQ(inner1.ArraySize(), 2);
  EXPECT_EQ(inner1.GetArrayElement(0).AsString(), "hello");
  EXPECT_EQ(inner1.GetArrayElement(1).AsString(), "world");

  // Verify second inner vector
  Value inner2 = result.GetArrayElement(1);
  EXPECT_TRUE(inner2.IsArray());
  EXPECT_EQ(inner2.ArraySize(), 2);
  EXPECT_EQ(inner2.GetArrayElement(0).AsString(), "foo");
  EXPECT_EQ(inner2.GetArrayElement(1).AsString(), "bar");
}

TEST_F(ValueTest, NestedArray_MathFunctionRecursiveApplication) {
  // Test that math functions recursively apply to nested elements
  // Create nested vector: [[1.5, 2.7], [3.2, 4.9]]
  Value nested =
      Value({Value({Value(1.5), Value(2.7)}), Value({Value(3.2), Value(4.9)})});

  // Apply FuncFloor - should recursively floor all numbers
  Value result = FuncFloor(nested);

  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 2);

  // Verify first inner vector
  Value inner1 = result.GetArrayElement(0);
  EXPECT_TRUE(inner1.IsArray());
  EXPECT_EQ(inner1.ArraySize(), 2);
  EXPECT_EQ(inner1.GetArrayElement(0).GetDouble(), 1.0);
  EXPECT_EQ(inner1.GetArrayElement(1).GetDouble(), 2.0);

  // Verify second inner vector
  Value inner2 = result.GetArrayElement(1);
  EXPECT_TRUE(inner2.IsArray());
  EXPECT_EQ(inner2.ArraySize(), 2);
  EXPECT_EQ(inner2.GetArrayElement(0).GetDouble(), 3.0);
  EXPECT_EQ(inner2.GetArrayElement(1).GetDouble(), 4.0);
}

TEST_F(ValueTest, NestedArray_ThreeLevelRecursiveApplication) {
  // Test recursive application on 3-level nested vector
  // Create: [[[1.1, 2.2]], [[3.3, 4.4]]]
  Value nested = Value({Value({Value({Value(1.1), Value(2.2)})}),
                        Value({Value({Value(3.3), Value(4.4)})})});

  // Apply FuncCeil - should recursively ceil all numbers
  Value result = FuncCeil(nested);

  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 2);

  // Navigate to innermost vectors and verify
  Value level2_1 = result.GetArrayElement(0);
  EXPECT_TRUE(level2_1.IsArray());
  Value level3_1 = level2_1.GetArrayElement(0);
  EXPECT_TRUE(level3_1.IsArray());
  EXPECT_EQ(level3_1.GetArrayElement(0).GetDouble(), 2.0);
  EXPECT_EQ(level3_1.GetArrayElement(1).GetDouble(), 3.0);

  Value level2_2 = result.GetArrayElement(1);
  EXPECT_TRUE(level2_2.IsArray());
  Value level3_2 = level2_2.GetArrayElement(0);
  EXPECT_TRUE(level3_2.IsArray());
  EXPECT_EQ(level3_2.GetArrayElement(0).GetDouble(), 4.0);
  EXPECT_EQ(level3_2.GetArrayElement(1).GetDouble(), 5.0);
}

TEST_F(ValueTest, NestedArray_ArithmeticWithScalar) {
  // Test arithmetic operations on nested vectors with scalar
  // Create nested vector: [[1, 2], [3, 4]]
  Value nested =
      Value({Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});

  // Add scalar to nested vector
  Value result = FuncAdd(nested, Value(10.0));

  EXPECT_TRUE(result.IsArray());
  EXPECT_EQ(result.ArraySize(), 2);

  // Verify first inner vector
  Value inner1 = result.GetArrayElement(0);
  EXPECT_TRUE(inner1.IsArray());
  EXPECT_EQ(inner1.ArraySize(), 2);
  EXPECT_EQ(inner1.GetArrayElement(0).GetDouble(), 11.0);
  EXPECT_EQ(inner1.GetArrayElement(1).GetDouble(), 12.0);

  // Verify second inner vector
  Value inner2 = result.GetArrayElement(1);
  EXPECT_TRUE(inner2.IsArray());
  EXPECT_EQ(inner2.ArraySize(), 2);
  EXPECT_EQ(inner2.GetArrayElement(0).GetDouble(), 13.0);
  EXPECT_EQ(inner2.GetArrayElement(1).GetDouble(), 14.0);
}

TEST_F(ValueTest, NestedArray_ElementAccess) {
  // Test element access for nested vectors
  // Create: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
  Value nested = Value({Value({Value(1.0), Value(2.0), Value(3.0)}),
                        Value({Value(4.0), Value(5.0), Value(6.0)}),
                        Value({Value(7.0), Value(8.0), Value(9.0)})});

  // Access middle row
  Value row2 = nested.GetArrayElement(1);
  EXPECT_TRUE(row2.IsArray());
  EXPECT_EQ(row2.ArraySize(), 3);

  // Access middle element of middle row
  Value elem = row2.GetArrayElement(1);
  EXPECT_TRUE(elem.IsDouble());
  EXPECT_EQ(elem.GetDouble(), 5.0);

  // Test FuncArrayAt on nested structure
  Value row2_via_func = FuncArrayAt(nested, Value(1.0));
  EXPECT_TRUE(row2_via_func.IsArray());
  EXPECT_EQ(row2_via_func.ArraySize(), 3);

  Value elem_via_func = FuncArrayAt(row2_via_func, Value(1.0));
  EXPECT_TRUE(elem_via_func.IsDouble());
  EXPECT_EQ(elem_via_func.GetDouble(), 5.0);
}

TEST_F(ValueTest, NestedArray_ArrayLenOnNestedStructure) {
  // Test FuncArrayLen on nested vectors
  // Create: [[1, 2], [3, 4, 5]]
  Value nested = Value({Value({Value(1.0), Value(2.0)}),
                        Value({Value(3.0), Value(4.0), Value(5.0)})});

  // Get length of outer vector
  Value outer_len = FuncArrayLen(nested);
  EXPECT_TRUE(outer_len.IsDouble());
  EXPECT_EQ(outer_len.GetDouble(), 2.0);

  // Get length of first inner vector
  Value inner1 = nested.GetArrayElement(0);
  Value inner1_len = FuncArrayLen(inner1);
  EXPECT_TRUE(inner1_len.IsDouble());
  EXPECT_EQ(inner1_len.GetDouble(), 2.0);

  // Get length of second inner vector
  Value inner2 = nested.GetArrayElement(1);
  Value inner2_len = FuncArrayLen(inner2);
  EXPECT_TRUE(inner2_len.IsDouble());
  EXPECT_EQ(inner2_len.GetDouble(), 3.0);
}

TEST_F(ValueTest, NestedArray_MixedTypesRecursive) {
  // Test operations on nested vectors with mixed types
  // Create: [[1, "hello"], [true, 3.14]]
  Value nested = Value({Value({Value(1.0), Value(std::string("hello"))}),
                        Value({Value(true), Value(3.14)})});

  EXPECT_TRUE(nested.IsArray());
  EXPECT_EQ(nested.ArraySize(), 2);

  // Verify structure is preserved
  Value inner1 = nested.GetArrayElement(0);
  EXPECT_TRUE(inner1.IsArray());
  EXPECT_TRUE(inner1.GetArrayElement(0).IsDouble());
  EXPECT_TRUE(inner1.GetArrayElement(1).IsString());

  Value inner2 = nested.GetArrayElement(1);
  EXPECT_TRUE(inner2.IsArray());
  EXPECT_TRUE(inner2.GetArrayElement(0).IsBool());
  EXPECT_TRUE(inner2.GetArrayElement(1).IsDouble());
}

}  // namespace valkey_search::expr
