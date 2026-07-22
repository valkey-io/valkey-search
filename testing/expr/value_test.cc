/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/expr/value.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>

#include "gtest/gtest.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::expr {

class ValueTest : public vmsdk::ValkeyTest {
 protected:
  // Save+restore the default emulate-release so tests that flip it
  // (to exercise both the legacy and fixed VALKEY_SEARCH_COMPATIBILITY_FIX
  // branches) don't bleed state to subsequent tests. Note: debug-mode
  // defaults to true in the unit-test environment (no ParseAndLoadArgv
  // flips it to false), so SetValue past kModuleVersion is permitted.
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    saved_emulate_release_ = options::GetEmulateRelease().GetValue();
  }
  void TearDown() override {
    auto ok = options::GetEmulateRelease().SetValue(saved_emulate_release_);
    ASSERT_TRUE(ok.ok()) << ok.message();
    vmsdk::ValkeyTest::TearDown();
  }

  // RAII wrapper: pins emulate-release for the duration of its scope and
  // restores the value that was in effect when the scope started. Nestable.
  class ScopedEmulateRelease {
   public:
    explicit ScopedEmulateRelease(vmsdk::ValkeyVersion v)
        : prev_(options::GetEmulateRelease().GetValue()) {
      auto ok = options::GetEmulateRelease().SetValue(v);
      EXPECT_TRUE(ok.ok()) << ok.message();
    }
    ~ScopedEmulateRelease() {
      auto ok = options::GetEmulateRelease().SetValue(prev_);
      EXPECT_TRUE(ok.ok()) << ok.message();
    }

   private:
    vmsdk::ValkeyVersion prev_;
  };

  vmsdk::ValkeyVersion saved_emulate_release_{0};

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
  // 1739565015 corresponds to Fri Feb 14 2025 20:30:15 (GMT). This value
  // is positive, finite, and post-epoch, so all VALKEY_SEARCH_COMPATIBILITY
  // _FIX guards in the date functions evaluate to the same answer on both
  // branches EXCEPT FuncMonth (which has a tm_mday=0-vs-1 off-by-one fix).
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

  // FuncMonth differs across the compatibility branches:
  //   legacy (pre-1.2.1): tm_mday=0 → mktime rolls back to last day of
  //     previous month, returning Jan 31 2025 00:00:00 UTC = 1738281600.
  //   fixed (>= 1.2.1):  tm_mday=1 → first day of the month, returning
  //     Feb  1 2025 00:00:00 UTC = 1738368000.
  // Default emulate-release in tests is 1.0.0 (legacy).
  EXPECT_EQ(FuncMonth(ts), Value(1738281600))
      << "default (pre-1.2.1) emulate-release should run legacy FuncMonth";
  {
    ScopedEmulateRelease s({1, 2, 1});
    EXPECT_EQ(FuncMonth(ts), Value(1738368000))
        << "emulate-release >= 1.2.1 should run the off-by-one fix";
  }
}

// Walk every COMPATIBILITY_FIX site in value.cc through both branches and
// verify the result actually differs as documented. Pin emulate-release
// explicitly per phase rather than relying on the runtime default.
TEST_F(ValueTest, CompatibilityFixGates) {
  // Force the legacy branch first — explicit pin (don't rely on default).
  ScopedEmulateRelease legacy_scope({1, 0, 0});

  // --- asbool_string_truthy ---------------------------------------------
  // FuncLand(string, true): the new branch promotes non-empty strings to
  // true, so true && "a" == true (1); legacy: true && "a" == false (0).
  Value s_a(std::string("a"));
  EXPECT_EQ(FuncLand(Value(true), s_a), Value(false)) << "legacy AsBool";
  {
    ScopedEmulateRelease scope({1, 2, 1});
    EXPECT_EQ(FuncLand(Value(true), s_a), Value(true))
        << "fix: non-empty truthy";
  }

  // --- numeric_unary_nan_on_unparsable ---------------------------------
  // abs("a"): legacy → Nil; fix → NaN.
  // std::isnan is unreliable under -ffast-math, so do a bit-pattern check
  // (same approach as the IsNan helper in value.cc).
  auto is_nan_bits = [](double d) {
    uint64_t bits;
    std::memcpy(&bits, &d, sizeof(bits));
    constexpr uint64_t kExp = 0x7FF0000000000000ull;
    constexpr uint64_t kMant = 0x000FFFFFFFFFFFFFull;
    return (bits & kExp) == kExp && (bits & kMant) != 0;
  };
  {
    auto legacy = FuncAbs(s_a);
    EXPECT_TRUE(legacy.IsNil()) << "legacy abs(\"a\") should be Nil";
  }
  {
    ScopedEmulateRelease scope({1, 2, 1});
    auto fixed = FuncAbs(s_a);
    ASSERT_TRUE(fixed.IsDouble());
    EXPECT_TRUE(is_nan_bits(fixed.AsDouble().value()))
        << "fix abs(\"a\") should be NaN";
  }

  // --- lower_non_string_to_nil / upper_non_string_to_nil ----------------
  // lower(0) / upper(0): legacy passes through ("0"); fix → Nil.
  Value zero(0.0);
  {
    auto l = FuncLower(zero);
    ASSERT_TRUE(l.IsString());
    EXPECT_EQ(l.AsStringView().value(), "0")
        << "legacy lower(0) passes through";
    auto u = FuncUpper(zero);
    ASSERT_TRUE(u.IsString());
    EXPECT_EQ(u.AsStringView().value(), "0");
  }
  {
    ScopedEmulateRelease scope({1, 2, 1});
    EXPECT_TRUE(FuncLower(zero).IsNil()) << "fix lower(0) → Nil";
    EXPECT_TRUE(FuncUpper(zero).IsNil()) << "fix upper(0) → Nil";
  }

  // --- timefmt_empty_format_to_nil --------------------------------------
  // timefmt(0, ""): legacy → ""; fix → Nil.
  {
    auto t = FuncTimefmt(zero, Value(""));
    ASSERT_TRUE(t.IsString());
    EXPECT_EQ(t.AsStringView().value(), "") << "legacy timefmt(_, \"\") → \"\"";
  }
  {
    ScopedEmulateRelease scope({1, 2, 1});
    EXPECT_TRUE(FuncTimefmt(zero, Value("")).IsNil())
        << "fix timefmt(_, \"\") → Nil";
  }

  // --- parsetime_format_mismatch_to_nil ---------------------------------
  // parsetime("","a"): strptime returns NULL. Legacy uses the zero-tm and
  // returns the constant -2209075200; fix → Nil.
  {
    auto p = FuncParsetime(Value(""), Value("a"));
    ASSERT_TRUE(p.IsDouble());
    EXPECT_EQ(p.AsDouble().value(), -2209075200.0)
        << "legacy parsetime on format-mismatch falls back to zero-tm";
  }
  {
    ScopedEmulateRelease scope({1, 2, 1});
    EXPECT_TRUE(FuncParsetime(Value(""), Value("a")).IsNil())
        << "fix parsetime on format-mismatch → Nil";
  }

  // --- date_fn_negative_ts_to_nil ---------------------------------------
  // hour(-1): legacy computes -3600 (one second before epoch, rounded);
  // fix → Nil. Spot-check one date function; the gate is shared by all
  // nine via DateNegativeTsReturnsNil.
  Value neg_one(-1.0);
  {
    auto h = FuncHour(neg_one);
    ASSERT_TRUE(h.IsDouble());
    EXPECT_EQ(h.AsDouble().value(), -3600.0)
        << "legacy hour(-1) computes the pre-epoch rounded value";
  }
  {
    ScopedEmulateRelease scope({1, 2, 1});
    EXPECT_TRUE(FuncHour(neg_one).IsNil()) << "fix hour(-1) → Nil";
  }
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

  // Test ArraySize() on scalar values (should CHECK-fail, so we just verify
  // IsArray() is false)
  EXPECT_FALSE(Value().IsArray());
  EXPECT_FALSE(Value(true).IsArray());
  EXPECT_FALSE(Value(42.0).IsArray());
  EXPECT_FALSE(Value(std::string("test")).IsArray());

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

  // Test GetArrayElement() with out of bounds index (should CHECK-fail)
  VMSDK_EXPECT_DEATH(
      vec.GetArrayElement(3),
      "GetArrayElement called with index out of range: 3. Array size = 3");
  VMSDK_EXPECT_DEATH(
      vec.GetArrayElement(100),
      "GetArrayElement called with index out of range: 100. Array size = 3");

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
  // Arithmetic on arrays returns Nil with per-function error messages
  Value vec1({Value(1.0), Value(2.0), Value(3.0)});
  Value scalar(5.0);

  // Test vector-scalar addition returns error
  Value result1 = FuncAdd(vec1, scalar);
  ASSERT_TRUE(result1.IsNil());
  EXPECT_EQ(result1.GetNil().GetReason(), "Add requires numeric operands");

  // Test scalar-vector addition returns error
  Value result2 = FuncAdd(scalar, vec1);
  ASSERT_TRUE(result2.IsNil());
  EXPECT_EQ(result2.GetNil().GetReason(), "Add requires numeric operands");

  // Test vector-vector addition returns error
  Value vec2({Value(10.0), Value(20.0), Value(30.0)});
  Value result3 = FuncAdd(vec1, vec2);
  ASSERT_TRUE(result3.IsNil());
  EXPECT_EQ(result3.GetNil().GetReason(), "Add requires numeric operands");

  // Test vector-scalar subtraction returns error
  Value result4 = FuncSub(vec1, Value(1.0));
  ASSERT_TRUE(result4.IsNil());
  EXPECT_EQ(result4.GetNil().GetReason(), "Subtract requires numeric operands");

  // Test vector-scalar multiplication returns error
  Value result5 = FuncMul(vec1, Value(2.0));
  ASSERT_TRUE(result5.IsNil());
  EXPECT_EQ(result5.GetNil().GetReason(), "Multiply requires numeric operands");

  // Test vector-scalar division returns error
  Value result6 = FuncDiv(vec1, Value(2.0));
  ASSERT_TRUE(result6.IsNil());
  EXPECT_EQ(result6.GetNil().GetReason(), "Divide requires numeric operands");

  // Test vector-scalar power returns error
  Value result7 = FuncPower(vec1, Value(2.0));
  ASSERT_TRUE(result7.IsNil());
  EXPECT_EQ(result7.GetNil().GetReason(), "Power requires numeric operands");
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

  // Array vs string
  EXPECT_EQ(Compare(vec, scalar_string), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_string, vec), Ordering::kUNORDERED);

  // Array vs bool
  EXPECT_EQ(Compare(vec, scalar_bool), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_bool, vec), Ordering::kUNORDERED);

  // Array vs nil
  EXPECT_EQ(Compare(vec, scalar_nil), Ordering::kUNORDERED);
  EXPECT_EQ(Compare(scalar_nil, vec), Ordering::kUNORDERED);

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
  // Redis compatibility: lower/upper on arrays returns nil
  Value nested =
      Value({Value({Value(std::string("HELLO")), Value(std::string("WORLD"))}),
             Value({Value(std::string("FOO")), Value(std::string("BAR"))})});

  Value result = FuncLower(nested);
  EXPECT_TRUE(result.IsNil());

  Value result2 = FuncUpper(nested);
  EXPECT_TRUE(result2.IsNil());
}

TEST_F(ValueTest, NestedArray_MathFunctionRecursiveApplication) {
  // Math functions on arrays return Nil (can't convert to double)
  Value nested =
      Value({Value({Value(1.5), Value(2.7)}), Value({Value(3.2), Value(4.9)})});

  Value result = FuncFloor(nested);
  EXPECT_TRUE(result.IsNil());
  EXPECT_EQ(result.GetNil().GetReason(), "floor couldn't convert to a double");

  Value result2 = FuncCeil(nested);
  EXPECT_TRUE(result2.IsNil());
  EXPECT_EQ(result2.GetNil().GetReason(), "ceil couldn't convert to a double");
}

TEST_F(ValueTest, NestedArray_ThreeLevelRecursiveApplication) {
  // Math functions on nested arrays return Nil (can't convert to double)
  Value nested = Value({Value({Value({Value(1.1), Value(2.2)})}),
                        Value({Value({Value(3.3), Value(4.4)})})});

  Value result = FuncCeil(nested);
  EXPECT_TRUE(result.IsNil());
  EXPECT_EQ(result.GetNil().GetReason(), "ceil couldn't convert to a double");
}

TEST_F(ValueTest, NestedArray_ArithmeticWithScalar) {
  // Arithmetic on nested arrays returns Nil
  Value nested =
      Value({Value({Value(1.0), Value(2.0)}), Value({Value(3.0), Value(4.0)})});

  Value result = FuncAdd(nested, Value(10.0));
  ASSERT_TRUE(result.IsNil());
  EXPECT_EQ(result.GetNil().GetReason(), "Add requires numeric operands");
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
}

TEST_F(ValueTest, NestedArray_ArrayLenOnNestedStructure) {
  // Test ArraySize on nested vectors
  // Create: [[1, 2], [3, 4, 5]]
  Value nested = Value({Value({Value(1.0), Value(2.0)}),
                        Value({Value(3.0), Value(4.0), Value(5.0)})});

  // Get length of outer vector
  EXPECT_EQ(nested.ArraySize(), 2);

  // Get length of first inner vector
  Value inner1 = nested.GetArrayElement(0);
  EXPECT_EQ(inner1.ArraySize(), 2);

  // Get length of second inner vector
  Value inner2 = nested.GetArrayElement(1);
  EXPECT_EQ(inner2.ArraySize(), 3);
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

// Regression for #1262: large integers must round-trip without precision loss.
TEST_F(ValueTest, FormatDoublePreservesLargeIntegers) {
  EXPECT_EQ(FormatDouble(20260201.0), "20260201");
  EXPECT_EQ(FormatDouble(20260202.0), "20260202");
  EXPECT_EQ(FormatDouble(202602011234.0), "202602011234");
  EXPECT_EQ(FormatDouble(1.0), "1");
  EXPECT_EQ(FormatDouble(0.5), "0.5");
  EXPECT_EQ(FormatDouble(0.0), "0");
  EXPECT_EQ(Value(20260201.0).AsString().value(), "20260201");
}

}  // namespace valkey_search::expr
