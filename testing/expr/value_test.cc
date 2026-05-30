/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/expr/value.h"

#include <cmath>
#include <cstdint>
#include <cstring>

#include "gtest/gtest.h"
#include "src/valkey_search_options.h"
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
  };

  std::vector<Testcase> t{{Value(), true, false, false, false},
                          {Value(false), false, true, false, false},
                          {Value(true), false, true, false, false},
                          {Value(0.0), false, false, true, false},
                          {Value(1.0), false, false, true, false},
                          {Value(std::numeric_limits<double>::infinity()),
                           false, false, true, false},
                          {Value(-std::numeric_limits<double>::infinity()),
                           false, false, true, false},
                          {Value(std::nan("a nan")), false, false, true, false},
                          {Value(std::string("")), false, false, false, true},
                          {Value(std::string("a")), false, false, false, true},
                          {Value(std::nan("nan")), false, false, true, false}};

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
}  // namespace valkey_search::expr
