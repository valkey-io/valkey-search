/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/expr/expr.h"

#include <map>
#include <set>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "src/expr/value.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace expr {

struct Record : public Expression::Record {
  std::map<std::string, Value> attrs;
};

class ExprTest : public vmsdk::ValkeyTest {
 protected:
  struct Ref : public Expression::AttributeReference {
    Ref(const absl::string_view s, Expression::Type type)
        : name_(s), type_(type) {}
    std::string name_;
    Expression::Type type_;
    void Dump(std::ostream& os) const override { os << name_; }
    Expression::Type GetResultType() const override { return type_; }
    Value GetValue(Expression::EvalContext& ctx,
                   const Expression::Record& record) const override {
      const Record& attrs = reinterpret_cast<const Record&>(record);
      auto itr = attrs.attrs.find(name_);
      if (itr != attrs.attrs.end()) {
        return itr->second;
      } else {
        return Value{};
      }
    }
  };

  struct CompileContext : public Expression::CompileContext {
    std::set<std::string> known_attr{"one", "two", "notfound", "s10", "d10"};
    // Attributes not listed here hold numbers. `d10` is declared a string but
    // holds the number 10, so a test can tell the compile-time choice of
    // comparison from the runtime one.
    std::map<std::string, Expression::Type> attr_types{
        {"s10", Expression::Type::kString}, {"d10", Expression::Type::kString}};
    absl::flat_hash_map<std::string, std::string> params{{"one", "1"},
                                                         {"two", "2"}};
    absl::StatusOr<std::unique_ptr<Expression::AttributeReference>>
    MakeReference(const absl::string_view s, bool create) override {
      auto itr = known_attr.find(std::string(s));
      if (itr == known_attr.end()) {
        if (create) {
          itr = known_attr.insert(std::string(s)).first;
        } else {
          return absl::NotFoundError("not found");
        }
      }
      auto type = attr_types.find(std::string(s));
      return std::make_unique<Ref>(s, type == attr_types.end()
                                          ? Expression::Type::kNumber
                                          : type->second);
    }
    absl::StatusOr<Value> GetParam(const absl::string_view s) const override {
      if (params.find(s) != params.end()) {
        return Value(params.find(s)->second);
      } else {
        return absl::NotFoundError("param not found");
      }
    }
    bool UseFilterComparisonSemantics() const override { return false; }
    // Off by default: the rest of these tests pin the current behavior,
    // whatever search.emulate-release says.
    bool honors_emulate_release = false;
    bool HonorsEmulateRelease() const override {
      return honors_emulate_release;
    }
  } cc;
  std::unique_ptr<Record> record_;

  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    record_ = std::make_unique<Record>();
    record_->attrs["one"] = Value(1.0);
    record_->attrs["two"] = Value(2.0);
    record_->attrs["s10"] = Value("10");
    record_->attrs["d10"] = Value(10.0);
  }
};

TEST_F(ExprTest, TypesTest) {
  std::vector<std::pair<std::string, std::optional<Value>>> x = {
      {"1<=2<=3", Value(1.0)},
      {"1==2==3", Value(0.0)},
      {"1>=2>=3", Value(0.0)},
      {"1!=2!=3", Value(1.0)},
      {"1--1-1", Value(1.0)},
      {"1--1+1", Value(3.0)},
      {"1+-1<1", Value(1.0)},
      {"1+-1<=1", Value(1.0)},
      {"1+-1==1", Value(0.0)},
      {"1+-1!=1", Value(1.0)},
      {"1+-1>=1", Value(0.0)},
      {"0*0^0", Value(1.0)},
      {"2*-2^4", Value(256.0)},
      {"2/-2*4", Value(-4.0)},
      {"2/-2/4", Value(-.25)},
      {"2/-2^4", Value(1.0)},
      {"0/0<0", Value(0.0)},
      {"1", Value(1.0)},
      {".5", Value(0.5)},
      {"1+1", Value(2.0)},
      {"1+1-2", Value(0.0)},
      {"1*1+3", Value(4.0)},
      {" 1 ", Value(1.0)},
      {" 1 + 1 ", Value(2.0)},
      {" 1 + 1 -2", Value(0.0)},
      {" 1 *1+ 3", Value(4.0)},
      {"1 - -1 -1", Value(1.0)},
      {" (1)", Value(1.0)},
      {" 1+(2*3)", Value(7.0)},
      {" -1+(2*3)", Value(5.0)},
      {" 1+2", Value(3.0)},
      {"@one", Value(1.0)},
      {"@two", Value(2.0)},
      {"floor(1+1/2)", Value(1.0)},
      {" ceil(1 + 1 / 2)", Value(2.0)},
      {" '1' ", Value("1")},
      {" startswith('11', '1')", Value(true)},
      {"exists(@notfound)", Value(false)},
      {"exists(@one)", Value(true)},
      {"exists(@xx)", std::nullopt},
      {"log(1.0)", Value(0.0)},
      {"abs(-1.0)", Value(1.0)},
      {"sqrt(4.0)", Value(2.0)},
      {"exp(0.0)", Value(1.0)},
      {"log2(4.0)", Value(2.0)},
      {"substr('', 1, 1)", Value("")},
      {"substr('abc', 1, 1)", Value("b")},
      {"substr('abc', -1, 1)", Value("c")},
      {"substr('abc', 1, 2)", Value("bc")},
      {"substr('abc', -1, 2)", Value("c")},
      {"substr('abc', -2, 2)", Value("bc")},
      {"substr('abc', 3, 0)", Value("")},
      {"substr('abc', 3, 1)", Value("")},
      {"substr('abc', 2, 10)", Value("c")},
      {"lower('A')", Value("a")},
      {"upper('a')", Value("A")},
      {"contains('abc', '')", Value(4.0)},
      {"contains('abc', '1')", Value(0.0)},
      {"contains('abcabc', 'abc')", Value(2.0)},
      {"strlen('')", Value(0.0)},
      {"strlen('a')", Value(1.0)},
      {"concat()", Value("")},
      {"concat('a')", Value("a")},
      {"concat('b','')", Value("b")},
      {"concat('a', 'b')", Value("ab")},
      {"concat('ab', 'cd', 'ef')", Value("abcdef")},
      {"!0", Value(1.0)},
      {"!1", Value(0.0)},
      {"!1+1", Value(1.0)},
      {"!1!=1", Value(1.0)},
      {"$one", Value("1")},
      {"$one+1", Value(2.0)},
      {"1>2", Value(0.0)},
      {"1<2", Value(1.0)},
      {"1>=2", Value(0.0)},
      {"1<=2", Value(1.0)},

  };
  for (auto& c : x) {
    std::cout << "Doing expression: '" << c.first << "' => '";
    if (c.second) {
      std::cout << *(c.second) << "'\n";
    } else {
      std::cout << " Error\n";
    }
    auto e = Expression::Compile(cc, c.first);
    if (e.ok()) {
      std::cerr << "Compiled expression: " << c.first << " is: ";
      (*e)->Dump(std::cerr);
      std::cerr << std::endl;
      Expression::EvalContext ec;
      auto v = (*e)->Evaluate(ec, *record_);
      ASSERT_TRUE(c.second);
      EXPECT_EQ(v, c.second);
    } else {
      std::cout << "Failed to compile:" << e.status() << "\n";
      EXPECT_FALSE(c.second);
    }
  }
}

TEST_F(ExprTest, EmptyExpressionIsRejected) {
  for (absl::string_view expr : {"", " "}) {
    auto compiled = Expression::Compile(cc, expr);
    EXPECT_FALSE(compiled.ok())
        << "Expression unexpectedly compiled: '" << expr << "'";
  }
}

TEST_F(ExprTest, NotOperatorRequiresOperand) {
  for (absl::string_view expr : {"!", "! ", "!()"}) {
    auto compiled = Expression::Compile(cc, expr);
    EXPECT_FALSE(compiled.ok())
        << "Expression unexpectedly compiled: '" << expr << "'";
  }
}

TEST_F(ExprTest, ResultTypes) {
  using Type = Expression::Type;
  std::vector<std::pair<absl::string_view, Type>> cases = {
      {"1", Type::kNumber},
      {"'1'", Type::kString},
      {"$one", Type::kString},
      {"@one", Type::kNumber},
      {"@s10", Type::kString},
      {"!@s10", Type::kNumber},
      {"'a' + 'b'", Type::kNumber},
      {"'a' < 'b'", Type::kNumber},
      {"'a' && 'b'", Type::kNumber},
      {"'a' || 'b'", Type::kNumber},
      {"strlen('a')", Type::kNumber},
      {"exists(@s10)", Type::kNumber},
      {"parsetime('a')", Type::kNumber},
      {"lower(1)", Type::kString},
      {"upper(1)", Type::kString},
      {"substr(1, 0, 1)", Type::kString},
      {"concat(1, 2)", Type::kString},
      {"timefmt(1)", Type::kString},
      {"(lower(1))", Type::kString},
  };
  for (auto& [expr, type] : cases) {
    auto e = Expression::Compile(cc, expr);
    ASSERT_TRUE(e.ok()) << expr << ": " << e.status();
    EXPECT_EQ((*e)->GetResultType(), type) << expr;
  }
}

// The compiler, not the runtime Values, decides how a comparison compares: a
// number operand makes it numeric, otherwise a string operand makes it a
// string comparison. @d10 is declared a string but holds the number 10.
TEST_F(ExprTest, ComparisonTypeIsChosenAtCompileTime) {
  std::vector<std::pair<absl::string_view, Value>> cases = {
      {"@s10 < 9", Value(false)},     // 10 < 9
      {"@s10 < '9'", Value(true)},    // "10" < "9"
      {"@d10 < '9'", Value(true)},    // "10" < "9", though @d10 holds 10
      {"@d10 < 9", Value(false)},     // a number operand wins
      {"'10' < 9", Value(false)},     // a number literal wins
      {"@d10 == '10'", Value(true)},  // "10" == "10"
      {"lower(@s10) < 9", Value(false)}, {"lower(@s10) < '9'", Value(true)},
  };
  for (auto& [expr, expected] : cases) {
    auto e = Expression::Compile(cc, expr);
    ASSERT_TRUE(e.ok()) << expr << ": " << e.status();
    Expression::EvalContext ec;
    EXPECT_EQ((*e)->Evaluate(ec, *record_), expected) << expr;
  }
}

// A command that predates 1.3.0 keeps the runtime choice of comparison when
// search.emulate-release names an older release.
TEST_F(ExprTest, TypedComparisonFollowsEmulateRelease) {
  auto eval = [&](absl::string_view expr) {
    auto e = Expression::Compile(cc, expr);
    EXPECT_TRUE(e.ok()) << expr << ": " << e.status();
    Expression::EvalContext ec;
    return (*e)->Evaluate(ec, *record_);
  };
  const auto saved = options::GetEmulateRelease().GetValue();
  cc.honors_emulate_release = true;

  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue({1, 2, 0}));
  EXPECT_EQ(eval("@d10 < '9'"), Value(false));  // 10 < 9, from the runtime
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue({1, 3, 0}));
  EXPECT_EQ(eval("@d10 < '9'"), Value(true));  // "10" < "9"

  // A command new in 1.3.0 always compiles the typed comparison.
  cc.honors_emulate_release = false;
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue({1, 2, 0}));
  EXPECT_EQ(eval("@d10 < '9'"), Value(true));
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue(saved));
}

// Behaviors measured on Redis 8, where a number is never turned into a string.
class RedisearchSemanticsTest : public ExprTest {
 protected:
  Value Eval(absl::string_view expr) {
    auto e = Expression::Compile(cc, expr);
    EXPECT_TRUE(e.ok()) << expr << ": " << e.status();
    if (!e.ok()) {
      return Value(Value::Nil("compile failed"));
    }
    Expression::EvalContext ec;
    return (*e)->Evaluate(ec, *record_);
  }
};

// A numeric comparison against a string that is not a number does not fall
// back to comparing strings: < <= > >= are an error, == false and != true.
TEST_F(RedisearchSemanticsTest, ConversionFailureInAComparison) {
  for (absl::string_view expr :
       {"@one < 'abc'", "@one <= 'abc'", "@one > 'abc'", "@one >= 'abc'",
        "'abc' < 5", "5 < ' 7x'"}) {
    EXPECT_TRUE(Eval(expr).IsError()) << expr;
  }
  EXPECT_EQ(Eval("@one == 'abc'"), Value(false));
  EXPECT_EQ(Eval("@one != 'abc'"), Value(true));
  EXPECT_EQ(Eval("@one < '5'"), Value(true));  // '5' converts
}

// An error is the result of everything it reaches, except the side of && or
// || that is never evaluated.
TEST_F(RedisearchSemanticsTest, ErrorsPropagate) {
  for (absl::string_view expr :
       {"(@one < 'abc') + 1", "!(@one < 'abc')", "exists(@one < 'abc')",
        "abs(@one < 'abc')", "concat('a', @one < 'abc')", "(@one < 'abc') || 1",
        "1 && (@one < 'abc')", "0 || (@one < 'abc')"}) {
    EXPECT_TRUE(Eval(expr).IsError()) << expr;
  }
  EXPECT_EQ(Eval("1 || (@one < 'abc')"), Value(true));
  EXPECT_EQ(Eval("0 && (@one < 'abc')"), Value(false));
}

TEST_F(RedisearchSemanticsTest, StringFunctionsRejectNumbers) {
  for (absl::string_view expr :
       {"strlen(5)", "strlen(@one)", "startswith(@one, '1')",
        "startswith('15', 1)", "contains(@one, '1')", "contains('15', 1)",
        "substr(@one, 0, 1)", "substr('abc', '0', 1)",
        "substr('abc', 0, '1')"}) {
    EXPECT_TRUE(Eval(expr).IsError()) << expr;
  }
  EXPECT_EQ(Eval("strlen(@s10)"), Value(2.0));
  EXPECT_EQ(Eval("substr(@s10, 0, 1)"), Value("1"));
  EXPECT_TRUE(Eval("lower(5)").IsNull());
  EXPECT_TRUE(Eval("upper(@one)").IsNull());
  EXPECT_EQ(Eval("lower(@s10)"), Value("10"));
}

// lower() and upper() of a number give a null, which orders below every other
// value, equals another null, and exists.
TEST_F(RedisearchSemanticsTest, NullOrdersFirst) {
  EXPECT_EQ(Eval("lower(5) < 5"), Value(true));
  EXPECT_EQ(Eval("lower(5) > 5"), Value(false));
  EXPECT_EQ(Eval("5 > lower(5)"), Value(true));
  EXPECT_EQ(Eval("lower(5) < 'a'"), Value(true));
  EXPECT_EQ(Eval("lower(5) == 0"), Value(false));
  EXPECT_EQ(Eval("lower(5) != 0"), Value(true));
  EXPECT_EQ(Eval("lower(5) == lower(6)"), Value(true));
  EXPECT_EQ(Eval("exists(lower(5))"), Value(true));
  EXPECT_EQ(Eval("!lower(5)"), Value(true));
}

// FT.AGGREGATE keeps the older behavior when search.emulate-release names a
// release before 1.3.0.
TEST_F(RedisearchSemanticsTest, FollowsEmulateRelease) {
  const auto saved = options::GetEmulateRelease().GetValue();
  cc.honors_emulate_release = true;
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue({1, 2, 0}));
  EXPECT_EQ(Eval("@one < 'abc'"), Value(true));  // "1" < "abc"
  EXPECT_EQ(Eval("strlen(@one)"), Value(1.0));
  EXPECT_FALSE(Eval("lower(5)").IsNull());
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue({1, 3, 0}));
  EXPECT_TRUE(Eval("@one < 'abc'").IsError());
  EXPECT_TRUE(Eval("strlen(@one)").IsError());
  EXPECT_TRUE(Eval("lower(5)").IsNull());
  EXPECT_EQ(Eval("1 || (@one < 'abc')"), Value(true));
  VMSDK_EXPECT_OK(options::GetEmulateRelease().SetValue(saved));
}

// ---------------------------------------------------------------------------
// FT.CREATE FILTER semantics.
//
// ExprTest above pins APPLY semantics (UseFilterComparisonSemantics() ==
// false). This fixture is its FILTER counterpart: it compiles with the flag
// on, so CmpOp() selects the FilterFunc* comparisons rather than the APPLY
// ones. Without it the FILTER-only branches have no unit coverage at all and
// are exercised only by the compatibility suite, which needs Docker to run.
// ---------------------------------------------------------------------------
class FilterExprTest : public ExprTest {
 protected:
  struct FilterCompileContext : public CompileContext {
    bool UseFilterComparisonSemantics() const override { return true; }
  } fcc;

  // Compile `expr` under FILTER semantics and evaluate it against record_.
  Value Eval(absl::string_view expr) {
    auto compiled = Expression::Compile(fcc, expr);
    EXPECT_TRUE(compiled.ok())
        << "failed to compile '" << expr << "': " << compiled.status();
    if (!compiled.ok()) {
      return Value(Value::Nil("compile failed"));
    }
    Expression::EvalContext ec;
    return (*compiled)->Evaluate(ec, *record_);
  }

  void SetUp() override {
    ExprTest::SetUp();
    // "missing" is resolvable at compile time but absent at evaluation time,
    // so Ref::GetValue returns a Nil -- the same shape a FILTER sees for a
    // field the document does not carry.
    fcc.known_attr.insert("missing");
  }
};

// A comparison involving a missing field is FALSE, not "unknown". The
// document is simply not admitted; nothing propagates.
TEST_F(FilterExprTest, MissingFieldComparisonIsFalse) {
  for (absl::string_view expr :
       {"@missing == 1", "@missing != 1", "@missing < 1", "@missing <= 1",
        "@missing > 1", "@missing >= 1"}) {
    auto v = Eval(expr);
    EXPECT_FALSE(v.IsNil()) << "'" << expr << "' must not yield Nil";
    EXPECT_EQ(v, Value(false)) << "'" << expr << "' must be false";
  }
  // A present field still produces a definite answer.
  EXPECT_EQ(Eval("@one == 1"), Value(true));
  EXPECT_EQ(Eval("@one == 2"), Value(false));
}

// Negating a missing-field comparison gives true, because the comparison was
// false. This is what admits every key for `!(@absent == 'x')`.
TEST_F(FilterExprTest, NegationOfMissingFieldIsTrue) {
  EXPECT_EQ(Eval("!(@missing == 1)"), Value(true));
  EXPECT_EQ(Eval("!(@missing != 1)"), Value(true));
  EXPECT_EQ(Eval("!(@one == 1)"), Value(false));
  EXPECT_EQ(Eval("!(@one == 2)"), Value(true));
}

// Two-valued && / ||, and order-insensitive -- there is no unknown left to
// propagate, so swapping the operands cannot change the answer. Regression
// guard for the three-valued FilterLogical node this replaced, which made
// `false && missing` differ from `missing && false`.
TEST_F(FilterExprTest, LogicalOperatorsAreTwoValuedAndOrderInsensitive) {
  EXPECT_EQ(Eval("(@one == 2) && (@missing == 1)"), Value(false));
  EXPECT_EQ(Eval("(@missing == 1) && (@one == 2)"), Value(false));

  EXPECT_EQ(Eval("(@one == 1) || (@missing == 1)"), Value(true));
  EXPECT_EQ(Eval("(@missing == 1) || (@one == 1)"), Value(true));
  EXPECT_EQ(Eval("(@missing == 1) || (@one == 2)"), Value(false));

  // A definite operand on both sides behaves normally.
  EXPECT_EQ(Eval("(@one == 1) && (@two == 2)"), Value(true));
  EXPECT_EQ(Eval("(@one == 1) && (@two == 1)"), Value(false));
  EXPECT_EQ(Eval("(@one == 2) || (@two == 2)"), Value(true));
}

// An unordered comparison -- reachable only via a NaN, since Nil is guarded
// above and no filter attribute reference yields an array -- reads as equal,
// matching Redisearch: ==, <= and >= are true while !=, < and > are false.
// Regression guard for FilterFuncNe, which used to answer != as true here
// and so admitted a document that Redisearch rejects. `0/0` is the NaN.
TEST_F(FilterExprTest, UnorderedComparisonReadsAsEqual) {
  EXPECT_EQ(Eval("(0/0) == 0"), Value(true));
  EXPECT_EQ(Eval("(0/0) != 0"), Value(false));
  EXPECT_EQ(Eval("(0/0) <= 0"), Value(true));
  EXPECT_EQ(Eval("(0/0) >= 0"), Value(true));
  EXPECT_EQ(Eval("(0/0) < 0"), Value(false));
  EXPECT_EQ(Eval("(0/0) > 0"), Value(false));

  // Unlike a missing field, a NaN is a real computed value: it never yields
  // the Nil that would keep the document.
  EXPECT_FALSE(Eval("(0/0) == 0").IsNil());
  EXPECT_FALSE(Eval("(0/0) != 0").IsNil());
}

// FILTER chooses the comparison type at compile time too, keeping its Nil
// and number-versus-non-numeric-string answers.
TEST_F(FilterExprTest, ComparisonTypeIsChosenAtCompileTime) {
  EXPECT_EQ(Eval("@s10 < 9"), Value(false));
  EXPECT_EQ(Eval("@s10 < '9'"), Value(true));
  EXPECT_EQ(Eval("@d10 < '9'"), Value(true));
  EXPECT_EQ(Eval("@missing < '9'"), Value(false));
  EXPECT_EQ(Eval("@one != 'abc'"), Value(true));
  EXPECT_EQ(Eval("@one == 'abc'"), Value(false));
}

// A conversion failure is an error, not false, so it rejects the document
// even under a negation -- Redis admits nothing for `!(@t < 5)` either.
TEST_F(FilterExprTest, ConversionFailureRejectsTheDocument) {
  EXPECT_TRUE(Eval("@one < 'abc'").IsError());
  EXPECT_TRUE(Eval("!(@one < 'abc')").IsError());
  EXPECT_FALSE(Eval("!(@one < 'abc')").IsTrue());
  EXPECT_EQ(Eval("!(0 && (@one < 'abc'))"), Value(true));
  // A missing field is still just false.
  EXPECT_EQ(Eval("!(@missing < 'abc')"), Value(true));
  EXPECT_EQ(Eval("lower(@one) < 5"), Value(true));
}

}  // namespace expr
}  // namespace valkey_search
