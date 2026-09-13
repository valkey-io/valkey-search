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
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace expr {

struct Record : public Expression::Record {
  std::map<std::string, Value> attrs;
};

class ExprTest : public vmsdk::ValkeyTest {
 protected:
  struct Ref : public Expression::AttributeReference {
    Ref(const absl::string_view s) : name_(s) {}
    std::string name_;
    void Dump(std::ostream& os) const override { os << name_; }
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
    std::set<std::string> known_attr{"one", "two", "notfound"};
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
      return std::make_unique<Ref>(s);
    }
    absl::StatusOr<Value> GetParam(const absl::string_view s) const override {
      if (params.find(s) != params.end()) {
        return Value(params.find(s)->second);
      } else {
        return absl::NotFoundError("param not found");
      }
    }
    bool UseFilterComparisonSemantics() const override { return false; }
  } cc;
  std::unique_ptr<Record> record_;

  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    record_ = std::make_unique<Record>();
    record_->attrs["one"] = Value(1.0);
    record_->attrs["two"] = Value(2.0);
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

}  // namespace expr
}  // namespace valkey_search
