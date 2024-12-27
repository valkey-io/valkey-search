#include "src/expr/value.h"
#include "src/expr/expr.h"

#include "gtest/gtest.h"

#include <cmath>
#include <map>
#include <set>

namespace valkey_search { namespace expr {

class ExprTest : public testing::Test {
 protected:

 struct Ref : public Expression::AttributeReference {
   Ref(const std::string& s) : name_(s) {}
   std::string name_;
   virtual Value getValue(Expression::EvalContext &ctx, const Expression::AttrValueSet &the_attrs) const {
    const Attrs& attrs = reinterpret_cast<const Attrs&>(the_attrs);
    auto itrs = attrs.attrs.find(name_);
    if (itrs != attrs.attrs.end()) {
      return itrs->second;
    } else {
      return Value();
    }
   }
 };

 struct CompileContext : public Expression::CompileContext {
    std::set<std::string> known_attr{"one", "two", "notfound"};
    virtual std::optional<std::unique_ptr<Expression::AttributeReference>> make_reference(const std::string& s) {
      auto itr = known_attr.find(s);
      if (itr == known_attr.end()) {
        return std::nullopt;
      } else {
        return make_unique<Ref>(s);
      }
    }
 } cc;
 struct Attrs : public Expression::AttrValueSet {
  std::map<std::string, Value> attrs;
 } attrs;

  void SetUp() override {
    attrs.attrs["one"] = std::move(Value(1.0));
    attrs.attrs["two"] = std::move(Value(2.0));
  }
  void TearDown() override { }

};

TEST_F(ExprTest, TypesTest) {
  std::vector<std::pair<std::string, std::optional<Value>>> x = {
    {"1", Value(1.0)},
    {".5", Value(0.5)},
    {"1+1", Value(2.0)},
    {"1+1-2", Value(0.0)},
    {"1*1+3", Value(4.0)},
    {" 1 ", Value(1.0)},
    {" 1 + 1 ", Value(2.0)},
    {" 1 + 1 -2", Value(0.0)},
    {" 1 *1+ 3", Value(4.0)},
    {" (1)", Value(1.0)},
    {" 1+(2*3)", Value(7.0)},
    {" -1+(2*3)", Value(5.0)},
    {" 1+2", Value(3.0)},
    { "@one", Value(1.0) },
    { "@two", Value(2.0) },
    { "floor(1+1/2)", Value(1.0)},
    { " ceil(1 + 1 / 2)", Value(2.0)},
    { " '1' ", Value("1") },
    { " startswith('11', '1')", Value(true)},
    {"exists(@notfound)", Value(false)},
    {"exists(@one)", Value(true)},
    {"exists(@xx)", std::nullopt},
    {"log(1.0)", Value(0.0) },
    {"abs(-1.0)", Value(1.0) },
    {"sqrt(4.0)", Value(2.0) },
    {"exp(0.0)", Value(1.0) },
    {"log2(4.0)", Value(2.0) },
    {"substr('', 1, 1)", Value() },
    {"substr('abc', 1, 1)", Value("b")},
    {"substr('abc', -1, 1)", Value("c")},
    {"substr('abc', 1, 2)", Value("bc")},
    {"substr('abc', -1, 2)", Value()},
    {"substr('abc', -2, 2)", Value("bc")},
    {"substr('abc', 3, 0)", Value("")},
    {"substr('abc', 3, 1)", Value()},
    {"lower('A')", Value("a")},
    {"upper('a')", Value("A")},
    {"contains('abc', '')", Value(4.0)},
    {"contains('abc', '1')", Value(0.0)},
    {"contains('abcabc', 'abc')", Value(2.0)},
    {"strlen('')", Value(0.0) },
    {"strlen('a')", Value(1.0) },
  };
  for (auto& c : x) {
    std::cout << "Doing expression: '" << c.first << "' => '";
    if (c.second) {
      std::cout << *(c.second) << "'\n";
    } else {
      std::cout << " Error\n";
    }
    auto e = Expression::compile(cc, c.first);
    if (e.ok()) {
      std::cerr << "Compiled expression: " << c.first << " is: ";
      (*e)->dump(std::cerr);
      std::cerr << std::endl;
      Expression::EvalContext ec;
      auto v = (*e)->evaluate(ec, attrs);
      ASSERT_TRUE(c.second);
      EXPECT_EQ(v, c.second);
    } else {
      std::cout << "Failed to compile:" << e << "\n";
      EXPECT_FALSE(c.second);
    }
  }
}

}}