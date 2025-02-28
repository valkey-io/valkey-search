#include "src/expr/expr.h"

#include <map>
#include <set>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "src/expr/value.h"

namespace valkey_search {
namespace expr {

struct Record : public Expression::Record {
  std::map<std::string, Value> attrs;
};

class ExprTest : public testing::Test {
 protected:
  struct Ref : public Expression::AttributeReference {
    Ref(const absl::string_view s) : name_(s) {}
    std::string name_;
    void Dump(std::ostream& os) const override { os << name_; }
    Value GetValue(Expression::EvalContext& ctx,
                   const Expression::Record& record) const override {
      const Record& attrs = reinterpret_cast<const Record&>(record);
      auto itrs = attrs.attrs.find(name_);
      if (itrs != attrs.attrs.end()) {
        return itrs->second;
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
  } cc;
  std::unique_ptr<Record> record_;

  void SetUp() override {
    record_ = std::make_unique<Record>();
    record_->attrs["one"] = Value(1.0);
    record_->attrs["two"] = Value(2.0);
  }
  void TearDown() override {}
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
      {"substr('', 1, 1)", Value()},
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

}  // namespace expr
}  // namespace valkey_search