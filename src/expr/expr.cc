/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/expr/expr.h"

#include <ctime>
#include <map>
#include <memory>
#include <optional>
#include <utility>

#include "absl/strings/str_cat.h"
#include "src/utils/scanner.h"
#include "src/valkey_search_options.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr

namespace valkey_search {
namespace expr {

using ExprPtr = std::unique_ptr<Expression>;
constexpr absl::string_view kInvalidOrMissingExpression{
    "Invalid or missing expression"};

// 1.3.0 fix shared by every expression site: a reference to a field the key
// does not have propagates as missing, rather than the operator or function
// substituting a reason of its own -- which read as "evaluated to nothing" and
// so kept the record and the alias. Returns true when the new (propagating)
// behavior should be taken. Single counter for all the sites below.
static bool MissingPropagates() {
  return VALKEY_SEARCH_COMPATIBILITY_FIX(
      1, 3, 0, "expr_missing_propagates", [] { return true; },
      [] { return false; });
}

struct Constant : Expression {
  Constant(std::string constant)
      : constant_(std::move(constant)), type_(Type::kString) {}
  Constant(double constant) : constant_(constant), type_(Type::kNumber) {}
  Value Evaluate(EvalContext &ctx, const Record &record) const override {
    return constant_;
  }
  Type GetResultType() const override { return type_; }
  void Dump(std::ostream &os) const override {
    os << "Constant(" << constant_ << ")";
  }

 private:
  Value constant_;
  Type type_;
};

struct Parameter : Expression {
  Parameter(std::string &&name, Value &&value)
      : name_(std::move(name)), value_(std::move(value)) {}
  Value Evaluate(EvalContext &ctx, const Record &record) const override {
    return value_;
  }
  // PARAMS values always arrive as strings.
  Type GetResultType() const override { return Type::kString; }
  void Dump(std::ostream &os) const override {
    os << "$" << name_ << "(" << value_ << ")";
  }

 private:
  std::string name_;
  Value value_;
};

struct AttributeValue : Expression {
  AttributeValue(std::string identifier,
                 std::unique_ptr<AttributeReference> ref)
      : identifier_(std::move(identifier)), ref_(std::move(ref)) {}
  Value Evaluate(EvalContext &ctx, const Record &record) const override {
    return ref_->GetValue(ctx, record);
  }
  Type GetResultType() const override { return ref_->GetResultType(); }
  void Dump(std::ostream &os) const override { os << '@' << identifier_; }

 private:
  std::string identifier_;
  std::unique_ptr<AttributeReference> ref_;
};

struct Not : Expression {
  explicit Not(ExprPtr &&p) : expr_(std::move(p)) {}
  Value Evaluate(EvalContext &ctx, const Record &record) const override {
    auto value = expr_->Evaluate(ctx, record);
    // An error is not a falsy value: negating it must not turn it into true.
    if (value.IsError()) {
      return value;
    }
    // No FILTER special case here: a missing field already made its
    // comparison false (FilterFunc* in value.cc), so negating it gives true,
    // which is what Redisearch answers for `!(@absent == 'x')`.
    //
    // AsBool reads a nil as false, so without this `!(@absent)` answers true
    // -- a wrong value rather than merely an unpropagated one.
    if (value.IsMissing() && MissingPropagates()) {
      return Value::Missing();
    }
    // AsBool has a result for every other alternative, so the optional is
    // always engaged; the dereference is safe.
    return Value(!*value.AsBool());
  }
  Type GetResultType() const override { return Type::kNumber; }
  void Dump(std::ostream &os) const override {
    os << '!';
    expr_->Dump(os);
  }

 private:
  ExprPtr expr_;
};

struct FunctionTableEntry;

struct FunctionCall : Expression {
  using Func = Value (*)(EvalContext &ctx, const Record &record,
                         const absl::InlinedVector<ExprPtr, 4> &params);
  static absl::StatusOr<const FunctionTableEntry *> LookUpAndValidate(
      const std::string &name, const absl::InlinedVector<ExprPtr, 4> &params);
  FunctionCall(std::string name, Func func, Type type,
               absl::InlinedVector<ExprPtr, 4> params)
      : name_(std::move(name)),
        func_(func),
        type_(type),
        params_(std::move(params)) {}
  Value Evaluate(EvalContext &ctx, const Record &record) const override {
    return (*func_)(ctx, record, params_);
  }
  Type GetResultType() const override { return type_; }
  void Dump(std::ostream &os) const override {
    os << name_ << '(';
    for (auto &p : params_) {
      if (&p != &params_[0]) {
        os << ',';
      }
      p->Dump(os);
    }
    os << ')';
  }

 private:
  std::string name_;
  Func func_;
  Type type_;
  absl::InlinedVector<ExprPtr, 4> params_;
};

// An error in any argument is the call's result, ahead of everything else.
template <typename... Values>
static const Value *FirstError(const Values &...values) {
  const Value *error = nullptr;
  ((error = error ? error : (values.IsError() ? &values : nullptr)), ...);
  return error;
}

// Redisearch drops a record whose expression reached for a field the key does
// not have, so a missing argument short-circuits the whole call. exists() is
// the exception: answering that question *is* its job, so it asks for the
// missing value with kMissingIsAnArgument.
constexpr bool kMissingIsAnArgument = true;

template <Value (*func1)(const Value &o), bool pass_missing = false>
Value MonadicFunctionProxy(
    Expression::EvalContext &ctx, const Expression::Record &record,
    const absl::InlinedVector<expr::ExprPtr, 4> &params) {
  CHECK(params.size() == 1);
  auto value = params[0]->Evaluate(ctx, record);
  if (value.IsError()) {
    return value;
  }
  if (!pass_missing && value.IsMissing() && MissingPropagates()) {
    return Value::Missing();
  }
  return (*func1)(value);
};

template <Value (*func2)(const Value &l, const Value &r)>
Value DyadicFunctionProxy(Expression::EvalContext &ctx,
                          const Expression::Record &record,
                          const absl::InlinedVector<expr::ExprPtr, 4> &params) {
  CHECK(params.size() == 2);
  auto l = params[0]->Evaluate(ctx, record);
  auto r = params[1]->Evaluate(ctx, record);
  if (auto error = FirstError(l, r)) {
    return *error;
  }
  if ((l.IsMissing() || r.IsMissing()) && MissingPropagates()) {
    return Value::Missing();
  }
  return (*func2)(l, r);
};

template <Value (*func3)(const Value &l, const Value &m, const Value &r)>
Value TriadicFunctionProxy(
    Expression::EvalContext &ctx, const Expression::Record &record,
    const absl::InlinedVector<expr::ExprPtr, 4> &params) {
  CHECK(params.size() == 3);
  auto l = params[0]->Evaluate(ctx, record);
  auto m = params[1]->Evaluate(ctx, record);
  auto r = params[2]->Evaluate(ctx, record);
  if (auto error = FirstError(l, m, r)) {
    return *error;
  }
  if ((l.IsMissing() || m.IsMissing() || r.IsMissing()) &&
      MissingPropagates()) {
    return Value::Missing();
  }
  return (*func3)(l, m, r);
};

using Func = Value (*)(Expression::EvalContext &ctx,
                       const Expression::Record &record,
                       const absl::InlinedVector<ExprPtr, 4> &params);

// A null is a value, so it exists.
Value FuncExists(const Value &o) { return Value(!o.IsNil() || o.IsNull()); }

Value ProxyConcat(Expression::EvalContext &ctx,
                  const Expression::Record &record,
                  const absl::InlinedVector<expr::ExprPtr, 4> &params) {
  absl::InlinedVector<Value, 4> values;
  for (auto &p : params) {
    values.emplace_back(p->Evaluate(ctx, record));
    if (values.back().IsError()) {
      return values.back();
    }
  }
  return FuncConcat(values);
}

Value ProxyTimefmt(Expression::EvalContext &ctx,
                   const Expression::Record &record,
                   const absl::InlinedVector<expr::ExprPtr, 4> &params) {
  CHECK(!params.empty());
  Value fmt("%FT%TZ");
  if (params.size() > 1) {
    fmt = params[1]->Evaluate(ctx, record);
  }
  auto value = params[0]->Evaluate(ctx, record);
  if (auto error = FirstError(value, fmt)) {
    return *error;
  }
  if ((value.IsMissing() || fmt.IsMissing()) && MissingPropagates()) {
    return Value::Missing();
  }
  return FuncTimefmt(value, fmt);
}

Value ProxyParsetime(Expression::EvalContext &ctx,
                     const Expression::Record &record,
                     const absl::InlinedVector<expr::ExprPtr, 4> &params) {
  CHECK(!params.empty());
  Value fmt("%FT%TZ");
  if (params.size() > 1) {
    fmt = params[1]->Evaluate(ctx, record);
  }
  auto value = params[0]->Evaluate(ctx, record);
  if (auto error = FirstError(value, fmt)) {
    return *error;
  }
  if ((value.IsMissing() || fmt.IsMissing()) && MissingPropagates()) {
    return Value::Missing();
  }
  return FuncParsetime(value, fmt);
}

struct FunctionTableEntry {
  size_t min_argc;
  size_t max_argc;
  Func function;
  Expression::Type result;
  // The function as Redisearch has it, when that differs: it never turns a
  // number argument into a string.
  Func typed_function = nullptr;
};

constexpr auto kNum = Expression::Type::kNumber;
constexpr auto kStr = Expression::Type::kString;

static std::map<std::string, FunctionTableEntry> function_table{
    {"exists",
     {1, 1, &MonadicFunctionProxy<FuncExists, kMissingIsAnArgument>, kNum}},

    {"abs", {1, 1, &MonadicFunctionProxy<FuncAbs>, kNum}},
    {"ceil", {1, 1, &MonadicFunctionProxy<FuncCeil>, kNum}},
    {"exp", {1, 1, &MonadicFunctionProxy<FuncExp>, kNum}},
    {"floor", {1, 1, &MonadicFunctionProxy<FuncFloor>, kNum}},
    {"log", {1, 1, &MonadicFunctionProxy<FuncLog>, kNum}},
    {"log2", {1, 1, &MonadicFunctionProxy<FuncLog2>, kNum}},
    {"sqrt", {1, 1, &MonadicFunctionProxy<FuncSqrt>, kNum}},

    {"startswith",
     {2, 2, &DyadicFunctionProxy<FuncStartswith>, kNum,
      &DyadicFunctionProxy<FuncStartswithTyped>}},
    {"lower",
     {1, 1, &MonadicFunctionProxy<FuncLower>, kStr,
      &MonadicFunctionProxy<FuncLowerTyped>}},
    {"upper",
     {1, 1, &MonadicFunctionProxy<FuncUpper>, kStr,
      &MonadicFunctionProxy<FuncUpperTyped>}},
    {"strlen",
     {1, 1, &MonadicFunctionProxy<FuncStrlen>, kNum,
      &MonadicFunctionProxy<FuncStrlenTyped>}},
    {"substr",
     {3, 3, &TriadicFunctionProxy<FuncSubstr>, kStr,
      &TriadicFunctionProxy<FuncSubstrTyped>}},
    {"contains",
     {2, 2, &DyadicFunctionProxy<FuncContains>, kNum,
      &DyadicFunctionProxy<FuncContainsTyped>}},
    {"concat", {0, 50, &ProxyConcat, kStr}},

    {"dayofweek", {1, 1, &MonadicFunctionProxy<FuncDayofweek>, kNum}},
    {"dayofmonth", {1, 1, &MonadicFunctionProxy<FuncDayofmonth>, kNum}},
    {"dayofyear", {1, 1, &MonadicFunctionProxy<FuncDayofyear>, kNum}},
    {"monthofyear", {1, 1, &MonadicFunctionProxy<FuncMonthofyear>, kNum}},
    {"year", {1, 1, &MonadicFunctionProxy<FuncYear>, kNum}},
    {"minute", {1, 1, &MonadicFunctionProxy<FuncMinute>, kNum}},
    {"hour", {1, 1, &MonadicFunctionProxy<FuncHour>, kNum}},
    {"day", {1, 1, &MonadicFunctionProxy<FuncDay>, kNum}},
    {"month", {1, 1, &MonadicFunctionProxy<FuncMonth>, kNum}},

    {"timefmt", {1, 2, &ProxyTimefmt, kStr}},
    {"parsetime", {1, 2, &ProxyParsetime, kNum}},
};

absl::StatusOr<const FunctionTableEntry *> FunctionCall::LookUpAndValidate(
    const std::string &name, const absl::InlinedVector<ExprPtr, 4> &params) {
  auto it = function_table.find(name);
  if (it == function_table.end()) {
    return absl::NotFoundError(absl::StrCat("Function ", name, " is unknown"));
  }
  if (it->second.min_argc > params.size()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Function ", name, " expects at least ", it->second.min_argc,
        " arguments, but only ", params.size(), " were found."));
  }
  if (it->second.max_argc < params.size()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Function ", name, " expects no more than ", it->second.max_argc,
        " arguments, but ", params.size(), " were found."));
  }
  return &it->second;
}

//
// Dyadic Operator Precedence
//
// Highest:
//    MulOps    *, /
//    AddOps    +, -
//    CmpOps    >, >=, ==, !=, <, <=
//    AndOps    &&
//    LorOps    ||
//

struct Dyadic : Expression {
  using ValueFunc = Value (*)(const Value &, const Value &);
  Dyadic(ExprPtr lexpr, ExprPtr rexpr, ValueFunc func, absl::string_view name,
         bool short_circuit = false)
      : lexpr_(std::move(lexpr)),
        rexpr_(std::move(rexpr)),
        func_(func),
        name_(name),
        short_circuit_(short_circuit) {}
  Value Evaluate(EvalContext &ctx, const Record &record) const override {
    auto lvalue = lexpr_->Evaluate(ctx, record);
    if (lvalue.IsError()) {
      return lvalue;
    }
    // Redisearch evaluates the right side of && and || only when the left
    // side does not decide the answer, so an error there goes unnoticed:
    // `0 && (@n < 'abc')` is 0. AsBool always has a result, and reads a nil
    // as false.
    if (short_circuit_) {
      const bool left = *lvalue.AsBool();
      if (name_ == "&&" && !left) {
        return Value(false);
      }
      if (name_ == "||" && left) {
        return Value(true);
      }
    }
    auto rvalue = rexpr_->Evaluate(ctx, record);
    if (rvalue.IsError()) {
      return rvalue;
    }
    // Redisearch drops a record whose APPLY expression reached for a field the
    // key does not have, however deep in the expression that reference sat.
    // Without this the operator manufactures its own reason ("Add requires
    // numeric operands"), which reads as "evaluated to nothing" and keeps the
    // record. Safe only because a field named by a stage is now loaded
    // implicitly, so an unpopulated slot means the key really lacks it.
    // `&&` is the exception, measured against Redis 8: a missing operand
    // there is simply falsy, so the record is kept and the alias replies 0,
    // whichever side the reference sat on. Letting the operator run gives
    // exactly that, because AsBool() already reads a nil as false. Redis 8
    // also truncates the result stream after such a row, which is not
    // reproduced here; see known_differences.md.
    const bool logical_and = name_ == "&&";
    if (!logical_and && (lvalue.IsMissing() || rvalue.IsMissing()) &&
        MissingPropagates()) {
      return Value::Missing();
    }
    return (*func_)(lvalue, rvalue);
  }
  // Every dyadic operator yields a number: arithmetic, and the 0/1 of a
  // comparison or a logical operator.
  Type GetResultType() const override { return Type::kNumber; }
  void Dump(std::ostream &os) const override {
    os << '(';
    lexpr_->Dump(os);
    os << name_;
    rexpr_->Dump(os);
    os << ')';
  }

 private:
  ExprPtr lexpr_;
  ExprPtr rexpr_;
  ValueFunc func_;
  absl::string_view name_;
  bool short_circuit_;
};

bool IsIdentifierChar(int c) {
  return c != EOF && (std::isalnum(c) || c == '_');
}

struct DepthGuard {
  int &depth;
  DepthGuard(int &d) : depth(d) { ++depth; }
  ~DepthGuard() { --depth; }
};

struct Compiler {
  int depth_ = 0;
  utils::Scanner s_;
  Compiler(absl::string_view sv) : s_(sv) {}

  using CompileContext = Expression::CompileContext;

  using ParseFunc = absl::StatusOr<ExprPtr> (Compiler::*)(CompileContext &ctx);

  // An operator's spelling and implementation. A comparison also carries
  // type-specific implementations; the operand types pick one of them.
  struct DyadicOp {
    absl::string_view name;
    Dyadic::ValueFunc func;
    Dyadic::ValueFunc number_func = nullptr;  // either operand is a number
    Dyadic::ValueFunc string_func = nullptr;  // either operand is a string
  };

  static Dyadic::ValueFunc SelectFunc(const DyadicOp &op, Expression::Type l,
                                      Expression::Type r) {
    using Type = Expression::Type;
    if (op.number_func && (l == Type::kNumber || r == Type::kNumber)) {
      return op.number_func;
    }
    if (op.string_func && (l == Type::kString || r == Type::kString)) {
      return op.string_func;
    }
    return op.func;
  }

  absl::StatusOr<ExprPtr> DoDyadic(CompileContext &ctx, ParseFunc func,
                                   const std::vector<DyadicOp> &ops) {
    utils::Scanner s = s_;
    DBG << "Start Dyadic: " << ops[0].name << " Remaining: '"
        << s_.GetUnscanned() << "'\n";
    VMSDK_ASSIGN_OR_RETURN(auto lvalue, (this->*func)(ctx));
    if (!lvalue) {
      DBG << "Dyadic Failed first: " << ops[0].name << "\n";
      return nullptr;
    }
    while (s_.SkipWhiteSpacePeekByte() != EOF) {
      s = s_;
      bool found = false;
      for (auto &op : ops) {
        DBG << "Dyadic looking for " << op.name
            << " Remaining: " << s_.GetUnscanned() << "\n";
        if (s_.SkipWhiteSpacePopWord(op.name)) {
          DBG << "Found " << op.name << "\n";
          VMSDK_ASSIGN_OR_RETURN(auto rvalue, (this->*func)(ctx));
          if (!rvalue) {
            // Error.
            return absl::InvalidArgumentError("Invalid or missing expression");
          } else {
            DBG << "Dyadic: " << lvalue << ' ' << op.name << ' ' << rvalue
                << " Remaining: '" << s_.GetUnscanned() << "'\n";
            auto func = SelectFunc(op, lvalue->GetResultType(),
                                   rvalue->GetResultType());
            const bool short_circuit =
                (op.name == "&&" || op.name == "||") &&
                (!ctx.HonorsEmulateRelease() ||
                 VALKEY_SEARCH_COMPATIBILITY_FIX(
                     1, 3, 0, "expr_short_circuit", [] { return true; },
                     [] { return false; }));
            lvalue =
                std::make_unique<Dyadic>(std::move(lvalue), std::move(rvalue),
                                         func, op.name, short_circuit);
            s = s_;
            found = true;
            break;
          }
        }
      }
      if (!found) {
        DBG << "Dyadic Not Found\n";
        break;
      }
    }
    return lvalue;
  }

  absl::StatusOr<ExprPtr> ParseParameter(CompileContext &ctx) {
    CHECK(s_.PopByte('$'));
    std::string param_name;
    while (IsIdentifierChar(s_.PeekByte())) {
      param_name += s_.NextByte();
    }
    VMSDK_ASSIGN_OR_RETURN(auto param_value, ctx.GetParam(param_name));
    return std::make_unique<Parameter>(std::move(param_name),
                                       std::move(param_value));
  }

  absl::StatusOr<ExprPtr> Invert(CompileContext &ctx) {
    CHECK(s_.PopByte('!'));
    VMSDK_ASSIGN_OR_RETURN(auto expr, Primary(ctx));
    if (!expr) {
      return absl::InvalidArgumentError(kInvalidOrMissingExpression);
    }
    return std::make_unique<Not>(std::move(expr));
  }

  absl::StatusOr<ExprPtr> Primary(CompileContext &ctx) {
    DepthGuard guard(depth_);
    if (depth_ > options::GetQueryStringDepth().GetValue()) {
      return absl::InvalidArgumentError("Expression too complex");
    }
    s_.SkipWhiteSpace();
    DBG << "Primary: '" << s_.GetUnscanned() << "'\n";
    switch (s_.PeekByte()) {
      case '(': {
        CHECK(s_.PopByte('('));
        auto result = LorOp(ctx);
        if (!s_.SkipWhiteSpacePopByte(')')) {
          return absl::InvalidArgumentError(absl::StrCat(
              "Expected ')' at or near position ", s_.GetPosition()));
        } else {
          return result;
        }
      };
      case '+':
      case '-':
      case '.':
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9': {
        return Number(ctx);
      };
      case '!':
        return Invert(ctx);
      case '@':
        return Attribute(ctx);
      case '\'':
      case '"':
        return QuotedString(ctx);
      case '$':
        return ParseParameter(ctx);
      case utils::Scanner::kEOF:
        return nullptr;
      default:
        return ParseFunctionCall(ctx);
    }
  }
  absl::StatusOr<ExprPtr> Attribute(CompileContext &ctx) {
    CHECK(s_.PopByte('@'));
    size_t pos = s_.GetPosition();
    std::string identifier;
    while (IsIdentifierChar(s_.PeekUtf8())) {
      utils::Scanner::PushBackUtf8(identifier, s_.NextUtf8());
    }
    DBG << "Identifier: " << identifier << " Remainder:'" << s_.GetUnscanned()
        << "'\n";
    VMSDK_ASSIGN_OR_RETURN(auto ref, ctx.MakeReference(identifier, false),
                           _ << " near position " << pos);
    return std::make_unique<AttributeValue>(identifier, std::move(ref));
  }
  absl::StatusOr<ExprPtr> ParseFunctionCall(CompileContext &ctx) {
    std::string name;
    utils::Scanner s = s_;
    while (IsIdentifierChar(s_.PeekByte())) {
      name.push_back(s_.NextByte());
    }
    DBG << "Function Name: " << name << "\n";
    if (!s_.SkipWhiteSpacePopByte('(')) {
      s_ = s;
      return nullptr;
    }
    absl::InlinedVector<ExprPtr, 4> params;
    while (1) {
      DBG << "Scanning for Parameter " << params.size() << " : "
          << s_.GetUnscanned() << "\n";
      s_.SkipWhiteSpace();
      if (s_.PopByte(')')) {
        VMSDK_ASSIGN_OR_RETURN(auto entry,
                               FunctionCall::LookUpAndValidate(name, params));
        DBG << "After function call: '" << s_.GetUnscanned() << "'\n";
        auto func = entry->function;
        if (entry->typed_function &&
            (!ctx.HonorsEmulateRelease() ||
             VALKEY_SEARCH_COMPATIBILITY_FIX(
                 1, 3, 0, "expr_string_fn_types", [] { return true; },
                 [] { return false; }))) {
          func = entry->typed_function;
        }
        return std::make_unique<FunctionCall>(std::move(name), func,
                                              entry->result, std::move(params));
      } else if (!params.empty() && !s_.PopByte(',')) {
        DBG << "func_call found comma\n";
        return absl::NotFoundError(
            absl::StrCat("Expected , or ) near position ", s_.GetPosition()));
      } else {
        DBG << "func_call scan for actual Parameter: " << s_.GetUnscanned()
            << "\n";
        VMSDK_ASSIGN_OR_RETURN(auto param, ParseExpression(ctx));
        if (!param) {
          return absl::InvalidArgumentError(
              absl::StrCat("Expected , or ) near position ", s_.GetPosition()));
        }
        params.emplace_back(std::move(param));
      }
    }
  }
  absl::StatusOr<ExprPtr> Number(CompileContext &ctx) {
    DBG << "Number Start: '" << s_.GetUnscanned() << "'\n";
    auto num = s_.PopDouble();
    if (!num) {
      return nullptr;
    }
    DBG << "Number End(" << (*num) << "): Remaining: '" << s_.GetUnscanned()
        << "'\n";
    return std::make_unique<Constant>(*num);
  }
  absl::StatusOr<ExprPtr> QuotedString(CompileContext &ctx) {
    std::string str;
    int start_byte = s_.NextByte();
    while (s_.PeekByte() != start_byte) {
      int this_byte = s_.NextByte();
      if (this_byte == '\\') {
        // todo Parse Unicode escape sequence
        this_byte = s_.NextByte();
      }
      if (this_byte == EOF) {
        return absl::InvalidArgumentError(
            absl::StrCat("Missing trailing quote"));
      }
      str.push_back(char(this_byte));
    }
    CHECK(s_.PopByte(start_byte));
    DBG << "QuotedString('" << str << "'): Remaining:'" << s_.GetUnscanned()
        << "'\n";
    return std::make_unique<Constant>(std::move(str));
  }
  //
  // The recursive descent parser
  //
  // The precedence ordering is (highest to lowest)
  //
  //  Primary: Number, Field, ()
  //  PowOp:   ^                 (Redisearch-compatible since 1.3.0)
  //  MulOp:   * /
  //  AddOp:   + -
  //  CmpOp:   < <= == != > >=
  //  LandOp:  &&                (Redisearch-compatible since 1.3.0)
  //  LorOp:   ||
  //
  // Pre-1.3.0 valkey-search put `^` at the same precedence as `*` and `/`,
  // making expressions like `@a*@b^@c` parse as `(@a*@b)^@c` instead of the
  // Redisearch-compatible `@a*(@b^@c)`. Pre-1.3.0 also put `&&` and `||` at
  // the same level, making `@a||@b&&@c` parse as `(@a||@b)&&@c` instead of
  // the C/SQL-standard `@a||(@b&&@c)` that Redisearch follows. Both fixes are
  // gated by search.emulate-release per COMPATIBILITY.md.
  absl::StatusOr<ExprPtr> LorOp(CompileContext &ctx) {
    static const std::vector<DyadicOp> kFixedLorOps{{"||", &FuncLor}};
    static const std::vector<DyadicOp> kLegacyLogicalOps{{"||", &FuncLor},
                                                         {"&&", &FuncLand}};
    auto fixed = [&] { return DoDyadic(ctx, &Compiler::LandOp, kFixedLorOps); };
    auto legacy = [&] {
      return DoDyadic(ctx, &Compiler::CmpOp, kLegacyLogicalOps);
    };
    return VALKEY_SEARCH_COMPATIBILITY_FIX(
        1, 3, 0, "ft_aggregate_logical_precedence", fixed, legacy);
  }
  absl::StatusOr<ExprPtr> LandOp(CompileContext &ctx) {
    static const std::vector<DyadicOp> ops{{"&&", &FuncLand}};
    return DoDyadic(ctx, &Compiler::CmpOp, ops);
  }
  absl::StatusOr<ExprPtr> CmpOp(CompileContext &ctx) {
    static std::vector<DyadicOp> apply_ops{
        {"<=", &FuncLe, &FuncNumLe, &FuncStrLe},
        {"<", &FuncLt, &FuncNumLt, &FuncStrLt},
        {"==", &FuncEq, &FuncNumEq, &FuncStrEq},
        {"!=", &FuncNe, &FuncNumNe, &FuncStrNe},
        {">=", &FuncGe, &FuncNumGe, &FuncStrGe},
        {">", &FuncGt, &FuncNumGt, &FuncStrGt}};
    static std::vector<DyadicOp> filter_ops{
        {"<=", &FilterFuncLe, &FilterFuncNumLe, &FilterFuncStrLe},
        {"<", &FilterFuncLt, &FilterFuncNumLt, &FilterFuncStrLt},
        {"==", &FilterFuncEq, &FilterFuncNumEq, &FilterFuncStrEq},
        {"!=", &FilterFuncNe, &FilterFuncNumNe, &FilterFuncStrNe},
        {">=", &FilterFuncGe, &FilterFuncNumGe, &FilterFuncStrGe},
        {">", &FilterFuncGt, &FilterFuncNumGt, &FilterFuncStrGt}};
    // Pre-1.3.0 comparisons decide numeric vs string from the runtime Values.
    static std::vector<DyadicOp> legacy_apply_ops{
        {"<=", &FuncLe}, {"<", &FuncLt},  {"==", &FuncEq},
        {"!=", &FuncNe}, {">=", &FuncGe}, {">", &FuncGt}};
    if (ctx.UseFilterComparisonSemantics()) {
      return DoDyadic(ctx, &Compiler::AddOp, filter_ops);
    }
    const bool typed = !ctx.HonorsEmulateRelease() ||
                       VALKEY_SEARCH_COMPATIBILITY_FIX(
                           1, 3, 0, "expr_typed_comparison",
                           [] { return true; }, [] { return false; });
    return DoDyadic(ctx, &Compiler::AddOp,
                    typed ? apply_ops : legacy_apply_ops);
  }
  absl::StatusOr<ExprPtr> AddOp(CompileContext &ctx) {
    static std::vector<DyadicOp> ops{{"+", &FuncAdd}, {"-", &FuncSub}};
    return DoDyadic(ctx, &Compiler::MulOp, ops);
  }
  absl::StatusOr<ExprPtr> MulOp(CompileContext &ctx) {
    // Hoisted out of the lambda bodies because the preprocessor splits macro
    // arguments on commas and would see the brace-initializers as multiple
    // args.
    static const std::vector<DyadicOp> kFixedMulOps{{"*", &FuncMul},
                                                    {"/", &FuncDiv}};
    static const std::vector<DyadicOp> kLegacyMulOps{
        {"*", &FuncMul}, {"/", &FuncDiv}, {"^", &FuncPower}};
    auto fixed = [&] { return DoDyadic(ctx, &Compiler::PowOp, kFixedMulOps); };
    auto legacy = [&] {
      return DoDyadic(ctx, &Compiler::Primary, kLegacyMulOps);
    };
    return VALKEY_SEARCH_COMPATIBILITY_FIX(
        1, 3, 0, "ft_aggregate_pow_precedence", fixed, legacy);
  }
  // `^` is right-associative in Redisearch: `a^b^c` == `a^(b^c)`. DoDyadic
  // would produce a left-fold (`(a^b)^c`), so recurse into PowOp for the rhs
  // instead. Each recursion grows the C++ stack, so this level must guard the
  // depth counter itself — Primary's DepthGuard releases on every return and
  // would otherwise let `a^b^c^…^z` bypass search.query-string-depth.
  absl::StatusOr<ExprPtr> PowOp(CompileContext &ctx) {
    DepthGuard guard(depth_);
    if (depth_ > options::GetQueryStringDepth().GetValue()) {
      return absl::InvalidArgumentError("Expression too complex");
    }
    VMSDK_ASSIGN_OR_RETURN(auto lvalue, Primary(ctx));
    if (!lvalue) {
      return nullptr;
    }
    if (s_.SkipWhiteSpacePopWord("^")) {
      VMSDK_ASSIGN_OR_RETURN(auto rvalue, PowOp(ctx));
      if (!rvalue) {
        return absl::InvalidArgumentError("Invalid or missing expression");
      }
      lvalue = std::make_unique<Dyadic>(std::move(lvalue), std::move(rvalue),
                                        &FuncPower, "^");
    }
    return lvalue;
  }

  absl::StatusOr<ExprPtr> ParseExpression(CompileContext &ctx) {
    DBG << "Start Expression: '" << s_.GetUnscanned() << "'\n";
    return LorOp(ctx);
  }

  absl::StatusOr<ExprPtr> Compile(CompileContext &ctx) {
    VMSDK_ASSIGN_OR_RETURN(auto result, ParseExpression(ctx));
    if (!result) {
      return absl::InvalidArgumentError(kInvalidOrMissingExpression);
    }
    if (s_.SkipWhiteSpacePeekByte() != EOF) {
      return absl::InvalidArgumentError(absl::StrCat(
          "Extra characters at or near position ", s_.GetPosition()));
    } else {
      return result;
    }
  }
};

absl::StatusOr<ExprPtr> Expression::Compile(CompileContext &ctx,
                                            absl::string_view s) {
  Compiler c(s);
  return c.Compile(ctx);
}

}  // namespace expr
}  // namespace valkey_search
