#include "src/expr/expr.h"
#include "src/utils/scanner.h"

#include "absl/strings/str_cat.h"
#include "vmsdk/src/status/status_macros.h"


#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <ctime>

//#define DBG std::cerr
#define DBG 0 && std::cerr

namespace valkey_search { namespace expr {

using ExprPtr = std::unique_ptr<Expression>;

struct Constant : Expression {
    Constant(std::string constant) : constant_(std::move(constant)) {}
    Constant(double constant) : constant_(constant) {}
    Value Evaluate(EvalContext &ctx, const Record &record) const override {
        return constant_;
    }
    void Dump(std::ostream& os) const override {
        os << "Constant(" << constant_ << ")";
    }
  private:
    Value constant_;
};

struct Parameter : Expression {
    Parameter(std::string&& name, Value&& value) : name_(std::move(name)), value_(std::move(value)) {}
    Value Evaluate(EvalContext &ctx, const Record &record) const override {
        return value_;
    }
    void Dump(std::ostream& os) const override {
        os << "$" << name_ << "(" << value_ << ")";
    }
  private:
    std::string name_;
    Value value_;
};

struct AttributeValue : Expression {
    AttributeValue(std::string identifier, std::unique_ptr<AttributeReference> ref) 
        : identifier_(std::move(identifier)), ref_(std::move(ref)) {}
    Value Evaluate(EvalContext &ctx, const Record &record) const override {
        return ref_->GetValue(ctx, record);
    }
    void Dump(std::ostream& os) const override {
        os << '@' << identifier_;
    }
  private:
    std::string identifier_;
    std::unique_ptr<AttributeReference> ref_;
};

struct Not : Expression {
    Not(ExprPtr && p) : expr_(std::move(p)) {}
    Value Evaluate(EvalContext &ctx, const Record& record) const override {
        auto Primary = expr_->Evaluate(ctx, record).AsBool();
        if (Primary) {
            return Value(!*Primary);
        } else {
            return Value{};
        }
    }
    void Dump(std::ostream& os) const override {
        os << '!';
        expr_->Dump(os);
    }
  private:
    ExprPtr expr_;
};

struct FunctionCall : Expression {
  using Func = Value (*)(EvalContext& ctx, const Record &record, const absl::InlinedVector<ExprPtr, 4>& params);
  static absl::StatusOr<Func> LookUpAndValidate(const std::string& name, const absl::InlinedVector<ExprPtr, 4>& params);
    FunctionCall(std::string name, Func func, absl::InlinedVector<ExprPtr, 4> params) :
        name_(std::move(name)), func_(func), params_(std::move(params)) {}
    Value Evaluate(EvalContext& ctx, const Record &record) const override {
        return (*func_)(ctx, record, params_);
    }
    void Dump(std::ostream& os) const override {
        os << name_ << '(';
        for (auto& p : params_) {
            if (&p != &params_[0]) os << ',';
            p->Dump(os);
        }
        os << ')';
    }
  private:
    std::string name_;
    Func func_;
    absl::InlinedVector<ExprPtr, 4> params_;    
};

template<Value (*func1)(const Value &o)> Value MonadicFunctionProxy(
  Expression::EvalContext& ctx,
  const Expression::Record& record,
  const absl::InlinedVector<expr::ExprPtr, 4>& params) {
  assert(params.size() == 1);
  return (*func1)(params[0]->Evaluate(ctx, record));
};

template<Value (*func2)(const Value &l, const Value &r)> Value DyadicFunctionProxy(
  Expression::EvalContext& ctx,
  const Expression::Record& record,
  const absl::InlinedVector<expr::ExprPtr, 4>& params) {
  assert(params.size() == 2);
  return (*func2)(params[0]->Evaluate(ctx, record), params[1]->Evaluate(ctx, record));
};

template<Value (*func3)(const Value &l, const Value &m, const Value &r)> Value TriadicFunctionProxy(
  Expression::EvalContext& ctx,
  const Expression::Record& record,
  const absl::InlinedVector<expr::ExprPtr, 4>& params) {
  assert(params.size() == 3);
  return (*func3)(
    params[0]->Evaluate(ctx, record),
    params[1]->Evaluate(ctx, record),
    params[2]->Evaluate(ctx, record)
    );
};

using Func = Value (*)(Expression::EvalContext& ctx, const Expression::Record &record, const absl::InlinedVector<ExprPtr, 4>& params);

Value FuncExists(const Value &o) {
    return Value(!o.IsNil());
}

Value ProxyConcat(Expression::EvalContext &ctx, 
  const Expression::Record& record,
  const absl::InlinedVector<expr::ExprPtr, 4>& params) {
  absl::InlinedVector<Value, 4> values;
  for (auto& p: params) {
    values.emplace_back(p->Evaluate(ctx, record));
  }
  return FuncConcat(values);
  }

Value ProxyTimefmt(Expression::EvalContext& ctx,
  const Expression::Record& record,
  const absl::InlinedVector<expr::ExprPtr, 4>& params) {
  assert(!params.empty());
  Value fmt("%FT%TZ");
  if (params.size() > 1) {
    fmt = params[1]->Evaluate(ctx, record);
  }
  return FuncTimefmt(params[0]->Evaluate(ctx, record), fmt);
}

Value ProxyParsetime(Expression::EvalContext& ctx,
  const Expression::Record& record,
  const absl::InlinedVector<expr::ExprPtr, 4>& params) {
  assert(!params.empty());
  Value fmt("%FT%TZ");
  if (params.size() > 1) {
    fmt = params[1]->Evaluate(ctx, record);
  }
  return FuncParsetime(params[0]->Evaluate(ctx, record), fmt);
}

struct FunctionTableEntry {
  size_t min_argc;
  size_t max_argc;
  Func function;
};

static std::map<std::string, FunctionTableEntry> function_table{
  { "exists", { 1, 1, &MonadicFunctionProxy<FuncExists> }},

  { "abs", { 1, 1, &MonadicFunctionProxy<FuncAbs> }},
  { "ceil", { 1, 1, &MonadicFunctionProxy<FuncCeil> }},
  { "exp", { 1, 1, &MonadicFunctionProxy<FuncExp> }},
  { "floor", { 1, 1, &MonadicFunctionProxy<FuncFloor> }},
  { "log", { 1, 1, &MonadicFunctionProxy<FuncLog> }},
  { "log2", { 1, 1, &MonadicFunctionProxy<FuncLog2> }},
  { "sqrt", { 1, 1, &MonadicFunctionProxy<FuncSqrt> }},

  { "startswith", { 2, 2, &DyadicFunctionProxy<FuncStartswith> }},
  { "lower", { 1, 1, &MonadicFunctionProxy<FuncLower> }},
  { "upper", { 1, 1, &MonadicFunctionProxy<FuncUpper> }},
  { "strlen", { 1, 1, &MonadicFunctionProxy<FuncStrlen> }},
  { "substr", { 3, 3, &TriadicFunctionProxy<FuncSubstr> }},
  { "contains", { 2, 2, &DyadicFunctionProxy<FuncContains> }},
  { "concat", { 0, 50, &ProxyConcat}},

  { "dayofweek", { 1, 1, &MonadicFunctionProxy<FuncDayofweek>}},
  { "dayofmonth", { 1, 1, &MonadicFunctionProxy<FuncDayofmonth>}},
  { "dayofyear", { 1, 1, &MonadicFunctionProxy<FuncDayofyear>}},
  { "monthofyear", { 1, 1, &MonadicFunctionProxy<FuncMonthofyear>}},
  { "year", { 1, 1, &MonadicFunctionProxy<FuncYear>}},
  { "minute", { 1, 1, &MonadicFunctionProxy<FuncMinute>}},
  { "hour", { 1, 1, &MonadicFunctionProxy<FuncHour>}},
  { "day", { 1, 1, &MonadicFunctionProxy<FuncDay>}},
  { "month", { 1, 1, &MonadicFunctionProxy<FuncMonth>}},

  { "timefmt", { 1, 2, &ProxyTimefmt}},
  { "parsetime", { 1, 2, &ProxyParsetime}},
};

absl::StatusOr<Func> FunctionCall::LookUpAndValidate(const std::string& name, const absl::InlinedVector<ExprPtr, 4>& params) {
  auto it = function_table.find(name);
  if (it == function_table.end()) {
    return absl::NotFoundError(absl::StrCat("Function ", name, " is unknown"));
  }
  if (it->second.min_argc > params.size()) {
    return absl::InvalidArgumentError(absl::StrCat("Function ", name, " expects at least ", it->second.min_argc, " arguments, but only ", params.size(), " were found."));
  }
  if (it->second.max_argc < params.size()) {
    return absl::InvalidArgumentError(absl::StrCat("Function ", name, " expects no more than ", it->second.max_argc, " arguments, but ", params.size(), " were found."));
  }
  return it->second.function;
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
    using ValueFunc = Value (*)(const Value&, const Value&);
    Dyadic(ExprPtr lexpr, ExprPtr rexpr, ValueFunc func, absl::string_view name) :
        lexpr_(std::move(lexpr)), rexpr_(std::move(rexpr)), func_(func), name_(name) {}
    Value Evaluate(EvalContext& ctx, const Record& record) const override {
        auto lvalue = lexpr_->Evaluate(ctx, record);
        auto rvalue = rexpr_->Evaluate(ctx, record);
        return (*func_)(lvalue, rvalue);
    }
    void Dump(std::ostream& os) const override {
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
};

bool IsIdentifierChar(int c) {
    return c != EOF && std::isalnum(c);
}

struct Compiler {
    utils::Scanner s_;
    Compiler(absl::string_view sv) : s_(sv) {}

    using CompileContext = Expression::CompileContext;

    using ParseFunc = absl::StatusOr<ExprPtr> (Compiler::*)(CompileContext &ctx);

    using DyadicOp = std::pair<absl::string_view, Dyadic::ValueFunc>;
    absl::StatusOr<ExprPtr> DoDyadic(
        CompileContext &ctx,
        ParseFunc lfunc,
        ParseFunc rfunc,
        const std::vector<DyadicOp>& ops
        ) {
        utils::Scanner s = s_;
        DBG << "Start Dyadic: " << ops[0].first << "\n";
        VMSDK_ASSIGN_OR_RETURN(auto lvalue, (this->*lfunc)(ctx));
        if (!lvalue) {
            DBG << "Dyadic Failed first: " << ops[0].first << "\n";
            return nullptr;
        }
        s = s_;
        for (auto& op : ops) {
            DBG << "Dyadic looking for " << op.first << " Remaining: " << s_.GetUnscanned() << "\n";
            if (s_.SkipWhiteSpacePopWord(op.first)) {
                DBG << "Found " << op.first << "\n";
                VMSDK_ASSIGN_OR_RETURN(auto rvalue, (this->*rfunc)(ctx));
                if (!rvalue) {
                    // Error.
                    return absl::InvalidArgumentError(
                        absl::StrCat("Invalid or missing expression after ", op.first, 
                        " at or near position ", s_.GetPosition()));
                } else {
                    return std::make_unique<Dyadic>(std::move(lvalue), std::move(rvalue), op.second, op.first);
                }
            }
        }
        s_ = s;
        return lvalue;
    }

    absl::StatusOr<ExprPtr> ParseParameter(CompileContext& ctx) {
        assert(s_.PopByte('$'));
        std::string param_name;
        while (IsIdentifierChar(s_.PeekByte())) {
            param_name += s_.NextByte();
        }
        VMSDK_ASSIGN_OR_RETURN(auto param_value, ctx.GetParam(param_name));
        return std::make_unique<Parameter>(std::move(param_name), std::move(param_value));
    }

    absl::StatusOr<ExprPtr> Invert(CompileContext &ctx) {
        assert(s_.PopByte('!'));
        VMSDK_ASSIGN_OR_RETURN(auto expr, Primary(ctx));
        return std::make_unique<Not>(std::move(expr));
    }

    absl::StatusOr<ExprPtr> Primary(CompileContext &ctx) {
        s_.SkipWhiteSpace();
        DBG << "Primary: '" << s_.GetUnscanned() << "'\n";
        switch (s_.PeekByte()) {
            case '(': {
                assert(s_.PopByte('('));
                auto result = LorOp(ctx);
                if (!s_.SkipWhiteSpacePopByte(')')) {
                    return absl::InvalidArgumentError(
                        absl::StrCat("Expected ')' at or near position ", s_.GetPosition()));
                } else {
                    return result;
                }
            };
            case '+':
            case '-':
            case '.':
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9': {
                return Number(ctx);
            };
            case '!':
                return Invert(ctx);
            case '@':
                return Attribute(ctx);
            case '\'': case '"':
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
        assert(s_.PopByte('@'));
        size_t pos = s_.GetPosition();
        std::string identifier;
        while (IsIdentifierChar(s_.PeekUtf8())) {
            utils::Scanner::PushBackUtf8(identifier, s_.NextUtf8());
        }
        DBG << "Identifier: " << identifier << " Remainder:'" << s_.GetUnscanned() << "'\n";
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
            DBG << "Scanning for Parameter " << params.size() << " : " << s_.GetUnscanned() << "\n";
            s_.SkipWhiteSpace();
            if (s_.PopByte(')')) {
                VMSDK_ASSIGN_OR_RETURN(auto func,FunctionCall::LookUpAndValidate(name, params));
                DBG << "After function call: '" << s_.GetUnscanned() << "'\n";
                return std::make_unique<FunctionCall>(std::move(name), *func, std::move(params));
            } else if (!params.empty() && !s_.PopByte(',')) {
                DBG << "func_call found comma\n";
                return absl::NotFoundError(
                    absl::StrCat("Expected , or ) near position ", s_.GetPosition()));
            } else {
                DBG << "func_call scan for actual Parameter: " << s_.GetUnscanned() << "\n";
                VMSDK_ASSIGN_OR_RETURN(auto param, Expression(ctx));
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
        DBG << "Number End(" << (*num) << "): Remaining: '" << s_.GetUnscanned() << "'\n";
        return std::make_unique<Constant>(*num);
    }
    absl::StatusOr<ExprPtr> QuotedString(CompileContext& ctx) {
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
        assert(s_.PopByte(start_byte));
        DBG << "Make constant ('" << str << "')\n";
        return std::make_unique<Constant>(std::move(str));
    }
    absl::StatusOr<ExprPtr> LorOp(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"||", &FuncLor},
        };
        return DoDyadic(ctx, &Compiler::AndOp, &Compiler::LorOp, ops);
    }
    absl::StatusOr<ExprPtr> AndOp(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"&&", &FuncLand},
        };
        return DoDyadic(ctx, &Compiler::CmpOp, &Compiler::AndOp, ops);
    }
    absl::StatusOr<ExprPtr> CmpOp(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"<",  &FuncLt},
            {"<=", &FuncLe},
            {"==", &FuncEq},
            {"!=", &FuncNe},
            {">",  &FuncGt},
            {">=", &FuncGe}
        };
        return DoDyadic(ctx, &Compiler::AddOp, &Compiler::CmpOp, ops);
    }
    absl::StatusOr<ExprPtr> AddOp(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"+", &FuncAdd},
            {"-", &FuncSub}
        };
        return DoDyadic(ctx, &Compiler::MulOp, &Compiler::CmpOp, ops);
    }
    absl::StatusOr<ExprPtr> MulOp(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"*", &FuncMul},
            {"/", &FuncDiv}
        };
        return DoDyadic(ctx, &Compiler::Primary, &Compiler::MulOp, ops);
    }

    absl::StatusOr<ExprPtr> Expression(CompileContext& ctx) {
        return LorOp(ctx);
    }

    absl::StatusOr<ExprPtr> Compile(CompileContext &ctx) {
        VMSDK_ASSIGN_OR_RETURN(auto result, Expression(ctx));
        if (s_.SkipWhiteSpacePeekByte() != EOF) {
            return absl::InvalidArgumentError(
                absl::StrCat("Extra characters at or near position ", s_.GetPosition()));
        } else {
            return result;
        }
    }
};

absl::StatusOr<ExprPtr> Expression::Compile(CompileContext &ctx, absl::string_view s) {
    Compiler c(s);
    return c.Compile(ctx);
}

}}