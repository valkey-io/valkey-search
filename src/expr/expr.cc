#include "src/expr/expr.h"
#include "src/utils/scanner.h"

#include "absl/strings/str_cat.h"
#include "vmsdk/src/status/status_macros.h"

#include <memory>
#include <optional>
#include <utility>
#include <ctime>

#define DBG std::cerr
//#define DBG 0 && std::cerr


namespace valkey_search { namespace expr {

typedef std::unique_ptr<Expression> ExprPtr;

struct Constant : Expression {
    Constant(std::string constant) : constant_(std::move(constant)) {}
    Constant(double constant) : constant_(constant) {}
    Value evaluate(EvalContext &ctx, const AttrValueSet &attrs) const {
        return constant_;
    }
    void dump(std::ostream& os) const {
        os << "Constant(" << constant_ << ")";
    }
  private:
    Value constant_;
};

struct AttributeValue : Expression {
    AttributeValue(std::string identifier, std::unique_ptr<AttributeReference> ref) 
        : identifier_(std::move(identifier)), ref_(std::move(ref)) {}
    Value evaluate(EvalContext &ctx, const AttrValueSet &attrs) const {
        return ref_->getValue(ctx, attrs);
    }
    void dump(std::ostream& os) const {
        os << '@' << identifier_;
    }
  private:
    std::string identifier_;
    std::unique_ptr<AttributeReference> ref_;
};

struct FunctionCall : Expression {
typedef Value (*Func)(EvalContext& ctx, const AttrValueSet &attrs, const std::vector<ExprPtr>& params);
static absl::StatusOr<Func> lookup_and_validate(const std::string& name, const std::vector<ExprPtr>& params);
    FunctionCall(std::string name, Func func, std::vector<ExprPtr> params) :
        name_(std::move(name)), func_(func), params_(std::move(params)) {}
    Value evaluate(EvalContext& ctx, const AttrValueSet &attrs) const {
        return (*func_)(ctx, attrs, params_);
    }
    void dump(std::ostream& os) const {
        os << name_ << '(';
        for (auto& p : params_) {
            if (&p != &params_[0]) os << ',';
            p->dump(os);
        }
        os << ')';
    }
  private:
    std::string name_;
    Func func_;
    std::vector<ExprPtr> params_;    
};

template<Value (*func1)(const Value &o)> Value MonadicFunctionProxy(
  Expression::EvalContext& ctx,
  const Expression::AttrValueSet& attrs,
  const std::vector<expr::ExprPtr>& params) {
  assert(params.size() == 1);
  return (*func1)(params[0]->evaluate(ctx, attrs));
};

template<Value (*func2)(const Value &l, const Value &r)> Value DyadicFunctionProxy(
  Expression::EvalContext& ctx,
  const Expression::AttrValueSet& attrs,
  const std::vector<expr::ExprPtr>& params) {
  assert(params.size() == 2);
  return (*func2)(params[0]->evaluate(ctx, attrs), params[1]->evaluate(ctx, attrs));
};

template<Value (*func3)(const Value &l, const Value &m, const Value &r)> Value TriadicFunctionProxy(
  Expression::EvalContext& ctx,
  const Expression::AttrValueSet& attrs,
  const std::vector<expr::ExprPtr>& params) {
  assert(params.size() == 3);
  return (*func3)(
    params[0]->evaluate(ctx, attrs),
    params[1]->evaluate(ctx, attrs),
    params[2]->evaluate(ctx, attrs)
    );
};

typedef Value (*Func)(Expression::EvalContext& ctx, const Expression::AttrValueSet &attrs, const std::vector<ExprPtr>& params);

Value func_exists(const Value &o) {
    return Value(!o.is_nil());
}

Value proxy_timefmt(Expression::EvalContext& ctx,
  const Expression::AttrValueSet& attrs,
  const std::vector<expr::ExprPtr>& params) {
  assert(!params.empty());
  Value fmt("%FT%TZ");
  if (params.size() > 1) {
    fmt = params[1]->evaluate(ctx, attrs);
  }
  return func_timefmt(params[0]->evaluate(ctx, attrs), fmt);
}

Value proxy_parsetime(Expression::EvalContext& ctx,
  const Expression::AttrValueSet& attrs,
  const std::vector<expr::ExprPtr>& params) {
  assert(!params.empty());
  Value fmt("%FT%TZ");
  if (params.size() > 1) {
    fmt = params[1]->evaluate(ctx, attrs);
  }
  return func_parsetime(params[0]->evaluate(ctx, attrs), fmt);
}

struct FunctionTableEntry {
  size_t min_argc;
  size_t max_argc;
  Func function;
};

static std::map<std::string, FunctionTableEntry> function_table{
  { "exists", { 1, 1, &MonadicFunctionProxy<func_exists> }},

  { "abs", { 1, 1, &MonadicFunctionProxy<func_abs> }},
  { "ceil", { 1, 1, &MonadicFunctionProxy<func_ceil> }},
  { "exp", { 1, 1, &MonadicFunctionProxy<func_exp> }},
  { "floor", { 1, 1, &MonadicFunctionProxy<func_floor> }},
  { "log", { 1, 1, &MonadicFunctionProxy<func_log> }},
  { "log2", { 1, 1, &MonadicFunctionProxy<func_log2> }},
  { "sqrt", { 1, 1, &MonadicFunctionProxy<func_sqrt> }},

  { "startswith", { 2, 2, &DyadicFunctionProxy<func_startswith> }},
  { "lower", { 1, 1, &MonadicFunctionProxy<func_lower> }},
  { "upper", { 1, 1, &MonadicFunctionProxy<func_upper> }},
  { "strlen", { 1, 1, &MonadicFunctionProxy<func_strlen> }},
  { "substr", { 3, 3, &TriadicFunctionProxy<func_substr> }},
  { "contains", { 2, 2, &DyadicFunctionProxy<func_contains> }},

  { "dayofweek", { 1, 1, &MonadicFunctionProxy<func_dayofweek>}},
  { "dayofmonth", { 1, 1, &MonadicFunctionProxy<func_dayofmonth>}},
  { "dayofyear", { 1, 1, &MonadicFunctionProxy<func_dayofyear>}},
  { "monthofyear", { 1, 1, &MonadicFunctionProxy<func_monthofyear>}},
  { "year", { 1, 1, &MonadicFunctionProxy<func_year>}},
  { "minute", { 1, 1, &MonadicFunctionProxy<func_minute>}},
  { "hour", { 1, 1, &MonadicFunctionProxy<func_hour>}},
  { "day", { 1, 1, &MonadicFunctionProxy<func_day>}},
  { "month", { 1, 1, &MonadicFunctionProxy<func_month>}},

  { "timefmt", { 1, 2, &proxy_timefmt}},
  { "parsetime", { 1, 2, &proxy_parsetime}},
};

absl::StatusOr<Func> FunctionCall::lookup_and_validate(const std::string& name, const std::vector<ExprPtr>& params) {
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
//    mul_ops    *, /
//    add_ops    +, -
//    cmp_ops    >, >=, ==, !=, <, <=
//    and_ops    &&
//    lor_ops    ||
//

struct Dyadic : Expression {
    typedef Value (*ValueFunc)(const Value&, const Value&);
    Dyadic(ExprPtr lexpr, ExprPtr rexpr, ValueFunc func, absl::string_view name) :
        lexpr_(std::move(lexpr)), rexpr_(std::move(rexpr)), func_(func), name_(name) {}
    Value evaluate(EvalContext& ctx, const AttrValueSet& attrs) const {
        auto lvalue = lexpr_->evaluate(ctx, attrs);
        auto rvalue = rexpr_->evaluate(ctx, attrs);
        return (*func_)(lvalue, rvalue);
    }
    void dump(std::ostream& os) const {
        os << '(';
        lexpr_->dump(os);
        os << name_;
        rexpr_->dump(os);
        os << ')';
    }
  private:
    ExprPtr lexpr_;
    ExprPtr rexpr_;
    ValueFunc func_;
    absl::string_view name_;
};

bool is_identifier_char(int c) {
    return c != EOF && std::isalnum(c);
}

struct Compiler {
    utils::Scanner s_;
    Compiler(absl::string_view sv) : s_(sv) {}

    typedef Expression::CompileContext CompileContext;

    typedef absl::StatusOr<ExprPtr> (Compiler::*ParseFunc)(CompileContext &ctx);

    typedef std::pair<absl::string_view, Dyadic::ValueFunc> DyadicOp;
    absl::StatusOr<ExprPtr> do_dyadic(
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
            DBG << "Dyadic looking for " << op.first << " Remaining: " << s_.get_unscanned() << "\n";
            if (s_.skip_whitespace_pop_word(op.first)) {
                DBG << "Found " << op.first << "\n";
                VMSDK_ASSIGN_OR_RETURN(auto rvalue, (this->*rfunc)(ctx));
                if (!rvalue) {
                    // Error.
                    return absl::InvalidArgumentError(
                        absl::StrCat("Invalid or missing expression after ", op.first, 
                        " at or near position ", s_.get_position()));
                } else {
                    return make_unique<Dyadic>(std::move(lvalue), std::move(rvalue), op.second, op.first);
                }
            }
        }
        s_ = s;
        return lvalue;
    }

    absl::StatusOr<ExprPtr> primary(CompileContext &ctx) {
        s_.skip_whitespace();
        DBG << "Primary: '" << s_.get_unscanned() << "'\n";
        switch (s_.peek_byte()) {
            case '(': {
                assert(s_.pop_byte('('));
                auto result = lor_op(ctx);
                if (!s_.skip_whitespace_pop_byte(')')) {
                    return absl::InvalidArgumentError(
                        absl::StrCat("Expected ')' at or near position ", s_.get_position()));
                } else {
                    return result;
                }
            };
            case '+':
            case '-':
            case '.':
            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9': {
                return number(ctx);
            };
            case '@':
                return attribute(ctx);
            case '\'': case '"':
                return quoted_string(ctx);
            case EOF:
                return nullptr;
            default: 
                return function_call(ctx);
        }
    }
    absl::StatusOr<ExprPtr> attribute(CompileContext &ctx) {
        assert(s_.pop_byte('@'));
        size_t pos = s_.get_position();
        std::string identifier;
        while (is_identifier_char(s_.peek_utf8())) {
            utils::Scanner::push_back_utf8(identifier, s_.next_utf8());
        }
        DBG << "Identifier: " << identifier << " Remainer:" << s_.get_unscanned() << "\n";
        auto ref = ctx.make_reference(identifier);
        if (!ref) {
            return absl::NotFoundError(
                absl::StrCat("Attribute `", identifier, "` Unknown/Invalid near position ", pos));
        }
        return make_unique<AttributeValue>(identifier, std::move(*ref));
    }
    absl::StatusOr<ExprPtr> function_call(CompileContext &ctx) {
        std::string name;
        utils::Scanner s = s_;
        while (is_identifier_char(s_.peek_byte())) {
            name.push_back(s_.next_byte());
        }
        DBG << "Function Name: " << name << "\n";
        if (!s_.skip_whitespace_pop_byte('(')) {
            s_ = s;
            return nullptr;
        }
        std::vector<ExprPtr> params;
        while (1) {
            DBG << "Scanning for parameter " << params.size() << " : " << s_.get_unscanned() << "\n";
            s_.skip_whitespace();
            if (s_.pop_byte(')')) {
                VMSDK_ASSIGN_OR_RETURN(auto func,FunctionCall::lookup_and_validate(name, params));
                DBG << "After function call: '" << s_.get_unscanned() << "'\n";
                return make_unique<FunctionCall>(std::move(name), *func, std::move(params));
            } else if (!params.empty() && !s_.pop_byte(',')) {
                DBG << "func_call found comma\n";
                return absl::NotFoundError(
                    absl::StrCat("Expected , or ) near position ", s_.get_position()));
            } else {
                DBG << "func_call scan for actual parameter: " << s_.get_unscanned() << "\n";
                VMSDK_ASSIGN_OR_RETURN(auto param, expression(ctx));
                if (!param) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Expected , or ) near position ", s_.get_position()));
                }
                params.emplace_back(std::move(param));
            }
        }
    }
    absl::StatusOr<ExprPtr> number(CompileContext &ctx) {
        DBG << "Number Start: '" << s_.get_unscanned() << "'\n";
        auto num = s_.pop_double();
        if (!num) {
            return nullptr;
        }
        DBG << "Number End(" << (*num) << "): Remaining: '" << s_.get_unscanned() << "'\n";
        return std::make_unique<Constant>(*num);
    }
    absl::StatusOr<ExprPtr> quoted_string(CompileContext& ctx) {
        std::string str;
        int start_byte = s_.next_byte();
        while (s_.peek_byte() != start_byte) {
            int this_byte = s_.next_byte();
            if (this_byte == '\\') {
                // todo Parse Unicode escape sequence
                this_byte = s_.next_byte();
            }
            if (this_byte == EOF) {
                return absl::InvalidArgumentError(
                    absl::StrCat("Missing trailing quote"));
            }
            str.push_back(char(this_byte));
        }
        assert(s_.pop_byte(start_byte));
        DBG << "Make constant ('" << str << "')\n";
        return std::make_unique<Constant>(std::move(str));
    }
    absl::StatusOr<ExprPtr> lor_op(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"||", &func_lor},
        };
        return do_dyadic(ctx, &Compiler::and_op, &Compiler::lor_op, ops);
    }
    absl::StatusOr<ExprPtr> and_op(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"&&", &func_land},
        };
        return do_dyadic(ctx, &Compiler::cmp_op, &Compiler::and_op, ops);
    }
    absl::StatusOr<ExprPtr> cmp_op(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"<",  &func_lt},
            {"<=", &func_le},
            {"==", &func_eq},
            {"!=", &func_ne},
            {">",  &func_gt},
            {">=", &func_ge}
        };
        return do_dyadic(ctx, &Compiler::add_op, &Compiler::cmp_op, ops);
    }
    absl::StatusOr<ExprPtr> add_op(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"+", &func_add},
            {"-", &func_sub}
        };
        return do_dyadic(ctx, &Compiler::mul_op, &Compiler::cmp_op, ops);
    }
    absl::StatusOr<ExprPtr> mul_op(CompileContext &ctx) {
        static std::vector<DyadicOp> ops{
            {"*", &func_mul},
            {"/", &func_div}
        };
        return do_dyadic(ctx, &Compiler::primary, &Compiler::mul_op, ops);
    }

    absl::StatusOr<ExprPtr> expression(CompileContext& ctx) {
        return lor_op(ctx);
    }

    absl::StatusOr<ExprPtr> compile(CompileContext &ctx) {
        VMSDK_ASSIGN_OR_RETURN(auto result, expression(ctx));
        if (s_.skip_whitespace_peek_byte() != EOF) {
            return absl::InvalidArgumentError(
                absl::StrCat("Extra characters at or near position ", s_.get_position()));
        } else {
            return result;
        }
    }
};

absl::StatusOr<ExprPtr> Expression::compile(CompileContext &ctx, absl::string_view s) {
    Compiler c(s);
    return c.compile(ctx);
}

}}