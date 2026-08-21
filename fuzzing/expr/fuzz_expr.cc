/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <unistd.h>

#include <memory>
#include <string>

#include "src/expr/expr.h"
#include "src/expr/value.h"

using valkey_search::expr::Expression;
using valkey_search::expr::Value;

struct FuzzCompileContext : public Expression::CompileContext {
  struct Ref : public Expression::AttributeReference {
    Ref(const absl::string_view s) : name_(s) {}
    std::string name_;
    void Dump(std::ostream& os) const override { os << name_; }
    Value GetValue(Expression::EvalContext& ctx,
                   const Expression::Record& record) const override {
      return Value(1.0);
    }
  };

  absl::StatusOr<std::unique_ptr<Expression::AttributeReference>> MakeReference(
      const absl::string_view s, bool create) override {
    return std::make_unique<Ref>(s);
  }

  absl::StatusOr<Value> GetParam(const absl::string_view s) const override {
    return Value(std::string(s));
  }
};

void TestExpr(const char *data, size_t size) {
  std::string input(data, size);
  FuzzCompileContext ctx;
  auto result = Expression::Compile(ctx, input);
  (void)result;
}

int main() {
#ifdef __AFL_HAVE_MANUAL_CONTROL
  __AFL_INIT();
#endif

  char buf[4096];

#ifdef __AFL_LOOP
  while (__AFL_LOOP(1000)) {
#endif
    ssize_t len = read(STDIN_FILENO, buf, sizeof(buf) - 1);
    if (len > 0) {
      buf[len] = '\0';
      TestExpr(buf, len);
    }
#ifdef __AFL_LOOP
  }
#endif

  return 0;
}
