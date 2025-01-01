#ifndef VALKEYSEARCH_EXPR_EXPR_H
#define VALKEYSEARCH_EXPR_EXPR_H

#include "src/expr/value.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace valkey_search { namespace expr {

//
// Generic expression compiler and evaluator.
//
// An expression is compiled into a AST stored in this object.
// The compiled expression can be repeated evaluated against different AttributeSets
// 
class Expression {
  public:
  virtual ~Expression() = default;
  //
  // These objects are provided at evaluation time.
  //
  // Callers extend EvalContext with information to aid run-time AttributeReference::getValue
  //
  class EvalContext {};  // A per-evaluation context
  //
  // Callers extend this class with the actual values of the Attributes for this evaluation.
  //
  class Record {}; // A set of Attribute/Value pairs
  //
  // A compiled reference to an Attribute (logically like a pointer-to-member)
  //
  class AttributeReference {
   public:
    virtual ~AttributeReference() = default;
    virtual Value GetValue(EvalContext &ctx, const Record &record) const = 0;
    virtual void Dump(std::ostream& os) const = 0;
    friend std::ostream& operator<<(std::ostream& os, const AttributeReference *p) {
      p->Dump(os);
      return os;
    }
  };
  //
  // These objects are provided at compile time. Callers can extend this class to provide context
  // for the make_reference operation.
  //
  class CompileContext {
   public:
    virtual absl::StatusOr<std::unique_ptr<AttributeReference>> MakeReference(const absl::string_view s, bool create) = 0;
    virtual absl::StatusOr<Value> GetParam(const absl::string_view s) const = 0;
  };

  // The two basic operations for Expression(s).
  static absl::StatusOr<std::unique_ptr<Expression>> Compile(CompileContext &ctx, absl::string_view s);
  virtual Value Evaluate(EvalContext &ctx, const Record &record) const = 0;
  virtual void Dump(std::ostream& os) const = 0;

  friend std::ostream& operator<<(std::ostream& os, const Expression& e) {
    e.Dump(os);
    return os;
  }
  friend std::ostream& operator<<(std::ostream& os, const Expression *ptr) {
    ptr->Dump(os);
    return os;
  }
};

}}
#endif