#ifndef _VALKEYSEARCH_EXPR_EXPR_H
#define _VALKEYSEARCH_EXPR_EXPR_H

#include <string>

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
  virtual ~Expression() {};
  //
  // These objects are provided at evaluation time.
  //
  // Callers can extend EvalContext with information to aid run-time AttributeReference::getValue
  //
  class EvalContext {};  // A per-evaluation context
  //
  // Callers extend this class with the actual values of the Attributes for this evaluation.
  //
  class AttrValueSet {}; // A set of Attribute/Value pairs
  //
  // A compiled reference to an Attribute (logically like a pointer-to-member)
  //
  class AttributeReference {
   public:
    virtual ~AttributeReference() {}
    virtual Value getValue(EvalContext &ctx, const AttrValueSet &attrs) const = 0;
  };
  //
  // These objects are provided at compile time. Callers can extend this class to provide context
  // for the make_reference operation.
  //
  class CompileContext {
   public:
    virtual std::optional<std::unique_ptr<AttributeReference>> make_reference(const std::string& s) = 0;
  };

  // The two basic operations for Expression(s).
  static absl::StatusOr<std::unique_ptr<Expression>> compile(CompileContext &ctx, absl::string_view s);
  virtual Value evaluate(EvalContext &ctx, const AttrValueSet &attrs) const = 0;//
  virtual void dump(std::ostream& os) const = 0;
};

}}
#endif