/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VMSDK_SRC_STATUS_STATUS_MACROS_H_
#define VMSDK_SRC_STATUS_STATUS_MACROS_H_

#include <utility>

#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "vmsdk/src/status/source_location.h"
#include "vmsdk/src/status/status_builder.h"

// Evaluates an expression that produces a `absl::Status`. If the
// status is not ok, returns it from the current function.
//
// For example:
//   absl::Status MultiStepFunction() {
//     VMSDK_RETURN_IF_ERROR(Function(args...));
//     VMSDK_RETURN_IF_ERROR(foo.Method(args...));
//     return absl::OkStatus();
//   }
//
// The macro ends with a `::vmsdk::StatusBuilder` which allows the
// returned status to be extended with more details.  Any chained expressions
// after the macro will not be evaluated unless there is an error.
//
// For example:
//   absl::Status MultiStepFunction() {
//     VMSDK_RETURN_IF_ERROR(Function(args...)) << "in MultiStepFunction";
//     VMSDK_RETURN_IF_ERROR(foo.Method(args...)).Log(base_logging::ERROR)
//         << "while processing query: " << query.DebugString();
//     return absl::OkStatus();
//   }
//
// `::vmsdk::StatusBuilder` supports adapting the builder chain using
// a `With` method and a functor.  This allows for powerful extensions to the
// macro.
//
// For example, teams can define local policies to use across their code:
//
//   StatusBuilder TeamPolicy(StatusBuilder builder) {
//     return std::move(builder.Log(base_logging::WARNING).Attach(...));
//   }
//
//   VMSDK_RETURN_IF_ERROR(foo()).With(TeamPolicy);
//   VMSDK_RETURN_IF_ERROR(bar()).With(TeamPolicy);
//
// Changing the return type allows the macro to be used with Task and Rpc
// interfaces.  See `::vmsdk::TaskReturn` and `rpc::RpcSetStatus` for
// details.
//
//   void Read(StringPiece name, ::vmsdk::Task* task) {
//     int64 id;
//     VMSDK_RETURN_IF_ERROR(GetIdForName(name, &id)).With(TaskReturn(task));
//     VMSDK_RETURN_IF_ERROR(ReadForId(id)).With(TaskReturn(task));
//     task->Return();
//   }
//
// If using this macro inside a lambda, you need to annotate the return type
// to avoid confusion between a `::vmsdk::StatusBuilder` and a
// `absl::Status` type. E.g.
//
//   []() -> absl::Status {
//     VMSDK_RETURN_IF_ERROR(Function(args...));
//     VMSDK_RETURN_IF_ERROR(foo.Method(args...));
//     return absl::OkStatus();
//   }
#define VMSDK_RETURN_IF_ERROR(expr)                                      \
  VMSDK_STATUS_MACROS_IMPL_ELSE_BLOCKER_                                 \
  if (::vmsdk::status_macro_internal::StatusAdaptorForMacros             \
          status_macro_internal_adaptor = {(expr), VMSDK_STREAMS_LOC}) { \
  } else /* NOLINT */                                                    \
    return status_macro_internal_adaptor.Consume()

// Executes an expression `rexpr` that returns a
// `absl::StatusOr<T>`. On OK, extracts its value into the variable defined by
// `lhs`; otherwise, returns from the current function. By default the error
// status is returned unchanged, but it may be modified by an
// `error_expression`. If there is an error, `lhs` is not evaluated; thus any
// side effects that `lhs` may have only occur in the success case.
//
// Interface:
//
//   VMSDK_ASSIGN_OR_RETURN(lhs, rexpr)
//   VMSDK_ASSIGN_OR_RETURN(lhs, rexpr, error_expression);
//
// WARNING: expands into multiple statements; it cannot be used in a single
// statement (e.g. as the body of an if statement without {})!
//
// Example: Declaring and initializing a new variable (ValueType can be anything
//          that can be initialized with assignment, including references):
//   VMSDK_ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(arg));
//
// Example: Assigning to an existing variable:
//   ValueType value;
//   VMSDK_ASSIGN_OR_RETURN(value, MaybeGetValue(arg));
//
// Example: Assigning to an expression with side effects:
//   MyProto data;
//   VMSDK_ASSIGN_OR_RETURN(*data.mutable_str(), MaybeGetValue(arg));
//   // No field "str" is added on error.
//
// Example: Assigning to a std::unique_ptr.
//   VMSDK_ASSIGN_OR_RETURN(std::unique_ptr<T> ptr, MaybeGetPtr(arg));
//
// If passed, the `error_expression` is evaluated to produce the return
// value. The expression may reference any variable visible in scope, as
// well as a `::vmsdk::StatusBuilder` object populated with the error
// and named by a single underscore `_`. The expression typically uses the
// builder to modify the status and is returned directly in manner similar
// to VMSDK_RETURN_IF_ERROR. The expression may, however, evaluate to any type
// returnable by the function, including (void). For example:
//
// Example: Adjusting the error message.
//   VMSDK_ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(query),
//                    _ << "while processing query " << query.DebugString());
//
// Example: Logging the error on failure.
//   VMSDK_ASSIGN_OR_RETURN(ValueType value, MaybeGetValue(query),
//   _.LogError());
//
#define VMSDK_ASSIGN_OR_RETURN(...)                                     \
  VMSDK_STATUS_MACROS_IMPL_GET_VARIADIC_(                               \
      (__VA_ARGS__, VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_3_, \
       VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_2_))             \
  (__VA_ARGS__)

// =================================================================
// == Implementation details, do not rely on anything below here. ==
// =================================================================
namespace vmsdk::internal {
constexpr bool HasPotentialConditionalOperator(const char* lhs, int index) {
  return (index == -1
              ? false
              : (lhs[index] == '?'
                     ? true
                     : ::vmsdk::internal::HasPotentialConditionalOperator(
                           lhs, index - 1)));
}
}  // namespace vmsdk::internal

#define VMSDK_STATUS_MACROS_IMPL_GET_VARIADIC_HELPER_(_1, _2, _3, NAME, ...) \
  NAME
#define VMSDK_STATUS_MACROS_IMPL_GET_VARIADIC_(args) \
  VMSDK_STATUS_MACROS_IMPL_GET_VARIADIC_HELPER_ args

#define VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_2_(lhs, rexpr)         \
  VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_(                            \
      VMSDK_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__), lhs,       \
      rexpr,                                                                   \
      return std::move(                                                        \
                 VMSDK_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__)) \
          .status())

#define VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_3_(lhs, rexpr,       \
                                                           error_expression) \
  VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_(                          \
      VMSDK_STATUS_MACROS_IMPL_CONCAT_(_status_or_value, __LINE__), lhs,     \
      rexpr,                                                                 \
      ::vmsdk::StatusBuilder _(std::move(VMSDK_STATUS_MACROS_IMPL_CONCAT_(   \
                                             _status_or_value, __LINE__))    \
                                   .status(),                                \
                               VMSDK_STREAMS_LOC);                           \
      (void)_; /* error_expression is allowed to not use this variable */    \
      return (error_expression))

#define VMSDK_STATUS_MACROS_IMPL_VMSDK_ASSIGN_OR_RETURN_(statusor, lhs, rexpr, \
                                                         error_expression)     \
  auto statusor = (rexpr);                                                     \
  if (ABSL_PREDICT_FALSE(!statusor.ok())) {                                    \
    error_expression;                                                          \
  }                                                                            \
  {                                                                            \
    static_assert(#lhs[0] != '(' || #lhs[sizeof(#lhs) - 2] != ')' ||           \
                      !vmsdk::internal::HasPotentialConditionalOperator(       \
                          #lhs, sizeof(#lhs) - 2),                             \
                  "Identified potential conditional operator, consider not "   \
                  "using VMSDK_ASSIGN_OR_RETURN");                             \
  }                                                                            \
  VMSDK_STATUS_MACROS_IMPL_UNPARENTHESIZE_IF_PARENTHESIZED(lhs) =              \
      std::move(statusor).value()

// Internal helpers to check an empty argument.
// The definitions of these macros are borrowed from
// https://t6847kimo.github.io/blog/2019/02/04/Remove-comma-in-variadic-macro.html,
// instead of the google internal approaches which relies on a GNU extension
// support for ##__VA_ARGS__ and is not a part of c++ standards.
#define VMSDK_STATUS_MACROS_IMPL_TRIGGER_PARENTHESIS(...) ,
#define VMSDK_STATUS_MACROS_IMPL_ARG3(_0, _1, _2, ...) _2
#define VMSDK_STATUS_MACROS_IMPL_HAS_COMMA(...) \
  VMSDK_STATUS_MACROS_IMPL_ARG3(__VA_ARGS__, 1, 0)
#define VMSDK_STATUS_MACROS_IMPL_IS_EMPTY(...)                       \
  VMSDK_STATUS_MACROS_IMPL_IS_EMPTY_HELPER(                          \
      VMSDK_STATUS_MACROS_IMPL_HAS_COMMA(__VA_ARGS__),               \
      VMSDK_STATUS_MACROS_IMPL_HAS_COMMA(                            \
          VMSDK_STATUS_MACROS_IMPL_TRIGGER_PARENTHESIS __VA_ARGS__), \
      VMSDK_STATUS_MACROS_IMPL_HAS_COMMA(__VA_ARGS__(/*empty*/)),    \
      VMSDK_STATUS_MACROS_IMPL_HAS_COMMA(                            \
          VMSDK_STATUS_MACROS_IMPL_TRIGGER_PARENTHESIS __VA_ARGS__(  \
              /*empty*/)))
#define VMSDK_STATUS_MACROS_IMPL_PASTES(_0, _1, _2, _3, _4) _0##_1##_2##_3##_4
#define VMSDK_STATUS_MACROS_IMPL_IS_EMPTY_CASE_0001 ,
#define VMSDK_STATUS_MACROS_IMPL_IS_EMPTY_HELPER(_0, _1, _2, _3)      \
  VMSDK_STATUS_MACROS_IMPL_HAS_COMMA(VMSDK_STATUS_MACROS_IMPL_PASTES( \
      VMSDK_STATUS_MACROS_IMPL_IS_EMPTY_CASE_, _0, _1, _2, _3))

// Internal helpers for macro expansion.
#define VMSDK_STATUS_MACROS_IMPL_EAT(...)
#define VMSDK_STATUS_MACROS_IMPL_REM(...) __VA_ARGS__
#define VMSDK_STATUS_MACROS_IMPL_EMPTY()

// Internal helpers for if statement.
#define VMSDK_STATUS_MACROS_IMPL_IF_1(_Then, _Else) _Then
#define VMSDK_STATUS_MACROS_IMPL_IF_0(_Then, _Else) _Else
#define VMSDK_STATUS_MACROS_IMPL_IF(_Cond, _Then, _Else)                \
  VMSDK_STATUS_MACROS_IMPL_CONCAT_(VMSDK_STATUS_MACROS_IMPL_IF_, _Cond) \
  (_Then, _Else)

// Expands to 1 if the input is parenthesized. Otherwise, expands to 0.
#define VMSDK_STATUS_MACROS_IMPL_IS_PARENTHESIZED(...) \
  VMSDK_STATUS_MACROS_IMPL_IS_EMPTY(VMSDK_STATUS_MACROS_IMPL_EAT __VA_ARGS__)

// If the input is parenthesized, removes the parentheses. Otherwise, expands to
// the input unchanged.
#define VMSDK_STATUS_MACROS_IMPL_UNPARENTHESIZE_IF_PARENTHESIZED(...) \
  VMSDK_STATUS_MACROS_IMPL_IF(                                        \
      VMSDK_STATUS_MACROS_IMPL_IS_PARENTHESIZED(__VA_ARGS__),         \
      VMSDK_STATUS_MACROS_IMPL_REM, VMSDK_STATUS_MACROS_IMPL_EMPTY()) \
  __VA_ARGS__

// Internal helper for concatenating macro values.
#define VMSDK_STATUS_MACROS_IMPL_CONCAT_INNER_(x, y) x##y
#define VMSDK_STATUS_MACROS_IMPL_CONCAT_(x, y) \
  VMSDK_STATUS_MACROS_IMPL_CONCAT_INNER_(x, y)

// The GNU compiler emits a warning for code like:
//
//   if (foo)
//     if (bar) { } else baz;
//
// because it thinks you might want the else to bind to the first if.  This
// leads to problems with code like:
//
//   if (do_expr) VMSDK_RETURN_IF_ERROR(expr) << "Some message";
//
// The "switch (0) case 0:" idiom is used to suppress this.
#define VMSDK_STATUS_MACROS_IMPL_ELSE_BLOCKER_ \
  switch (0)                                   \
  case 0:                                      \
  default:  // NOLINT

namespace vmsdk {
namespace status_macro_internal {

// Provides a conversion to bool so that it can be used inside an if statement
// that declares a variable.
class StatusAdaptorForMacros {
 public:
  StatusAdaptorForMacros(const absl::Status& status, SourceLocation loc)
      : builder_(status, loc) {}

  StatusAdaptorForMacros(absl::Status&& status, SourceLocation loc)
      : builder_(std::move(status), loc) {}

  StatusAdaptorForMacros(const StatusBuilder& builder, SourceLocation loc)
      : builder_(builder) {}

  StatusAdaptorForMacros(StatusBuilder&& builder, SourceLocation loc)
      : builder_(std::move(builder)) {}

  StatusAdaptorForMacros(const StatusAdaptorForMacros&) = delete;
  StatusAdaptorForMacros& operator=(const StatusAdaptorForMacros&) = delete;

  explicit operator bool() const { return ABSL_PREDICT_TRUE(builder_.ok()); }

  StatusBuilder&& Consume() { return std::move(builder_); }

 private:
  StatusBuilder builder_;
};

}  // namespace status_macro_internal
}  // namespace vmsdk

#endif  // VMSDK_SRC_STATUS_STATUS_MACROS_H_
