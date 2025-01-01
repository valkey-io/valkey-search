#ifndef VALKEYSEARCH_EXPR_VALUE_H
#define VALKEYSEARCH_EXPR_VALUE_H

#include <variant>
#include <iostream>

#include "absl/strings/string_view.h"
#include "absl/container/inlined_vector.h"

namespace valkey_search {
namespace expr {

class Value {
 public:

  class Nil {
   public:
    Nil() : reason_("ctor") {}
    Nil(const char *reason) : reason_(reason) {}
    const char *GetReason() const { return reason_; }
   private:
      const char *reason_;
  };

  Value() = default;
  explicit Value(Nil n) : value_(n) {}
  explicit Value(bool b) : value_(b) {}
  explicit Value(int i) : value_(double(i)) {}
  explicit Value(double d);
  explicit Value(const absl::string_view s) : value_(s) {}
  explicit Value(const char *s) : value_(absl::string_view(s)) {}
  explicit Value(std::string&& s) : value_(std::move(s)) {}

  // test for type of Value
  bool IsNil() const;
  bool IsBool() const;
  bool IsDouble() const;
  bool IsString() const;

  // When you already know the type, will assert if you're wrong
  Nil GetNil() const;
  bool GetBool() const;
  double GetDouble() const;
  absl::string_view GetStringView() const;

  // convert to type
  std::optional<Nil> AsNil() const;
  std::optional<bool> AsBool() const;
  std::optional<double> AsDouble() const;
  std::optional<int64_t> AsInteger() const;
  absl::string_view AsStringView() const;
  std::string AsString() const;

  bool IsTrue() const {
    auto r = AsBool();
    return (r && *r);
  }

  friend std::ostream& operator<<(std::ostream& ios, const Value& v);

  template <typename H>
  friend H AbslHashValue(H h, const Value& v) {
    if (v.IsNil()) {
      return H::combine(std::move(h), 0);
    } else if (v.IsDouble()) {
      return H::combine(std::move(h), *v.AsDouble());
    } else {
      return H::combine(std::move(h), v.AsString());
    }
  }

 private:

  mutable std::optional<std::string> storage_;

  std::variant<
    Nil,
    bool,
    double,
    absl::string_view,
    std::string
  > value_;
};

enum Ordering {
  kLESS,
  kEQUAL,
  kGREATER,
  kUNORDERED
};

std::ostream& operator<<(std::ostream& os, Ordering o) {
  switch(o) {
    case Ordering::kLESS: return os << "LESS";
    case Ordering::kEQUAL: return os << "EQUAL";
    case Ordering::kGREATER: return os << "GREATER";
    case Ordering::kUNORDERED: return os << "UNORDERED";
    default: return os << "?";
  }
}

Ordering Compare(const Value& l, const Value& r);

bool operator==(const Value& l, const Value& r) {
  return Compare(l, r) == Ordering::kEQUAL;
}

bool operator!=(const Value& l, const Value& r) {
  return Compare(l, r) != Ordering::kEQUAL;
}

bool operator<(const Value& l, const Value& r) {
  return Compare(l, r) == Ordering::kLESS;
}

bool operator<=(const Value& l, const Value& r) {
  auto res = Compare(l, r);
  return res == Ordering::kLESS || res == Ordering::kEQUAL;
}

bool operator>(const Value& l, const Value& r) {
  return Compare(l, r) == Ordering::kGREATER;
}

bool operator>=(const Value& l, const Value& r) {
  auto res = Compare(l, r);
  return res == Ordering::kGREATER || res == Ordering::kEQUAL;
}


// Dyadic Numerical Functions
Value FuncAdd(const Value& l, const Value& r);
Value FuncSub(const Value& l, const Value& r);
Value FuncMul(const Value& l, const Value& r);
Value FuncDiv(const Value& l, const Value& r);

// Compare Functions
Value FuncGt(const Value& l, const Value& r);
Value FuncGe(const Value& l, const Value& r);
Value FuncEq(const Value& l, const Value& r);
Value FuncNe(const Value& l, const Value& r);
Value FuncLt(const Value& l, const Value& r);
Value FuncLe(const Value& l, const Value& r);

// Logical Functions
Value FuncLor(const Value& l, const Value& r);
Value FuncLand(const Value&l, const Value& r);

// Function Functions
Value FuncAbs(const Value& o);
Value FuncCeil(const Value& o);
Value FuncExp(const Value& o);
Value FuncLog(const Value& o);
Value FuncLog2(const Value& o);
Value FuncFloor(const Value& o);
Value FuncSqrt(const Value& o);


Value FuncLower(const Value& o);
Value FuncUpper(const Value& o);
Value FuncStrlen(const Value& o);
Value FuncContains(const Value &l, const Value& r);
Value FuncStartswith(const Value& l, const Value &r);
Value FuncSubstr(const Value& l, const Value& m, const Value &r);
Value FuncConcat(const absl::InlinedVector<Value, 4>& values);

Value FuncTimefmt(const Value& t, const Value& fmt);
Value FuncParsetime(const Value& t, const Value& fmt);
Value FuncDay(const Value& t);
Value FuncHour(const Value& t);
Value FuncMinute(const Value& t);
Value FuncMonth(const Value& t);
Value FuncDayofweek(const Value& t);
Value FuncDayofmonth(const Value& t);
Value FuncDayofyear(const Value& t);
Value FuncYear(const Value& t);
Value FuncMonthofyear(const Value& t);

}
}

#endif