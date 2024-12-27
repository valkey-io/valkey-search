#ifndef _VALKEYSEARCH_EXPR_VALUE_H
#define _VALKEYSEARCH_EXPR_VALUE_H

#include <compare>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "absl/strings/string_view.h"

namespace valkey_search {
namespace expr {

class Value {
 public:

  class Nil {
   public:
    Nil() : reason_("ctor") {}
    Nil(const char *reason) : reason_(reason) {}
    const char *get_reason() const { return reason_; }
   private:
      const char *reason_;
  };

  Value() {}
  explicit Value(Nil n) : value_(n) {}
  explicit Value(bool b) : value_(b) {}
  explicit Value(int i) : value_(double(i)) {}
  explicit Value(double d);
  explicit Value(const absl::string_view s) : value_(s) {}
  explicit Value(const char *s) : value_(absl::string_view(s)) {}
  explicit Value(std::string&& s) : value_(std::move(s)) {}

  // test for type of Value
  bool is_nil() const;
  bool is_bool() const;
  bool is_double() const;
  bool is_string() const;

  // When you already know the type, will assert if you're wrong
  Nil get_nil() const;
  bool get_bool() const;
  double get_double() const;
  absl::string_view get_string_view() const;

  // convert to type
  std::optional<Nil> as_nil() const;
  std::optional<bool> as_bool() const;
  std::optional<double> as_double() const;
  std::optional<int64_t> as_integer() const;
  absl::string_view as_string_view() const;
  std::string as_string() const;

  friend std::ostream& operator<<(std::ostream& ios, const Value& v);

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
  LESS,
  EQUAL,
  GREATER,
  UNORDERED
};

std::ostream& operator<<(std::ostream& os, Ordering o) {
  switch(o) {
    case Ordering::LESS: return os << "LESS";
    case Ordering::EQUAL: return os << "EQUAL";
    case Ordering::GREATER: return os << "GREATER";
    case Ordering::UNORDERED: return os << "UNORDERED";
    default: return os << "?";
  }
}

Ordering compare(const Value& l, const Value& r);

bool operator==(const Value& l, const Value& r) {
  return compare(l, r) == Ordering::EQUAL;
}

bool operator!=(const Value& l, const Value& r) {
  return compare(l, r) != Ordering::EQUAL;
}

bool operator<(const Value& l, const Value& r) {
  return compare(l, r) == Ordering::LESS;
}

bool operator<=(const Value& l, const Value& r) {
  auto res = compare(l, r);
  return res == Ordering::LESS || res == Ordering::EQUAL;
}

bool operator>(const Value& l, const Value& r) {
  return compare(l, r) == Ordering::GREATER;
}

bool operator>=(const Value& l, const Value& r) {
  auto res = compare(l, r);
  return res == Ordering::GREATER || res == Ordering::EQUAL;
}


// Dyadic Numerical Functions
Value func_add(const Value& l, const Value& r);
Value func_sub(const Value& l, const Value& r);
Value func_mul(const Value& l, const Value& r);
Value func_div(const Value& l, const Value& r);

// Compare Functions
Value func_gt(const Value& l, const Value& r);
Value func_ge(const Value& l, const Value& r);
Value func_eq(const Value& l, const Value& r);
Value func_ne(const Value& l, const Value& r);
Value func_lt(const Value& l, const Value& r);
Value func_le(const Value& l, const Value& r);

// Logical Functions
Value func_lor(const Value& l, const Value& r);
Value func_land(const Value&l, const Value& r);

// Function Functions
Value func_abs(const Value& o);
Value func_ceil(const Value& o);
Value func_exp(const Value& o);
Value func_log(const Value& o);
Value func_log2(const Value& o);
Value func_floor(const Value& o);
Value func_sqrt(const Value& o);


Value func_lower(const Value& o);
Value func_upper(const Value& o);
Value func_strlen(const Value& o);
Value func_contains(const Value &l, const Value& r);
Value func_startswith(const Value& l, const Value &r);
Value func_substr(const Value& l, const Value& m, const Value &r);

Value func_timefmt(const Value& t, const Value& fmt);
Value func_parsetime(const Value& t, const Value& fmt);
Value func_day(const Value& t);
Value func_hour(const Value& t);
Value func_minute(const Value& t);
Value func_month(const Value& t);
Value func_dayofweek(const Value& t);
Value func_dayofmonth(const Value& t);
Value func_dayofyear(const Value& t);
Value func_year(const Value& t);
Value func_monthofyear(const Value& t);

}
}

#endif