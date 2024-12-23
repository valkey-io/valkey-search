#ifndef _EXPR_VALUE_H
#define _EXPR_VALUE_H

#include <compare>
#include <iostream>
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
  explicit Value(bool b) : value_(b) {}
  explicit Value(double d);
  explicit Value(const absl::string_view s) : value_(s) {}
  explicit Value(std::string&& s) : storage_(std::move(s)), value_(storage_) {}

  // test for type of Value
  bool is_nil() const;
  bool is_bool() const;
  bool is_double() const;
  bool is_string() const;

  // When you already know the type, will assert if you're wrong
  Nil get_nil() const;
  bool get_bool() const;
  double get_double() const;
  const absl::string_view get_string() const;

  // convert to type
  std::optional<Nil> as_nil() const;
  std::optional<bool> as_bool() const;
  std::optional<double> as_double() const;
  std::optional<const absl::string_view> as_string() const;

  friend std::ostream& operator<<(std::ostream& ios, const Value& v);

 private:

  mutable std::string storage_;

  std::variant<
    Nil,
    bool,
    double,
    const absl::string_view
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
Value add(const Value& l, const Value& r);
Value sub(const Value& l, const Value& r);
Value mul(const Value& l, const Value& r);
Value div(const Value& l, const Value& r);

}
}

#endif