#include "src/expr/value.h"

#include <charconv>
#include <cmath>

namespace valkey_search{
namespace expr {

// -ffast-math disables std::is_nan and std::is_inf
static const uint64_t SIGN_BIT_MASK = 0x8000000000000000ull;
static const uint64_t EXPONENT_MASK = 0x7FF0000000000000ull;
static const uint64_t MANTISSA_MASK = 0x000FFFFFFFFFFFFFull;

static bool isnan(double& d) {
  uint64_t v = *(uint64_t *)&d;
  return ((v & EXPONENT_MASK) == EXPONENT_MASK) && ((v & MANTISSA_MASK) != 0);
}

Value::Value(double d) {
  if (isnan(d)) {
    value_ = Nil("Computation was not a number");
  } else {
    value_ = d;
  }
}

bool Value::is_nil() const {
  return std::get_if<Nil>(&value_);
}

bool Value::is_bool() const {
  return std::get_if<bool>(&value_);
}

bool Value::is_double() const {
  return std::get_if<double>(&value_);
}

bool Value::is_string() const {
  return std::get_if<const absl::string_view>(&value_);
}

Value::Nil Value::get_nil() const {
  return std::get<Nil>(value_);
}

bool Value::get_bool() const {
  return std::get<bool>(value_);
}

double Value::get_double() const {
  return std::get<double>(value_);
}

const absl::string_view Value::get_string() const {
  return std::get<const absl::string_view>(value_);
}

std::optional<Value::Nil> Value::as_nil() const {
  if (auto result = std::get_if<Nil>(&value_)) {
    return *result;
  }
  return std::nullopt;
}

std::optional<bool> Value::as_bool() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<double>(&value_)) {
    return *result != 0.0;
  } else if (std::get_if<const absl::string_view>(&value_)) {
    auto dble = as_double();
    if (dble) {
      return dble != 0.0;
    }
    return std::nullopt;
  };
  return std::nullopt;
}

std::optional<double> Value::as_double() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<double>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<const absl::string_view>(&value_)) {
    double val;
    auto scan_result = std::from_chars(result->begin(), result->end(), val);
    if (scan_result.ec != std::errc{} 
       || scan_result.ptr != result->end()
       || isnan(val)) {
      return std::nullopt;
    } else {
      return val;
    }
  };
  return std::nullopt;
}

std::optional<const absl::string_view> Value::as_string() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result ? "1" : "0";
  } else if (auto result = std::get_if<double>(&value_)) {
    storage_ = std::to_string(*result);
    return storage_;
  } else if (auto result = std::get_if<const absl::string_view>(&value_)) {
    return *result;
  };
  return std::nullopt;
}

std::ostream& operator<<(std::ostream& os, const Value& v) {
  if (v.is_nil()) {
    return os << "Nil(" << v.as_nil().value().get_reason() << ")";
  } else if (v.is_bool()) {
    return os << std::boolalpha << v.as_bool().value();
  } else if (v.is_double()) {
    return os << v.as_double().value();
  } else if (v.is_string()) {
    return os << v.as_string().value();
  }
  assert(false);
}

static Ordering compare_doubles(double l, double r) {
    // -ffast-math doesn't handle compares correctly with infinities, we do it integer.
    union {
      double d;
      int64_t i;
      uint64_t u;
    } ld, rd;
    ld.d = l;
    rd.d = r;
    // Kill negative zero
    if (ld.u == SIGN_BIT_MASK) ld.u = 0;
    if (rd.u == SIGN_BIT_MASK) rd.u = 0;
    if ((ld.i ^ rd.i) < 0) {
      // Signs differ. this is the easy case.
      return (ld.i < 0) ? Ordering::LESS : Ordering::GREATER;
    } if (ld.i < 0) {
      // Signs Same and Negative, convert to 2's complement
      ld.u = -ld.u;
      rd.u = -rd.u;
    }
    return ld.u == rd.u ? Ordering::EQUAL :
      (ld.u < rd.u ? Ordering::LESS : Ordering::GREATER);
}

// Todo, does this handle UTF-8 Correctly?
static Ordering compare_strings(const absl::string_view l, const absl::string_view r) {
    if (l < r) {
      return Ordering::LESS;
    } else if (l == r) {
      return Ordering::EQUAL;
    } else {
      return Ordering::GREATER;
    }
}

Ordering compare(const Value& l, const Value& r) {

  // First equvalent types
  
  if (l.is_nil() || r.is_nil()) {
    return (l.is_nil() && r.is_nil()) ? Ordering::EQUAL : Ordering::UNORDERED;
  }

  if (l.is_double() && r.is_double()) {
    return compare_doubles(l.get_double(), r.get_double());
  }

  if (l.is_string() && r.is_string()) {
    return compare_strings(l.get_string(), r.get_string());
  }

  // Need to handle non-equivalent types.
  // Prefer to promote to double unless that fails.

  auto ld = l.as_double();
  auto rd = r.as_double();
  if (ld && rd) {
    return compare_doubles(ld.value(), rd.value());
  }

  return compare_strings(l.as_string().value(), r.as_string().value());
}

Value add(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() + rv.value());
  } else {
    return Value();
  }
}

Value sub(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() - rv.value());
  } else {
    return Value();
  }
}

Value mul(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() * rv.value());
  } else {
    return Value();
  }
}

Value div(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() / rv.value());
  } else {
    return Value();
  }
}

} // valkey_search::expr
} // valkey_search
