#include "src/expr/value.h"
#include "src/utils/scanner.h"

#include <charconv>
#include <cmath>
#include <algorithm>
#include <time.h>

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
  return std::get_if<absl::string_view>(&value_) || std::get_if<std::string>(&value_);
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

absl::string_view Value::get_string_view() const {
  if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else {
    return std::get<absl::string_view>(value_);
  }
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
  } else if (std::get_if<absl::string_view>(&value_)) {
    auto dble = as_double();
    if (dble) {
      return dble != 0.0;
    }
    return std::nullopt;
  };
  return std::nullopt;
}

std::optional<double> Value::as_double() const {
  absl::string_view sv;
  if (auto result = std::get_if<bool>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<double>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<std::string>(&value_)) {
    sv = *result;
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    sv = *result;
  } else {
    return std::nullopt;
  }
  double val;
  auto scan_result = std::from_chars(sv.begin(), sv.end(), val);
  if (scan_result.ec != std::errc{} 
      || scan_result.ptr != sv.end()
      || isnan(val)) {
    return std::nullopt;
  } else {
    return val;
  }
}

std::optional<int64_t> Value::as_integer() const {
  auto d = as_double();
  if (d) return int64_t(*d);
  return std::nullopt;
}

absl::string_view Value::as_string_view() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result ? "1" : "0";
  } else if (auto result = std::get_if<double>(&value_)) {
    if (!storage_) {
      storage_ = std::to_string(*result);
    }
    return *storage_;
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else {
    assert(false);
  }
}

std::string Value::as_string() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result ? "1" : "0";
  } else if (auto result = std::get_if<double>(&value_)) {
    return std::to_string(*result);
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    return std::string(*result);
  } else if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else {
    assert(false);
  }
}

std::ostream& operator<<(std::ostream& os, const Value& v) {
  if (v.is_nil()) {
    return os << "Nil(" << v.as_nil().value().get_reason() << ")";
  } else if (v.is_bool()) {
    return os << std::boolalpha << v.as_bool().value();
  } else if (v.is_double()) {
    return os << v.as_double().value();
  } else if (v.is_string()) {
    return os << "'" << v.as_string_view() << "'";
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
    return compare_strings(l.get_string_view(), r.get_string_view());
  }

  // Need to handle non-equivalent types.
  // Prefer to promote to double unless that fails.

  auto ld = l.as_double();
  auto rd = r.as_double();
  if (ld && rd) {
    return compare_doubles(ld.value(), rd.value());
  }

  return compare_strings(l.as_string_view(), r.as_string_view());
}

Value func_add(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() + rv.value());
  } else {
    return Value(Value::Nil("Add requires numeric operands"));
  }
}

Value func_sub(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() - rv.value());
  } else {
    return Value(Value::Nil("Subtract requires numeric operands"));
  }
}

Value func_mul(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() * rv.value());
  } else {
    return Value(Value::Nil("Multiply requires numeric operands"));
  }
}

Value func_div(const Value& l, const Value& r) {
  auto lv = l.as_double();
  auto rv = r.as_double();
  if (lv && rv) {
    return Value(lv.value() / rv.value());
  } else {
    return Value(Value::Nil("Divide requires numberic operands"));
  }
}

Value func_lt(const Value& l, const Value& r) {
  return Value(l < r);
}

Value func_le(const Value& l, const Value& r) {
  return Value(l <= r);
}

Value func_eq(const Value& l, const Value& r) {
  return Value(l == r);
}

Value func_ne(const Value& l, const Value& r) {
  return Value(l != r);
}

Value func_gt(const Value& l, const Value& r) {
  return Value(l > r);
}

Value func_ge(const Value& l, const Value& r) {
  return Value(l >= r);
}

Value func_lor(const Value&l, const Value& r) {
  auto lv = l.as_bool();
  auto rv = r.as_bool();
  if (lv && rv) {
    return Value(lv.value() || rv.value());
  } else {
    return Value(Value::Nil("lor requires booleans"));
  }
}

Value func_land(const Value&l, const Value& r) {
  auto lv = l.as_bool();
  auto rv = r.as_bool();
  if (lv && rv) {
    return Value(lv.value() && rv.value());
  } else {
    return Value(Value::Nil("land requires booleans"));
  }
}

Value func_floor(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("floor couldn't convert to a double"));
  return Value(std::floor(*d));
}

Value func_ceil(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("ceil couldn't convert to a double"));
  return Value(std::ceil(*d));
}

Value func_abs(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("abs couldn't convert to a double"));
  return Value(std::abs(*d));
}

Value func_log(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("log couldn't convert to a double"));
  return Value(std::log(*d));
}

Value func_log2(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("log2 couldn't convert to a double"));
  return Value(std::log2(*d));
}

Value func_exp(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("exp couldn't convert to a double"));
  return Value(std::exp(*d));
}

Value func_sqrt(const Value &o) {
  auto d = o.as_double();
  if (!d) return Value(Value::Nil("sqrt couldn't convert to a double"));
  return Value(std::sqrt(*d));
}

Value func_strlen(const Value &o) {
  return Value(double(o.as_string_view().size()));
}

Value func_startswith(const Value& l, const Value& r) {
  auto ls = l.as_string_view();
  auto rs = r.as_string_view();
  if (rs.size() > ls.size()) {
    return Value(false);
  } else {
    return Value(ls.substr(0, rs.size()) == rs);
  }
}

Value func_contains(const Value& l, const Value& r) {
  auto ls = l.as_string_view();
  auto rs = r.as_string_view();
  size_t count = 0;
  size_t pos = 0;
  if (rs.size() == 0) {
    return Value(double(ls.size()+1));
  } else {
    while ((pos = ls.find(rs, pos)) != std::string::npos) {
      count++;
      pos += rs.size();
    }
    return Value(double(count));
  }
}

Value func_substr(const Value& l, const Value& m, const Value& r) {
  auto ls = l.as_string_view();
  auto md = m.as_double();
  auto rd = r.as_double();
  if (md && rd) {
    size_t offset = *md >= 0 ? size_t(*md) : (size_t(*md) + ls.size());
    size_t length = *rd >= 0 ? size_t(*rd) : ls.size();
    if (offset > ls.size() || (offset + length) > ls.size()) {
      return Value(Value::Nil("Substr position or length out of range"));
    } else {
      return Value(std::move(std::string(ls.substr(offset, length))));
    }
  } else {
    return Value(Value::Nil("substr requires numbers for offset and length"));
  }
}

Value func_lower(const Value& o) {
  auto os = o.as_string_view();
  std::string result;
  result.reserve(os.size());
  utils::Scanner in(os);
  for (auto utf8 = in.next_utf8(); utf8 != utils::Scanner::kEOF; utf8 = in.next_utf8()) {
    if (utf8 < 0x80) utf8 = std::tolower(utf8);
    utils::Scanner::push_back_utf8(result, utf8);
  }
  return Value(std::move(result));
}

Value func_upper(const Value& o) {
  auto os = o.as_string_view();
  std::string result;
  result.reserve(os.size());
  utils::Scanner in(os);
  for (auto utf8 = in.next_utf8(); utf8 != utils::Scanner::kEOF; utf8 = in.next_utf8()) {
    if (utf8 < 0x80) utf8 = std::toupper(utf8);
    utils::Scanner::push_back_utf8(result, utf8);
  }
  return Value(std::move(result));
}

#define TIME_FUNCTION(funcname, field, adjustment) \
Value funcname(const Value& timestamp) { \
    auto ts = timestamp.as_double(); \
    if (!ts) { \
      return Value(Value::Nil("timestamp not a number")); \
    } \
    time_t time = (time_t) *ts; \
    struct ::tm tm; \
    gmtime_r(&time, &tm); \
    return Value(double(tm.field + (adjustment))); \
}

TIME_FUNCTION(func_dayofweek, tm_wday, 0)
TIME_FUNCTION(func_dayofmonth, tm_mday, 0)
TIME_FUNCTION(func_dayofyear, tm_yday, 0)
TIME_FUNCTION(func_monthofyear, tm_mon, 0)
TIME_FUNCTION(func_year, tm_year, 1900)

Value func_timefmt(const Value& ts, const Value& fmt) {
  auto timestampd = ts.as_double();
  if (!timestampd) {
    return Value(Value::Nil("timefmt: timestamp was not a number"));
  }
  struct tm tm;
  time_t timestamp = (time_t) *timestampd;
  ::gmtime_r(&timestamp, &tm);

  std::string result;
  result.resize(100);
  size_t result_bytes = 0;
  while ((result_bytes = strftime(result.data(), result.size(), fmt.as_string_view().data(), &tm)) == 0) {
    result.resize(result.size() * 2);
  }
  result.resize(result_bytes);
  return Value(std::move(result));
}

Value func_parsetime(const Value& str, const Value& fmt) {
  auto timestr = str.as_string(); // Ensure 0 terminated
  auto fmtstr = fmt.as_string();
  struct tm tm;
  ::strptime(timestr.data(), fmtstr.data(), &tm);
  return Value(double(::mktime(&tm)));
}

#define TIME_ROUND(func, zero_day, zero_hour, zero_minute) \
Value func(const Value& o) { \
  auto tsd = o.as_double(); \
  if (!tsd) { \
    return Value(Value::Nil(#func ": timestamp not a number")); \
  } \
  time_t ts = (time_t)(*tsd); \
  struct tm tm; \
  gmtime_r(&ts, &tm); \
  tm.tm_sec = 0; \
  if (zero_day) { \
    tm.tm_mday = 0; \
  } \
  if (zero_hour) { \
    tm.tm_hour = 0; \
  } \
  if (zero_minute) { \
    tm.tm_min = 0; \
  } \
  return Value(double(::mktime(&tm))); \
}

TIME_ROUND(func_month, true, true, true)
TIME_ROUND(func_day, false, true, true)
TIME_ROUND(func_hour, false, false, true)
TIME_ROUND(func_minute, false, false, false)

} // valkey_search::expr
} // valkey_search
