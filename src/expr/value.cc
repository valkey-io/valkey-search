#include "src/expr/value.h"

#include <cmath>
#include <ctime>
#include <iomanip>
#include <sstream>

#include "src/utils/scanner.h"

namespace valkey_search {
namespace expr {

// -ffast-math disables std::is_nan and std::is_inf
static const uint64_t kSignBitMask = 0x8000000000000000ull;
static const uint64_t kExponentMask = 0x7FF0000000000000ull;
static const uint64_t kMantissaMask = 0x000FFFFFFFFFFFFFull;

// Built-in isnan doesn't work when compiling with fast-math, which is what we
// want to do.
static bool IsNan(double& d) {
  uint64_t v = *(uint64_t*)&d;
  return ((v & kExponentMask) == kExponentMask) && ((v & kMantissaMask) != 0);
}

Value::Value(double d) { value_ = d; }

bool Value::IsNil() const { return std::get_if<Nil>(&value_); }

bool Value::IsBool() const { return std::get_if<bool>(&value_); }

bool Value::IsDouble() const { return std::get_if<double>(&value_); }

bool Value::IsString() const {
  return std::get_if<absl::string_view>(&value_) ||
         std::get_if<std::string>(&value_);
}

Value::Nil Value::GetNil() const { return std::get<Nil>(value_); }

bool Value::GetBool() const { return std::get<bool>(value_); }

double Value::GetDouble() const { return std::get<double>(value_); }

absl::string_view Value::GetStringView() const {
  if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else {
    return std::get<absl::string_view>(value_);
  }
}

std::optional<Value::Nil> Value::AsNil() const {
  if (auto result = std::get_if<Nil>(&value_)) {
    return *result;
  }
  return std::nullopt;
}

std::string FormatDouble(double d) {
  if (IsNan(d)) {
    if (std::signbit(d)) {
      return "-nan";
    } else {
      return "nan";
    }
  } else {
    std::ostringstream os;
    os << std::setprecision(11) << d;
    return os.str();
  }
}

std::optional<bool> Value::AsBool() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<double>(&value_)) {
    return *result != 0.0;
  } else if (std::get_if<absl::string_view>(&value_)) {
    auto dble = AsDouble();
    if (dble) {
      return dble != 0.0;
    }
    return std::nullopt;
  };
  return std::nullopt;
}

std::optional<double> Value::AsDouble() const {
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
  char* end{nullptr};
  double val = std::strtod(sv.begin(), &end);
  if (end != sv.end() || IsNan(val)) {
    return std::nullopt;
  } else {
    return val;
  }
}

std::optional<int64_t> Value::AsInteger() const {
  auto d = AsDouble();
  if (d) {
    return int64_t(*d);
  }
  return std::nullopt;
}

absl::string_view Value::AsStringView() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result ? "1" : "0";
  } else if (auto result = std::get_if<double>(&value_)) {
    if (!storage_) {
      storage_ = FormatDouble(*result);
    }
    return *storage_;
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else {
    RedisModule_Assert(false);
  }
}

std::string Value::AsString() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result ? "1" : "0";
  } else if (auto result = std::get_if<double>(&value_)) {
    return FormatDouble(*result);
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    return std::string(*result);
  } else if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else {
    RedisModule_Assert(false);
  }
}

std::ostream& operator<<(std::ostream& os, const Value& v) {
  if (v.IsNil()) {
    return os << "Nil(" << v.AsNil().value().GetReason() << ")";
  } else if (v.IsBool()) {
    return os << std::boolalpha << v.AsBool().value();
  } else if (v.IsDouble()) {
    return os << std::setprecision(10) << v.AsDouble().value();
  } else if (v.IsString()) {
    return os << "'" << v.AsStringView() << "'";
  }
  RedisModule_Assert(false);
}

static Ordering CompareDoubles(double l, double r) {
  // -ffast-math doesn't handle compares correctly with infinities or nans, we
  // do it integer.
  if (IsNan(l) || IsNan(r)) {
    return Ordering::kUNORDERED;
  }
  union {
    double d;
    int64_t i;
    uint64_t u;
  } ld, rd;
  ld.d = l;
  rd.d = r;
  // Kill negative zero
  if (ld.u == kSignBitMask) {
    ld.u = 0;
  }
  if (rd.u == kSignBitMask) {
    rd.u = 0;
  }
  if ((ld.i ^ rd.i) < 0) {
    // Signs differ. this is the easy case.
    return (ld.i < 0) ? Ordering::kLESS : Ordering::kGREATER;
  }
  if (ld.i < 0) {
    // Signs Same and Negative, convert to 2's complement
    ld.u = -ld.u;
    rd.u = -rd.u;
  }
  return ld.u == rd.u ? Ordering::kEQUAL
                      : (ld.u < rd.u ? Ordering::kLESS : Ordering::kGREATER);
}

// Todo, does this handle UTF-8 Correctly?
static Ordering CompareStrings(const absl::string_view l,
                               const absl::string_view r) {
  if (l < r) {
    return Ordering::kLESS;
  } else if (l == r) {
    return Ordering::kEQUAL;
  } else {
    return Ordering::kGREATER;
  }
}

Ordering Compare(const Value& l, const Value& r) {
  // First equivalent types

  if (l.IsNil() || r.IsNil()) {
    return (l.IsNil() && r.IsNil()) ? Ordering::kEQUAL : Ordering::kUNORDERED;
  }

  if (l.IsDouble() && r.IsDouble()) {
    return CompareDoubles(l.GetDouble(), r.GetDouble());
  }

  if (l.IsString() && r.IsString()) {
    return CompareStrings(l.GetStringView(), r.GetStringView());
  }

  // Need to handle non-equivalent types.
  // Prefer to promote to double unless that fails.

  auto ld = l.AsDouble();
  auto rd = r.AsDouble();
  if (ld && rd) {
    return CompareDoubles(ld.value(), rd.value());
  }

  return CompareStrings(l.AsStringView(), r.AsStringView());
}

Value FuncAdd(const Value& l, const Value& r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(lv.value() + rv.value());
  } else {
    return Value(Value::Nil("Add requires numeric operands"));
  }
}

Value FuncSub(const Value& l, const Value& r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(lv.value() - rv.value());
  } else {
    return Value(Value::Nil("Subtract requires numeric operands"));
  }
}

Value FuncMul(const Value& l, const Value& r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(lv.value() * rv.value());
  } else {
    return Value(Value::Nil("Multiply requires numeric operands"));
  }
}

Value FuncDiv(const Value& l, const Value& r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    if (rv.value() == 0) {
      // if (std::signbit(lv.value())) {
      return Value(std::nan(""));
      //} else {
      //  return Value(-std::abs(std::nan("nan")));
      //}
    } else {
      return Value(lv.value() / rv.value());
    }
  } else {
    return Value(Value::Nil("Divide requires numeric operands"));
  }
}

Value FuncPower(const Value& l, const Value& r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(std::pow(lv.value(), rv.value()));
  } else {
    return Value(Value::Nil("Power requires numeric operands"));
  }
}

Value FuncLt(const Value& l, const Value& r) { return Value(l < r); }

Value FuncLe(const Value& l, const Value& r) { return Value(l <= r); }

Value FuncEq(const Value& l, const Value& r) { return Value(l == r); }

Value FuncNe(const Value& l, const Value& r) { return Value(l != r); }

Value FuncGt(const Value& l, const Value& r) { return Value(l > r); }

Value FuncGe(const Value& l, const Value& r) { return Value(l >= r); }

Value FuncLor(const Value& l, const Value& r) {
  auto lv = l.AsBool();
  auto rv = r.AsBool();
  if (lv && rv) {
    return Value(lv.value() || rv.value());
  } else {
    return Value(Value::Nil("lor requires booleans"));
  }
}

Value FuncLand(const Value& l, const Value& r) {
  auto lv = l.AsBool();
  auto rv = r.AsBool();
  if (lv && rv) {
    return Value(lv.value() && rv.value());
  } else {
    return Value(Value::Nil("land requires booleans"));
  }
}

Value FuncFloor(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("floor couldn't convert to a double"));
  }
  return Value(std::floor(*d));
}

Value FuncCeil(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("ceil couldn't convert to a double"));
  }
  return Value(std::ceil(*d));
}

Value FuncAbs(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("abs couldn't convert to a double"));
  }
  return Value(std::abs(*d));
}

Value FuncLog(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("log couldn't convert to a double"));
  }
  return Value(std::log(*d));
}

Value FuncLog2(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("log2 couldn't convert to a double"));
  }
  return Value(std::log2(*d));
}

Value FuncExp(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("exp couldn't convert to a double"));
  }
  return Value(std::exp(*d));
}

Value FuncSqrt(const Value& o) {
  auto d = o.AsDouble();
  if (!d) {
    return Value(Value::Nil("sqrt couldn't convert to a double"));
  }
  return Value(std::sqrt(*d));
}

Value FuncStrlen(const Value& o) {
  return Value(double(o.AsStringView().size()));
}

Value FuncStartswith(const Value& l, const Value& r) {
  auto ls = l.AsStringView();
  auto rs = r.AsStringView();
  if (rs.size() > ls.size()) {
    return Value(false);
  } else {
    return Value(ls.substr(0, rs.size()) == rs);
  }
}

Value FuncContains(const Value& l, const Value& r) {
  auto ls = l.AsStringView();
  auto rs = r.AsStringView();
  size_t count = 0;
  size_t pos = 0;
  if (rs.size() == 0) {
    return Value(double(ls.size() + 1));
  } else {
    while ((pos = ls.find(rs, pos)) != std::string::npos) {
      count++;
      pos += rs.size();
    }
    return Value(double(count));
  }
}

Value FuncSubstr(const Value& l, const Value& m, const Value& r) {
  auto ls = l.AsStringView();
  auto offset_p = m.AsInteger();
  auto length_p = r.AsInteger();
  if (offset_p && length_p) {
    int64_t offset = *offset_p >= 0 ? *offset_p : *offset_p + ls.size();
    if (offset > ls.size() || offset < 0 || *length_p == 0) {
      return Value("");
    } else {
      if (*length_p >= 0) {
        return Value(std::string(ls.substr(offset, *length_p)));
      } else {
        int64_t len = (ls.size() - offset) + *length_p;
        if (len < 0) {
          return Value("");
        } else {
          return Value(std::string(ls.substr(offset, len)));
        }
      }
    }
  } else {
    return Value(Value::Nil("substr requires numbers for offset and length"));
  }
}

Value FuncLower(const Value& o) {
  auto os = o.AsStringView();
  std::string result;
  result.reserve(os.size());
  utils::Scanner in(os);
  for (auto utf8 = in.NextUtf8(); utf8 != utils::Scanner::kEOF;
       utf8 = in.NextUtf8()) {
    if (utf8 < 0x80) {
      utf8 = std::tolower(utf8);
    }
    utils::Scanner::PushBackUtf8(result, utf8);
  }
  return Value(std::move(result));
}

Value FuncUpper(const Value& o) {
  auto os = o.AsStringView();
  std::string result;
  result.reserve(os.size());
  utils::Scanner in(os);
  for (auto utf8 = in.NextUtf8(); utf8 != utils::Scanner::kEOF;
       utf8 = in.NextUtf8()) {
    if (utf8 < 0x80) {
      utf8 = std::toupper(utf8);
    }
    utils::Scanner::PushBackUtf8(result, utf8);
  }
  return Value(std::move(result));
}

Value FuncConcat(const absl::InlinedVector<Value, 4>& values) {
  std::string result;
  for (auto& v : values) {
    result.append(v.AsStringView());
  }
  return Value(std::move(result));
}

#define TIME_FUNCTION(funcname, field, adjustment)        \
  Value funcname(const Value& timestamp) {                \
    auto ts = timestamp.AsDouble();                       \
    if (!ts) {                                            \
      return Value(Value::Nil("timestamp not a number")); \
    }                                                     \
    time_t time = (time_t) * ts;                          \
    struct ::tm tm;                                       \
    gmtime_r(&time, &tm);                                 \
    return Value(double(tm.field + (adjustment)));        \
  }

TIME_FUNCTION(FuncDayofweek, tm_wday, 0)
TIME_FUNCTION(FuncDayofmonth, tm_mday, 0)
TIME_FUNCTION(FuncDayofyear, tm_yday, 0)
TIME_FUNCTION(FuncMonthofyear, tm_mon, 0)
TIME_FUNCTION(FuncYear, tm_year, 1900)

Value FuncTimefmt(const Value& ts, const Value& fmt) {
  auto timestampd = ts.AsDouble();
  if (!timestampd) {
    return Value(Value::Nil("timefmt: timestamp was not a number"));
  }
  struct tm tm;
  time_t timestamp = (time_t)*timestampd;
  ::gmtime_r(&timestamp, &tm);

  std::string result;
  result.resize(100);
  size_t result_bytes = 0;
  while ((result_bytes = strftime(result.data(), result.size(),
                                  fmt.AsStringView().data(), &tm)) == 0) {
    result.resize(result.size() * 2);
  }
  result.resize(result_bytes);
  return Value(std::move(result));
}

Value FuncParsetime(const Value& str, const Value& fmt) {
  auto timestr = str.AsString();  // Ensure 0 terminated
  auto fmtstr = fmt.AsString();
  struct tm tm;
  ::strptime(timestr.data(), fmtstr.data(), &tm);
  return Value(double(::mktime(&tm)));
}

#define TIME_ROUND(func, zero_day, zero_hour, zero_minute)        \
  Value func(const Value& o) {                                    \
    auto tsd = o.AsDouble();                                      \
    if (!tsd) {                                                   \
      return Value(Value::Nil(#func ": timestamp not a number")); \
    }                                                             \
    time_t ts = (time_t)(*tsd);                                   \
    struct tm tm;                                                 \
    gmtime_r(&ts, &tm);                                           \
    tm.tm_sec = 0;                                                \
    if (zero_day) {                                               \
      tm.tm_mday = 0;                                             \
    }                                                             \
    if (zero_hour) {                                              \
      tm.tm_hour = 0;                                             \
    }                                                             \
    if (zero_minute) {                                            \
      tm.tm_min = 0;                                              \
    }                                                             \
    return Value(double(::mktime(&tm)));                          \
  }

TIME_ROUND(FuncMonth, true, true, true)
TIME_ROUND(FuncDay, false, true, true)
TIME_ROUND(FuncHour, false, false, true)
TIME_ROUND(FuncMinute, false, false, false)

}  // namespace expr
}  // namespace valkey_search
