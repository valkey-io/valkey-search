/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/expr/value.h"

#include <cmath>
#include <ctime>
#include <iomanip>

#include "src/utils/scanner.h"
#include "src/valkey_search_options.h"  // VALKEY_SEARCH_COMPATIBILITY_FIX
#include "vmsdk/src/info.h"

// #define DBG std::cerr
#define DBG 0 && std::cerr
namespace valkey_search {
namespace expr {

// -ffast-math disables std::is_nan and std::is_inf
static const uint64_t kSignBitMask = 0x8000000000000000ull;
static const uint64_t kExponentMask = 0x7FF0000000000000ull;
static const uint64_t kMantissaMask = 0x000FFFFFFFFFFFFFull;

// Built-in isnan doesn't work when compiling with fast-math, which is what we
// want to do.
static bool IsNan(const double &d) {
  uint64_t v = *(uint64_t *)&d;
  return ((v & kExponentMask) == kExponentMask) && ((v & kMantissaMask) != 0);
}

static bool IsInf(const double &d) {
  uint64_t v = *(uint64_t *)&d;
  return ((v & kExponentMask) == kExponentMask) && ((v & kMantissaMask) == 0);
}

Value::Value(double d) { value_ = d; }

bool Value::IsNil() const { return std::get_if<Nil>(&value_); }

bool Value::IsBool() const { return std::get_if<bool>(&value_); }

bool Value::IsDouble() const { return std::get_if<double>(&value_); }

bool Value::IsString() const {
  return std::get_if<absl::string_view>(&value_) ||
         std::get_if<std::string>(&value_);
}

bool Value::IsArray() const { return std::holds_alternative<Array>(value_); }

size_t Value::ArraySize() const {
  CHECK(IsArray());
  return std::get<Array>(value_)->size();
}

bool Value::IsEmptyArray() const {
  if (auto vec_ptr = std::get_if<Array>(&value_)) {
    return (*vec_ptr)->empty();
  }
  return false;
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
    char storage[50];
    size_t output_chars = snprintf(storage, sizeof(storage), "%.11g", d);
    return {storage, output_chars};
  }
}

std::optional<bool> Value::AsBool() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result;
  }
  if (auto result = std::get_if<double>(&value_)) {
    if (IsNan(*result)) {
      return true;
    }
    return !(*result == 0.0);
  }
  // 1.2.1 fix: non-empty strings are truthy (matches Redisearch). Pre-1.2.1
  // every non-numeric value (Nil, both string variants) evaluated to false.
  // Both string variants share the same counter via a common literal.
  absl::string_view sv;
  if (auto p = std::get_if<absl::string_view>(&value_)) {
    sv = *p;
  } else if (auto p = std::get_if<std::string>(&value_)) {
    sv = *p;
  } else {
    return false;  // Nil
  }
  return VALKEY_SEARCH_COMPATIBILITY_FIX(
      1, 2, 1, "asbool_string_truthy",
      [&] { return !sv.empty(); },  // new: JS-style truthiness
      [&] { return false; });       // legacy: always false
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
  char *end{nullptr};
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

std::optional<absl::string_view> Value::AsStringView() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return *result ? "1" : "0";
  } else if (auto result = std::get_if<double>(&value_)) {
    if (!storage_) {
      storage_ = FormatDouble(*result);
    }
    return absl::string_view(*storage_);
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<std::string>(&value_)) {
    return absl::string_view(*result);
  } else {
    return std::nullopt;
  }
}

std::optional<std::string> Value::AsString() const {
  if (auto result = std::get_if<bool>(&value_)) {
    return std::string(*result ? "1" : "0");
  } else if (auto result = std::get_if<double>(&value_)) {
    return FormatDouble(*result);
  } else if (auto result = std::get_if<absl::string_view>(&value_)) {
    return std::string(*result);
  } else if (auto result = std::get_if<std::string>(&value_)) {
    return *result;
  } else if (auto result = std::get_if<Value::Array>(&value_)) {
    return "";
  } else {
    return std::nullopt;
  }
}

std::optional<Value::Array> Value::AsArray() const {
  if (auto result = std::get_if<Array>(&value_)) {
    return *result;
  }
  return std::nullopt;
}

Value::Array Value::GetArray() const { return std::get<Array>(value_); }

Value Value::GetArrayElement(size_t index) const {
  auto arr = GetArray();

  CHECK(index < arr->size())
      << "GetArrayElement called with index out of range: " << index
      << ". Array size = " << arr->size();

  return (*arr)[index];
}

std::ostream &operator<<(std::ostream &os, const Value &v) {
  if (v.IsNil()) {
    return os << "Nil(" << v.AsNil().value().GetReason() << ")";
  } else if (v.IsBool()) {
    return os << "Bool(" << std::boolalpha << v.AsBool().value() << ")";
  } else if (v.IsDouble()) {
    return os << "Dble(" << std::setprecision(10) << v.AsDouble().value()
              << ")";
  } else if (v.IsString()) {
    // IsString() guarantees AsStringView() succeeds.
    return os << "'" << *v.AsStringView() << "'";
  }
  CHECK(false);
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

Ordering Compare(const Value &l, const Value &r) {
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

  // Array comparisons
  if (l.IsArray() && r.IsArray()) {
    auto lvec = l.GetArray();
    auto rvec = r.GetArray();

    size_t min_size = std::min(lvec->size(), rvec->size());
    if (min_size > 0) {
      // Match RediSearch behavior by only comparing first elements
      return Compare((*lvec)[0], (*rvec)[0]);
    }

    // All elements equal, compare by length
    if (lvec->size() < rvec->size()) {
      return Ordering::kLESS;
    } else if (lvec->size() > rvec->size()) {
      return Ordering::kGREATER;
    }
    return Ordering::kEQUAL;
  } else if (l.IsArray() || r.IsArray()) {
    // Array vs scalar
    return Ordering::kUNORDERED;
  }

  // Need to handle non-equivalent types.
  // Prefer to promote to double unless that fails.
  auto ld = l.AsDouble();
  auto rd = r.AsDouble();
  if (ld && rd) {
    return CompareDoubles(ld.value(), rd.value());
  }

  // Nil cases were filtered above; both sides have a string representation.
  return CompareStrings(*l.AsStringView(), *r.AsStringView());
}

Value FuncAdd(const Value &l, const Value &r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(lv.value() + rv.value());
  } else {
    return Value(Value::Nil("Add requires numeric operands"));
  }
}

Value FuncSub(const Value &l, const Value &r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(lv.value() - rv.value());
  } else {
    return Value(Value::Nil("Subtract requires numeric operands"));
  }
}

Value FuncMul(const Value &l, const Value &r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(lv.value() * rv.value());
  } else {
    return Value(Value::Nil("Multiply requires numeric operands"));
  }
}

Value FuncDiv(const Value &l, const Value &r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    if (rv.value() == 0) {
      return Value(std::nan(""));
    } else {
      return Value(lv.value() / rv.value());
    }
  } else {
    return Value(Value::Nil("Divide requires numeric operands"));
  }
}

Value FuncPower(const Value &l, const Value &r) {
  auto lv = l.AsDouble();
  auto rv = r.AsDouble();
  if (lv && rv) {
    return Value(std::pow(lv.value(), rv.value()));
  } else {
    return Value(Value::Nil("Power requires numeric operands"));
  }
}

Value FuncLt(const Value &l, const Value &r) { return Value(l < r); }

Value FuncLe(const Value &l, const Value &r) { return Value(l <= r); }

Value FuncEq(const Value &l, const Value &r) { return Value(l == r); }

Value FuncNe(const Value &l, const Value &r) { return Value(l != r); }

Value FuncGt(const Value &l, const Value &r) { return Value(l > r); }

Value FuncGe(const Value &l, const Value &r) { return Value(l >= r); }

Value FuncLor(const Value &l, const Value &r) {
  DBG << "FuncLor: " << l << " || " << r << "\n";
  auto lv = l.AsBool();
  auto rv = r.AsBool();
  if (lv && rv) {
    return Value(lv.value() || rv.value());
  } else {
    return Value(Value::Nil("lor requires booleans"));
  }
}

Value FuncLand(const Value &l, const Value &r) {
  DBG << "FuncLand: " << l << " && " << r << "\n";
  auto lv = l.AsBool();
  auto rv = r.AsBool();
  if (lv && rv) {
    DBG << "FuncLand -> " << lv.value() << " && " << rv.value() << " -> "
        << Value(lv.value() && rv.value()) << "\n";
    return Value(lv.value() && rv.value());
  } else {
    return Value(Value::Nil("land requires booleans"));
  }
}

// Shared "AsDouble failed" return path for unary numeric functions.
// Pre-1.2.1 path: always returns Nil. 1.2.1 fix: returns NaN for inputs
// that aren't Nil (typically a non-numeric string like "a"), matching
// Redisearch's NaN-on-coercion-failure semantics. Nil-typed inputs still
// propagate as Nil regardless of emulate-release, so layered expressions
// like `abs(@missing_field)` keep clean Nil propagation.
static Value NumericUnaryNil(const Value &o, const char *fname) {
  if (o.IsNil()) {
    return Value(Value::Nil(fname));
  }
  return VALKEY_SEARCH_COMPATIBILITY_FIX(
      1, 2, 1, "numeric_unary_nan_on_unparsable",
      [&] { return Value(std::nan("")); },        // new: NaN propagation
      [&] { return Value(Value::Nil(fname)); });  // legacy: Nil
}

Value FuncFloor(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "floor couldn't convert to a double");
  }
  return Value(std::floor(*d));
}

Value FuncCeil(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "ceil couldn't convert to a double");
  }
  return Value(std::ceil(*d));
}

Value FuncAbs(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "abs couldn't convert to a double");
  }
  return Value(std::abs(*d));
}

Value FuncLog(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "log couldn't convert to a double");
  }
  return Value(std::log(*d));
}

Value FuncLog2(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "log2 couldn't convert to a double");
  }
  return Value(std::log2(*d));
}

Value FuncExp(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "exp couldn't convert to a double");
  }
  return Value(std::exp(*d));
}

Value FuncSqrt(const Value &o) {
  auto d = o.AsDouble();
  if (!d) {
    return NumericUnaryNil(o, "sqrt couldn't convert to a double");
  }
  return Value(std::sqrt(*d));
}

Value FuncStrlen(const Value &o) {
  if (o.IsArray()) {
    return Value();
  }
  auto os = o.AsStringView();
  if (!os) {
    return Value(Value::Nil("strlen: operand has no string representation"));
  }
  return Value(double(os->size()));
}

Value FuncStartswith(const Value &l, const Value &r) {
  if (l.IsArray() || r.IsArray()) {
    return Value();
  }
  auto ls = l.AsStringView();
  auto rs = r.AsStringView();
  if (!ls || !rs) {
    return Value(
        Value::Nil("startswith: operand has no string representation"));
  }
  if (rs->size() > ls->size()) {
    return Value(false);
  } else {
    return Value(ls->substr(0, rs->size()) == *rs);
  }
}

Value FuncContains(const Value &l, const Value &r) {
  if (l.IsArray() || r.IsArray()) {
    return Value();
  }

  auto ls = l.AsStringView();
  auto rs = r.AsStringView();
  if (!ls || !rs) {
    return Value(Value::Nil("contains: operand has no string representation"));
  }
  size_t count = 0;
  size_t pos = 0;
  if (rs->size() == 0) {
    return Value(double(ls->size() + 1));
  } else {
    while ((pos = ls->find(*rs, pos)) != std::string::npos) {
      count++;
      pos += rs->size();
    }
    return Value(double(count));
  }
}

Value FuncSubstr(const Value &l, const Value &m, const Value &r) {
  if (l.IsArray() || m.IsArray() || r.IsArray()) {
    return Value(Value::Nil("Invalid type for substr. Expected string"));
  }

  auto ls = l.AsStringView();
  auto offset_p = m.AsInteger();
  auto length_p = r.AsInteger();
  if (!ls) {
    return Value(Value::Nil("substr: source has no string representation"));
  }
  if (offset_p && length_p) {
    int64_t offset = *offset_p >= 0 ? *offset_p : *offset_p + ls->size();
    if (offset > static_cast<int64_t>(ls->size()) || offset < 0 ||
        *length_p == 0) {
      return Value("");
    } else {
      if (*length_p >= 0) {
        return Value(std::string(ls->substr(offset, *length_p)));
      } else {
        int64_t len = (ls->size() - offset) + *length_p;
        if (len < 0) {
          return Value("");
        } else {
          return Value(std::string(ls->substr(offset, len)));
        }
      }
    }
  } else {
    return Value(Value::Nil("substr requires numbers for offset and length"));
  }
}

Value FuncLower(const Value &o) {
  if (o.IsArray()) {
    return Value();
  }
  // 1.2.1 fix: refuse non-string inputs (matches Redisearch — lower(0) → Nil).
  // Pre-1.2.1: passed numeric/bool through via AsStringView, returning
  // their string form unchanged.
  if (!o.IsString() && VALKEY_SEARCH_COMPATIBILITY_FIX(
                           1, 2, 1, "lower_non_string_to_nil",
                           [] { return true; }, [] { return false; })) {
    return Value(Value::Nil("lower: operand is not a string"));
  }
  auto os = o.AsStringView();
  if (!os) {
    return Value(Value::Nil("lower: operand has no string representation"));
  }
  std::string result;
  result.reserve(os->size());
  utils::Scanner in(*os);
  for (auto utf8 = in.NextUtf8(); utf8 != utils::Scanner::kEOF;
       utf8 = in.NextUtf8()) {
    if (utf8 < 0x80) {
      utf8 = std::tolower(utf8);
    }
    utils::Scanner::PushBackUtf8(result, utf8);
  }
  return Value(std::move(result));
}

Value FuncUpper(const Value &o) {
  if (o.IsArray()) {
    return Value();
  }
  // See FuncLower above for rationale.
  if (!o.IsString() && VALKEY_SEARCH_COMPATIBILITY_FIX(
                           1, 2, 1, "upper_non_string_to_nil",
                           [] { return true; }, [] { return false; })) {
    return Value(Value::Nil("upper: operand is not a string"));
  }
  auto os = o.AsStringView();
  if (!os) {
    return Value(Value::Nil("upper: operand has no string representation"));
  }
  std::string result;
  result.reserve(os->size());
  utils::Scanner in(*os);
  for (auto utf8 = in.NextUtf8(); utf8 != utils::Scanner::kEOF;
       utf8 = in.NextUtf8()) {
    if (utf8 < 0x80) {
      utf8 = std::toupper(utf8);
    }
    utils::Scanner::PushBackUtf8(result, utf8);
  }
  return Value(std::move(result));
}

// 1.2.1 fix shared by all date functions: pre-epoch (negative) timestamps
// return Nil instead of computing a (negative) calendar value. Returns true
// when the new (Nil) behavior should be taken; false to fall through to the
// legacy path. Single counter for all date functions.
static bool DateNegativeTsReturnsNil() {
  return VALKEY_SEARCH_COMPATIBILITY_FIX(
      1, 2, 1, "date_fn_negative_ts_to_nil", [] { return true; },
      [] { return false; });
}

Value FuncConcat(const absl::InlinedVector<Value, 4> &values) {
  std::string result;
  for (auto &v : values) {
    auto s = v.AsStringView();
    if (!s) {
      return Value(Value::Nil("concat: operand has no string representation"));
    }
    result.append(*s);
  }
  return Value(std::move(result));
}

// Macro for the date-component extractors that still depend on gmtime_r.
// Guards: timestamp must be a finite number. The negative-timestamp guard
// is the 1.2.1 fix (gated via DateNegativeTsReturnsNil); the finite guard
// is always-on UB hardening — restoring it under emulate-release would
// re-introduce UB at the (time_t) cast / gmtime_r partial-write on
// overflow.
#define TIME_FUNCTION(funcname, field, adjustment)               \
  Value funcname(const Value &timestamp) {                       \
    auto ts = timestamp.AsDouble();                              \
    if (!ts) {                                                   \
      return Value(Value::Nil("timestamp not a number"));        \
    }                                                            \
    if (IsNan(*ts) || IsInf(*ts)) {                              \
      return Value(Value::Nil("timestamp is not finite"));       \
    }                                                            \
    if (*ts < 0 && DateNegativeTsReturnsNil()) {                 \
      return Value(Value::Nil("timestamp is before the epoch")); \
    }                                                            \
    time_t time = (time_t) * ts;                                 \
    struct ::tm tm;                                              \
    gmtime_r(&time, &tm);                                        \
    return Value(double(tm.field + (adjustment)));               \
  }

TIME_FUNCTION(FuncDayofmonth, tm_mday, 0)
TIME_FUNCTION(FuncDayofyear, tm_yday, 0)
TIME_FUNCTION(FuncMonthofyear, tm_mon, 0)
TIME_FUNCTION(FuncYear, tm_year, 1900)

// Pure-arithmetic dayofweek: avoids gmtime_r entirely. Jan 1 1970 (ts=0)
// was a Thursday — POSIX day index 4 (0=Sun..6=Sat).
Value FuncDayofweek(const Value &o) {
  auto tsd = o.AsDouble();
  if (!tsd) {
    return Value(Value::Nil("dayofweek: timestamp not a number"));
  }
  // Always-on UB hardening: pure-arithmetic implementation propagates
  // NaN/inf into UB at the int64 cast below, so refuse them here.
  if (IsNan(*tsd) || IsInf(*tsd)) {
    return Value(Value::Nil("dayofweek: timestamp is not finite"));
  }
  // 1.2.1 fix: pre-epoch → Nil.
  if (*tsd < 0 && DateNegativeTsReturnsNil()) {
    return Value(Value::Nil("dayofweek: timestamp is before the epoch"));
  }
  int64_t days = static_cast<int64_t>(std::floor(*tsd / 86400.0));
  // Floored modulo: works correctly even when the < 0 guard is relaxed.
  int64_t r = ((days + 4) % 7 + 7) % 7;
  return Value(double(r));
}

Value FuncTimefmt(const Value &ts, const Value &fmt) {
  auto timestampd = ts.AsDouble();
  if (!timestampd) {
    return Value(Value::Nil("timefmt: timestamp was not a number"));
  }
  if (IsInf(*timestampd)) {
    return Value(Value::Nil("timefmt: timestamp is not finite"));
  }
  // Note: unlike the component extractors (month/day/hour/…), timefmt
  // happily formats pre-epoch (negative) timestamps to match Redisearch.
  auto fmtstr = fmt.AsStringView();
  if (!fmtstr) {
    return Value(Value::Nil("timefmt: format has no string representation"));
  }
  if (fmtstr->empty()) {
    // 1.2.1 fix: empty format → Nil (matches Redisearch).
    // Pre-1.2.1: returned an empty string as a fast-path.
    return VALKEY_SEARCH_COMPATIBILITY_FIX(
        1, 2, 1, "timefmt_empty_format_to_nil",
        [] { return Value(Value::Nil("timefmt: empty format string")); },
        [] { return Value(""); });
  }
  // strftime needs a NUL-terminated format string. AsStringView() may return
  // a view into storage that isn't NUL-terminated (e.g. a substring), so copy.
  std::string fmt_z(*fmtstr);
  struct tm tm;
  time_t timestamp = (time_t)*timestampd;
  ::gmtime_r(&timestamp, &tm);

  std::string result;
  result.resize(100);
  size_t result_bytes = 0;
  while ((result_bytes = strftime(result.data(), result.size(), fmt_z.c_str(),
                                  &tm)) == 0) {
    result.resize(result.size() * 2);
  }
  result.resize(result_bytes);
  return Value(std::move(result));
}

Value FuncParsetime(const Value &str, const Value &fmt) {
  auto timestr = str.AsString();  // Ensure 0 terminated
  auto fmtstr = fmt.AsString();
  if (!timestr || !fmtstr) {
    return Value(Value::Nil("parsetime: operand has no string representation"));
  }
  // Zero-init is always-on: reverting it would restore an uninitialized-tm
  // read (UB). Without zero-init the result was nondeterministic, so no
  // user could have depended on it.
  struct tm tm = {};
  char *res = ::strptime(timestr->data(), fmtstr->data(), &tm);
  if (res == nullptr) {
    // 1.2.1 fix: strptime returning NULL → Nil (matches Redisearch).
    // Pre-1.2.1: ignored the NULL return and fed the zeroed tm to mktime,
    // producing a constant -2209075200 (Dec 31 1899 UTC). That happened to
    // line up with how Redisearch handles _successful_ no-op parses, but
    // differs for failed parses.
    if (VALKEY_SEARCH_COMPATIBILITY_FIX(
            1, 2, 1, "parsetime_format_mismatch_to_nil", [] { return true; },
            [] { return false; })) {
      return Value(Value::Nil("parsetime: format mismatch"));
    }
  }
  tm.tm_isdst = -1;  // Don't try to figure out DST, just use UTC
  return Value(double(::mktime(&tm)));
}

// Month-rounding: still needs gmtime_r/mktime because month boundaries
// are not a fixed period (28/29/30/31 days). The finite guard is always-on
// UB hardening; the negative-ts guard is the 1.2.1 fix.
Value FuncMonth(const Value &o) {
  auto tsd = o.AsDouble();
  if (!tsd) {
    return Value(Value::Nil("month: timestamp not a number"));
  }
  if (IsNan(*tsd) || IsInf(*tsd)) {
    return Value(Value::Nil("month: timestamp is not finite"));
  }
  if (*tsd < 0 && DateNegativeTsReturnsNil()) {
    return Value(Value::Nil("month: timestamp is before the epoch"));
  }
  time_t ts = (time_t)(*tsd);
  struct tm tm;
  gmtime_r(&ts, &tm);
  tm.tm_sec = 0;
  tm.tm_min = 0;
  tm.tm_hour = 0;
  // 1.2.1 fix: tm_mday=1 (first day of the month, matches Redisearch).
  // Pre-1.2.1: tm_mday=0 which mktime rolls back to the last day of the
  // previous month — off by 86400 seconds.
  tm.tm_mday = VALKEY_SEARCH_COMPATIBILITY_FIX(
      1, 2, 1, "month_mday_off_by_one",
      [] { return 1; },   // new: first of the month
      [] { return 0; });  // legacy: rolled back one day
  return Value(double(::mktime(&tm)));
}

// Fixed-period rounding: pure arithmetic, no gmtime_r/mktime, no UB on
// non-finite inputs. Matches Redisearch's arithmetic-style implementation
// (which is why minute(+inf) returns NaN there).
//
// `notnum`, `notfinite`, `negative` are static-storage string literals
// supplied by each caller — Value::Nil holds a non-owning const char*.
static Value RoundToPeriod(const Value &o, double period, const char *notnum,
                           const char *notfinite, const char *negative) {
  auto tsd = o.AsDouble();
  if (!tsd) {
    return Value(Value::Nil(notnum));
  }
  if (IsNan(*tsd) || IsInf(*tsd)) {
    return Value(Value::Nil(notfinite));
  }
  // 1.2.1 fix: pre-epoch → Nil.
  if (*tsd < 0 && DateNegativeTsReturnsNil()) {
    return Value(Value::Nil(negative));
  }
  // floor (not trunc) so negative timestamps still round correctly when
  // the < 0 guard is relaxed under emulate-release < 1.2.1.
  return Value(std::floor(*tsd / period) * period);
}

Value FuncDay(const Value &o) {
  return RoundToPeriod(o, 86400.0, "day: timestamp not a number",
                       "day: timestamp is not finite",
                       "day: timestamp is before the epoch");
}
Value FuncHour(const Value &o) {
  return RoundToPeriod(o, 3600.0, "hour: timestamp not a number",
                       "hour: timestamp is not finite",
                       "hour: timestamp is before the epoch");
}
Value FuncMinute(const Value &o) {
  return RoundToPeriod(o, 60.0, "minute: timestamp not a number",
                       "minute: timestamp is not finite",
                       "minute: timestamp is before the epoch");
}

}  // namespace expr
}  // namespace valkey_search
