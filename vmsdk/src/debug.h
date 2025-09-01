/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_DEBUG_H_
#define VMSDK_SRC_DEBUG_H_

#include <absl/status/statusor.h>
#include <absl/strings/ascii.h>
#include <absl/strings/string_view.h>

#include <source_location>

#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace debug {

//
// PausePoints are a tool to help with debugging of background processes.
//

//
// This function is put into your code at points that you want to pause the
// current thread. A unique label is provided to distinguish this pause point
// with others.
//
#define PAUSEPOINT(name) \
  vmsdk::debug::PausePoint(name, std::source_location::current())
void PausePoint(absl::string_view point, std::source_location location);

//
// This function is used by the control machinery (FT.DEBUG) to enable/disable
// and test PausePoints.
//
void PausePointControl(absl::string_view point, bool enable);

//
// This function is used to determine how many threads are waiting at the
// PausePoint
//
absl::StatusOr<size_t> PausePointWaiters(absl::string_view point);

//
// List current state as a reply
//
void PausePointList(ValkeyModuleCtx* ctx);

//
// Controlled variables are similar to Configurables in that they are
// variables that code can use to control their behavior. Controlled Variables
// are not part of the stable API as they are specifically targetted at internal
// test code.
//
// Typical use is to declare a variable locally in a file (usually a .cc) at
// file scope, and then to use that variable in your code to control something.
//
//  CONTROLLED_BOOLEAN(variable, default_value)
//  CONTROLLED_<numeric_type>(variable, default_value)
//
//   if (variable.GetValue() ....)
//
// There is a global list of controlled variables, allowing their values to be
// controlled and examined.
//

struct ControlledBase {
  virtual ~ControlledBase();
  virtual std::string DisplayValue() const = 0;
  virtual absl::Status SetValue(absl::string_view value) = 0;
  const std::string& GetName() const { return name_; }

 protected:
  ControlledBase(absl::string_view name);
  ControlledBase(const ControlledBase&) = delete;
  ControlledBase(const ControlledBase&&) = delete;
  void operator=(const ControlledBase&) = delete;
  std::string name_;
};

template <typename T>
struct Controlled : private ControlledBase {
  bool GetValue() const { return value_; }
  std::string DisplayValue() const override {
    std::ostringstream os;
    os << value_.load(std::memory_order_relaxed);
    return os.str();
  }
  absl::Status SetValue(absl::string_view value) override {
    VMSDK_ASSIGN_OR_RETURN(value_, vmsdk::To<T>(value));
    return absl::OkStatus();
  }
  Controlled(absl::string_view name, T default_value)
      : ControlledBase(name), value_(default_value) {}

 private:
  std::atomic<T> value_;
};

//
// Override default template for boolean
//
template <>
std::string Controlled<bool>::DisplayValue() const {
  return value_ ? "on" : "off";
};

template <>
absl::Status Controlled<bool>::SetValue(absl::string_view value) {
  std::string lc_value = absl::AsciiStrToLower(value);
  if (lc_value == "on" || lc_value == "yes" || value == "1") {
    value_ = true;
    return absl::OkStatus();
  } else if (lc_value == "off" || lc_value == "no" || value == "0") {
    value_ = false;
    return absl::OkStatus();
  } else {
    return absl::InvalidArgumentError("expected yes/no or on/off or 0/1");
  }
};

#define CONTROLLED_BOOLEAN(name, default_value) \
  static vmsdk::debug::Controlled<bool> name(#name, default_value)
#define CONTROLLED_INT(name, default_value) \
  static vmsdk::debug::Controlled<int> name(#name, default_value)
#define CONTROLLED_SIZE_T(name, default_value) \
  static vmsdk::debug::Controlled<size_t> name(#name, default_value)

absl::Status ControlledSet(absl::string_view name, absl::string_view value);
absl::StatusOr<std::string> ControlledGet(absl::string_view name);
std::vector<std::pair<std::string, std::string>> ControlledGetValues();

//
// TEST_COUNTER(name)
//
// ... name.Increment(<number>)
//
// TEST_COUNTER must be declared at file level and is always static.
//
// The corresponding INFO field will be in section "test-counters" and will have
// the name test-counter-<name>
//
// Is shorthand for defining a counter that's only used for test purposes. It
// won't be visible in non-test instances.
//
#define TEST_COUNTER(name)                    \
  static vmsdk::info_field::Integer name(     \
      "test-counters", "test-counter-" #name, \
      vmsdk::info_field::IntegerBuilder().Dev())

}  // namespace debug
}  // namespace vmsdk

#endif