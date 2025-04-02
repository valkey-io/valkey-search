/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VMSDK_SRC_CONFIGURABLE_H_
#define VMSDK_SRC_CONFIGURABLE_H_

#include <iostream>
#include <limits>
#include <map>
#include <string>

#include "absl/log/check.h"
#include "absl/strings/ascii.h"
#include "status_macros.h"
#include "type_conversions.h"
#include "vmsdk/src/log.h"

namespace vmsdk {
namespace config {
//
// Flags to further specify the behavior of the config
// These can be specified in a constructor.
//
enum class Flags {
  kDefault = REDISMODULE_CONFIG_DEFAULT,
  kImmutable = REDISMODULE_CONFIG_IMMUTABLE,
  kSensitive = REDISMODULE_CONFIG_SENSITIVE,
  kHidden = REDISMODULE_CONFIG_HIDDEN,
  kProtected = REDISMODULE_CONFIG_PROTECTED,
  kDenyLoading = REDISMODULE_CONFIG_DENY_LOADING,
  kMemory = REDISMODULE_CONFIG_MEMORY,
  kBitFlags = REDISMODULE_CONFIG_BITFLAGS,
};

inline Flags operator|(Flags lhs, Flags rhs) {
  return static_cast<Flags>(static_cast<int>(lhs) | static_cast<int>(rhs));
}

inline bool operator&(Flags lhs, Flags rhs) {
  return (static_cast<int>(lhs) & static_cast<int>(rhs)) != 0;
}

class ConfigurableBase {
 public:
  //
  // Called at startup to register all configurables
  //
  static absl::Status OnStartup(RedisModuleCtx*);

  static absl::Status ParseCommandLine(RedisModuleString** argv, int argc);

  // Debug/Diagnostics functions
  static void DumpAll(std::ostream& os);
  static std::map<std::string, std::pair<std::string, std::string>>
  GetAllAsMap();
  static void Reset();

  const std::string& GetName() const { return name_; }
  Flags GetFlags() const { return flags_; }

  virtual std::string ToString() const = 0;
  virtual absl::Status FromRedisString(RedisModuleString* str) = 0;

 protected:
  ConfigurableBase(const char* name, Flags flags)
      : name_(absl::AsciiStrToLower(name)), flags_(flags) {
    if (!bases) {
      //
      // Doing manual creation of the this map eliminates any dependency on link
      // order.
      //
      bases = std::make_unique<std::map<std::string, ConfigurableBase*>>();
    }
    auto& base_map = *bases;
    if (base_map[name_]) {
      VMSDK_LOG(WARNING, nullptr)
          << "Configurable " << name << " Is defined twice.\n";
      CHECK(false);
    }
    base_map[name_] = this;
  }
  virtual int Register(RedisModuleCtx*) = 0;
  virtual void SetDefault() = 0;

  static bool Initialized;

 private:
  const std::string name_;
  const Flags flags_;

  static std::unique_ptr<std::map<std::string, ConfigurableBase*>> bases;
};

template <typename CType, typename ValkeyType>
class Configurable : public ConfigurableBase {
 public:
  //
  // Get the current value
  //
  const CType& Get() const {
    CHECK(Initialized);
    return c_value_;
  }
  Configurable(Flags flags, const char* name)
      : ConfigurableBase(name, flags), c_value_(), valkey_value_() {}

 protected:
  virtual int SetFunction(ValkeyType new_value, RedisModuleString** error) = 0;
  virtual int ApplyFunction(RedisModuleCtx* ctx, RedisModuleString** error) {
    return REDISMODULE_OK;
  }
  virtual ValkeyType GetFunction() const { return valkey_value_; }

  static ValkeyType GetFn(const char* name, void* privdata) {
    return static_cast<Configurable*>(privdata)->GetFunction();
  }
  static int SetFn(const char* name, ValkeyType value, void* privdata,
                   RedisModuleString** error) {
    return static_cast<Configurable*>(privdata)->SetFunction(value, error);
  }
  static int ApplyFn(RedisModuleCtx* ctx, void* privdata,
                     RedisModuleString** error) {
    return static_cast<Configurable*>(privdata)->ApplyFunction(ctx, error);
  }

  //
  // Keep both the "C" value and the Valkey value. They will always be kept in
  // sync.
  //
  CType c_value_;
  ValkeyType valkey_value_;
};

//
// Concrete class for Integer.
// Offers range validation
//
class Number : public Configurable<long long, long long> {
 public:
  Number(const char* name, long long default_value,
         long long min_value = std::numeric_limits<long long>::lowest(),
         long long max_value = std::numeric_limits<long long>::max())
      : Configurable(Flags::kDefault, name),
        default_value_(default_value),
        min_value_(min_value),
        max_value_(max_value) {}
  Number(Flags flags, const char* name, long long default_value,
         long long min_value = std::numeric_limits<long long>::lowest(),
         long long max_value = std::numeric_limits<long long>::max())
      : Configurable(flags, name),
        default_value_(default_value),
        min_value_(min_value),
        max_value_(max_value) {}

 private:
  long long default_value_;
  long long min_value_;
  long long max_value_;

  void SetDefault() override { valkey_value_ = c_value_ = default_value_; }

  int Register(RedisModuleCtx* ctx) override {
    return RedisModule_RegisterNumericConfig(
        ctx, GetName().data(), default_value_, static_cast<int>(GetFlags()),
        min_value_, max_value_, GetFn, SetFn, ApplyFn,
        static_cast<void*>(this));
  }

  std::string ToString() const override { return std::to_string(c_value_); }
  absl::Status FromRedisString(RedisModuleString* str) override {
    long long value;
    VMSDK_ASSIGN_OR_RETURN(value, vmsdk::To<long long>(str));
    if (value < min_value_ || value > max_value_) {
      return absl::InvalidArgumentError(absl::StrCat(
          "For parameter:", GetName(), " Value:", value,
          " is out of the valid range:[", min_value_, ":", max_value_, "]"));
    }
    CHECK(SetFunction(value, nullptr) == REDISMODULE_OK);
    return absl::OkStatus();
  }

  int SetFunction(long long new_value, RedisModuleString** error) override {
    valkey_value_ = c_value_ = new_value;
    return REDISMODULE_OK;
  }
};

//
// Concrete class for Boolean
//
class Boolean : public Configurable<bool, int> {
 public:
  Boolean(const char* name, bool default_value)
      : Configurable(Flags::kDefault, name), default_value_(default_value) {}
  Boolean(Flags flags, const char* name, bool default_value)
      : Configurable(flags, name), default_value_(default_value) {}

 private:
  bool default_value_;

  void SetDefault() override {
    c_value_ = default_value_;
    valkey_value_ = static_cast<int>(default_value_);
  }

  int Register(RedisModuleCtx* ctx) override {
    return RedisModule_RegisterBoolConfig(
        ctx, GetName().data(), default_value_, static_cast<int>(GetFlags()),
        GetFn, SetFn, ApplyFn, static_cast<void*>(this));
  }

  std::string ToString() const override { return c_value_ ? "On" : "Off"; }
  absl::Status FromRedisString(RedisModuleString* str) override {
    bool value;
    VMSDK_ASSIGN_OR_RETURN(value, vmsdk::To<bool>(str));
    CHECK(SetFunction(value ? 1 : 0, nullptr) == REDISMODULE_OK);
    return absl::OkStatus();
  }

  int SetFunction(int new_value, RedisModuleString** error) override {
    valkey_value_ = new_value;
    c_value_ = valkey_value_ != 0;
    return REDISMODULE_OK;
  }
};

//
// Concrete class for String
//
class String : public Configurable<std::string, RedisModuleString*> {
 public:
  String(const char* name, absl::string_view default_value)
      : Configurable(Flags::kDefault, name), default_value_(default_value) {}
  String(Flags flags, const char* name, absl::string_view default_value)
      : Configurable(flags, name), default_value_(default_value) {}

 private:
  std::string default_value_;

  std::string ToString() const override { return c_value_; }
  absl::Status FromRedisString(RedisModuleString* str) override {
    CHECK(SetFunction(str, nullptr) == REDISMODULE_OK);
    return absl::OkStatus();
  }

  void SetDefault() override {
    c_value_ = default_value_;
    valkey_value_ = RedisModule_CreateString(nullptr, default_value_.data(),
                                             default_value_.size());
  }

  int Register(RedisModuleCtx* ctx) override {
    return RedisModule_RegisterStringConfig(
        ctx, GetName().data(), default_value_.data(),
        static_cast<int>(GetFlags()), GetFn, SetFn, ApplyFn,
        static_cast<void*>(this));
  }
  int SetFunction(RedisModuleString* new_value,
                  RedisModuleString** error) override {
    if (valkey_value_) {
      RedisModule_Free(valkey_value_);
    }
    RedisModule_RetainString(nullptr, new_value);
    valkey_value_ = new_value;

    size_t len;
    auto ptr = RedisModule_StringPtrLen(new_value, &len);
    c_value_ = std::string(ptr, len);
    return REDISMODULE_OK;
  }
};

//
// class for Enums
//
class Enum : public Configurable<int, int> {
 public:
  Enum(const char* name, int default_value, std::vector<const char*> names,
       std::vector<int> values)
      : Enum(Flags::kDefault, name, default_value, names, values) {}
  Enum(Flags flags, const char* name, int default_value,
       std::vector<const char*> names, std::vector<int> values)
      : Configurable(flags, name),
        default_value_(default_value),
        names_(names),
        values_(values) {
    CHECK(names_.size() == values_.size());
  }

 private:
  int default_value_;
  std::vector<const char*> names_;
  std::vector<int> values_;

  void SetDefault() override { c_value_ = valkey_value_ = default_value_; }

  int Register(RedisModuleCtx* ctx) override {
    return RedisModule_RegisterEnumConfig(
        ctx, GetName().data(), static_cast<int>(default_value_),
        static_cast<int>(GetFlags()), names_.data(), values_.data(),
        static_cast<int>(values_.size()), GetFn, SetFn, ApplyFn,
        static_cast<void*>(this));
  }
  int SetFunction(int new_value, RedisModuleString** error) override {
    c_value_ = valkey_value_ = new_value;
    return REDISMODULE_OK;
  }
  absl::Status FromRedisString(RedisModuleString*) override;

  std::string ToString() const override;
};

}  // namespace config
}  // namespace vmsdk

#endif
