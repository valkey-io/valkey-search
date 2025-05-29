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
#pragma once

#include <absl/container/flat_hash_map.h>

#include <type_traits>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "gtest/gtest_prod.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace config {

/// Flags to further specify the behavior of the config
/// These can be specified using the Builder().WithFlags(...) method (see below)
enum Flags {
  kDefault = REDISMODULE_CONFIG_DEFAULT,
  kImmutable = REDISMODULE_CONFIG_IMMUTABLE,
  kSensitive = REDISMODULE_CONFIG_SENSITIVE,
  kHidden = REDISMODULE_CONFIG_HIDDEN,
  kProtected = REDISMODULE_CONFIG_PROTECTED,
  kDenyLoading = REDISMODULE_CONFIG_DENY_LOADING,
  kMemory = REDISMODULE_CONFIG_MEMORY,
  kBitFlags = REDISMODULE_CONFIG_BITFLAGS,
};

/// Support Valkey configuration entries in a one-liner.
///
/// Example usage:
///
/// At the top of the file that you need this configuration entry, add a code
/// block similar to this:
///
///
/// ```c++
/// namespace config = vmsdk::config;
///
/// // Create a Valkey configuration item named "config-name" of type `long
/// // long`
/// static auto my_config =
///    config::Number(
///        "config-name",         // name
///        8,                     // default size
///        1,                     // min size
///        1024)                  // max size
///        .WithModifyCallback(   // set an "On-Modify" callback
///            [](long long new_value) {
///              // ..do something ...
///            })
///        .WithValidationCallback(   // Add validation function
///            [](const long long new_value) -> absl::Status {
///              return absl::OkStatus();
///            })
///        .WithFlags(REDISMODULE_CONFIG_DEFAULT)
///        .Build();
/// ```
///
/// Adding the above lines, enables the following commands within Valkey:
///
/// ```
/// CONFIG SET search.config-name <value>
/// CONFIG GET search.config-name
/// ```

class Registerable;
class ModuleConfigManager {
 public:
  ModuleConfigManager() = default;

  static ModuleConfigManager &Instance();

  /// Do the actual registration with Valkey for all configuration items that
  /// previously registered themselves with this manager
  absl::Status Init(RedisModuleCtx *ctx);

  /// Parse and load command line arguments
  absl::Status ParseAndLoadArgv(RedisModuleCtx *ctx, RedisModuleString **argv,
                                int argc);
  /// Call this method to register a configuration item with this manager. This
  /// method is mainly used by the constructor of `ConfigBase` so users should
  /// not call it directly.
  void RegisterConfig(Registerable *config_item);

  /// Call this method to un-register a configuration item with this manager.
  /// Usually this is done by the destructor of the `Registerable` and is not
  /// needed manually
  void UnregisterConfig(Registerable *config_item);

 private:
  absl::Status UpdateConfigFromKeyVal(RedisModuleCtx *ctx, std::string_view key,
                                      std::string_view value);
  absl::flat_hash_map<std::string, Registerable *> entries_;
  friend class Configbase;
};

/// A self registering configuration class
class Registerable {
 public:
  Registerable(std::string_view name) : name_(name) {}
  virtual ~Registerable() = default;

  virtual absl::Status Register(RedisModuleCtx *ctx) = 0;
  /// Attempt to initialize the value from a string. For example, a subclass of
  /// `Boolean` should check that `value` is one of: [`yes`, `no`, `true`,
  /// `false`] otherwise return an error status code
  virtual absl::Status FromString(std::string_view value) = 0;
  std::string_view GetName() const { return name_; }

  // bitwise OR'ed flags of `Flags`
  inline void SetFlags(size_t flags) { flags_ = flags; }
  inline bool IsHidden() const { return flags_ & REDISMODULE_CONFIG_HIDDEN; }

 protected:
  std::string name_;
  size_t flags_{kDefault};
};

template <typename T>
class ConfigBase : public Registerable {
 public:
  using OnModifyCB = std::function<void(T)>;
  using ValidateCB = std::function<absl::Status(const T)>;

  ~ConfigBase() override {
    ModuleConfigManager::Instance().UnregisterConfig(this);
  }

  void SetModifyCallback(OnModifyCB modify_callback) {
    modify_callback_ = std::move(modify_callback);
  }

  void SetValidateCallback(ValidateCB validate_callback) {
    validate_callback_ = std::move(validate_callback);
  }

  absl::Status SetValue(T value) {
    auto res = Validate(value);
    if (!res.ok()) {
      return res;
    }

    SetValueImpl(value);
    NotifyChanged();
    return absl::OkStatus();
  }

  /// Set the value to this configuration item, or log a warning message.
  void SetValueOrLog(T value, LogLevel log_level) {
    auto res = Validate(value);
    if (!res.ok()) {
      VMSDK_LOG(log_level, nullptr)
          << "Failed to update configuration entry: " << GetName() << ". "
          << res.message();
      return;
    }

    SetValueImpl(value);
    NotifyChanged();
  }

  T GetValue() const { return GetValueImpl(); }

  void NotifyChanged() {
    if (modify_callback_) {
      modify_callback_(GetValue());
    }
  }

  absl::Status Validate(T val) const {
    if (validate_callback_) {
      return validate_callback_(val);
    }
    return absl::OkStatus();
  }

 protected:
  ConfigBase(std::string_view name) : Registerable(name) {
    ModuleConfigManager::Instance().RegisterConfig(this);
  }

  /// subclasses should derive these 2 methods to provide the concrete
  /// store/fetch for the value
  virtual void SetValueImpl(T value) = 0;
  virtual T GetValueImpl() const = 0;

  OnModifyCB modify_callback_;
  ValidateCB validate_callback_;

  FRIEND_TEST(Builder, ConfigBuilder);
};

class Number : public ConfigBase<long long> {
 public:
  Number(std::string_view name, int64_t default_value, int64_t min_value,
         int64_t max_value);
  ~Number() override = default;
  absl::Status FromString(std::string_view value) override;

 protected:
  // Implementation specific
  absl::Status Register(RedisModuleCtx *ctx) override;
  long long GetValueImpl() const override {
    return current_value_.load(std::memory_order_relaxed);
  }

  void SetValueImpl(long long val) override {
    current_value_.store(val, std::memory_order_relaxed);
  }

  int64_t default_value_{0};
  int64_t min_value_{0};
  int64_t max_value_{0};
  std::atomic_int64_t current_value_{0};
  FRIEND_TEST(Builder, ConfigBuilder);
};

/// Enum configs are a set of string tokens to corresponding integer values
class Enum : public ConfigBase<int> {
 public:
  Enum(std::string_view name, int default_value,
       const std::vector<std::string_view> &names,
       const std::vector<int> &value);
  ~Enum() override = default;
  absl::Status FromString(std::string_view value) override;

 protected:
  // Implementation specific
  absl::Status Register(RedisModuleCtx *ctx) override;
  int GetValueImpl() const override {
    return current_value_.load(std::memory_order_relaxed);
  }

  void SetValueImpl(int val) override {
    current_value_.store(val, std::memory_order_relaxed);
  }

  int default_value_{0};
  std::vector<std::string> names_;
  std::vector<int> values_;
  std::atomic_int current_value_{0};
  FRIEND_TEST(Builder, ConfigBuilder);
};

class Boolean : public ConfigBase<bool> {
 public:
  Boolean(std::string_view name, bool default_value);
  ~Boolean() override = default;
  absl::Status FromString(std::string_view value) override;

 protected:
  // Implementation specific
  absl::Status Register(RedisModuleCtx *ctx) override;
  bool GetValueImpl() const override {
    return current_value_.load(std::memory_order_relaxed);
  }

  void SetValueImpl(bool val) override {
    current_value_.store(val, std::memory_order_relaxed);
  }

  bool default_value_{false};
  std::atomic_bool current_value_{0};

  FRIEND_TEST(Builder, ConfigBuilder);
};

template <typename ValkeyT>
class ConfigBuilder {
 public:
  ConfigBuilder() = delete;
  ConfigBuilder(const ConfigBuilder &) = delete;
  ConfigBuilder &operator=(const ConfigBuilder &) = delete;

  ConfigBuilder(ConfigBase<ValkeyT> *obj) : config_(obj) {
    CHECK(config_) << "Attempted to construct ConfigBuilder with nullptr";
  }

  auto &WithModifyCallback(ConfigBase<ValkeyT>::OnModifyCB modify_cb) {
    config_->SetModifyCallback(std::move(modify_cb));
    return *this;
  }

  auto &WithValidationCallback(ConfigBase<ValkeyT>::ValidateCB validate_cb) {
    config_->SetValidateCallback(std::move(validate_cb));
    return *this;
  }

  auto &WithFlags(size_t flags) {
    config_->SetFlags(flags);
    return *this;
  }

  std::shared_ptr<ConfigBase<ValkeyT>> Build() { return config_; }

 private:
  std::shared_ptr<ConfigBase<ValkeyT>> config_;
};

/// Construct Configuration object of type `T`.
/// Mapping between native types and Valkey types:
///
/// `bool` -> Boolean configuration.
/// `long long` -> Number configuration.
/// `int` -> Enum configuration.
template <typename ValkeyT, typename... Args>
  requires(std::is_same<ValkeyT, long long>() == true ||
           std::is_same<ValkeyT, int>() == true ||
           std::is_same<ValkeyT, bool>() == true)
ConfigBuilder<ValkeyT> Builder(Args &&...args) {
  if constexpr (std::is_same<ValkeyT, long long>()) {
    // Number
    return ConfigBuilder<long long>(new Number(std::forward<Args>(args)...));
  } else if constexpr (std::is_same<ValkeyT, bool>()) {
    // Boolean
    return ConfigBuilder<bool>(new Boolean(std::forward<Args>(args)...));
  } else if constexpr (std::is_same<ValkeyT, int>()) {
    // Boolean
    return ConfigBuilder<int>(new Enum(std::forward<Args>(args)...));
  } else {
    static_assert(false, "Unreachable");
  }
}

/// Wrapper for building Number
template <typename... Args>
ConfigBuilder<long long> NumberBuilder(Args &&...args) {
  return Builder<long long>(std::forward<Args>(args)...);
}

/// Wrapper for building Enum
template <typename... Args>
ConfigBuilder<int> EnumBuilder(Args &&...args) {
  return Builder<int>(std::forward<Args>(args)...);
}

/// Wrapper for building Boolean
template <typename... Args>
ConfigBuilder<bool> BooleanBuilder(Args &&...args) {
  return Builder<bool>(std::forward<Args>(args)...);
}

}  // namespace config
}  // namespace vmsdk