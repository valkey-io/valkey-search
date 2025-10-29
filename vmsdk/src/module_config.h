/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */
#pragma once

#include <absl/container/flat_hash_map.h>

#include <type_traits>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "gtest/gtest_prod.h"
#include "managed_pointers.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace config {

/// Flags to further specify the behavior of the config
/// These can be specified using the Builder().WithFlags(...) method (see below)
enum Flags {
  kDefault = VALKEYMODULE_CONFIG_DEFAULT,
  kImmutable = VALKEYMODULE_CONFIG_IMMUTABLE,
  kSensitive = VALKEYMODULE_CONFIG_SENSITIVE,
  kHidden = VALKEYMODULE_CONFIG_HIDDEN,
  kProtected = VALKEYMODULE_CONFIG_PROTECTED,
  kDenyLoading = VALKEYMODULE_CONFIG_DENY_LOADING,
  kMemory = VALKEYMODULE_CONFIG_MEMORY,
  kBitFlags = VALKEYMODULE_CONFIG_BITFLAGS,
};

/// Return true if debug mode is enabled. "search.debug-mode == yes"
constexpr absl::string_view kDebugMode{"debug-mode"};
bool IsDebugModeEnabled();

//config updated persist
int64_t g_updatedmax_index;

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
///        .WithFlags(VALKEYMODULE_CONFIG_DEFAULT)
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
  absl::Status Init(ValkeyModuleCtx *ctx);

  /// Parse and load command line arguments
  absl::Status ParseAndLoadArgv(ValkeyModuleCtx *ctx, ValkeyModuleString **argv,
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
  absl::Status UpdateConfigFromKeyVal(ValkeyModuleCtx *ctx,
                                      std::string_view key,
                                      std::string_view value);
  absl::flat_hash_map<std::string, Registerable *> entries_;
  friend class Configbase;
};

/// A self registering configuration class
class Registerable {
 public:
  Registerable(std::string_view name) : name_(name) {}
  virtual ~Registerable() = default;

  virtual absl::Status Register(ValkeyModuleCtx *ctx) = 0;
  /// Attempt to initialize the value from a string. For example, a subclass of
  /// `Boolean` should check that `value` is one of: [`yes`, `no`, `true`,
  /// `false`] otherwise return an error status code
  virtual absl::Status FromString(std::string_view value) = 0;
  std::string_view GetName() const { return name_; }

  // bitwise OR'ed flags of `Flags`
  inline void SetFlags(size_t flags) { flags_ = flags; }
  inline bool IsHidden() const { return flags_ & VALKEYMODULE_CONFIG_HIDDEN; }
  inline void EnableFlag(size_t flag) { flags_ |= flag; }

  inline void SetDeveloperConfig(bool b) { this->developer_config_ = b; }
  inline bool IsDeveloperConfig() const { return developer_config_; }

 protected:
  std::string name_;
  size_t flags_{kDefault};
  bool developer_config_{false};
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
    if (IsDeveloperConfig() && !IsDebugModeEnabled()) {
      return absl::PermissionDeniedError(
          absl::StrFormat("Modification of '%s' requires '%s' to be enabled.",
                          GetName(), kDebugMode));
    }
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
  absl::Status Register(ValkeyModuleCtx *ctx) override;
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
  absl::Status Register(ValkeyModuleCtx *ctx) override;
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
  absl::Status Register(ValkeyModuleCtx *ctx) override;
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

class String : public ConfigBase<std::string> {
 public:
  String(std::string_view name, std::string_view default_value);
  ~String() override = default;
  absl::Status FromString(std::string_view value) override;
  const std::string &GetString() const { return value_; }
  ValkeyModuleString *GetCachedValkeyString() const {
    if (!cached_string_) {
      cached_string_ = vmsdk::MakeUniqueValkeyString(value_);
    }
    return cached_string_.get();
  }

 protected:
  // Implementation specific
  absl::Status Register(ValkeyModuleCtx *ctx) override;

  std::string GetValueImpl() const override ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock{&mutex_};
    return value_;
  }

  void SetValueImpl(std::string val) override ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock{&mutex_};
    value_ = val;
    cached_string_.reset();
  }

  mutable absl::Mutex mutex_;
  std::string value_;
  mutable UniqueValkeyString cached_string_;
  FRIEND_TEST(Builder, ConfigBuilder);
};

//callbacks for index updates
using GetMaxIndexesCallback = Number &(*)();
void SetGetMaxIndexesCallback(GetMaxIndexesCallback cb, uint32_t *);

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

  auto &Immutable() {
    config_->EnableFlag(VALKEYMODULE_CONFIG_IMMUTABLE);
    return *this;
  }

  auto &Hidden() {
    config_->EnableFlag(VALKEYMODULE_CONFIG_HIDDEN);
    return *this;
  }

  auto &Sensitive() {
    config_->EnableFlag(VALKEYMODULE_CONFIG_SENSITIVE);
    return *this;
  }

  auto &Protected() {
    config_->EnableFlag(VALKEYMODULE_CONFIG_PROTECTED);
    return *this;
  }

  /// This configuration setting is restricted to developer use only. It can be
  /// modified exclusively when `search.debug-mode` is set to `yes` (the default
  /// setting is `no`). When a configuration entry is marked as `Dev()`, it
  /// becomes both `Hidden` and `Immutable` if `search.debug-mode` is set to
  /// `no`, preventing any runtime modifications.
  auto &Dev() {
    config_->SetDeveloperConfig(true);
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
           std::is_same<ValkeyT, std::string>() == true ||
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
  } else if constexpr (std::is_same<ValkeyT, std::string>()) {
    // String
    return ConfigBuilder<std::string>(new String(std::forward<Args>(args)...));
  } else {
    static_assert(!std::is_same_v<ValkeyT, ValkeyT>, "Unreachable");
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

/// Wrapper for building String
template <typename... Args>
ConfigBuilder<std::string> StringBuilder(Args &&...args) {
  return Builder<std::string>(std::forward<Args>(args)...);
}

#define CHECK_RANGE(MIN, MAX, CONFIG_NAME)                         \
  [](const int value) {                                            \
    if (value < MIN || value > MAX) {                              \
      return absl::OutOfRangeError(absl::StrFormat(                \
          "%s must be between %u and %u", CONFIG_NAME, MIN, MAX)); \
    }                                                              \
    return absl::OkStatus();                                       \
  }

}  // namespace config
}  // namespace vmsdk