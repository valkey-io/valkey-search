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

#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest_prod.h"
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

/// Configuration entry.
///
/// Example usage:
///
/// At the top of the file that you need this configuration entry, add a code
/// block similar to this:
///
///
/// ```
/// using namespace vmsdk;
/// static auto reader_threads_count =
///    config::Builder<config::Number, long long>(
///        "readers-count",       // name
///        8,                     // default size
///        1,                     // min size
///        kMaxThreadsCount)      // max size
///        .WithModifyCallback(   // set an "On-Modify" callback
///            [](long long new_value) {
///              // ..do something ...
///            })
///        .Build();
/// ```
/// By adding the above line, you can use the following from "valkey-cli"
///
/// ```
/// CONFIG SET search.config-name <value>
/// CONFIG GET search.config-name
/// ```
///
/// If your code requires to be notified about configuration updates, you can
/// pass a callback as the last argument of the constructor.

/// A self registering configuration class
class Registerable {
 public:
  virtual absl::Status Register(RedisModuleCtx *ctx) = 0;
};

class ModuleConfigManager {
 public:
  static ModuleConfigManager &Instance();

  /// Do the actual registration with Valkey for all configuration items that
  /// previously registered themselves with this manager.
  absl::Status RegisterAll(RedisModuleCtx *ctx);

  /// Call this method to register a configuration item with this manager. This
  /// method is mainly used by the constructor of `ConfigBase` so users should
  /// not call it directly.
  void RegisterConfig(Registerable *config_item);

 private:
  std::vector<Registerable *> entries_;
  friend class Configbase;
};

template <typename T>
class ConfigBase : public Registerable {
 public:
  using OnModifyCB = std::function<void(T)>;
  using ValidateCB = std::function<bool(const T)>;

  ConfigBase(std::string_view name) : name_(name) {
    ModuleConfigManager::Instance().RegisterConfig(this);
  }

  void SetModifyCallback(OnModifyCB modify_callback) {
    modify_callback_ = std::move(modify_callback);
  }

  void SetValidateCallback(ValidateCB validate_callback) {
    validate_callback_ = std::move(validate_callback);
  }

  // bitwise OR'ed flags of `Flags`
  void SetFlags(size_t flags) { flags_ = flags; }

  void SetValue(T value) {
    if (!Validate(value)) {
      return;
    }
    SetValueImpl(value);
    NotifyChanged();
  }

  T GetValue() const { return GetValueImpl(); }

  void NotifyChanged() {
    if (modify_callback_ != nullptr) {
      modify_callback_(GetValue());
    }
  }

  bool Validate(T val) const {
    if (validate_callback_ == nullptr) {
      return true;
    }
    return validate_callback_(val);
  }

 protected:
  /// subclasses should derive these 2 methods to provide the concrete
  /// store/fetch for the value
  virtual void SetValueImpl(T value) = 0;
  virtual T GetValueImpl() const = 0;

  std::string name_;
  size_t flags_ = kDefault;
  OnModifyCB modify_callback_ = nullptr;
  ValidateCB validate_callback_ = nullptr;

  FRIEND_TEST(Builder, ConfigBuilder);
};

class Number : public ConfigBase<long long> {
 public:
  Number(std::string_view name, int64_t default_value, int64_t min_value,
         int64_t max_value);

 protected:
  // Implementation specific
  absl::Status Register(RedisModuleCtx *ctx) override;
  long long GetValueImpl() const override {
    return current_value_.load(std::memory_order_relaxed);
  }

  void SetValueImpl(long long val) override {
    current_value_.store(val, std::memory_order_relaxed);
  }

  int64_t default_value_ = 0;
  int64_t min_value_ = 0;
  int64_t max_value_ = 0;
  std::atomic_int64_t current_value_ = 0;
  FRIEND_TEST(Builder, ConfigBuilder);
};

class Boolean : public ConfigBase<bool> {
 public:
  Boolean(std::string_view name, bool default_value);

 protected:
  // Implementation specific
  absl::Status Register(RedisModuleCtx *ctx) override;
  bool GetValueImpl() const override {
    return current_value_.load(std::memory_order_relaxed);
  }

  void SetValueImpl(bool val) override {
    current_value_.store(val, std::memory_order_relaxed);
  }

  bool default_value_ = false;
  std::atomic_bool current_value_ = 0;

  FRIEND_TEST(Builder, ConfigBuilder);
};

template <typename T, typename ValkeyT>
class ConfigBuilder {
 public:
  ConfigBuilder(T *obj) : config_(obj) {}

  ConfigBuilder<T, ValkeyT> &WithModifyCallback(
      ConfigBase<ValkeyT>::OnModifyCB modify_cb) {
    config_->SetModifyCallback(std::move(modify_cb));
    return *this;
  }

  ConfigBuilder<T, ValkeyT> &WithValidationCallback(
      ConfigBase<ValkeyT>::ValidateCB validate_cb) {
    config_->SetValidateCallback(std::move(validate_cb));
    return *this;
  }

  ConfigBuilder<T, ValkeyT> &WithFlags(size_t flags) {
    config_->SetFlags(flags);
    return *this;
  }

  std::unique_ptr<T> Build() { return std::unique_ptr<T>(std::move(config_)); }

 private:
  T *config_ = nullptr;
};

/// Construct Configuration object of type `T`
template <typename T, typename ValkeyType, typename... Args>
ConfigBuilder<T, ValkeyType> Builder(Args &&...args) {
  return ConfigBuilder<T, ValkeyType>(new T(std::forward<Args>(args)...));
}

}  // namespace config
}  // namespace vmsdk