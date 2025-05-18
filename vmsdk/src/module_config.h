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
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace vmsdk {
namespace config {
/// Configuration entry.
///
/// Example usage:
///
/// At the top of the file that you need this configuration entry, add a line
/// similar to this:
///
/// static Number my_number_config{"config-name", 1024, 0, 2048};
///
/// By adding the above line, you can use the following from "valkey-cli"
///
/// CONFIG SET search.config-name <value>
/// CONFIG GET search.config-name
///
/// If your code requires to be notified about configuration updates, you can
/// pass a callback as the last argument of the constructor.
class EntryBase {
 public:
  using OnModifyCB = std::function<void(EntryBase *)>;
  EntryBase(std::string_view name, OnModifyCB modify_callback = nullptr);

  virtual absl::Status Register(RedisModuleCtx *ctx) = 0;

 protected:
  void NotifyChanged() {
    if (modify_callback_ != nullptr) {
      modify_callback_(this);
    }
  }

  std::string name_;
  OnModifyCB modify_callback_ = nullptr;
};

class Number : public EntryBase {
 public:
  Number(std::string_view name, int64_t default_value, int64_t min_value,
         int64_t max_value, OnModifyCB modify_callback = nullptr);

  int64_t GetValue() const {
    return current_value_.load(std::memory_order_relaxed);
  }
  void SetValue(int64_t val) {
    current_value_.store(val, std::memory_order_relaxed);
  }

 protected:
  absl::Status Register(RedisModuleCtx *ctx) override;
  static long long OnGetNumericConfig(const char *config_name, void *priv_data);
  static int OnSetNumericConfig(const char *config_name, long long value,
                                void *priv_data, RedisModuleString **err);
  int64_t default_value_ = 0;
  int64_t min_value_ = 0;
  int64_t max_value_ = 0;
  std::atomic_int64_t current_value_ = 0;
};

class Boolean : public EntryBase {
 public:
  Boolean(std::string_view name, bool default_value,
          OnModifyCB modify_callback = nullptr);

  bool GetValue() const {
    return current_value_.load(std::memory_order_relaxed);
  }
  void SetValue(bool val) {
    current_value_.store(val, std::memory_order_relaxed);
  }

 protected:
  absl::Status Register(RedisModuleCtx *ctx) override;
  static int OnGetBoolConfig(const char *config_name, void *priv_data);
  static int OnSetBoolConfig(const char *config_name, int value,
                             void *priv_data, RedisModuleString **err);

  bool default_value_ = false;
  std::atomic_bool current_value_ = 0;
};

class ModuleConfigManager {
 public:
  static ModuleConfigManager &Instance();

  /// Do the actual registration with Valkey
  absl::Status Init(RedisModuleCtx *ctx);

 private:
  friend class EntryBase;
  void Register(EntryBase *config_item);
  static long long OnGetNumericConfig(const char *config_name, void *priv_data);
  static int OnSetNumericConfig(const char *config_name, long long value,
                                void *priv_data, RedisModuleString **err);

  std::vector<EntryBase *> entries_;
};

}  // namespace config
}  // namespace vmsdk