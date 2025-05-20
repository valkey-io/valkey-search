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
#include "vmsdk/src/module_config.h"

#include <absl/log/check.h>
#include <absl/strings/str_cat.h>

#include "vmsdk/src/status/status_macros.h"

namespace vmsdk {
namespace config {

namespace {
template <typename T>
static T OnGetConfig(const char *config_name, void *priv_data) {
  auto entry = static_cast<ConfigBase<T> *>(priv_data);
  CHECK(entry) << "null private data for Boolean configuration entry.";
  return static_cast<int>(entry->GetValue());
}

template <typename T>
static int OnSetConfig(const char *config_name, T value, void *priv_data,
                       RedisModuleString **err) {
  auto entry = static_cast<ConfigBase<T> *>(priv_data);
  CHECK(entry) << "null private data for configuration Number entry.";
  if (!entry->Validate(value)) {
    return REDISMODULE_ERR;
  }
  entry->SetValue(value);
  entry->NotifyChanged();
  return REDISMODULE_OK;
}
}  // namespace

ModuleConfigManager &ModuleConfigManager::Instance() {
  static ModuleConfigManager manager;
  return manager;
}

void ModuleConfigManager::RegisterConfig(Registerable *config_item) {
  entries_.push_back(config_item);
}

absl::Status ModuleConfigManager::RegisterAll(RedisModuleCtx *ctx) {
  for (auto entry : entries_) {
    VMSDK_RETURN_IF_ERROR(entry->Register(ctx));
  }
  // once registered, clear the list
  entries_.clear();
  return absl::OkStatus();
}

Number::Number(std::string_view name, int64_t default_value, int64_t min_value,
               int64_t max_value)
    : ConfigBase(name),
      default_value_(default_value),
      min_value_(min_value),
      max_value_(max_value),
      current_value_(default_value) {}

absl::Status Number::Register(RedisModuleCtx *ctx) {
  if (RedisModule_RegisterNumericConfig(ctx,
                                        name_.data(),    // Name
                                        default_value_,  // Default value
                                        flags_,          // Flags
                                        min_value_,      // Minimum value
                                        max_value_,      // Maximum value
                                        OnGetConfig<long long>,  // Get callback
                                        OnSetConfig<long long>,  // Set callback
                                        nullptr,  // Apply callback (optional)
                                        this      // privdata
                                        ) != REDISMODULE_OK) {
    return absl::InternalError(absl::StrCat(
        "Failed to register numeric configuration entry: ", name_));
  }
  return absl::OkStatus();
}

Boolean::Boolean(std::string_view name, bool default_value)
    : ConfigBase(name),
      default_value_(default_value),
      current_value_(default_value) {}

absl::Status Boolean::Register(RedisModuleCtx *ctx) {
  if (RedisModule_RegisterBoolConfig(ctx,
                                     name_.data(),      // Name
                                     default_value_,    // Default value
                                     flags_,            // Flags
                                     OnGetConfig<int>,  // Get callback
                                     OnSetConfig<int>,  // Set callback
                                     nullptr,  // Apply callback (optional)
                                     this      // privdata
                                     ) != REDISMODULE_OK) {
    return absl::InternalError(absl::StrCat(
        "Failed to register boolean configuration entry: ", name_));
  }
  return absl::OkStatus();
}

}  // namespace config
}  // namespace vmsdk
