#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

template <typename T>
void DefaultSetFunc([[maybe_unused]] T val) {}

template <typename T, T DefaultValue>
T DefaultGetFunc() {
  return DefaultValue;
}

template <typename T, T DefaultValue>
class ConfigEntry {
 public:
  using OnSetFunc = std::function<void(T)>;
  using OnGetFunc = std::function<T()>;
  using ValidateFunc = std::function<bool(T, RedisModuleString **)>;
  ConfigEntry(T value, OnSetFunc set_func, OnGetFunc get_func,
              ValidateFunc func)
      : set_func_(std::move(set_func)),
        get_func_(std::move(get_func)),
        validation_func_(std::move(func)) {}

  bool Validate(long long new_value, RedisModuleString **err) const {
    if (validation_func_ == nullptr) {
      return true;
    }
    return validation_func_(new_value, err);
  }

  void SetValue(T val) const { set_func_(val); }
  T GetValue() const { return get_func_(); }

 private:
  OnSetFunc set_func_ = DefaultSetFunc<T>;
  OnGetFunc get_func_ = DefaultGetFunc<T, DefaultValue>;
  ValidateFunc validation_func_ = nullptr;
};

using NumericConfigEntry = ConfigEntry<long long, -1>;

class ValkeySearchConfig {
 public:
  /// Register numeric configuration entry within Valkey. If
  /// `value_validation_func` is not `nullptr`, it is used to validate the value
  /// before set when user calls `config set`.
  absl::Status RegisterNumericConfig(
      RedisModuleCtx *ctx, std::string_view config_name,
      long long default_value, long long min_value, long long max_value,
      NumericConfigEntry::OnSetFunc &&set_func,
      NumericConfigEntry::OnGetFunc &&get_func,
      NumericConfigEntry::ValidateFunc &&validation_func = nullptr);

 private:
  static long long OnGetNumericConfig(const char *config_name, void *priv_data);
  static int OnSetNumericConfig(const char *config_name, long long value,
                                void *priv_data, RedisModuleString **err);
  std::vector<std::unique_ptr<NumericConfigEntry>> numeric_entries_;
};
}  // namespace valkey_search
