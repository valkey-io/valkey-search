
#include "valkey_search_config.h"

#include "absl/log/check.h"
#include "absl/strings/str_cat.h"
#include "vmsdk/src/log.h"

namespace valkey_search {

absl::Status ValkeySearchConfig::RegisterNumericConfig(
    RedisModuleCtx *ctx, std::string_view config_name, long long default_value,
    long long min_value, long long max_value,
    NumericConfigEntry::OnSetFunc &&set_func,
    NumericConfigEntry::OnGetFunc &&get_func,
    NumericConfigEntry::ValidateFunc &&value_validation_func) {
  auto entry = std::make_unique<NumericConfigEntry>(
      default_value, std::move(set_func), std::move(get_func),
      std::move(value_validation_func));
  numeric_entries_.push_back(std::move(entry));
  auto &d = numeric_entries_.back();
  if (RedisModule_RegisterNumericConfig(
          ctx,
          config_name.data(),          // Name
          default_value,               // Default value
          REDISMODULE_CONFIG_DEFAULT,  // Flags (mutable, can be changed via
                                       // CONFIG SET)
          min_value,                   // Minimum value
          max_value,                   // Maximum value
          ValkeySearchConfig::OnGetNumericConfig,  // Get callback
          ValkeySearchConfig::OnSetNumericConfig,  // Set callback
          nullptr,                                 // Apply callback (optional)
          d.get()                                  // privdata
          ) != REDISMODULE_OK) {
    numeric_entries_.pop_back();  // remove the entry
    return absl::InternalError(
        absl::StrCat("Failed to register configuration entry: ", config_name));
  }
  return absl::OkStatus();
}

long long ValkeySearchConfig::OnGetNumericConfig(const char *config_name,
                                                 void *priv_data) {
  auto entry = reinterpret_cast<NumericConfigEntry *>(priv_data);
  CHECK(entry) << "null private data for configuration entry: " << config_name;
  return entry->GetValue();
}

int ValkeySearchConfig::OnSetNumericConfig(const char *config_name,
                                           long long value, void *priv_data,
                                           RedisModuleString **err) {
  auto entry = reinterpret_cast<NumericConfigEntry *>(priv_data);
  CHECK(entry) << "null private data for configuration entry: " << config_name;
  if (!entry->Validate(value, err)) {
    return REDISMODULE_ERR;
  }
  entry->SetValue(value);
  VMSDK_LOG(NOTICE, nullptr)
      << "configuration item: " << config_name << " is set to " << value;
  return REDISMODULE_OK;
}
}  // namespace valkey_search
