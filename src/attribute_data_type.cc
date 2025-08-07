/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/attribute_data_type.h"

#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "src/valkey_search.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
static JsonSharedAPIGetValueFn json_get;
static std::optional<bool> is_json_loaded;
void ResetJsonLoadedCache() { is_json_loaded = std::nullopt; }

absl::StatusOr<vmsdk::UniqueValkeyString> HashAttributeDataType::GetRecord(
    [[maybe_unused]] ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key,
    [[maybe_unused]] absl::string_view key,
    absl::string_view identifier) const {
  vmsdk::VerifyMainThread();
  ValkeyModuleString *record{nullptr};
  ValkeyModule_HashGet(open_key, VALKEYMODULE_HASH_CFIELDS, identifier.data(),
                       &record, nullptr);
  if (!record) {
    return absl::NotFoundError("No such record with identifier");
  }
  return vmsdk::UniqueValkeyString(record);
}

struct HashScanCallbackData {
  const absl::flat_hash_set<absl::string_view> &identifiers;
  RecordsMap key_value_content;
};

void HashScanCallback(ValkeyModuleKey *key, ValkeyModuleString *field,
                      ValkeyModuleString *value, void *privdata) {
  vmsdk::VerifyMainThread();
  if (!field || !value) {
    return;
  }
  HashScanCallbackData *callback_data = (HashScanCallbackData *)(privdata);
  auto field_str = vmsdk::ToStringView(field);
  if (field_str.empty()) {
    return;
  }
  // This needs to be empty for non vector queries.
  if (callback_data->identifiers.empty() ||
      callback_data->identifiers.contains(field_str)) {
    callback_data->key_value_content.emplace(
        field_str, RecordsMapValue(vmsdk::RetainUniqueValkeyString(field),
                                   vmsdk::RetainUniqueValkeyString(value)));
  }
}

bool HashHasRecord(ValkeyModuleKey *key, absl::string_view identifier) {
  int exists;
  ValkeyModule_HashGet(key,
                       VALKEYMODULE_HASH_CFIELDS | VALKEYMODULE_HASH_EXISTS,
                       identifier.data(), &exists, nullptr);
  return exists;
}

absl::StatusOr<RecordsMap> HashAttributeDataType::FetchAllRecords(
    ValkeyModuleCtx *ctx, const std::string &vector_identifier,
    [[maybe_unused]] ValkeyModuleKey *open_key, absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  auto key_obj =
      vmsdk::MakeUniqueValkeyOpenKey(ctx, key_str.get(), VALKEYMODULE_READ);
  if (!key_obj) {
    return absl::NotFoundError(
        absl::StrCat("No such record with key: `", vector_identifier, "`"));
  }
  // Only check for vector_identifier if it's not empty (vector queries)
  if (!vector_identifier.empty() &&
      !HashHasRecord(key_obj.get(), vector_identifier)) {
    return absl::NotFoundError(absl::StrCat("No such record with identifier: `",
                                            vector_identifier, "`"));
  }
  vmsdk::UniqueValkeyScanCursor cursor = vmsdk::MakeUniqueValkeyScanCursor();
  HashScanCallbackData callback_data{identifiers};
  while (ValkeyModule_ScanKey(key_obj.get(), cursor.get(), HashScanCallback,
                              &callback_data)) {
  }
  return std::move(callback_data.key_value_content);
}

absl::string_view TrimBrackets(absl::string_view record) {
  if (absl::ConsumePrefix(&record, "[")) {
    absl::ConsumeSuffix(&record, "]");
  }
  return record;
}

absl::Status NormalizeJsonRecord(absl::string_view record,
                                 vmsdk::UniqueValkeyString &out_record) {
  if (!record.empty() && record[0] != '[') {
    return absl::NotFoundError("Invalid record");
  }
  if (absl::ConsumePrefix(&record, "[")) {
    absl::ConsumeSuffix(&record, "]");
    if (absl::ConsumePrefix(&record, "\"")) {
      absl::ConsumeSuffix(&record, "\"");
    }
  }
  if (record.empty()) {
    return absl::NotFoundError("Empty record");
  }
  auto record_ptr = vmsdk::MakeUniqueValkeyString(record);
  out_record.swap(record_ptr);
  return absl::OkStatus();
}
// GetJsonRecord is the actual implementation for retrieving a JSON value.
// If the JSON module is not loaded, it returns an error.
// It prefers using the JSON shared API, and falls back to VM_Call if the API is
// unavailable. On success, the result is stored in the `record` input
// parameter. The caller may only check for the existence of the identifier
// by passing nullptr as the `record` value.
absl::Status GetJsonRecord(ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key,
                           absl::string_view key, absl::string_view identifier,
                           vmsdk::UniqueValkeyString *record) {
  vmsdk::VerifyMainThread();
  if (!IsJsonModuleSupported(ctx)) {
    return absl::UnavailableError("The JSON module is not supported");
  }
  if (json_get) {
    if (!open_key) {
      return absl::NotFoundError(absl::StrCat("Key not found: `", key, "`"));
    }
    ValkeyModuleString *record_str = nullptr;
    if (json_get(open_key, identifier.data(), &record_str) ==
        VALKEYMODULE_ERR) {
      return absl::NotFoundError(
          absl::StrCat("No such record with identifier: `", identifier, "`"));
    }
    if (!record) {
      return absl::OkStatus();
    }
    auto record_tmp = vmsdk::UniqueValkeyString(record_str);
    return NormalizeJsonRecord(vmsdk::ToStringView(record_tmp.get()), *record);
  }
  auto reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), identifier.data()));
  if (reply == nullptr) {
    return absl::NotFoundError(
        absl::StrCat("No such record with identifier: `", identifier, "`"));
  }
  auto reply_type = ValkeyModule_CallReplyType(reply.get());
  if (reply_type != VALKEYMODULE_REPLY_STRING) {
    return absl::NotFoundError(
        absl::StrCat(kJsonCmd.data(), " returned a non string value"));
  }
  if (!record) {
    return absl::OkStatus();
  }
  auto reply_str = vmsdk::UniqueValkeyString(
      ValkeyModule_CreateStringFromCallReply(reply.get()));
  return NormalizeJsonRecord(vmsdk::ToStringView(reply_str.get()), *record);
}

absl::StatusOr<vmsdk::UniqueValkeyString> JsonAttributeDataType::GetRecord(
    ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key, absl::string_view key,
    absl::string_view identifier) const {
  vmsdk::UniqueValkeyString record;
  VMSDK_RETURN_IF_ERROR(GetJsonRecord(ctx, open_key, key, identifier, &record));
  return record;
}

absl::StatusOr<RecordsMap> JsonAttributeDataType::FetchAllRecords(
    ValkeyModuleCtx *ctx, const std::string &vector_identifier,
    ValkeyModuleKey *open_key, absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  // First, validate that a JSON object exists for the given key using the
  // vector identifier.
  VMSDK_RETURN_IF_ERROR(
      GetJsonRecord(ctx, open_key, key, vector_identifier, nullptr));
  RecordsMap key_value_content;
  for (const auto &identifier : identifiers) {
    auto str = GetRecord(ctx, open_key, key, identifier);
    if (!str.ok()) {
      continue;
    }
    key_value_content.emplace(
        identifier, RecordsMapValue(vmsdk::MakeUniqueValkeyString(identifier),
                                    std::move(str.value())));
  }
  return key_value_content;
}

bool IsJsonModuleSupported(ValkeyModuleCtx *ctx) {
  // Use positive caching only. Note: the JSON module may be loaded after
  // initialization.
  if (is_json_loaded.has_value() && is_json_loaded.value()) {
    return is_json_loaded.value();
  }
  is_json_loaded = vmsdk::IsModuleLoaded(ctx, "json");
  if (!is_json_loaded.value()) {
    return false;
  }
  json_get = (int (*)(ValkeyModuleKey *, const char *, ValkeyModuleString **))
      ValkeyModule_GetSharedAPI(ctx, "JSON_GetValue");
  // Note: In cluster mode, replicas must have the JSON module loaded to access
  // the JSON shared API. Otherwise, invoking commands via ValkeyModule_Call
  // from a replica will result in a MOVED response.
  if (!json_get && ValkeySearch::Instance().IsCluster()) {
    VMSDK_LOG(WARNING, ctx)
        << "Note: When cluster mode is enabled, valkey-search requires "
           "valkey-json version 1.02 or higher for proper JSON support.";
  }
  return true;
}
}  // namespace valkey_search
