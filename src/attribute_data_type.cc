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

absl::StatusOr<vmsdk::UniqueValkeyString> HashAttributeDataType::GetAttribute(
    [[maybe_unused]] ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key,
    [[maybe_unused]] absl::string_view key,
    absl::string_view identifier) const {
  vmsdk::VerifyMainThread();
  ValkeyModuleString *attribute{nullptr};
  ValkeyModule_HashGet(open_key, VALKEYMODULE_HASH_CFIELDS, identifier.data(),
                       &attribute, nullptr);
  if (!attribute) {
    return absl::NotFoundError("No such attribute with identifier");
  }
  return vmsdk::UniqueValkeyString(attribute);
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

bool HashHasAttribute(ValkeyModuleKey *key, absl::string_view identifier) {
  int exists;
  ValkeyModule_HashGet(key,
                       VALKEYMODULE_HASH_CFIELDS | VALKEYMODULE_HASH_EXISTS,
                       identifier.data(), &exists, nullptr);
  return exists;
}

absl::StatusOr<RecordsMap> HashAttributeDataType::FetchAllAttributes(
    ValkeyModuleCtx *ctx, const std::optional<std::string> &vector_identifier,
    ValkeyModuleKey *open_key, absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  if (!open_key) {
    return absl::NotFoundError(absl::StrCat("Key not found: `", key, "`"));
  }
  if (vector_identifier.has_value() &&
      !HashHasAttribute(open_key, vector_identifier.value())) {
    return absl::NotFoundError(
        absl::StrCat("No such attribute with identifier: `",
                     vector_identifier.value_or(""), "`"));
  }
  if (!identifiers.empty()) {
    size_t hash_len = ValkeyModule_ValueLength(open_key);
    if (identifiers.size() <= hash_len / 2) {
      return FetchSpecificFields(open_key, identifiers);
    }
  }
  return FetchAllFields(open_key, identifiers);
}

RecordsMap HashAttributeDataType::FetchAllFields(
    ValkeyModuleKey *open_key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::UniqueValkeyScanCursor cursor = vmsdk::MakeUniqueValkeyScanCursor();
  HashScanCallbackData callback_data{identifiers};
  while (ValkeyModule_ScanKey(open_key, cursor.get(), HashScanCallback,
                              &callback_data)) {
  }
  return std::move(callback_data.key_value_content);
}

// Fetch only the requested fields by name using HashGet, avoiding a full scan.
// This is faster than scanning when the hash has many more fields than
// requested. Fields that don't exist in the hash are silently skipped.
RecordsMap HashAttributeDataType::FetchSpecificFields(
    ValkeyModuleKey *open_key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  RecordsMap content;
  for (const auto &id : identifiers) {
    ValkeyModuleString *value = nullptr;
    ValkeyModule_HashGet(open_key, VALKEYMODULE_HASH_CFIELDS, id.data(), &value,
                         nullptr);
    if (value) {
      auto field_str = vmsdk::MakeUniqueValkeyString(id);
      auto field_view = vmsdk::ToStringView(field_str.get());
      content.emplace(field_view,
                      RecordsMapValue(std::move(field_str),
                                      vmsdk::UniqueValkeyString(value)));
    }
  }
  return content;
}

absl::Status NormalizeJsonAttribute(absl::string_view attribute,
                                    vmsdk::UniqueValkeyString &out_attribute) {
  if (!attribute.empty() && attribute[0] != '[') {
    return absl::NotFoundError("Invalid attribute");
  }
  bool was_string = false;
  if (absl::ConsumePrefix(&attribute, "[")) {
    absl::ConsumeSuffix(&attribute, "]");
    if (absl::ConsumePrefix(&attribute, "\"")) {
      absl::ConsumeSuffix(&attribute, "\"");
      was_string = true;
    }
  }
  // The JSON module returns string values still JSON-escaped; decode them so
  // the indexed value matches the (already unescaped) query side. Done before
  // the empty check so a valid empty string ("") indexes as "" rather than
  // being treated as a missing attribute.
  if (was_string) {
    auto decoded = vmsdk::JsonUnquote(attribute);
    if (!decoded.has_value()) {
      return absl::InvalidArgumentError("Invalid JSON string value");
    }
    auto attribute_ptr = vmsdk::MakeUniqueValkeyString(*decoded);
    out_attribute.swap(attribute_ptr);
    return absl::OkStatus();
  }
  if (attribute.empty()) {
    return absl::NotFoundError("Empty attribute");
  }
  auto attribute_ptr = vmsdk::MakeUniqueValkeyString(attribute);
  out_attribute.swap(attribute_ptr);
  return absl::OkStatus();
}
// GetJsonAttribute is the actual implementation for retrieving a JSON value.
// If the JSON module is not loaded, it returns an error.
// It prefers using the JSON shared API, and falls back to VM_Call if the API is
// unavailable. On success, the result is stored in the `attribute` input
// parameter. The caller may only check for the existence of the identifier
// by passing nullptr as the `attribute` value.
absl::Status GetJsonAttribute(ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key,
                              absl::string_view key,
                              absl::string_view identifier,
                              vmsdk::UniqueValkeyString *attribute) {
  vmsdk::VerifyMainThread();
  if (!IsJsonModuleSupported(ctx)) {
    return absl::UnavailableError("The JSON module is not supported");
  }
  if (json_get) {
    if (!open_key) {
      return absl::NotFoundError(absl::StrCat("Key not found: `", key, "`"));
    }
    ValkeyModuleString *attribute_str = nullptr;
    if (json_get(open_key, identifier.data(), &attribute_str) ==
        VALKEYMODULE_ERR) {
      return absl::NotFoundError(absl::StrCat(
          "No such attribute with identifier: `", identifier, "`"));
    }
    auto attribute_tmp = vmsdk::UniqueValkeyString(attribute_str);
    if (!attribute) {
      return absl::OkStatus();
    }
    return NormalizeJsonAttribute(vmsdk::ToStringView(attribute_tmp.get()),
                                  *attribute);
  }
  auto reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), identifier.data()));
  if (reply == nullptr) {
    return absl::NotFoundError(
        absl::StrCat("No such attribute with identifier: `", identifier, "`"));
  }
  auto reply_type = ValkeyModule_CallReplyType(reply.get());
  if (reply_type != VALKEYMODULE_REPLY_STRING) {
    return absl::NotFoundError(
        absl::StrCat(kJsonCmd.data(), " returned a non string value"));
  }
  auto reply_str = vmsdk::UniqueValkeyString(
      ValkeyModule_CreateStringFromCallReply(reply.get()));
  if (!attribute) {
    return absl::OkStatus();
  }
  return NormalizeJsonAttribute(vmsdk::ToStringView(reply_str.get()),
                                *attribute);
}

absl::StatusOr<vmsdk::UniqueValkeyString> JsonAttributeDataType::GetAttribute(
    ValkeyModuleCtx *ctx, ValkeyModuleKey *open_key, absl::string_view key,
    absl::string_view identifier) const {
  vmsdk::UniqueValkeyString attribute;
  VMSDK_RETURN_IF_ERROR(
      GetJsonAttribute(ctx, open_key, key, identifier, &attribute));
  return attribute;
}

absl::StatusOr<RecordsMap> JsonAttributeDataType::FetchAllAttributes(
    ValkeyModuleCtx *ctx, const std::optional<std::string> &vector_identifier,
    ValkeyModuleKey *open_key, absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  // First, validate that a JSON object exists for the given key using the
  // vector identifier.
  VMSDK_RETURN_IF_ERROR(GetJsonAttribute(
      ctx, open_key, key, vector_identifier.value_or(""), nullptr));
  RecordsMap key_value_content;
  for (const auto &identifier : identifiers) {
    auto str = GetAttribute(ctx, open_key, key, identifier);
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
  json_get =
      (JsonSharedAPIGetValueFn)ValkeyModule_GetSharedAPI(ctx, "JSON_GetValue");
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
