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
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
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
    absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  auto key_str = vmsdk::MakeUniqueValkeyString(key);
  auto key_obj =
      vmsdk::MakeUniqueValkeyOpenKey(ctx, key_str.get(), VALKEYMODULE_READ);
  if (!key_obj) {
    return absl::NotFoundError(
        absl::StrCat("No such record with key: `", vector_identifier, "`"));
  }
  if (!HashHasRecord(key_obj.get(), vector_identifier)) {
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

absl::StatusOr<vmsdk::UniqueValkeyString> NormalizeJsonRecord(
    absl::string_view record) {
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
  return vmsdk::MakeUniqueValkeyString(record);
}

absl::StatusOr<vmsdk::UniqueValkeyString> JsonAttributeDataType::GetRecord(
    ValkeyModuleCtx *ctx, [[maybe_unused]] ValkeyModuleKey *open_key,
    absl::string_view key, absl::string_view identifier) const {
  vmsdk::VerifyMainThread();

  auto reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), identifier.data()));
  if (reply == nullptr) {
    return absl::NotFoundError(
        absl::StrCat("No such record with identifier: `", identifier, "`"));
  }
  auto reply_type = ValkeyModule_CallReplyType(reply.get());
  if (reply_type == VALKEYMODULE_REPLY_STRING) {
    auto reply_str = vmsdk::UniqueValkeyString(
        ValkeyModule_CreateStringFromCallReply(reply.get()));
    return NormalizeJsonRecord(vmsdk::ToStringView(reply_str.get()));
  }
  return absl::NotFoundError("Json.get returned a non string value");
}

absl::StatusOr<RecordsMap> JsonAttributeDataType::FetchAllRecords(
    ValkeyModuleCtx *ctx, const std::string &vector_identifier,
    absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  // Validating that a JSON object with the key exists with the vector
  // identifier
  auto reply = vmsdk::UniquePtrValkeyCallReply(ValkeyModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), vector_identifier.c_str()));
  if (reply == nullptr ||
      ValkeyModule_CallReplyType(reply.get()) != VALKEYMODULE_REPLY_STRING) {
    return absl::NotFoundError(absl::StrCat("No such record with identifier: `",
                                            vector_identifier, "`"));
  }
  RecordsMap key_value_content;
  for (const auto &identifier : identifiers) {
    auto str = GetRecord(ctx, nullptr, key, identifier);
    if (!str.ok()) {
      continue;
    }
    key_value_content.emplace(
        identifier, RecordsMapValue(vmsdk::MakeUniqueValkeyString(identifier),
                                    std::move(str.value())));
  }
  return key_value_content;
}

bool IsJsonModuleLoaded(ValkeyModuleCtx *ctx) {
  return vmsdk::IsModuleLoaded(ctx, "json");
}
}  // namespace valkey_search
