/*
 * Copyright (c) 2025, ValkeySearch contributors
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
absl::StatusOr<vmsdk::UniqueRedisString> HashAttributeDataType::GetRecord(
    [[maybe_unused]] RedisModuleCtx *ctx, RedisModuleKey *open_key,
    [[maybe_unused]] absl::string_view key,
    absl::string_view identifier) const {
  vmsdk::VerifyMainThread();
  RedisModuleString *record{nullptr};
  RedisModule_HashGet(open_key, REDISMODULE_HASH_CFIELDS, identifier.data(),
                      &record, nullptr);
  if (!record) {
    return absl::NotFoundError("No such record with identifier");
  }
  return vmsdk::UniqueRedisString(record);
}

struct HashScanCallbackData {
  const absl::flat_hash_set<absl::string_view> &identifiers;
  RecordsMap key_value_content;
};

void HashScanCallback(RedisModuleKey *key, RedisModuleString *field,
                      RedisModuleString *value, void *privdata) {
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
        field_str, RecordsMapValue(vmsdk::RetainUniqueRedisString(field),
                                   vmsdk::RetainUniqueRedisString(value)));
  }
}

bool HashHasRecord(RedisModuleKey *key, absl::string_view identifier) {
  int exists;
  RedisModule_HashGet(key, REDISMODULE_HASH_CFIELDS | REDISMODULE_HASH_EXISTS,
                      identifier.data(), &exists, nullptr);
  return exists;
}

absl::StatusOr<RecordsMap> HashAttributeDataType::FetchAllRecords(
    RedisModuleCtx *ctx, const std::string &vector_identifier,
    absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  auto key_str = vmsdk::MakeUniqueRedisString(key);
  auto key_obj =
      vmsdk::MakeUniqueRedisOpenKey(ctx, key_str.get(), REDISMODULE_READ);
  if (!key_obj) {
    return absl::NotFoundError(
        absl::StrCat("No such record with key: `", vector_identifier, "`"));
  }
  if (!HashHasRecord(key_obj.get(), vector_identifier)) {
    return absl::NotFoundError(absl::StrCat("No such record with identifier: `",
                                            vector_identifier, "`"));
  }
  vmsdk::UniqueRedisScanCursor cursor = vmsdk::MakeUniqueRedisScanCursor();
  HashScanCallbackData callback_data{identifiers};
  while (RedisModule_ScanKey(key_obj.get(), cursor.get(), HashScanCallback,
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

absl::StatusOr<vmsdk::UniqueRedisString> NormalizeJsonRecord(
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
  return vmsdk::MakeUniqueRedisString(record);
}

absl::StatusOr<vmsdk::UniqueRedisString> JsonAttributeDataType::GetRecord(
    RedisModuleCtx *ctx, [[maybe_unused]] RedisModuleKey *open_key,
    absl::string_view key, absl::string_view identifier) const {
  vmsdk::VerifyMainThread();

  auto reply = vmsdk::UniquePtrRedisCallReply(RedisModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), identifier.data()));
  if (reply == nullptr) {
    return absl::NotFoundError(
        absl::StrCat("No such record with identifier: `", identifier, "`"));
  }
  auto reply_type = RedisModule_CallReplyType(reply.get());
  if (reply_type == REDISMODULE_REPLY_STRING) {
    auto reply_str = vmsdk::UniqueRedisString(
        RedisModule_CreateStringFromCallReply(reply.get()));
    return NormalizeJsonRecord(vmsdk::ToStringView(reply_str.get()));
  }
  return absl::NotFoundError("Json.get returned a non string value");
}

absl::StatusOr<RecordsMap> JsonAttributeDataType::FetchAllRecords(
    RedisModuleCtx *ctx, const std::string &vector_identifier,
    absl::string_view key,
    const absl::flat_hash_set<absl::string_view> &identifiers) const {
  vmsdk::VerifyMainThread();
  // Validating that the a JSON object with the key exists with the vector
  // identifier
  auto reply = vmsdk::UniquePtrRedisCallReply(RedisModule_Call(
      ctx, kJsonCmd.data(), "cc", key.data(), vector_identifier.c_str()));
  if (reply == nullptr ||
      RedisModule_CallReplyType(reply.get()) != REDISMODULE_REPLY_STRING) {
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
        identifier, RecordsMapValue(vmsdk::MakeUniqueRedisString(identifier),
                                    std::move(str.value())));
  }
  return key_value_content;
}

bool IsJsonModuleLoaded(RedisModuleCtx *ctx) {
  return vmsdk::IsModuleLoaded(ctx, "json");
}
}  // namespace valkey_search
