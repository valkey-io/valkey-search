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

#ifndef VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
#define VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/variant.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

bool HashHasRecord(RedisModuleKey *key, absl::string_view identifier);

class RecordsMapValue {
 public:
  RecordsMapValue(vmsdk::UniqueRedisString identifier,
                  vmsdk::UniqueRedisString value)
      : value(std::move(value)), identifier_(std::move(identifier)) {}
  RecordsMapValue(RedisModuleString *identifier, vmsdk::UniqueRedisString value)
      : value(std::move(value)), identifier_(identifier) {}
  vmsdk::UniqueRedisString value;
  RedisModuleString *GetIdentifier() const {
    if (absl::holds_alternative<vmsdk::UniqueRedisString>(identifier_)) {
      return absl::get<vmsdk::UniqueRedisString>(identifier_).get();
    }
    return absl::get<RedisModuleString *>(identifier_);
  }

 private:
  absl::variant<RedisModuleString *, vmsdk::UniqueRedisString> identifier_;
};

using RecordsMap = absl::flat_hash_map<absl::string_view, RecordsMapValue>;

class AttributeDataType {
 public:
  virtual ~AttributeDataType() = default;
  virtual absl::StatusOr<vmsdk::UniqueRedisString> GetRecord(
      RedisModuleCtx *ctx, RedisModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const = 0;
  virtual int GetRedisEventTypes() const {
    return REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_EXPIRED |
           REDISMODULE_NOTIFY_EVICTED;
  };
  virtual absl::StatusOr<RecordsMap> FetchAllRecords(
      RedisModuleCtx *ctx, const std::string &vector_identifier,
      absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const = 0;
  virtual data_model::AttributeDataType ToProto() const = 0;
  virtual std::string ToString() const = 0;
  virtual bool IsProperType(RedisModuleKey *key) const = 0;
  // This provides indication whether the fetched content need special
  // normalization.
  virtual bool RecordsProvidedAsString() const = 0;
};

class HashAttributeDataType : public AttributeDataType {
 public:
  absl::StatusOr<vmsdk::UniqueRedisString> GetRecord(
      RedisModuleCtx *ctx, RedisModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const override;
  inline int GetRedisEventTypes() const override {
    return REDISMODULE_NOTIFY_HASH | AttributeDataType::GetRedisEventTypes();
  }

  inline data_model::AttributeDataType ToProto() const override {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
  }
  inline std::string ToString() const override { return "HASH"; }
  absl::StatusOr<RecordsMap> FetchAllRecords(
      RedisModuleCtx *ctx, const std::string &vector_identifier,
      absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const override;
  bool IsProperType(RedisModuleKey *key) const override {
    return RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_HASH;
  }
  bool RecordsProvidedAsString() const override { return false; }
};

inline constexpr absl::string_view kJsonCmd = "JSON.GET";
inline constexpr absl::string_view kJsonRootElementQuery = "$";

class JsonAttributeDataType : public AttributeDataType {
 public:
  absl::StatusOr<vmsdk::UniqueRedisString> GetRecord(
      RedisModuleCtx *ctx, RedisModuleKey *open_key, absl::string_view key,
      absl::string_view identifier) const override;
  inline int GetRedisEventTypes() const override {
    return REDISMODULE_NOTIFY_MODULE | AttributeDataType::GetRedisEventTypes();
  }
  inline data_model::AttributeDataType ToProto() const override {
    return data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON;
  }
  inline std::string ToString() const override { return "JSON"; }
  absl::StatusOr<RecordsMap> FetchAllRecords(
      RedisModuleCtx *ctx, const std::string &vector_identifier,
      absl::string_view key,
      const absl::flat_hash_set<absl::string_view> &identifiers) const override;
  bool IsProperType(RedisModuleKey *key) const override {
    return RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_MODULE;
  }
  bool RecordsProvidedAsString() const override { return true; }
};

bool IsJsonModuleLoaded(RedisModuleCtx *ctx);
absl::string_view TrimBrackets(absl::string_view record);
}  // namespace valkey_search
#endif  // VALKEYSEARCH_SRC_ATTRIBUTE_DATA_TYPE_H_
