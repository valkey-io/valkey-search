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

#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {

using testing::An;
using testing::TestParamInfo;
using testing::TypedEq;

struct FetchAllRecordsTestCase {
  std::string test_name;
  absl::flat_hash_set<std::string> identifiers;
  std::unordered_map<std::string, std::string> expected_records_map;
};

class HashAttributeDataTypeTest
    : public ValkeySearchTestWithParam<
          ::testing::tuple<bool, bool, FetchAllRecordsTestCase>> {
 protected:
  void SetUp() override {
    ValkeySearchTestWithParam<
        ::testing::tuple<bool, bool, FetchAllRecordsTestCase>>::SetUp();
    exists_key = vmsdk::MakeUniqueRedisString(std::string("exists_key"));
    not_exists_key =
        vmsdk::MakeUniqueRedisString(std::string("not_exists_key"));
    EXPECT_CALL(*kMockRedisModule, OpenKey(&fake_ctx, testing::_, testing::_))
        .WillRepeatedly(TestRedisModule_OpenKeyDefaultImpl);
    opened_exists_key =
        vmsdk::MakeUniqueRedisOpenKey(&fake_ctx, exists_key.get(), 0);
    opened_not_exists_key =
        vmsdk::MakeUniqueRedisOpenKey(&fake_ctx, not_exists_key.get(), 0);
  }
  void TearDown() override {
    exists_key = nullptr;
    not_exists_key = nullptr;
    opened_exists_key = nullptr;
    opened_not_exists_key = nullptr;
    ValkeySearchTestWithParam<
        ::testing::tuple<bool, bool, FetchAllRecordsTestCase>>::TearDown();
  }
  RedisModuleCtx fake_ctx;
  vmsdk::UniqueRedisString exists_key;
  vmsdk::UniqueRedisString not_exists_key;
  vmsdk::UniqueRedisOpenKey opened_exists_key;
  vmsdk::UniqueRedisOpenKey opened_not_exists_key;
  absl::string_view exists_identifier{"exists_identifier"};
  absl::string_view not_exists_identifier{"not_exists_identifier"};
  HashAttributeDataType hash_attribute_data_type;
};

TEST_F(HashAttributeDataTypeTest, HashBasic) {
  EXPECT_EQ(hash_attribute_data_type.GetRedisEventTypes(),
            REDISMODULE_NOTIFY_HASH | REDISMODULE_NOTIFY_GENERIC |
                REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_EVICTED);

  EXPECT_EQ(hash_attribute_data_type.ToProto(),
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
}

TEST_F(HashAttributeDataTypeTest, HashHasRecord) {
  for (auto expect_exists_key : {true, false}) {
    auto key = expect_exists_key ? opened_exists_key.get()
                                 : opened_not_exists_key.get();
    for (auto expect_exists_identifier : {true, false}) {
      absl::string_view identifier =
          expect_exists_identifier ? exists_identifier : not_exists_identifier;
      EXPECT_CALL(*kMockRedisModule,
                  HashGet(testing::_,
                          REDISMODULE_HASH_CFIELDS | REDISMODULE_HASH_EXISTS,
                          testing::_, An<int *>(), TypedEq<void *>(nullptr)))
          .WillOnce([&](RedisModuleKey *key, int flags, const char *identifier,
                        int *exists_out, void *terminating_null) {
            *exists_out = opened_exists_key.get() == key &&
                          absl::string_view(identifier) == exists_identifier;
            return REDISMODULE_OK;
          });

      EXPECT_EQ(HashHasRecord(key, identifier),
                expect_exists_identifier && expect_exists_key);
    }
  }
}

TEST_F(HashAttributeDataTypeTest, HashGetRecord) {
  for (auto expect_found_record : {true, false}) {
    RedisModuleString *found_record =
        expect_found_record
            ? TestRedisModule_CreateStringPrintf(nullptr, "found_record")
            : nullptr;
    EXPECT_CALL(*kMockRedisModule,
                HashGet(testing::_, REDISMODULE_HASH_CFIELDS, testing::_,
                        An<RedisModuleString **>(), TypedEq<void *>(nullptr)))
        .WillOnce([found_record](
                      RedisModuleKey *key, int flags, const char *field,
                      RedisModuleString **value_out, void *terminating_null) {
          *value_out = found_record;
          return REDISMODULE_OK;
        });
    auto record = hash_attribute_data_type.GetRecord(
        &fake_ctx, opened_exists_key.get(),
        vmsdk::ToStringView(exists_key.get()), exists_identifier);
    if (expect_found_record) {
      VMSDK_EXPECT_OK(record);
      EXPECT_EQ(record.value().get(), found_record);
    } else {
      EXPECT_EQ(record.status().code(), absl::StatusCode::kNotFound);
    }
  }
}

absl::flat_hash_set<absl::string_view> ToStringViewSet(
    const absl::flat_hash_set<std::string> &identifiers) {
  absl::flat_hash_set<absl::string_view> identifiers_view;
  for (const auto &identifier : identifiers) {
    identifiers_view.insert(absl::string_view(identifier));
  }
  return identifiers_view;
}

std::string HexStringToBinary(absl::string_view hex_str) {
  std::vector<float> vec;
  // Ensure the input string has a size that's a multiple of the size of a float
  EXPECT_EQ(hex_str.size() % sizeof(float), 0);

  vec.resize(hex_str.size() / sizeof(float));

  std::memcpy(vec.data(), hex_str.data(), hex_str.size());
  std::string str;
  str.resize(vec.size() * sizeof(float));
  std::memcpy(str.data(), vec.data(), str.size());

  return str;
}

TEST_P(HashAttributeDataTypeTest, HashFetchAllRecords) {
  auto &params = GetParam();
  auto expect_exists_key = std::get<0>(params);
  auto expect_exists_identifier = std::get<1>(params);
  const FetchAllRecordsTestCase &test_case = std::get<2>(params);
  auto key = expect_exists_key ? exists_key.get() : not_exists_key.get();
  EXPECT_CALL(*kMockRedisModule, OpenKey(&fake_ctx, testing::_, testing::_))
      .WillOnce([&](RedisModuleCtx *ctx, RedisModuleString *key, int flags) {
        return vmsdk::ToStringView(key) == vmsdk::ToStringView(exists_key.get())
                   ? TestRedisModule_OpenKeyDefaultImpl(&fake_ctx,
                                                        exists_key.get(), 0)
                   : nullptr;
      });
  absl::string_view identifier =
      expect_exists_identifier ? exists_identifier : not_exists_identifier;
  if (expect_exists_key) {
    EXPECT_CALL(
        *kMockRedisModule,
        HashGet(testing::_, REDISMODULE_HASH_CFIELDS | REDISMODULE_HASH_EXISTS,
                testing::_, An<int *>(), TypedEq<void *>(nullptr)))
        .WillOnce([&](RedisModuleKey *key, int flags, const char *identifier,
                      int *exists_out, void *terminating_null) {
          *exists_out = absl::string_view(identifier) == exists_identifier;
          return REDISMODULE_OK;
        });
    if (expect_exists_identifier) {
      auto itr = test_case.expected_records_map.begin();
      EXPECT_CALL(*kMockRedisModule,
                  ScanKey(An<RedisModuleKey *>(), An<RedisModuleScanCursor *>(),
                          An<RedisModuleScanKeyCB>(), An<void *>()))
          .WillRepeatedly([&](RedisModuleKey *key,
                              RedisModuleScanCursor *scan_cursor,
                              RedisModuleScanKeyCB fn, void *privdata) {
            if (itr == test_case.expected_records_map.end()) {
              return 0;
            }
            auto value = itr->second;
            if (itr->first == identifier) {
              value = HexStringToBinary(itr->second);
            }
            fn(key, vmsdk::MakeUniqueRedisString(itr->first).get(),
               vmsdk::MakeUniqueRedisString(value).get(), privdata);
            itr++;
            return 1;
          });
    }
  }

  auto identifiers = ToStringViewSet(test_case.identifiers);
  auto records = hash_attribute_data_type.FetchAllRecords(
      &fake_ctx, std::string(identifier), vmsdk::ToStringView(key),
      identifiers);
  if (expect_exists_key && expect_exists_identifier) {
    VMSDK_EXPECT_OK(records);
    EXPECT_EQ(ToStringMap(records.value()), test_case.expected_records_map);
  } else {
    EXPECT_EQ(records.status().code(), absl::StatusCode::kNotFound);
  }
}

INSTANTIATE_TEST_SUITE_P(
    HashHashAttributeDataTypeTests, HashAttributeDataTypeTest,
    testing::Combine(
        testing::Bool(), testing::Bool(),
        testing::ValuesIn<FetchAllRecordsTestCase>({
            {
                .test_name = "empty_identifiers",
                .identifiers = {},
                .expected_records_map =
                    {{"field1", "value1"},
                     {"field2", "value2"},
                     {"exists_identifier",
                      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?"}},
            },
            {
                .test_name = "full_identifiers",
                .identifiers = {"field1", "field2"},
                .expected_records_map = {{"field1", "value1"},
                                         {"field2", "value2"}},
            },
            {
                .test_name = "field2_identifier",
                .identifiers = {"field2"},
                .expected_records_map = {{"field2", "value2"}},
            },
            {
                .test_name = "field1_identifier",
                .identifiers = {"field1"},
                .expected_records_map = {{"field1", "value1"}},
            },
        })),
    [](const TestParamInfo<
        ::testing::tuple<bool, bool, FetchAllRecordsTestCase>> &info) {
      auto expect_exists_key = std::get<0>(info.param);
      auto expect_exists_identifier = std::get<1>(info.param);

      return std::get<2>(info.param).test_name + "_" +
             (expect_exists_key ? "expect_exists_key"
                                : "expect_not_exists_key") +
             "_" +
             (expect_exists_identifier ? "expect_exists_identifier"
                                       : "expect_not_exists_identifier");
    });

class JsonAttributeDataTypeTest
    : public ValkeySearchTestWithParam<
          ::testing::tuple<bool, FetchAllRecordsTestCase>> {
 protected:
  RedisModuleCtx fake_ctx;
  JsonAttributeDataType json_attribute_data_type;
};

TEST_F(JsonAttributeDataTypeTest, JsonBasic) {
  EXPECT_EQ(json_attribute_data_type.GetRedisEventTypes(),
            REDISMODULE_NOTIFY_MODULE | REDISMODULE_NOTIFY_GENERIC |
                REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_EVICTED);

  EXPECT_EQ(json_attribute_data_type.ToProto(),
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON);
}

void CheckJsonGetRecord(
    RedisModuleCtx &fake_ctx, absl::string_view identifier,
    int module_reply_type,
    const absl::flat_hash_map<std::string, std::string> &json_path_results) {
  EXPECT_CALL(*kMockRedisModule,
              Call(&fake_ctx, testing::StrEq(kJsonCmd), testing::StrEq("cc"),
                   testing::_, testing::StrEq(identifier)))
      .WillOnce([identifier, json_path_results, module_reply_type](
                    RedisModuleCtx *ctx, const char *cmd, const char *fmt,
                    const char *arg1,
                    const char *arg2) -> RedisModuleCallReply * {
        if (!json_path_results.contains(identifier)) {
          return nullptr;
        }
        auto reply = new RedisModuleCallReply;
        reply->msg = json_path_results.at(identifier);
        EXPECT_CALL(*kMockRedisModule, FreeCallReply(reply))
            .WillOnce([](RedisModuleCallReply *reply) { delete reply; });
        EXPECT_CALL(*kMockRedisModule, CallReplyType(reply))
            .WillOnce([module_reply_type](RedisModuleCallReply *reply) {
              return module_reply_type;
            });
        if (module_reply_type == REDISMODULE_REPLY_STRING) {
          EXPECT_CALL(*kMockRedisModule, CreateStringFromCallReply(reply))
              .WillOnce([](RedisModuleCallReply *reply) {
                return vmsdk::MakeUniqueRedisString(
                           absl::string_view(reply->msg))
                    .release();
              });
        }
        return reply;
      });
}

TEST_F(JsonAttributeDataTypeTest, JsonGetRecord) {
  absl::flat_hash_map<std::string, std::string> json_path_results{
      {"$", "res0"},
      {"json_path_results1", "res1"},
      {"json_path_results2", "res2"}};
  absl::flat_hash_set<std::string> identifiers{"$", "false"};
  for (auto identifier : identifiers) {
    for (int module_reply_type = REDISMODULE_REPLY_UNKNOWN;
         module_reply_type < REDISMODULE_REPLY_ATTRIBUTE * 2;
         module_reply_type++) {
      CheckJsonGetRecord(fake_ctx, identifier, module_reply_type,
                         json_path_results);
      auto record = json_attribute_data_type.GetRecord(&fake_ctx, nullptr,
                                                       "key", identifier);
      if (record.ok()) {
        EXPECT_TRUE(json_path_results.contains(identifier));
        EXPECT_EQ(module_reply_type, REDISMODULE_REPLY_STRING);
        EXPECT_EQ(vmsdk::ToStringView(record.value().get()),
                  json_path_results[identifier]);
      } else {
        EXPECT_FALSE(json_path_results.contains(identifier) &&
                     module_reply_type == REDISMODULE_REPLY_STRING);
        EXPECT_EQ(record.status().code(), absl::StatusCode::kNotFound);
      }
    }
  }
}

TEST_P(JsonAttributeDataTypeTest, JsonFetchAllRecords) {
  absl::flat_hash_map<std::string, std::string> json_path_results{
      {"$", "res0"},
      {"json_path_results1", "res1"},
      {"json_path_results2", "res2"}};
  RedisModuleCallReply reply;
  auto &params = GetParam();
  auto expect_exists_key = std::get<0>(params);
  const FetchAllRecordsTestCase &test_case = std::get<1>(params);
  std::string query_attribute_name = "vector_json_path";
  for (int module_reply_type = REDISMODULE_REPLY_UNKNOWN;
       module_reply_type < REDISMODULE_REPLY_ATTRIBUTE * 2;
       module_reply_type++) {
    EXPECT_CALL(
        *kMockRedisModule,
        Call(&fake_ctx, testing::StrEq(kJsonCmd), testing::StrEq("cc"),
             testing::StrEq("key"), testing::StrEq(query_attribute_name)))
        .WillOnce([&](RedisModuleCtx *ctx, const char *cmd, const char *fmt,
                      const char *arg1,
                      const char *arg2) -> RedisModuleCallReply * {
          if (!expect_exists_key) return nullptr;
          EXPECT_CALL(*kMockRedisModule, FreeCallReply(&reply))
              .WillOnce([](RedisModuleCallReply *reply) {});
          return &reply;
        });
    if (expect_exists_key) {
      EXPECT_CALL(*kMockRedisModule, CallReplyType(&reply))
          .WillOnce(
              [&](RedisModuleCallReply *reply) { return module_reply_type; });
      if (module_reply_type == REDISMODULE_REPLY_STRING) {
        if (test_case.identifiers.empty()) {
          CheckJsonGetRecord(fake_ctx, kJsonRootElementQuery, module_reply_type,
                             json_path_results);
        } else {
          for (const auto &identifier : test_case.identifiers) {
            CheckJsonGetRecord(fake_ctx, identifier, module_reply_type,
                               json_path_results);
          }
        }
      }
    }

    auto records = json_attribute_data_type.FetchAllRecords(
        &fake_ctx, query_attribute_name, "key",
        ToStringViewSet(test_case.identifiers));
    if (records.ok()) {
      EXPECT_EQ(module_reply_type, REDISMODULE_REPLY_STRING);
      EXPECT_EQ(ToStringMap(records.value()), test_case.expected_records_map);
    } else {
      EXPECT_FALSE(expect_exists_key &&
                   module_reply_type == REDISMODULE_REPLY_STRING);
      EXPECT_EQ(records.status().code(), absl::StatusCode::kNotFound);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    JsonHashAttributeDataTypeTests, JsonAttributeDataTypeTest,
    testing::Combine(
        testing::Bool(),
        testing::ValuesIn<FetchAllRecordsTestCase>({
            {
                .test_name = "single_identifier",
                .identifiers = {"$"},
                .expected_records_map = {{"$", "res0"}},
            },
            {
                .test_name = "multiple_identifier",
                .identifiers = {"json_path_results1", "json_path_results2"},
                .expected_records_map = {{"json_path_results1", "res1"},
                                         {"json_path_results2", "res2"}},
            },
        })),
    [](const TestParamInfo<::testing::tuple<bool, FetchAllRecordsTestCase>>
           &info) {
      auto expect_exists_key = std::get<0>(info.param);
      return std::get<1>(info.param).test_name + "_" +
             (expect_exists_key ? "expect_exists_key"
                                : "expect_not_exists_key");
    });

}  // namespace

}  // namespace valkey_search
