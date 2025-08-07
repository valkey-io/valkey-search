/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/attribute_data_type.h"

#include <absl/strings/strip.h>

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
#include "vmsdk/src/module.h"
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
    exists_key = vmsdk::MakeUniqueValkeyString(std::string("exists_key"));
    not_exists_key =
        vmsdk::MakeUniqueValkeyString(std::string("not_exists_key"));
    EXPECT_CALL(*kMockValkeyModule, OpenKey(&fake_ctx, testing::_, testing::_))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    opened_exists_key =
        vmsdk::MakeUniqueValkeyOpenKey(&fake_ctx, exists_key.get(), 0);
    opened_not_exists_key =
        vmsdk::MakeUniqueValkeyOpenKey(&fake_ctx, not_exists_key.get(), 0);
  }
  void TearDown() override {
    exists_key = nullptr;
    not_exists_key = nullptr;
    opened_exists_key = nullptr;
    opened_not_exists_key = nullptr;
    ValkeySearchTestWithParam<
        ::testing::tuple<bool, bool, FetchAllRecordsTestCase>>::TearDown();
  }
  ValkeyModuleCtx fake_ctx;
  vmsdk::UniqueValkeyString exists_key;
  vmsdk::UniqueValkeyString not_exists_key;
  vmsdk::UniqueValkeyOpenKey opened_exists_key;
  vmsdk::UniqueValkeyOpenKey opened_not_exists_key;
  absl::string_view exists_identifier{"exists_identifier"};
  absl::string_view not_exists_identifier{"not_exists_identifier"};
  HashAttributeDataType hash_attribute_data_type;
};

TEST_F(HashAttributeDataTypeTest, HashBasic) {
  EXPECT_EQ(hash_attribute_data_type.GetValkeyEventTypes(),
            VALKEYMODULE_NOTIFY_HASH | VALKEYMODULE_NOTIFY_GENERIC |
                VALKEYMODULE_NOTIFY_EXPIRED | VALKEYMODULE_NOTIFY_EVICTED);

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
      EXPECT_CALL(*kMockValkeyModule,
                  HashGet(testing::_,
                          VALKEYMODULE_HASH_CFIELDS | VALKEYMODULE_HASH_EXISTS,
                          testing::_, An<int *>(), TypedEq<void *>(nullptr)))
          .WillOnce([&](ValkeyModuleKey *key, int flags, const char *identifier,
                        int *exists_out, void *terminating_null) {
            *exists_out = opened_exists_key.get() == key &&
                          absl::string_view(identifier) == exists_identifier;
            return VALKEYMODULE_OK;
          });

      EXPECT_EQ(HashHasRecord(key, identifier),
                expect_exists_identifier && expect_exists_key);
    }
  }
}

TEST_F(HashAttributeDataTypeTest, HashGetRecord) {
  for (auto expect_found_record : {true, false}) {
    ValkeyModuleString *found_record =
        expect_found_record
            ? TestValkeyModule_CreateStringPrintf(nullptr, "found_record")
            : nullptr;
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
        .WillOnce([found_record](
                      ValkeyModuleKey *key, int flags, const char *field,
                      ValkeyModuleString **value_out, void *terminating_null) {
          *value_out = found_record;
          return VALKEYMODULE_OK;
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
  EXPECT_CALL(*kMockValkeyModule, OpenKey(&fake_ctx, testing::_, testing::_))
      .WillRepeatedly([&](ValkeyModuleCtx *ctx, ValkeyModuleString *key,
                          int flags) {
        return vmsdk::ToStringView(key) == vmsdk::ToStringView(exists_key.get())
                   ? TestValkeyModule_OpenKeyDefaultImpl(&fake_ctx,
                                                         exists_key.get(), 0)
                   : nullptr;
      });
  absl::string_view identifier =
      expect_exists_identifier ? exists_identifier : not_exists_identifier;
  // Needed to make sure that the mocked fields/values generated by ScanKey
  // outlive the lifetime of the returned RecordMap
  std::list<vmsdk::UniqueValkeyString> values;
  std::list<vmsdk::UniqueValkeyString> fields;
  auto itr = test_case.expected_records_map.begin();
  if (expect_exists_key) {
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_,
                        VALKEYMODULE_HASH_CFIELDS | VALKEYMODULE_HASH_EXISTS,
                        testing::_, An<int *>(), TypedEq<void *>(nullptr)))
        .WillOnce([&](ValkeyModuleKey *key, int flags, const char *identifier,
                      int *exists_out, void *terminating_null) {
          *exists_out = absl::string_view(identifier) == exists_identifier;
          return VALKEYMODULE_OK;
        });
    if (expect_exists_identifier) {
      EXPECT_CALL(
          *kMockValkeyModule,
          ScanKey(An<ValkeyModuleKey *>(), An<ValkeyModuleScanCursor *>(),
                  An<ValkeyModuleScanKeyCB>(), An<void *>()))
          .WillRepeatedly([&](ValkeyModuleKey *key,
                              ValkeyModuleScanCursor *scan_cursor,
                              ValkeyModuleScanKeyCB fn, void *privdata) {
            if (itr == test_case.expected_records_map.end()) {
              return 0;
            }
            auto value = itr->second;
            if (itr->first == identifier) {
              value = HexStringToBinary(itr->second);
            }
            fields.push_back(vmsdk::MakeUniqueValkeyString(itr->first));
            values.push_back(vmsdk::MakeUniqueValkeyString(value));
            fn(key, fields.back().get(), values.back().get(), privdata);
            itr++;
            return 1;
          });
    }
  }
  auto identifiers = ToStringViewSet(test_case.identifiers);
  EXPECT_EQ(identifiers.size(), test_case.identifiers.size());
  auto key_str = vmsdk::MakeUniqueValkeyString(vmsdk::ToStringView(key));
  auto key_obj = vmsdk::MakeUniqueValkeyOpenKey(
      &fake_ctx, key_str.get(),
      VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
  auto records = hash_attribute_data_type.FetchAllRecords(
      &fake_ctx, std::string(identifier), key_obj.get(),
      vmsdk::ToStringView(key), identifiers);
  if (expect_exists_key && expect_exists_identifier) {
    VMSDK_EXPECT_OK(records);
    if (records.ok()) {
      EXPECT_EQ(ToStringMap(records.value()), test_case.expected_records_map);
    }
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
          ::testing::tuple<bool, bool, FetchAllRecordsTestCase>> {
 public:
  void SetUp() override {
    ValkeySearchTestWithParam::SetUp();
    key_str = vmsdk::MakeUniqueValkeyString("key");
    key_obj = vmsdk::MakeUniqueValkeyOpenKey(
        &fake_ctx, key_str.get(),
        VALKEYMODULE_OPEN_KEY_NOEFFECTS | VALKEYMODULE_READ);
    vmsdk::SetModuleLoaded("json");
  }
  ValkeyModuleCtx fake_ctx;
  JsonAttributeDataType json_attribute_data_type;
  vmsdk::UniqueValkeyString key_str;
  vmsdk::UniqueValkeyOpenKey key_obj;
  const std::string query_attribute_name = "vector_json_path";
  absl::flat_hash_map<std::string, std::string> json_path_results = {
      {"$", "[\"res0\"]"},
      {"json_path_results1", "[\"res1\"]"},
      {"json_path_results2", "[\"res2\"]"}};
  ;
};

static JsonAttributeDataTypeTest *current_test;

TEST_F(JsonAttributeDataTypeTest, JsonBasic) {
  EXPECT_EQ(json_attribute_data_type.GetValkeyEventTypes(),
            VALKEYMODULE_NOTIFY_MODULE | VALKEYMODULE_NOTIFY_GENERIC |
                VALKEYMODULE_NOTIFY_EXPIRED | VALKEYMODULE_NOTIFY_EVICTED);

  EXPECT_EQ(json_attribute_data_type.ToProto(),
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON);
}

void CheckJsonGetRecord(
    ValkeyModuleCtx &fake_ctx, absl::string_view identifier,
    int module_reply_type,
    const absl::flat_hash_map<std::string, std::string> &json_path_results) {
  EXPECT_CALL(*kMockValkeyModule,
              Call(&fake_ctx, testing::StrEq(kJsonCmd), testing::StrEq("cc"),
                   testing::_, testing::StrEq(identifier)))
      .WillOnce([identifier, json_path_results, module_reply_type](
                    ValkeyModuleCtx *ctx, const char *cmd, const char *fmt,
                    const char *arg1,
                    const char *arg2) -> ValkeyModuleCallReply * {
        if (!json_path_results.contains(identifier)) {
          return nullptr;
        }
        auto reply = new ValkeyModuleCallReply;
        reply->msg = json_path_results.at(identifier);
        EXPECT_CALL(*kMockValkeyModule, FreeCallReply(reply))
            .WillOnce([](ValkeyModuleCallReply *reply) { delete reply; });
        EXPECT_CALL(*kMockValkeyModule, CallReplyType(reply))
            .WillOnce([module_reply_type](ValkeyModuleCallReply *reply) {
              return module_reply_type;
            });
        if (module_reply_type == VALKEYMODULE_REPLY_STRING) {
          EXPECT_CALL(*kMockValkeyModule, CreateStringFromCallReply(reply))
              .WillOnce([](ValkeyModuleCallReply *reply) {
                return vmsdk::MakeUniqueValkeyString(
                           absl::string_view(reply->msg))
                    .release();
              });
        }
        return reply;
      });
}

int MyJsonSharedAPIGetValue(ValkeyModuleKey *key, const char *path,
                            ValkeyModuleString **result) {
  auto itr = current_test->json_path_results.find(path);
  if (itr == current_test->json_path_results.end()) {
    return VALKEYMODULE_ERR;
  }
  *result =
      vmsdk::MakeUniqueValkeyString(absl::string_view(itr->second)).release();
  return VALKEYMODULE_OK;
}

absl::string_view NormalizeValue(absl::string_view record) {
  if (absl::ConsumePrefix(&record, "[")) {
    absl::ConsumeSuffix(&record, "]");
  }
  if (absl::ConsumePrefix(&record, "\"")) {
      absl::ConsumeSuffix(&record, "\"");
  }
  return record;
}

TEST_P(JsonAttributeDataTypeTest, JsonGetRecord) {
  auto &params = GetParam();
  auto use_shared_api = std::get<1>(params);
  const FetchAllRecordsTestCase &test_case = std::get<2>(params);
  const auto identifiers = ToStringViewSet(test_case.identifiers);
  current_test = this;
  for (const auto &identifier : identifiers) {
    if (use_shared_api) {
      ResetJsonLoadedCache();
      EXPECT_CALL(*kMockValkeyModule,
                  GetSharedAPI(&fake_ctx, testing::StrEq("JSON_GetValue")))
          .WillOnce([&](ValkeyModuleCtx *ctx, const char *cmd) {
            return (void *)MyJsonSharedAPIGetValue;
          });
      auto record = json_attribute_data_type.GetRecord(
          &fake_ctx, key_obj.get(), vmsdk::ToStringView(key_str.get()).data(),
          identifier);
      if (record.ok()) {
        EXPECT_TRUE(json_path_results.contains(identifier));
        auto res_str = std::string(json_path_results.find(identifier)->second);
        EXPECT_EQ(vmsdk::ToStringView(record.value().get()),
                  NormalizeValue(res_str));
      } else {
        EXPECT_FALSE(json_path_results.contains(identifier));
        EXPECT_EQ(record.status().code(), absl::StatusCode::kNotFound);
      }
      continue;
    }
    ResetJsonLoadedCache();
    for (int module_reply_type = VALKEYMODULE_REPLY_UNKNOWN;
         module_reply_type < VALKEYMODULE_REPLY_ATTRIBUTE * 2;
         module_reply_type++) {
      CheckJsonGetRecord(fake_ctx, identifier, module_reply_type,
                         json_path_results);
      auto record = json_attribute_data_type.GetRecord(
          &fake_ctx, key_obj.get(), vmsdk::ToStringView(key_str.get()).data(),
          identifier);
      if (record.ok()) {
        EXPECT_TRUE(json_path_results.contains(identifier));
        EXPECT_EQ(module_reply_type, VALKEYMODULE_REPLY_STRING);
        auto res_str = std::string(json_path_results.find(identifier)->second);
        EXPECT_EQ(vmsdk::ToStringView(record.value().get()),
                  NormalizeValue(res_str));
      } else {
        EXPECT_FALSE(json_path_results.contains(identifier) &&
                     module_reply_type == VALKEYMODULE_REPLY_STRING);
        EXPECT_EQ(record.status().code(), absl::StatusCode::kNotFound);
      }
    }
  }
}

std::unordered_map<std::string, std::string> NormalizeExpected(
    const std::unordered_map<std::string, std::string> &map) {
  auto ret = map;
  for (const auto &entry : ret) {
    absl::string_view value = NormalizeValue(entry.second);
    ret[entry.first] = std::string(value);
  }
  return ret;
}

TEST_P(JsonAttributeDataTypeTest, JsonFetchAllRecords) {
  ValkeyModuleCallReply reply;
  auto &params = GetParam();
  auto expect_exists_key = std::get<0>(params);
  auto use_shared_api = std::get<1>(params);
  const FetchAllRecordsTestCase &test_case = std::get<2>(params);
  const auto identifiers = ToStringViewSet(test_case.identifiers);
  current_test = this;
  if (use_shared_api) {
    ResetJsonLoadedCache();
    EXPECT_CALL(*kMockValkeyModule,
                GetSharedAPI(&fake_ctx, testing::StrEq("JSON_GetValue")))
        .WillOnce([&](ValkeyModuleCtx *ctx, const char *cmd) {
          return (void *)MyJsonSharedAPIGetValue;
        });
    auto records = json_attribute_data_type.FetchAllRecords(
        &fake_ctx, query_attribute_name, key_obj.get(),
        vmsdk::ToStringView(key_str.get()).data(), identifiers);
    if (records.ok()) {
      EXPECT_EQ(ToStringMap(records.value()),
                NormalizeExpected(test_case.expected_records_map));
      return;
    }
    EXPECT_EQ(records.status().code(), absl::StatusCode::kNotFound);
    return;
  }
  ResetJsonLoadedCache();
  for (int module_reply_type = VALKEYMODULE_REPLY_UNKNOWN;
       module_reply_type < VALKEYMODULE_REPLY_ATTRIBUTE * 2;
       module_reply_type++) {
    EXPECT_CALL(*kMockValkeyModule,
                Call(&fake_ctx, testing::StrEq(kJsonCmd), testing::StrEq("cc"),
                     testing::StrEq(vmsdk::ToStringView(key_str.get()).data()),
                     testing::StrEq(query_attribute_name)))
        .WillOnce([&](ValkeyModuleCtx *ctx, const char *cmd, const char *fmt,
                      const char *arg1,
                      const char *arg2) -> ValkeyModuleCallReply * {
          if (!expect_exists_key) {
            return nullptr;
          }
          EXPECT_CALL(*kMockValkeyModule, FreeCallReply(&reply))
              .WillOnce([](ValkeyModuleCallReply *reply) {});
          return &reply;
        });
    if (expect_exists_key) {
      EXPECT_CALL(*kMockValkeyModule, CallReplyType(&reply))
          .WillOnce(
              [&](ValkeyModuleCallReply *reply) { return module_reply_type; });
      if (module_reply_type == VALKEYMODULE_REPLY_STRING) {
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
        &fake_ctx, query_attribute_name, key_obj.get(),
        vmsdk::ToStringView(key_str.get()).data(), identifiers);
    if (records.ok()) {
      EXPECT_EQ(module_reply_type, VALKEYMODULE_REPLY_STRING);
      EXPECT_EQ(ToStringMap(records.value()), NormalizeExpected(test_case.expected_records_map));
      return;
    }
    EXPECT_FALSE(expect_exists_key &&
                 module_reply_type == VALKEYMODULE_REPLY_STRING);
    EXPECT_EQ(records.status().code(), absl::StatusCode::kNotFound);
  }
}

INSTANTIATE_TEST_SUITE_P(
    JsonHashAttributeDataTypeTests, JsonAttributeDataTypeTest,
    testing::Combine(
        testing::Bool(), testing::Bool(),
        testing::ValuesIn<FetchAllRecordsTestCase>({
            {
                .test_name = "single_identifier",
                .identifiers = {"$", "false"},
                .expected_records_map = {{"$", "res0"}},
            },
            {
                .test_name = "multiple_identifier",
                .identifiers = {"json_path_results1", "json_path_results2"},
                .expected_records_map = {{"json_path_results1", "res1"},
                                         {"json_path_results2", "res2"}},
            },
        })),
    [](const TestParamInfo<
        ::testing::tuple<bool, bool, FetchAllRecordsTestCase>> &info) {
      auto expect_exists_key = std::get<0>(info.param);
      auto use_shared_api = std::get<1>(info.param);
      return std::get<2>(info.param).test_name + "_" +
             (expect_exists_key ? "expect_exists_key"
                                : "expect_not_exists_key") +
             "_" + (use_shared_api ? "use_shared_api" : "use_call_api");
    });

}  // namespace

}  // namespace valkey_search
