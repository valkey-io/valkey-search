/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_search_parser.h"

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/numeric.h"
#include "src/indexes/vector_flat.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

namespace {
using testing::TestParamInfo;
using testing::ValuesIn;

const std::vector<std::pair<bool, absl::string_view>> kDialectOptions = {
    {true, ""},
    {true, "DIALECt 2"},
    {true, "DIALECt 3"},
    {true, "DIALECt 4"},
    {true, "DIALECT 2"},
    {false, "DIALECT 1"},
    {false, "DIALECT -2"},
    {false, "DIALECT 5"},
};
const std::vector<std::pair<bool, absl::string_view>> kLimitOptions = {
    {true, ""},           {true, "LIMiT 0 0"},    {true, "LIMIT 0 0"},
    {true, "LIMIT 10 5"}, {false, "LIMIT -10 5"}, {false, "LIMIT 10 -5"},
};

struct FTSearchParserTestCase {
  std::string test_name;
  bool success{false};
  absl::string_view params_str;
  absl::string_view filter_str;
  std::string attribute_alias = "vec";
  int k{-1};
  std::optional<int> ef;
  std::string score_as;
  std::string expected_error_message;
  std::string return_str;
  std::unordered_map<std::string, std::string> return_attributes;
  bool no_content{false};
  std::string search_parameters_str;
  uint64_t timeout_ms{query::kTimeoutMS};
  bool vector_query{true};
};

class FTSearchParserTest
    : public ValkeySearchTestWithParam<FTSearchParserTestCase> {};

std::vector<ValkeyModuleString *> FloatToValkeyStringVector(
    const std::vector<float> &floats) {
  std::vector<ValkeyModuleString *> ret;
  const absl::string_view blob_str = "BLOB";
  ret.push_back(
      ValkeyModule_CreateString(nullptr, blob_str.data(), blob_str.size()));
  std::string vector_str((char *)(&floats[0]), floats.size() * sizeof(float));
  ret.push_back(ValkeyModule_CreateString(nullptr, vector_str.c_str(),
                                          vector_str.size()));
  return ret;
}

void DoVectorSearchParserTest(const FTSearchParserTestCase &test_case,
                              size_t dialect_itr, size_t limit_itr,
                              bool add_end_unexpected_param, bool no_content,
                              std::optional<uint64_t> timeout_ms) {
  std::cerr << test_case.test_name
            << " - dialect: " << kDialectOptions[dialect_itr].second
            << ", limit: " << kLimitOptions[limit_itr].second
            << ", add_end_unexpected_param: " << add_end_unexpected_param
            << ", no_content: " << no_content;
  if (timeout_ms.has_value()) {
    std::cerr << ", timeout_ms: " << timeout_ms.value();
  }
  std::cerr << ", no_content: " << no_content << "\n";

  std::vector<float> floats = {0.1, 0.2, 0.3};
  std::vector<ValkeyModuleString *> args;
  const std::string key_str = "my_schema_name";
  ValkeyModuleCtx fake_ctx;
  SchemaManager::InitInstance(
      std::make_unique<TestableSchemaManager>(&fake_ctx));
  auto index_schema = CreateIndexSchema(key_str, &fake_ctx).value();
  EXPECT_CALL(
      *kMockValkeyModule,
      OpenKey(testing::_, testing::An<ValkeyModuleString *>(), testing::_))
      .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());
  if (test_case.vector_query) {
    // Vector index setup
    data_model::VectorIndex vector_index_proto;
    vector_index_proto.set_dimension_count(3);
    vector_index_proto.set_initial_cap(100);
    vector_index_proto.set_vector_data_type(
        data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32);
    auto flat_algorithm_proto = std::make_unique<data_model::FlatAlgorithm>();
    flat_algorithm_proto->set_block_size(100);
    vector_index_proto.set_allocated_flat_algorithm(
        flat_algorithm_proto.release());
    auto index = indexes::VectorFlat<float>::Create(
                     vector_index_proto, "attribute_identifier_1",
                     data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
                     .value();
    VMSDK_EXPECT_OK(
        index_schema->AddIndex(test_case.attribute_alias, "id1", index));
  } else {
    // Non Vector index setup
    data_model::NumericIndex numeric_index_proto;
    auto numeric_index =
        std::make_shared<indexes::Numeric>(numeric_index_proto);
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute_identifier_1", "id1", numeric_index));
    data_model::TagIndex tag_index_proto;
    auto tag_index = std::make_shared<indexes::Tag>(tag_index_proto);
    VMSDK_EXPECT_OK(
        index_schema->AddIndex("attribute_identifier_2", "id2", tag_index));
  }
  args.push_back(
      ValkeyModule_CreateString(nullptr, key_str.data(), key_str.size()));
  args.push_back(ValkeyModule_CreateString(nullptr, test_case.filter_str.data(),
                                           test_case.filter_str.size()));
  if (no_content) {
    const absl::string_view kNoContentParam = "NoContent";
    args.push_back(ValkeyModule_CreateString(nullptr, kNoContentParam.data(),
                                             kNoContentParam.size()));
  }
  auto return_vec = vmsdk::ToValkeyStringVector(test_case.return_str);
  args.insert(args.end(), return_vec.begin(), return_vec.end());
  bool timeout_expected_success = true;
  if (timeout_ms.has_value()) {
    const absl::string_view kTimeoutParam = "Timeout";
    args.push_back(ValkeyModule_CreateString(nullptr, kTimeoutParam.data(),
                                             kTimeoutParam.size()));
    auto timeout_str = std::to_string(timeout_ms.value());
    args.push_back(ValkeyModule_CreateString(nullptr, timeout_str.data(),
                                             timeout_str.size()));
    if (timeout_ms.value() >= query::kMaxTimeoutMs + 1) {
      timeout_expected_success = false;
    }
  }
  bool limit_expected_success = true;
  if (!kLimitOptions[limit_itr].second.empty()) {
    auto limit_vec =
        vmsdk::ToValkeyStringVector(kLimitOptions[limit_itr].second);
    args.insert(args.end(), limit_vec.begin(), limit_vec.end());
    limit_expected_success = kLimitOptions[limit_itr].first;
  }
  auto params_vec = vmsdk::ToValkeyStringVector(test_case.params_str);
  args.insert(args.end(), params_vec.begin(), params_vec.end());
  bool dialect_expected_success = true;
  if (test_case.vector_query) {
    auto floats_vec = FloatToValkeyStringVector(floats);
    args.insert(args.end(), floats_vec.begin(), floats_vec.end());
    auto search_parameters_vec =
        vmsdk::ToValkeyStringVector(test_case.search_parameters_str);
    args.insert(args.end(), search_parameters_vec.begin(),
                search_parameters_vec.end());
    if (!kDialectOptions[dialect_itr].second.empty()) {
      auto dialect_vec =
          vmsdk::ToValkeyStringVector(kDialectOptions[dialect_itr].second);
      args.insert(args.end(), dialect_vec.begin(), dialect_vec.end());
      dialect_expected_success = kDialectOptions[dialect_itr].first;
    }
  }
  if (add_end_unexpected_param) {
    args.push_back(
        ValkeyModule_CreateString(nullptr, "END_UNEXPECTED_PARAM", 0));
  }
  auto &schema_manager = SchemaManager::Instance();

  std::cerr << "Executing cmd: ";
  for (auto &a : args) {
    std::cerr << "'" << vmsdk::ToStringView(a) << "' ";
  }
  std::cerr << "\n";

  // Repro semantics of command startup
  vmsdk::ArgsIterator itr{&args[0], int(args.size())};
  absl::StatusOr<std::unique_ptr<SearchCommand>> search_params(
      std::make_unique<SearchCommand>());
  (*search_params)->timeout_ms = 50000;
  (*search_params)->index_schema_name = vmsdk::ToStringView(*itr.PopNext());
  (*search_params)->parse_vars.query_string =
      vmsdk::ToStringView(*itr.PopNext());
  auto this_index_schema =
      schema_manager.GetIndexSchema(0, (*search_params)->index_schema_name);
  if (!this_index_schema.ok()) {
    search_params = this_index_schema.status();
  } else {
    (*search_params)->index_schema = *this_index_schema;
    auto sts = (*search_params)->ParseCommand(itr);
    if (!sts.ok()) {
      search_params = sts;
    }
  }

  bool expected_success = dialect_expected_success && limit_expected_success &&
                          test_case.success && !add_end_unexpected_param &&
                          timeout_expected_success;

  EXPECT_EQ(search_params.ok(), expected_success);
  if (search_params.ok()) {
    EXPECT_EQ(search_params.value()->index_schema_name, key_str);
    if (test_case.vector_query) {
      // Vector query specific checks
      std::string vector_str((char *)(&floats[0]),
                             floats.size() * sizeof(float));
      EXPECT_EQ(search_params.value()->query, vector_str.c_str());
      EXPECT_EQ(search_params.value()->k, test_case.k);
      EXPECT_EQ(search_params.value()->ef, test_case.ef);
      EXPECT_EQ(search_params.value()->attribute_alias,
                test_case.attribute_alias);
      auto score_as = vmsdk::MakeUniqueValkeyString(test_case.score_as);
      if (test_case.score_as.empty()) {
        score_as = index_schema->DefaultReplyScoreAs(test_case.attribute_alias)
                       .value();
      }
      EXPECT_EQ(vmsdk::ToStringView(search_params.value()->score_as.get()),
                vmsdk::ToStringView(score_as.get()));
    } else {
      // Non vector query specific checks
      EXPECT_TRUE(search_params.value()->query.empty());
      EXPECT_EQ(search_params.value()->k, 0);
      EXPECT_FALSE(search_params.value()->ef.has_value());
      EXPECT_TRUE(search_params.value()->attribute_alias.empty());
    }
    EXPECT_EQ(search_params.value()->no_content,
              no_content || test_case.no_content);
    EXPECT_EQ(search_params.value()->return_attributes.size(),
              test_case.return_attributes.size());
    if (!test_case.no_content) {
      for (const auto &attribute : search_params.value()->return_attributes) {
        auto it = test_case.return_attributes.find(
            std::string(vmsdk::ToStringView(attribute.identifier.get())));
        EXPECT_NE(it, test_case.return_attributes.end());
        EXPECT_EQ(vmsdk::ToStringView(attribute.alias.get()), it->second);
      }
    }
    if (timeout_ms.has_value()) {
      EXPECT_EQ(search_params.value()->timeout_ms, timeout_ms.value());
    } else {
      EXPECT_EQ(search_params.value()->timeout_ms, test_case.timeout_ms);
    }
  } else {
    std::cerr << "Failed to parse command: `" << vmsdk::ToStringView(args[0])
              << "` Because: " << search_params.status().message() << "\n";
    if (!test_case.expected_error_message.empty() &&
        !search_params.status().message().starts_with(
            test_case.expected_error_message)) {
      if (!timeout_expected_success) {
        EXPECT_EQ(search_params.status().message(),
                  "TIMEOUT must be a positive integer greater than 0 and "
                  "cannot exceed 60000.");
      } else if (!limit_expected_success) {
        EXPECT_TRUE(search_params.status().message().starts_with(
            "Bad arguments for LIMIT: `"));
        EXPECT_TRUE(search_params.status().message().ends_with(
            "` is outside acceptable bounds"));
      } else if (!dialect_expected_success) {
        EXPECT_TRUE(search_params.status().message().starts_with(
            "Error parsing value for the parameter `PARAMS` - Unexpected "
            "argument "
            "`DIALEC"));
      } else {
        EXPECT_TRUE(add_end_unexpected_param || !test_case.success);
        std::cerr << "Status Message: " << search_params.status().message()
                  << "\n";
        EXPECT_TRUE(search_params.status().message().starts_with(
            "Error parsing vector similarity parameters"));
      }
    }
  }
  for (const auto &arg : args) {
    TestValkeyModule_FreeString(nullptr, arg);
  }
}

TEST_P(FTSearchParserTest, Parse) {
  const FTSearchParserTestCase &test_case = GetParam();
  if (!test_case.success || !test_case.search_parameters_str.empty()) {
    DoVectorSearchParserTest(test_case, 0, 0, false, false, std::nullopt);
    return;
  }
  for (size_t dialect_itr = 0; dialect_itr < kDialectOptions.size();
       ++dialect_itr) {
    for (size_t limit_itr = 0; limit_itr < kLimitOptions.size(); ++limit_itr) {
      for (bool add_end_unexpected_param : {false, true}) {
        for (bool no_content : {false, true}) {
          for (uint64_t timeout_ms : {100, 200}) {
            DoVectorSearchParserTest(test_case, dialect_itr, limit_itr,
                                     add_end_unexpected_param, no_content,
                                     timeout_ms);
          }
          DoVectorSearchParserTest(test_case, dialect_itr, limit_itr,
                                   add_end_unexpected_param, no_content,
                                   std::nullopt);
          DoVectorSearchParserTest(test_case, dialect_itr, limit_itr,
                                   add_end_unexpected_param, no_content,
                                   query::kMaxTimeoutMs + 1);
        }
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    FTSearchParserTests, FTSearchParserTest,
    ValuesIn<FTSearchParserTestCase>({
        {
            .test_name = "happy_path",
            .success = true,
            .params_str = " PARAMS 4 EF 150",
            .filter_str = "*=>[KNN 10 @vec $BLOB EF_RUNTIME $EF]",
            .k = 10,
            .ef = 150,
        },
        {
            .test_name = "happy_path_k_as_param",
            .success = true,
            .params_str = " PARAMS 6 EF 150 K 10",
            .filter_str = "*=>[KNN $K @vec $BLOB EF_RUNTIME $EF]",
            .k = 10,
            .ef = 150,
        },
        {
            .test_name = "happy_path_include_search_params_1",
            .success = true,
            .params_str = " PARAMS 6 EF 150 K 10 ",
            .filter_str = "*=>[KNN $K @vec $BLOB EF_RUNTIME $EF]",
            .k = 10,
            .ef = 150,
            .return_attributes = {{"r1", "r1"}, {"r2", "r2"}},
            .no_content = true,
            .search_parameters_str = "NoContent RETURN 2 r1 r2 TIMEOUT 100",
            .timeout_ms = 100,
        },
        {
            .test_name = "happy_path_include_search_params_2",
            .success = true,
            .params_str = " PARAMS 6 EF 150 K 10 ",
            .filter_str = "*=>[KNN $K @vec $BLOB EF_RUNTIME $EF]",
            .k = 10,
            .ef = 150,
            .return_attributes = {{"r1", "r1"}, {"r2", "r2"}},
            .no_content = true,
            .search_parameters_str = "TIMEOUT 200 RETURN 2 r1 r2 NOCONTENT ",
            .timeout_ms = 200,
        },
        {
            .test_name = "happy_path_braces_prefilter",
            .success = true,
            .params_str = " PARAMS 4 EF 190",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB EF_RUNTIMe $EF]",
            .k = 10,
            .ef = 190,
        },
        {
            .test_name = "happy_path_braces_prefilter_with_score_as",
            .success = true,
            .params_str = " PARAMS 4 EF 190",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB EF_RUNTIMe $EF As as_test]",
            .k = 10,
            .ef = 190,
            .score_as = "as_test",
        },
        {
            .test_name = "happy_path_numeric",
            .success = true,
            .params_str = "",
            .filter_str = "@attribute_identifier_1:[300 1000]",
            .attribute_alias = "",
            .k = 0,
            .ef = 0,
            .score_as = "",
            .vector_query = false,
        },
        {
            .test_name = "happy_path_tag",
            .success = true,
            .params_str = "",
            .filter_str = "@attribute_identifier_2:{electronics}",
            .attribute_alias = "",
            .k = 0,
            .ef = 0,
            .score_as = "",
            .vector_query = false,
        },
        {
            .test_name = "happy_path_numeric_and_tag",
            .success = true,
            .params_str = "",
            .filter_str = "@attribute_identifier_2:{electronics} "
                          "@attribute_identifier_1:[300 1000]",
            .attribute_alias = "",
            .k = 0,
            .ef = 0,
            .score_as = "",
            .vector_query = false,
        },
        {
            .test_name = "unexpected_prefilter_param",
            .success = false,
            .params_str = " PARAMS 4 EF 190",
            .filter_str =
                "(*)=>[KNN 10 @vec $BLOB EF_RUNTIMe $EF bubu 3 As as_test]",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[KNN 10 @vec "
                "$BLOB EF_RUNTIMe $EF bubu 3 As as_test]`. Unexpected argument "
                "`bubu`",
        },
        {
            .test_name = "missing_ef_runtime_value",
            .success = false,
            .params_str = " PARAMS 4 EF 190",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB EF_RUNTIMe]",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[KNN 10 @vec "
                "$BLOB EF_RUNTIMe]`. EF_RUNTIME argument is missing",
        },
        {
            .test_name = "missing_as_score_value",
            .success = false,
            .params_str = " PARAMS 4 EF 190",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB EF_RUNTIMe 10 AS]",
            .expected_error_message =
                "Error parsing vector similarity "
                "parameters: `[KNN 10 @vec $BLOB "
                "EF_RUNTIMe 10 AS]`. AS argument is missing",
        },
        {
            .test_name = "happy_path_as_before_ef_runtime",
            .success = true,
            .params_str = " PARAMS 4 EF 190",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB As as_test EF_RUNTIMe $EF]",
            .k = 10,
            .ef = 190,
            .score_as = "as_test",
        },
        {
            .test_name = "empty_hash_field",
            .success = false,
            .params_str = " PARAMS 4 EF 190",
            .filter_str = "(*)=>[KNN 10 @ $BLOB As as_test EF_RUNTIMe $EF]",
            .expected_error_message =
                "Error parsing vector similarity "
                "parameters: `[KNN 10 @ $BLOB As as_test EF_RUNTIMe $EF]`."
                " Unexpected argument `@`. Expecting a vector field name, "
                "starting with '@'",
        },
        {
            .test_name = "happy_path_1",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = " * => [KNN 10 @vec $BLOB]",
            .k = 10,
        },
        {
            .test_name = "happy_path_1_with_score_as",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = " * => [KNN 10 @vec $BLOB as as_test_1]",
            .k = 10,
            .score_as = "as_test_1",
        },
        {
            .test_name = "happy_path_2",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .k = 5,
        },
        {
            .test_name = "happy_path_with_return_1",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .k = 5,
            .return_str = "return 2 r1 r2",
            .return_attributes = {{"r1", "r1"}, {"r2", "r2"}},
        },
        {
            .test_name = "happy_path_with_return_2",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .k = 5,
            .return_str = "return 4 r1 as r11 r2",
            .return_attributes = {{"r1", "r11"}, {"r2", "r2"}},
        },
        {
            .test_name = "happy_path_with_return_3",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .k = 5,
            .return_str = "return 0",
            .no_content = true,
        },
        {
            .test_name = "missing_index_field",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .k = 5,
            .expected_error_message = "Index field `vec1` does not exist",
        },
        {
            .test_name = "missing_index_field_w_score_as",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .k = 5,
            .score_as = "as_test_1",
            .expected_error_message = "Index field `vec1` does not exist",
        },
        {
            .test_name = "missing_return_1",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>[KNN 5 @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .k = 5,
            .expected_error_message = "Unexpected argument `r2`",
            .return_str = "return 3 r1 as r11 r2",
        },
        {
            .test_name = "missing_params",
            .success = false,
            .params_str = " PARAMS 4",
            .filter_str = "* =>[KNn 10 @vec $BLOB]",
            .expected_error_message = "Error parsing value for the parameter "
                                      "`PARAMS` - Missing argument",
        },
        {
            .test_name = "bad_blob_name",
            .success = false,
            .params_str = " PARAMS 4 EF 150",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB1 EF_RUNTIME $EF]",
            .expected_error_message = "Error parsing value for the parameter "
                                      "`PARAMS` - Unexpected argument `BLOB`",
        },
        {
            .test_name = "missing_blob",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*)=>[KNN 10 @vec ]",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[KNN 10 @vec ]`. "
                "Blob attribute "
                "argument is missing",
        },
        {
            .test_name = "extra_blob",
            .success = false,
            .params_str = " PARAMS 4 EXTRABLOB 123",
            .filter_str = " * => [KNN 10 @vec $BLOB]",
            .expected_error_message = "Parameter `EXTRABLOB` not used.",
        },
        {
            .test_name = "duplicate_blob",
            .success = false,
            .params_str = " PARAMS 6 EXTRABLOB 123 EXTRABLOB 123",
            .filter_str = " * => [KNN 10 @vec $BLOB]",
            .expected_error_message =
                "Error parsing value for the parameter `PARAMS` - Parameter "
                "EXTRABLOB is already defined.",
        },
        {
            .test_name = "odd_param_count",
            .success = false,
            .params_str = " PARAMS 1",
            .filter_str = " * => [KNN 10 @vec $BLOB]",
            .expected_error_message =
                "Error parsing value for the parameter `PARAMS` - Parameter "
                "count must be an even number.",
        },
        {
            .test_name = "missing_hash_field",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*)=>[KNN 10 $BLOB1 EF_RUNTIME $EF ]",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[KNN 10 $BLOB1 "
                "EF_RUNTIME $EF ]`. Unexpected "
                "argument `$BLOB1`. Expecting a vector field name, starting "
                "with '@'",
        },
        {
            .test_name = "invalid_prefilter_1",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "*)=>[KNN 10 @vec $BLOB]",
            .expected_error_message = "Invalid filter expression: `*)`. "
                                      "Unexpected character at position 2: `)`",
        },
        {
            .test_name = "invalid_prefilter_2",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*=>[KNN 10 @vec $BLOB]",
            .expected_error_message =
                "Invalid filter expression: `(*`. Missing `)`",
        },
        {
            .test_name = "invalid_prefilter_3",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*)=[KNN 10 @vec $BLOB]",
            .expected_error_message = "Invalid filter expression:",
        },
        {
            .test_name = "invalid_vector_parameters_1",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*)=>ss[KNN 10 @vec $BLOB]",
            .expected_error_message =
                "Error parsing vector similarity parameters: `ss[KNN 10 @vec "
                "$BLOB]`. Expecting '[' got 's'",
        },
        {
            .test_name = "invalid_vector_parameters_2",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*)=>[KNN 10 @vec $BLOB] aa",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[KNN 10 @vec "
                "$BLOB] aa`. Expecting ']' got 'a'",
        },
        {
            .test_name = "invalid_vector_parameters_3",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "(*)=>[]",
            .expected_error_message = "Error parsing vector similarity "
                                      "parameters: `[]`. Missing parameters",
        },
        {
            .test_name = "happy_path_3",
            .success = true,
            .params_str = " PARAMS 2",
            .filter_str = "* =>  [KNN 5 @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .k = 5,
        },

        {
            .test_name = "missing_knn_param",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>  [@vec1 $BLOB]",
            .attribute_alias = "vec1",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[@vec1 $BLOB]`. "
                "`@vec1`. Expecting `KNN`",
        },
        {
            .test_name = "missing_knn_argument",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>  [KNN aa @vec1 $BLOB]",
            .attribute_alias = "vec1",
            .expected_error_message =
                "Error parsing value for the parameter `PARAMS` - Error "
                "parsing vector similarity parameters: `aa` is not a valid "
                "numeric value",
        },

        {
            .test_name = "single_params",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>  [KNN]",
            .expected_error_message =
                "Error parsing vector similarity "
                "parameters: `[KNN]`. KNN argument is missing",
        },
        {
            .test_name = "two_params",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>  [KNN 10]",
            .expected_error_message = "Error parsing vector similarity "
                                      "parameters: `[KNN 10]`. Vector field "
                                      "argument is missing",
        },
        {
            .test_name = "three_params",
            .success = false,
            .params_str = " PARAMS 2",
            .filter_str = "* =>  [KNN 10 @vec1 ]",
            .attribute_alias = "vec1",
            .expected_error_message =
                "Error parsing vector similarity parameters: `[KNN 10 @vec1 "
                "]`. Blob attribute "
                "argument is missing",
        },
    }),
    [](const TestParamInfo<FTSearchParserTestCase> &info) {
      return info.param.test_name;
    });
}  // namespace

}  // namespace valkey_search
