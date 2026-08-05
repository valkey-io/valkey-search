/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_parser.h"

#include <map>
#include <vector>

#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_flat.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/type_conversions.h"

std::ostream &operator<<(std::ostream &os, ValkeyModuleString *s) {
  return os << "S=" << *(std::string *)s;
}

namespace valkey_search {
namespace aggregate {

struct FakeIndexInterface : public IndexInterface {
  std::map<std::string, indexes::IndexerType> fields_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(
      absl::string_view fld_name) const override {
    std::string field_name(fld_name);
    std::cout << "Fake make reference " << field_name << "\n";
    auto itr = fields_.find(field_name);
    if (itr == fields_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Unknown field ", fld_name, " in index."));
    } else {
      return itr->second;
    }
  }
  absl::StatusOr<std::string> GetIdentifier(
      absl::string_view alias) const override {
    std::cout << "Fake get identifier for " << alias << "\n";
    VMSDK_ASSIGN_OR_RETURN([[maybe_unused]] auto type, GetFieldType(alias));
    return std::string(alias);
  }
  absl::StatusOr<std::string> GetAlias(
      absl::string_view identifier) const override {
    std::cout << "Fake get alias for " << identifier << "\n";
    auto itr = fields_.find(std::string(identifier));
    if (itr == fields_.end()) {
      return absl::NotFoundError(
          absl::StrCat("Unknown identifier ", identifier, " in index."));
    } else {
      return itr->first;
    }
  }
};

struct AggregateTest : public vmsdk::ValkeyTest {
  void SetUp() override {
    fake_index.fields_ = {
        {"n1", indexes::IndexerType::kNumeric},
        {"n2", indexes::IndexerType::kNumeric},
    };
    vmsdk::ValkeyTest::SetUp();
  }
  void TearDown() override { vmsdk::ValkeyTest::TearDown(); }
  FakeIndexInterface fake_index;
};

static struct TimeoutTestValue {
  std::string text_;
  std::optional<size_t> value_;
} TimeoutCases[]{{"", query::kTimeoutMS},
                 {"TIMEOUT", std::nullopt},
                 {"TimeOut 1", 1},
                 {"Timeout 0", 0},
                 {"Timeout 60000", 60000},
                 {"Timeout 60001", 60001},
                 {"Timeout fred", std::nullopt}};

static struct DialectTestValue {
  std::string text_;
  std::optional<size_t> value_;
} DialectCases[]{{"", query::kDialect},
                 {"DIALecT", std::nullopt},
                 {"Dialect 0", 0},
                 {"Dialect 1", 1},
                 {"Dialect 2", 2},
                 {"Dialect 3", 3},
                 {"Dialect 4", 4},
                 {"Dialect 5", 5},
                 {"Dialect fred", std::nullopt}};

static struct LoadsTestValue {
  std::string text_;
  std::optional<std::vector<std::string>> value_;
} LoadCases[]{
    {"", std::vector<std::string>{}},
    {"load *", std::vector<std::string>{"*"}},
    {"LOAD 55", std::nullopt},
    {"LOAD 0", std::vector<std::string>{}},
    {"LOAD 1 x", std::vector<std::string>{"x"}},
    {"LOAD 2 x", std::nullopt},
    {"LOAD 2 x y", std::vector<std::string>{"x", "y"}},
};

static struct InorderTestValue {
  std::string text_;
  bool value_;
} InorderCases[]{{"", false}, {"INORDER", true}};

static struct SlopTestValue {
  std::string text_;
  std::optional<size_t> value_;
} SlopCases[]{{"", std::nullopt}, {"SLOP", std::nullopt},
              {"SLOP 0", 0},      {"SLOP 1", 1},
              {"SLOP 10", 10},    {"SLOP fred", std::nullopt}};

static struct VerbatimTestValue {
  std::string text_;
  bool value_;
} VerbatimCases[]{{"", false}, {"VERBATIM", true}};

static void DoPrefaceTestCase(FakeIndexInterface *fake_index, std::string test,
                              TimeoutTestValue timeout_test,
                              DialectTestValue dialect_test,
                              LoadsTestValue loads_test,
                              InorderTestValue inorder_test,
                              SlopTestValue slop_test,
                              VerbatimTestValue verbatim_test) {
  std::cerr << "Running test: '" << test << "'\n";
  auto argv = vmsdk::ToValkeyStringVector(test);
  vmsdk::ArgsIterator itr(argv.data(), argv.size());

  AggregateParameters params(0);
  params.timeout_ms = query::kTimeoutMS;
  params.parse_vars_.index_interface_ = fake_index;

  auto parser = CreateAggregateParser();

  auto result = parser.Parse(params, itr);
  if (timeout_test.value_ && dialect_test.value_ && loads_test.value_ &&
      slop_test.value_) {
    EXPECT_TRUE(result.ok()) << " Status: " << result;
    EXPECT_EQ(params.timeout_ms, *timeout_test.value_);
    EXPECT_EQ(params.dialect, *dialect_test.value_);
    EXPECT_TRUE(loads_test.value_);
    if (loads_test.value_ == std::vector<std::string>{"*"}) {
      EXPECT_TRUE(params.loadall_);
      EXPECT_TRUE(params.loads_.empty());
    } else {
      EXPECT_FALSE(params.loadall_);
      EXPECT_EQ(params.loads_.size(), loads_test.value_->size());
      for (auto i = 0; i < loads_test.value_->size(); ++i) {
        EXPECT_EQ(loads_test.value_->at(i), params.loads_[i]);
      }
    }
    EXPECT_EQ(params.inorder, inorder_test.value_);
    EXPECT_EQ(params.slop, slop_test.value_);
    EXPECT_EQ(params.verbatim, verbatim_test.value_);
  } else {
    if (!timeout_test.value_) {
      EXPECT_EQ(params.timeout_ms, query::kTimeoutMS);
    }
    if (!dialect_test.value_) {
      EXPECT_EQ(params.dialect, query::kDialect);
    }
  }
  // Need to manually free the string vector
  for (auto arg : argv) {
    ValkeyModule_FreeString(nullptr, arg);
  }
}

TEST_F(AggregateTest, PrefaceParserTest) {
  for (const auto &timeout_test : TimeoutCases) {
    for (const auto &dialect_test : DialectCases) {
      for (const auto &loads_test : LoadCases) {
        for (const auto &inorder_test : InorderCases) {
          for (const auto &slop_test : SlopCases) {
            for (const auto &verbatim_test : VerbatimCases) {
              std::string test = timeout_test.text_ + " " + dialect_test.text_ +
                                 " " + loads_test.text_ + " " +
                                 inorder_test.text_ + " " + slop_test.text_ +
                                 " " + verbatim_test.text_;
              DoPrefaceTestCase(&fake_index, test, timeout_test, dialect_test,
                                loads_test, inorder_test, slop_test,
                                verbatim_test);
            }
          }
        }
      }
    }
  }
}

struct TestStage {
  const char *stage_in_;
  const char *stage_out_;
};
static std::vector<TestStage> TestStages{
    {"bogus", nullptr},
    {"LiMiT", nullptr},
    {"LIMIT 10", nullptr},
    {"LIMIT fred", nullptr},
    {"LIMIT 0 10", "LIMIT: 0 10"},
    {"LIMIT 0 10 fred", nullptr},
    {"FiLTER", nullptr},
    {"FILTER fred", nullptr},
    {"FILTER @fred", nullptr},
    {"FILTER @n1 + @n2", nullptr},
    {"FILTER @n1", "FILTER: @n1"},
    {"SORtBY 1 @n1", "SORTBY: ASC:@n1 MAX:10"},
    {"SORTBY 2 @n1 ASC", "SORTBY: ASC:@n1 MAX:10"},
    {"SORTBY 2 @n1 DESC", "SORTBY: DESC:@n1 MAX:10"},
    {"SORTBY", nullptr},
    {"SORTBY 1", nullptr},
    {"SOrTBY 2 @n1", nullptr},
    {"SORTBY 1 @n1 MAX", nullptr},
    {"SORTBY 1 @n1 max 5", "SORTBY: ASC:@n1 MAX:5"},
    {"SOrTBY 2 @n1 max", nullptr},
    {"GrOUPBY 0", nullptr},
    {"GROUPBY 1", nullptr},
    {"GROUPBY 1 fred", nullptr},
    {"GROUPBY 1 @n1", "GROUPBY @n1"},
    {"GROUPBY 1 @n1 REDUCE", nullptr},
    {"GROUPBY 1 @n1 REDUCE COUNT", nullptr},
    {"GROUPBY 1 @n1 REDUCE COUNT 0", "GROUPBY @n1 COUNT() => COUNT()"},
    {"GROUPBY 1 @n1 REDUCE COUNT 0 AS Y", "GROUPBY @n1 COUNT() => Y"},
    {"GROUPBY 1 @n1 REDUCE MIN 1 @n2 as Z", "GROUPBY @n1 MIN(@n2) => Z"},
    {"GROUPBY 1 @n1 REDUCE TOLIST 1 @n2",
     "GROUPBY @n1 TOLIST(@n2) => TOLIST(@n2)"},
    {"GROUPBY 1 @n1 REDUCE TOLIST 1 @n2 AS items",
     "GROUPBY @n1 TOLIST(@n2) => items"},
    {"GROUPBY 1 @n1 REDUCE TOLIST 0", nullptr},
    {"GROUPBY 1 @n1 REDUCE TOLIST 2 @n1 @n2", nullptr},
    {"apply", nullptr},
    {"apply x", nullptr},
    {"apply @n1", nullptr},
    {"apply @n1 xx", nullptr},
    {"APPLY @n1 as freddy", "APPLY: freddy := @n1"},
};

static void DoStageTest(FakeIndexInterface *fake_index,
                        std::vector<size_t> indexes) {
  std::string text;
  bool any_bad = false;
  for (auto ix : indexes) {
    text += " ";
    text += TestStages[ix].stage_in_;
    any_bad |= TestStages[ix].stage_out_ == nullptr;
  }
  std::cout << "Doing case " << text << "\n";
  auto argv = vmsdk::ToValkeyStringVector(text);
  vmsdk::ArgsIterator itr(argv.data(), argv.size());

  AggregateParameters params(0);
  params.timeout_ms = 0;
  params.parse_vars_.index_interface_ = fake_index;

  auto parser = CreateAggregateParser();
  auto result = parser.Parse(params, itr);
  if (any_bad) {
    std::cout << "Failed status: " << result << "\n";
    EXPECT_FALSE(result.ok());
  } else {
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(params.stages_.size(), indexes.size());
    for (auto i = 0; i < std::min(params.stages_.size(), indexes.size()); ++i) {
      std::ostringstream os;
      params.stages_[i]->Dump(os);
      EXPECT_EQ(os.str(), TestStages[indexes[i]].stage_out_);
    }
  }
  // Need to manually free the string vector
  for (auto arg : argv) {
    ValkeyModule_FreeString(nullptr, arg);
  }
}

TEST_F(AggregateTest, StageParserTest) {
  for (size_t i = 0; i < TestStages.size(); ++i) {
    DoStageTest(&fake_index, std::vector<size_t>{i});
    for (size_t j = 0; j < TestStages.size(); ++j) {
      DoStageTest(&fake_index, std::vector<size_t>{i, j});
      for (size_t k = 0; k < TestStages.size(); ++k) {
        DoStageTest(&fake_index, std::vector<size_t>{i, j, k});
      }
    }
  }
}

TEST_F(AggregateTest, EmptyApplyAndFilterExpressionsAreRejected) {
  for (absl::string_view test_case :
       {"FILTER ''", "FILTER ' '", "APPLY '' AS r", "APPLY ' ' AS r"}) {
    auto argv = vmsdk::ToValkeyStringVector(test_case);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    AggregateParameters params(0);
    params.timeout_ms = 0;
    params.parse_vars_.index_interface_ = &fake_index;

    auto parser = CreateAggregateParser();
    auto result = parser.Parse(params, itr);

    EXPECT_FALSE(result.ok()) << "Parser unexpectedly accepted: " << test_case;

    for (auto arg : argv) {
      ValkeyModule_FreeString(nullptr, arg);
    }
  }
}

TEST_F(AggregateTest, GetSerializationRange_NoStages) {
  AggregateParameters params(0);
  auto range = params.GetSerializationRange();
  EXPECT_EQ(range, query::SerializationRange::All());
}

TEST_F(AggregateTest, GetSerializationRange_WithLimitStage) {
  AggregateParameters params(0);
  auto limit = std::make_unique<Limit>();
  limit->offset_ = 10;
  limit->limit_ = 20;
  params.stages_.push_back(std::move(limit));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range.start_index, 10u);
  EXPECT_EQ(range.end_index, 30u);
}

TEST_F(AggregateTest, GetSerializationRange_WithApplyStage) {
  AggregateParameters params(0);
  auto apply = std::make_unique<Apply>();
  params.stages_.push_back(std::move(apply));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range, query::SerializationRange::All());
}

TEST_F(AggregateTest, GetSerializationRange_WithFilterStage) {
  AggregateParameters params(0);
  auto filter = std::make_unique<Filter>();
  params.stages_.push_back(std::move(filter));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range, query::SerializationRange::All());
}

TEST_F(AggregateTest, GetSerializationRange_WithSortByStage) {
  AggregateParameters params(0);
  auto sortby = std::make_unique<SortBy>();
  params.stages_.push_back(std::move(sortby));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range, query::SerializationRange::All());
}

TEST_F(AggregateTest, GetSerializationRange_WithGroupByStage) {
  AggregateParameters params(0);
  auto groupby = std::make_unique<GroupBy>();
  params.stages_.push_back(std::move(groupby));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range, query::SerializationRange::All());
}

TEST_F(AggregateTest, GetSerializationRange_LimitBeforeOtherStages) {
  AggregateParameters params(0);
  auto limit = std::make_unique<Limit>();
  limit->offset_ = 5;
  limit->limit_ = 15;
  params.stages_.push_back(std::move(limit));

  auto filter = std::make_unique<Filter>();
  params.stages_.push_back(std::move(filter));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range.start_index, 5u);
  EXPECT_EQ(range.end_index, 20u);
}

TEST_F(AggregateTest, GetSerializationRange_OtherStagesBeforeLimit) {
  AggregateParameters params(0);
  auto filter = std::make_unique<Filter>();
  params.stages_.push_back(std::move(filter));

  auto limit = std::make_unique<Limit>();
  limit->offset_ = 0;
  limit->limit_ = 100;
  params.stages_.push_back(std::move(limit));

  auto range = params.GetSerializationRange();
  EXPECT_EQ(range, query::SerializationRange::All());
}

TEST_F(AggregateTest, ExpressionDepthAtLimit) {
  auto limit = options::GetQueryStringDepth().GetValue();
  std::string deep_expr(limit - 1, '(');
  deep_expr += "1";
  deep_expr += std::string(limit - 1, ')');

  AggregateParameters params(0);
  EXPECT_TRUE(expr::Expression::Compile(params, deep_expr).ok());
}

TEST_F(AggregateTest, ExpressionDepthExceedsLimit) {
  auto limit = options::GetQueryStringDepth().GetValue();
  std::string deep_expr(limit, '(');
  deep_expr += "1";
  deep_expr += std::string(limit, ')');

  AggregateParameters params(0);
  auto result = expr::Expression::Compile(params, deep_expr);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(absl::IsInvalidArgument(result.status()));
}

// ---------------------------------------------------------------------------
// ParseCommand registration tests for VR score fields (task 3.2)
// ---------------------------------------------------------------------------

// Helper: build a 3-float (12-byte) blob string for dimension-3 vector indexes.
static std::string MakeBlob3() {
  std::vector<float> v = {0.1f, 0.2f, 0.3f};
  return std::string(reinterpret_cast<const char*>(v.data()),
                     v.size() * sizeof(float));
}

class ParseCommandRegistrationTest : public ValkeySearchTest {
 protected:
  // Creates a schema with one 3-dim flat vector field named `vec_alias`.
  std::shared_ptr<MockIndexSchema> MakeSchemaWithVec(
      absl::string_view vec_alias) {
    auto schema = CreateIndexSchema("test_schema", &fake_ctx_).value();
    EXPECT_CALL(*schema, GetIdentifier(::testing::_))
        .Times(::testing::AnyNumber())
        .WillRepeatedly([&schema](absl::string_view field) {
          return schema->IndexSchema::GetIdentifier(field);
        });
    data_model::VectorIndex proto;
    proto.set_dimension_count(3);
    proto.set_initial_cap(100);
    proto.set_vector_data_type(
        data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32);
    auto flat = std::make_unique<data_model::FlatAlgorithm>();
    flat->set_block_size(100);
    proto.set_allocated_flat_algorithm(flat.release());
    auto idx =
        indexes::VectorFlat<float>::Create(
            proto, std::string(vec_alias) + "_id",
            data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH)
            .value();
    VMSDK_EXPECT_OK(schema->AddIndex(vec_alias, vec_alias, idx));
    return schema;
  }

  // Runs ParseCommand on `params` and returns whether it succeeded.
  bool RunParseCommand(AggregateParameters& params) {
    vmsdk::ArgsIterator itr(nullptr, 0);
    auto status = params.ParseCommand(itr);
    if (!status.ok()) {
      ADD_FAILURE() << "ParseCommand failed: " << status;
      return false;
    }
    return true;
  }
};

// Non-vector query with one VR predicate: score_as set to VR alias, only one
// extra attribute registered at index 1.
TEST_F(ParseCommandRegistrationTest, NonVectorOneVrPredicate) {
  auto schema = MakeSchemaWithVec("vec");
  AggregateParameters params(0);
  params.index_schema = schema;
  params.parse_vars.query_string =
      "@vec:[VECTOR_RANGE 0.5 $blob]=>{$yield_distance_as: my_dist}";
  params.parse_vars.params["blob"] = {1, MakeBlob3()};

  ASSERT_TRUE(RunParseCommand(params));

  EXPECT_EQ(vmsdk::ToStringView(params.score_as.get()), "my_dist");

  auto it = params.record_indexes_by_alias_.find("my_dist");
  ASSERT_NE(it, params.record_indexes_by_alias_.end());
  EXPECT_EQ(it->second, 1u);

  EXPECT_EQ(params.vr_score_field_names_.size(), 1u);
  EXPECT_EQ(params.vr_score_field_names_[0], "my_dist");
}

// Non-vector query with two VR predicates: score_as set to slot-0 alias, both
// aliases registered as record attributes.
TEST_F(ParseCommandRegistrationTest, NonVectorTwoVrPredicates) {
  auto schema = MakeSchemaWithVec("vec");
  AggregateParameters params(0);
  params.index_schema = schema;
  params.parse_vars.query_string =
      "(@vec:[VECTOR_RANGE 0.5 $b1]=>{$yield_distance_as: d1} "
      "@vec:[VECTOR_RANGE 1.0 $b2]=>{$yield_distance_as: d2})";
  params.parse_vars.params["b1"] = {1, MakeBlob3()};
  params.parse_vars.params["b2"] = {1, MakeBlob3()};

  ASSERT_TRUE(RunParseCommand(params));

  EXPECT_EQ(vmsdk::ToStringView(params.score_as.get()), "d1");

  EXPECT_NE(params.record_indexes_by_alias_.find("d1"),
            params.record_indexes_by_alias_.end());
  EXPECT_NE(params.record_indexes_by_alias_.find("d2"),
            params.record_indexes_by_alias_.end());

  ASSERT_EQ(params.vr_score_field_names_.size(), 2u);
  EXPECT_EQ(params.vr_score_field_names_[0], "d1");
  EXPECT_EQ(params.vr_score_field_names_[1], "d2");
}

// KNN query with one VR predicate in the filter: score_as unchanged (KNN
// alias), VR alias registered as a separate record attribute.
TEST_F(ParseCommandRegistrationTest, KnnWithOneVrPredicate) {
  auto schema = MakeSchemaWithVec("vec");
  AggregateParameters params(0);
  params.index_schema = schema;
  params.parse_vars.query_string =
      "@vec:[VECTOR_RANGE 0.5 $vrblob]=>{$yield_distance_as: vr_dist}"
      "=>[KNN 5 @vec $kblob AS knn_dist]";
  params.parse_vars.params["vrblob"] = {1, MakeBlob3()};
  params.parse_vars.params["kblob"] = {1, MakeBlob3()};
  params.parse_vars.score_as_string = "knn_dist";

  ASSERT_TRUE(RunParseCommand(params));

  // score_as must still be the KNN alias.
  EXPECT_EQ(vmsdk::ToStringView(params.score_as.get()), "knn_dist");

  // KNN alias at index 1.
  auto knn_it = params.record_indexes_by_alias_.find("knn_dist");
  ASSERT_NE(knn_it, params.record_indexes_by_alias_.end());
  EXPECT_EQ(knn_it->second, 1u);

  // VR alias at a different index.
  auto vr_it = params.record_indexes_by_alias_.find("vr_dist");
  ASSERT_NE(vr_it, params.record_indexes_by_alias_.end());
  EXPECT_NE(vr_it->second, 1u);

  ASSERT_EQ(params.vr_score_field_names_.size(), 1u);
  EXPECT_EQ(params.vr_score_field_names_[0], "vr_dist");
}

// KNN query with no VR predicate: vr_score_field_names_ remains empty.
TEST_F(ParseCommandRegistrationTest, KnnWithNoVrPredicate) {
  auto schema = MakeSchemaWithVec("vec");
  AggregateParameters params(0);
  params.index_schema = schema;
  params.parse_vars.query_string = "*=>[KNN 5 @vec $kblob AS knn_dist]";
  params.parse_vars.params["kblob"] = {1, MakeBlob3()};
  params.parse_vars.score_as_string = "knn_dist";

  ASSERT_TRUE(RunParseCommand(params));

  EXPECT_EQ(vmsdk::ToStringView(params.score_as.get()), "knn_dist");
  EXPECT_TRUE(params.vr_score_field_names_.empty());
}

}  // namespace aggregate
}  // namespace valkey_search
