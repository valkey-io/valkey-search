/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_parser.h"

#include <map>

#include "gtest/gtest.h"
#include "vmsdk/src/testing_infra/utils.h"

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

}  // namespace aggregate
}  // namespace valkey_search
