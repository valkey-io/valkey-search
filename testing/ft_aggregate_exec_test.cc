/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <random>

#include "gtest/gtest.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/testing_infra/utils.h"

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

static std::unique_ptr<Record> RecordNOfM(size_t n, size_t m) {
  auto rec = std::make_unique<Record>(2);
  rec->fields_[0] = expr::Value(double(n));
  rec->fields_[1] = expr::Value(double(m));
  return rec;
}

static RecordSet MakeData(size_t m) {
  RecordSet result(nullptr);
  for (auto i = 0; i < m; ++i) {
    result.emplace_back(RecordNOfM(i, m));
  }
  return result;
}

struct AggregateExecTest : public vmsdk::ValkeyTest {
  void SetUp() override {
    fakeIndex.fields_ = {
        {"n1", indexes::IndexerType::kNumeric},
        {"n2", indexes::IndexerType::kNumeric},
        {"n3", indexes::IndexerType::kNumeric},
    };
    vmsdk::ValkeyTest::SetUp();
  }
  void TearDown() override { vmsdk::ValkeyTest::TearDown(); }
  FakeIndexInterface fakeIndex;

  std::unique_ptr<AggregateParameters> MakeStages(absl::string_view test) {
    auto argv = vmsdk::ToValkeyStringVector(test);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    EXPECT_EQ(
        params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric),
        0);
    EXPECT_EQ(
        params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric),
        1);
    // params->attr_record_indexes_["n1"] = 0;
    // params->attr_record_indexes_["n2"] = 1;

    auto parser = CreateAggregateParser();

    auto result = parser.Parse(*params, itr);
    EXPECT_TRUE(result.ok()) << " Status is: " << result << "\n";

    // Free the allocated ValkeyModuleStrings to avoid memory leaks
    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
    return params;
  }

  // Helper for FirstValue tests that need n3 attribute
  std::unique_ptr<AggregateParameters> MakeStagesWithN3(
      absl::string_view test) {
    auto argv = vmsdk::ToValkeyStringVector(test);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    EXPECT_EQ(
        params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric),
        0);
    EXPECT_EQ(
        params->AddRecordAttribute("n2", "n2", indexes::IndexerType::kNumeric),
        1);
    EXPECT_EQ(
        params->AddRecordAttribute("n3", "n3", indexes::IndexerType::kNumeric),
        2);

    auto parser = CreateAggregateParser();

    auto result = parser.Parse(*params, itr);
    EXPECT_TRUE(result.ok()) << " Status is: " << result << "\n";

    // Free the allocated ValkeyModuleStrings to avoid memory leaks
    for (auto *str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
    return params;
  }

};

TEST_F(AggregateExecTest, LimitTest) {
  std::cerr << "LimitTest\n";
  auto param = MakeStages("LIMIT 1 2");
  auto records = MakeData(4);
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 2);
  std::cerr << "Results:\n";
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_EQ(*records[0], *RecordNOfM(1, 4));
  EXPECT_EQ(*records[1], *RecordNOfM(2, 4));
}

TEST_F(AggregateExecTest, FilterTest) {
  std::cerr << "FilterTest\n";
  auto param = MakeStages("FILTER @n1==1");
  auto records = MakeData(4);
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  std::cerr << "Results:\n";
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_EQ(*records[0], *RecordNOfM(1, 4));
}

TEST_F(AggregateExecTest, ApplyTest) {
  std::cerr << "ApplyTest\n";
  auto param = MakeStages("APPLY @n1+1 as fred");
  auto records = MakeData(2);
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 2);
  std::cerr << "Results:\n";
  for (auto& r : records) {
    std::cerr << *r << "\n";
  }
  auto r0 = RecordNOfM(0, 2);
  r0->fields_.resize(3);
  r0->fields_[2] = expr::Value(double(1.0));
  auto r1 = RecordNOfM(1, 2);
  r1->fields_.resize(3);
  r1->fields_[2] = expr::Value(double(2.0));
  EXPECT_EQ(*records[0], *r0);
  EXPECT_EQ(*records[1], *r1);
}

TEST_F(AggregateExecTest, SortTest) {
  struct Testcase {
    std::string text_;
    std::vector<size_t> order_;
    bool ordered;
    std::vector<size_t> max_order_;
  };
  Testcase testcases[]{
      {"Sortby 2 @n1 desc", {1, 0}, true, {9, 8}},
      {"sortby 2 @n1 asc", {0, 1}, true, {0, 1}},
      {"sortby 2 @n2 asc", {0, 1}, false, {0, 1}},
      {"sortby 2 @n2 desc", {0, 1}, false, {0, 1}},
      {"sortby 4 @n1 desc @n2 asc", {1, 0}, true, {9, 8}},
      {"sortby 4 @n1 asc  @n2 asc", {0, 1}, true, {0, 1}},
      {"sortby 4 @n1 desc @n2 desc", {1, 0}, true, {9, 8}},
      {"sortby 4 @n1 asc  @n2 desc", {0, 1}, true, {0, 1}},
      {"sortby 4 @n2 asc  @n1 asc", {0, 1}, false, {0, 1}},
      {"sortby 4 @n2 asc  @n1 desc", {1, 0}, false, {9, 8}},
      {"sortby 4 @n2 desc @n1 asc", {0, 1}, false, {0, 1}},
      {"sortby 4 @n2 desc @n1 desc", {1, 0}, false, {9, 8}},

  };
  for (auto do_max : {false, true}) {
    for (auto& tc : testcases) {
      std::string text = tc.text_;
      size_t input_count = tc.order_.size();
      auto order = tc.order_;
      if (do_max) {
        text += " MAX ";
        text += std::to_string(tc.max_order_.size());
        input_count = 10;
        order = tc.max_order_;
      }
      std::cerr << "SortTest: " << text << "\n";
      auto param = MakeStages(text);
      auto records = MakeData(input_count);
      for (auto& r : records) {
        std::cerr << *r << "\n";
      }
      EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
      EXPECT_EQ(records.size(), order.size());
      std::cerr << "Results:\n";
      for (auto& r : records) {
        std::cerr << *r << "\n";
      }
      if (!do_max || tc.ordered) {
        for (auto i = 0; i < order.size(); ++i) {
          EXPECT_EQ(*records[i], *RecordNOfM(order[i], input_count));
        }
      }
    }
  }
}

TEST_F(AggregateExecTest, GroupTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    size_t num_groups;
  };
  Testcase testcases[]{
      {"groupby 1 @n1", 2, 2},
      {"groupby 2 @n1 @n2", 2, 2},
      {"groupby 1 @n2", 2, 1},
  };
  for (auto& tc : testcases) {
    std::cerr << "GroupTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), tc.num_groups);
    std::cerr << "Results:\n";
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
  }
}

TEST_F(AggregateExecTest, ReducerTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    std::vector<double> values_;
  };
  Testcase testcases[]{
      {"groupby 1 @n2 reduce count 0", 4, {4}},
      {"groupby 1 @n2 reduce min 1 @n1", 4, {0}},
      {"groupby 1 @n2 reduce min 1 @n1 reduce count 0", 4, {0, 4}},
      {"groupby 1 @n2 reduce max 1 @n1", 4, {3}},
      {"groupby 1 @n2 reduce sum 1 @n1", 4, {6}},
      {"groupby 1 @n2 reduce stddev 1 @n1", 4, {1.2909944487358056}},
      {"groupby 1 @n2 reduce count_distinct 1 @n1", 4, {4}},
      {"groupby 1 @n2 reduce avg 1 @n1", 4, {1.5}}};
  for (auto& tc : testcases) {
    std::cerr << "GroupTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto& r : records) {
      std::cerr << *r << "\n";
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    std::cerr << "Result: " << *record << "\n";
    for (auto i = 0; i < tc.values_.size(); ++i) {
      EXPECT_TRUE(record->fields_.at(i + 2).IsDouble());
      EXPECT_NEAR(*(record->fields_.at(i + 2).AsDouble()), tc.values_[i], .001);
    }
  }
}
/*
TEST_F(AggregateExecTest, testHash) {
  GroupKey key1({expr::Value(1.0), expr::Value(2.0)});
  GroupKey key2({expr::Value(true), expr::Value(), expr::Value(2.0)});
  std::cerr << (key1 == key2) << "\n";
  std::cerr << "Key1: " << key1 << " Key2: " << key2 << "\n";
  EXPECT_TRUE(absl::VerifyTypeImplementsAbslHashCorrectly({
      GroupKey{{expr::Value(0.0)}},
      GroupKey{{expr::Value(1.0), expr::Value(2.0)}},
      GroupKey{{expr::Value("a"), expr::Value("b")}},
      GroupKey{{expr::Value("a"), expr::Value(), expr::Value(2.0)}},
      GroupKey{{expr::Value(true), expr::Value()}},
      GroupKey{{expr::Value(false), expr::Value("1.2")}},
  }));
}
*/

TEST_F(AggregateExecTest, FirstValueReducerTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    std::vector<double> values_;
    bool should_succeed;
  };
  Testcase testcases[]{
      // Simple mode: returns first record's value.
      {"groupby 1 @n2 reduce first_value 1 @n1", 4, {0}, true},
      // Sorted ASC: returns value from record with minimum comparison value.
      {"groupby 1 @n2 reduce first_value 3 @n1 BY @n1", 4, {0}, true},
      // Sorted ASC explicit: same result.
      {"groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC", 4, {0}, true},
      // Sorted DESC: returns value from record with maximum comparison value.
      {"groupby 1 @n2 reduce first_value 4 @n1 BY @n1 DESC", 4, {3}, true},
      // Case-insensitive BY keyword.
      {"groupby 1 @n2 reduce first_value 3 @n1 by @n1", 4, {0}, true},
      // Case-insensitive direction.
      {"groupby 1 @n2 reduce first_value 4 @n1 BY @n1 desc", 4, {3}, true},
      // Invalid: nargs=2 (BY with no sort field) — parse error.
      {"groupby 1 @n2 reduce first_value 2 @n1 BY", 4, {}, false},
      // Invalid: unrecognised keyword instead of BY — parse error.
      {"groupby 1 @n2 reduce first_value 3 @n1 WITH @n1", 4, {}, false},
      // Invalid: unrecognised direction — parse error.
      {"groupby 1 @n2 reduce first_value 4 @n1 BY @n1 UP", 4, {}, false},
  };

  for (auto &tc : testcases) {
    std::cerr << "FirstValueReducerTest: " << tc.text_ << "\n";
    if (!tc.should_succeed) {
      // Parse errors are now surfaced at parse time, not execution time.
      auto argv = vmsdk::ToValkeyStringVector(tc.text_);
      vmsdk::ArgsIterator itr(argv.data(), argv.size());
      auto params = std::make_unique<AggregateParameters>(0);
      params->parse_vars_.index_interface_ = &fakeIndex;
      params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric);
      params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric);
      auto parser = CreateAggregateParser();
      auto result = parser.Parse(*params, itr);
      EXPECT_FALSE(result.ok()) << tc.text_ << ": expected parse failure";
      for (auto *str : argv) ValkeyModule_FreeString(nullptr, str);
      continue;
    }
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    auto status = param->stages_[0]->Execute(records);
    EXPECT_TRUE(status.ok()) << tc.text_ << ": " << status;
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    std::cerr << "Result: " << *record << "\n";
    for (size_t i = 0; i < tc.values_.size(); ++i) {
      EXPECT_TRUE(record->fields_.at(i + 2).IsDouble());
      EXPECT_NEAR(*(record->fields_.at(i + 2).AsDouble()), tc.values_[i],
                  .001);
    }
  }
}

TEST_F(AggregateExecTest, FirstValueReducerEdgeCasesTest) {
  // Empty group produces no results.
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: empty group\n";
    auto param = MakeStages("groupby 1 @n2 reduce first_value 1 @n1");
    RecordSet empty_records(nullptr);
    EXPECT_TRUE(param->stages_[0]->Execute(empty_records).ok());
    EXPECT_EQ(empty_records.size(), 0);
  }

  // All nil comparison values: result is nil.
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: all nil comparison values\n";
    auto param = MakeStages(
        "groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC");
    RecordSet records(nullptr);
    for (size_t i = 0; i < 4; ++i) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value();
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 1);
    auto result = records.pop_front();
    std::cerr << "Result: " << *result << "\n";
    EXPECT_TRUE(result->fields_.at(2).IsNil());
  }

  // Nil return value is preserved when that record has the optimal comparison.
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: nil return value preserved\n";
    auto param = MakeStages(
        "groupby 1 @n2 reduce first_value 4 @n1 BY @n2 ASC");
    RecordSet records(nullptr);
    // rec with n2=1 (best ASC) has nil n1 — that nil should be returned.
    auto r1 = std::make_unique<Record>(2);
    r1->fields_[0] = expr::Value();
    r1->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(r1));
    auto r2 = std::make_unique<Record>(2);
    r2->fields_[0] = expr::Value(100.0);
    r2->fields_[1] = expr::Value(5.0);
    records.emplace_back(std::move(r2));
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 2);
    std::cerr << "Results:\n";
    for (auto &rec : records) {
      std::cerr << *rec << "\n";
    }
    // Find the group with n2=1 and verify its result is nil.
    bool found = false;
    for (auto &rec : records) {
      if (rec->fields_.at(1).IsDouble() &&
          *rec->fields_.at(1).AsDouble() == 1.0) {
        EXPECT_TRUE(rec->fields_.at(2).IsNil());
        found = true;
      }
    }
    EXPECT_TRUE(found);
  }

  // Tie-breaking: first encountered record wins (ASC).
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: tie-breaking ASC\n";
    auto param = MakeStages(
        "groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC");
    RecordSet records(nullptr);
    for (double v : {10.0, 10.0, 50.0}) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(v);
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 1);
    auto result = records.pop_front();
    std::cerr << "Result: " << *result << "\n";
    EXPECT_NEAR(*result->fields_.at(2).AsDouble(), 10.0, 0.001);
  }

  // Tie-breaking: first encountered record wins (DESC).
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: tie-breaking DESC\n";
    auto param = MakeStages(
        "groupby 1 @n2 reduce first_value 4 @n1 BY @n1 DESC");
    RecordSet records(nullptr);
    for (double v : {50.0, 100.0, 100.0}) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(v);
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 1);
    auto result = records.pop_front();
    std::cerr << "Result: " << *result << "\n";
    EXPECT_NEAR(*result->fields_.at(2).AsDouble(), 100.0, 0.001);
  }

  // Mixed nil/non-nil comparison values: nils are skipped.
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: mixed nil/non-nil\n";
    auto param = MakeStages(
        "groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC");
    RecordSet records(nullptr);
    // nil, 50, nil, 100 — minimum non-nil is 50.
    for (auto v : {std::optional<double>{}, std::optional<double>{50.0},
                   std::optional<double>{}, std::optional<double>{100.0}}) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = v ? expr::Value(*v) : expr::Value();
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 1);
    auto result = records.pop_front();
    std::cerr << "Result: " << *result << "\n";
    EXPECT_NEAR(*result->fields_.at(2).AsDouble(), 50.0, 0.001);
  }
}


}  // namespace aggregate
}  // namespace valkey_search
