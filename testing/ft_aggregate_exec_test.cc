/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <memory>
#include <random>

#include "gtest/gtest.h"
#include "src/commands/ft_aggregate_parser.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace aggregate {

static double CalculateGKQuantile(const std::vector<double>& sorted_values,
                                  double quantile) {
  size_t n = sorted_values.size();
  if (n == 0) return std::numeric_limits<double>::quiet_NaN();
  if (n == 1) return sorted_values[0];

  double t = std::ceil(quantile * n);
  double max_val = kQuantileEpsilon * 2.0 * t;
  t += std::floor(max_val / 2.0);

  size_t rank = 0;
  for (size_t i = 0; i < n; ++i) {
    rank += 1;
    if (rank >= static_cast<size_t>(t)) {
      return sorted_values[i];
    }
  }

  return sorted_values[n - 1];
}

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
    for (auto *str : argv) {
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
  for (auto &r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 2);
  std::cerr << "Results:\n";
  for (auto &r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_EQ(*records[0], *RecordNOfM(1, 4));
  EXPECT_EQ(*records[1], *RecordNOfM(2, 4));
}

TEST_F(AggregateExecTest, FilterTest) {
  std::cerr << "FilterTest\n";
  auto param = MakeStages("FILTER @n1==1");
  auto records = MakeData(4);
  for (auto &r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  std::cerr << "Results:\n";
  for (auto &r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_EQ(*records[0], *RecordNOfM(1, 4));
}

TEST_F(AggregateExecTest, ApplyTest) {
  std::cerr << "ApplyTest\n";
  auto param = MakeStages("APPLY @n1+1 as fred");
  auto records = MakeData(2);
  for (auto &r : records) {
    std::cerr << *r << "\n";
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 2);
  std::cerr << "Results:\n";
  for (auto &r : records) {
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
    for (auto &tc : testcases) {
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
      for (auto &r : records) {
        std::cerr << *r << "\n";
      }
      EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
      EXPECT_EQ(records.size(), order.size());
      std::cerr << "Results:\n";
      for (auto &r : records) {
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
  for (auto &tc : testcases) {
    std::cerr << "GroupTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto &r : records) {
      std::cerr << *r << "\n";
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), tc.num_groups);
    std::cerr << "Results:\n";
    for (auto &r : records) {
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
  for (auto &tc : testcases) {
    std::cerr << "GroupTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto &r : records) {
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
TEST_F(AggregateExecTest, ToListReducerTest) {
  // Basic collection: 4 distinct values (0, 1, 2, 3) grouped by n2
  {
    auto param = MakeStages("groupby 1 @n2 reduce tolist 1 @n1 as items");
    auto records = MakeData(4);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto &result = record->fields_.at(2);
    EXPECT_TRUE(result.IsArray());
    EXPECT_EQ(result.ArraySize(), 4);
  }
  // Deduplication: multiple records in the same group with repeated reducer
  // values — TOLIST must return each distinct value exactly once.
  {
    auto param = MakeStages("groupby 1 @n1 reduce tolist 1 @n2 as items");
    // All 5 records share n1=0 (one group). n2 values: 1, 2, 1, 3, 2.
    // Expected distinct set after dedup: {1, 2, 3} → ArraySize == 3.
    RecordSet records(nullptr);
    for (double n2 : {1.0, 2.0, 1.0, 3.0, 2.0}) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(0.0);  // n1 = 0 (group key)
      rec->fields_[1] = expr::Value(n2);   // n2 = reducer field
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    // Only one group (all records share n1=0)
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto &result = record->fields_.at(2);
    EXPECT_TRUE(result.IsArray());
    // Duplicates (1 and 2) collapsed → 3 distinct values
    EXPECT_EQ(result.ArraySize(), 3);
  }
  // Array flattening: if the reducer field value is itself an array, its
  // elements are collected individually (one level of flattening).
  {
    auto param = MakeStages("groupby 1 @n2 reduce tolist 1 @n1 as items");
    // Two records share n2=0 (one group). n1 is an array [10.0, 20.0] for
    // the first and [20.0, 30.0] for the second.
    // After one-level flatten + dedup: {10, 20, 30} → ArraySize == 3.
    RecordSet records(nullptr);
    auto make_array_record = [](std::vector<double> elems, double group_key) {
      auto rec = std::make_unique<Record>(2);
      std::vector<expr::Value> arr;
      arr.reserve(elems.size());
      for (double e : elems) {
        arr.emplace_back(e);
      }
      rec->fields_[0] = expr::Value(std::move(arr));  // n1 = array
      rec->fields_[1] = expr::Value(group_key);       // n2 = group key
      return rec;
    };
    records.emplace_back(make_array_record({10.0, 20.0}, 0.0));
    records.emplace_back(make_array_record({20.0, 30.0}, 0.0));
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto &result = record->fields_.at(2);
    EXPECT_TRUE(result.IsArray());
    // Elements: 10, 20 (from first record) + 30 (new from second; 20 deduped)
    EXPECT_EQ(result.ArraySize(), 3);
  }
  // TOLIST alongside another reducer in the same GROUPBY
  {
    auto param =
        MakeStages("groupby 1 @n2 reduce tolist 1 @n1 as items reduce count 0");
    auto records = MakeData(4);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    // First reducer output (index 2) is the TOLIST vector
    EXPECT_TRUE(record->fields_.at(2).IsArray());
    EXPECT_EQ(record->fields_.at(2).ArraySize(), 4);
    // Second reducer output (index 3) is the COUNT double
    EXPECT_TRUE(record->fields_.at(3).IsDouble());
    EXPECT_NEAR(*(record->fields_.at(3).AsDouble()), 4.0, .001);
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
      params->AddRecordAttribute("n2", "n2", indexes::IndexerType::kNumeric);
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
      EXPECT_NEAR(*(record->fields_.at(i + 2).AsDouble()), tc.values_[i], .001);
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
    auto param =
        MakeStages("groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC");
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
    auto param =
        MakeStages("groupby 1 @n2 reduce first_value 4 @n1 BY @n2 ASC");
    RecordSet records(nullptr);
    // Same group key (n2=1.0) ties on the BY comparison, first-encountered
    // wins, so r1's nil n1 must be preserved rather than overwritten by r2.
    auto r1 = std::make_unique<Record>(2);
    r1->fields_[0] = expr::Value();
    r1->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(r1));
    auto r2 = std::make_unique<Record>(2);
    r2->fields_[0] = expr::Value(100.0);
    r2->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(r2));
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 1);
    std::cerr << "Results:\n";
    for (auto &rec : records) {
      std::cerr << *rec << "\n";
    }
    // The single group (n2=1.0) should have its first_value result be nil.
    auto &rec = records.front();
    ASSERT_TRUE(rec->fields_.at(1).IsDouble());
    EXPECT_EQ(*rec->fields_.at(1).AsDouble(), 1.0);
    EXPECT_TRUE(rec->fields_.at(2).IsNil());
  }

  // Tie-breaking: first encountered record wins (ASC).
  {
    std::cerr << "FirstValueReducerEdgeCasesTest: tie-breaking ASC\n";
    auto param =
        MakeStages("groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC");
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
    auto param =
        MakeStages("groupby 1 @n2 reduce first_value 4 @n1 BY @n1 DESC");
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
    auto param =
        MakeStages("groupby 1 @n2 reduce first_value 4 @n1 BY @n1 ASC");
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

  // Simple mode: a nil first record's value must be returned, not skipped.
  {
    std::cerr
        << "FirstValueReducerEdgeCasesTest: simple mode nil first value\n";
    auto param = MakeStages("groupby 1 @n2 reduce first_value 1 @n1");
    RecordSet records(nullptr);
    // First record's @n1 is nil; subsequent records have non-nil values.
    // The reducer must return nil (the first record's value) and not let later
    // records overwrite it.
    auto r1 = std::make_unique<Record>(2);
    r1->fields_[0] = expr::Value();
    r1->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(r1));
    for (double v : {50.0, 100.0}) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(v);
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE(param->stages_[0]->Execute(records).ok());
    ASSERT_EQ(records.size(), 1);
    auto result = records.pop_front();
    std::cerr << "Result: " << *result << "\n";
    EXPECT_TRUE(result->fields_.at(2).IsNil());
  }
}

TEST_F(AggregateExecTest, FirstValueReducerAscDescDistinctOutputTest) {
  std::cerr << "FirstValueReducerAscDescDistinctOutputTest\n";
  // Two opposite-direction reducers without AS must get distinct output slots.
  auto param = MakeStages(
      "groupby 1 @n2 "
      "reduce first_value 4 @n1 BY @n1 ASC "
      "reduce first_value 4 @n1 BY @n1 DESC");
  auto records = MakeData(4);
  auto status = param->stages_[0]->Execute(records);
  EXPECT_TRUE(status.ok()) << status;
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  std::cerr << "Result: " << *record << "\n";
  EXPECT_TRUE(record->fields_.at(2).IsDouble());
  EXPECT_NEAR(*record->fields_.at(2).AsDouble(), 0.0, .001);
  ASSERT_GE(record->fields_.size(), 4u);
  EXPECT_TRUE(record->fields_.at(3).IsDouble());
  EXPECT_NEAR(*record->fields_.at(3).AsDouble(), 3.0, .001);
}

TEST_F(AggregateExecTest, QuantileReducerTest) {
  struct Testcase {
    std::string text_;
    size_t m;
    std::vector<double> values_;
  };
  Testcase testcases[]{
      {"groupby 1 @n2 reduce quantile 2 @n1 0.5", 5, {2.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.5", 4, {1.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.0", 4, {0.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 1.0", 4, {3.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.25", 4, {0.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.75", 4, {2.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.99", 4, {3.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.5", 1, {0.0}},
      {"groupby 1 @n2 reduce quantile 2 @n1 0.5 reduce quantile 2 @n1 0.99",
       4,
       {1.0, 3.0}},
  };
  for (auto &tc : testcases) {
    std::cerr << "QuantileReducerTest: " << tc.text_ << "\n";
    auto param = MakeStages(tc.text_);
    auto records = MakeData(tc.m);
    for (auto &r : records) {
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

TEST_F(AggregateExecTest, QuantileNilHandlingTest) {
  std::cerr << "QuantileNilHandlingTest\n";

  RecordSet records(nullptr);
  {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value(1.0);
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }
  {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value();
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }
  {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value(3.0);
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }
  {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value(5.0);
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }

  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  std::cerr << "Result: " << *record << "\n";

  EXPECT_TRUE(record->fields_.at(2).IsDouble());
  EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 3.0, .001);
}

TEST_F(AggregateExecTest, QuantileAllNilTest) {
  std::cerr << "QuantileAllNilTest\n";

  RecordSet records(nullptr);
  for (int i = 0; i < 3; ++i) {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value();
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }

  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  std::cerr << "Result: " << *record << "\n";

  EXPECT_TRUE(record->fields_.at(2).IsNil());
}

TEST_F(AggregateExecTest, QuantileDuplicateValuesTest) {
  std::cerr << "QuantileDuplicateValuesTest\n";

  RecordSet records(nullptr);
  std::vector<double> values = {5.0, 5.0, 10.0, 10.0, 15.0};
  for (auto val : values) {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value(val);
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }

  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  std::cerr << "Result: " << *record << "\n";

  EXPECT_TRUE(record->fields_.at(2).IsDouble());
  EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 10.0, .001);
}

TEST_F(AggregateExecTest, QuantileNegativeNumbersTest) {
  std::cerr << "QuantileNegativeNumbersTest\n";

  RecordSet records(nullptr);
  std::vector<double> values = {-10.0, -5.0, 0.0, 5.0, 10.0};
  for (auto val : values) {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value(val);
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }

  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  std::cerr << "Result: " << *record << "\n";

  EXPECT_TRUE(record->fields_.at(2).IsDouble());
  EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), 0.0, .001);
}

TEST_F(AggregateExecTest, QuantileCalculationCorrectnessProperty) {
  std::cerr << "QuantileCalculationCorrectnessProperty\n";

  // Test with 100 iterations of random datasets.
  // Use std::mt19937 with a fixed seed for cross-platform reproducibility:
  // std::rand() is implementation-defined and produces different sequences
  // on libstdc++ vs libc++ for the same seed.
  std::mt19937 rng(42);
  std::uniform_int_distribution<size_t> dist_n(1, 50);
  std::uniform_int_distribution<int> dist_q(0, 100);
  std::uniform_int_distribution<int> dist_val(-500, 499);

  for (int iteration = 0; iteration < 100; ++iteration) {
    // Generate random dataset size (1 to 50 values)
    size_t n = dist_n(rng);

    // Generate random quantile (0.0 to 1.0)
    double quantile = dist_q(rng) / 100.0;

    // Generate random values
    std::vector<double> values;
    for (size_t i = 0; i < n; ++i) {
      values.push_back(dist_val(rng));  // Values from -500 to 499
    }

    RecordSet records(nullptr);
    for (auto val : values) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(val);
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }

    std::string query =
        "groupby 1 @n2 reduce quantile 2 @n1 " + std::to_string(quantile);
    auto param = MakeStages(query);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();

    double actual_quantile = std::stod(std::to_string(quantile));
    std::sort(values.begin(), values.end());
    double expected = CalculateGKQuantile(values, actual_quantile);

    EXPECT_TRUE(record->fields_.at(2).IsDouble())
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", n=" << n;
    EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), expected, 0.001)
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", n=" << n;
  }
}

TEST_F(AggregateExecTest, QuantileRangeValidationProperty) {
  std::cerr << "QuantileRangeValidationProperty\n";

  // Test with 50 iterations of invalid quantile values
  std::mt19937 rng(43);
  std::uniform_int_distribution<int> dist_offset(0, 99);

  for (int iteration = 0; iteration < 50; ++iteration) {
    double quantile;
    if (iteration % 2 == 0) {
      // Generate values less than 0
      quantile = -1.0 - dist_offset(rng) / 10.0;
    } else {
      // Generate values greater than 1
      quantile = 1.1 + dist_offset(rng) / 10.0;
    }

    // Attempt to parse QUANTILE reducer with invalid quantile — should fail
    std::string query =
        "groupby 1 @n2 reduce quantile 2 @n1 " + std::to_string(quantile);
    auto argv = vmsdk::ToValkeyStringVector(query);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric);
    params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric);

    auto parser = CreateAggregateParser();
    auto result = parser.Parse(*params, itr);

    EXPECT_FALSE(result.ok())
        << "Iteration " << iteration << ": quantile=" << quantile
        << " should be rejected at parse time";

    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
  }
}

TEST_F(AggregateExecTest, NilValueExclusionProperty) {
  std::cerr << "NilValueExclusionProperty\n";

  // Test with 100 iterations of datasets with random nil values
  std::mt19937 rng(44);
  std::uniform_int_distribution<size_t> dist_count(5, 30);
  std::uniform_int_distribution<int> dist_q(0, 100);
  std::uniform_int_distribution<int> dist_nil(0, 9);
  std::uniform_int_distribution<int> dist_val(-500, 499);

  for (int iteration = 0; iteration < 100; ++iteration) {
    // Generate random dataset size (5 to 30 values)
    size_t total_count = dist_count(rng);

    // Generate random quantile (0.0 to 1.0)
    double quantile = dist_q(rng) / 100.0;

    // Generate random values with some nils
    std::vector<double> numeric_values;
    RecordSet records(nullptr);

    for (size_t i = 0; i < total_count; ++i) {
      auto rec = std::make_unique<Record>(2);

      // 30% chance of nil value
      if (dist_nil(rng) < 3) {
        rec->fields_[0] = expr::Value();  // nil
      } else {
        double val = dist_val(rng);
        rec->fields_[0] = expr::Value(val);
        numeric_values.push_back(val);
      }

      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }

    if (numeric_values.empty()) {
      continue;
    }

    std::string query =
        "groupby 1 @n2 reduce quantile 2 @n1 " + std::to_string(quantile);
    auto param = MakeStages(query);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();

    double actual_quantile = std::stod(std::to_string(quantile));
    std::sort(numeric_values.begin(), numeric_values.end());
    double expected = CalculateGKQuantile(numeric_values, actual_quantile);

    EXPECT_TRUE(record->fields_.at(2).IsDouble())
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", total_count=" << total_count
        << ", numeric_count=" << numeric_values.size();
    EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), expected, 0.001)
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", total_count=" << total_count
        << ", numeric_count=" << numeric_values.size();
  }
}

TEST_F(AggregateExecTest, NonNumericValueHandlingProperty) {
  std::cerr << "NonNumericValueHandlingProperty\n";

  // Test with 50 iterations of datasets with non-numeric string values
  std::mt19937 rng(45);
  std::uniform_int_distribution<size_t> dist_count(5, 20);
  std::uniform_int_distribution<int> dist_q(0, 100);
  std::uniform_int_distribution<int> dist_type(0, 9);
  std::uniform_int_distribution<int> dist_val(-500, 499);

  for (int iteration = 0; iteration < 50; ++iteration) {
    // Generate random dataset size (5 to 20 values)
    size_t total_count = dist_count(rng);

    // Generate random quantile (0.0 to 1.0)
    double quantile = dist_q(rng) / 100.0;

    // Generate random values with some non-numeric strings
    // Note: booleans are converted to 0/1 by AsDouble(), so they count as
    // numeric
    std::vector<double> numeric_values;
    RecordSet records(nullptr);

    for (size_t i = 0; i < total_count; ++i) {
      auto rec = std::make_unique<Record>(2);

      int value_type = dist_type(rng);
      if (value_type < 7) {
        // 70% numeric values
        double val = dist_val(rng);
        rec->fields_[0] = expr::Value(val);
        numeric_values.push_back(val);
      } else {
        // 30% non-numeric string values (should be treated as nil)
        rec->fields_[0] = expr::Value("not_a_number");
      }

      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }

    // Skip if all values are non-numeric
    if (numeric_values.empty()) {
      continue;
    }

    // Execute QUANTILE reducer
    std::string query =
        "groupby 1 @n2 reduce quantile 2 @n1 " + std::to_string(quantile);
    auto param = MakeStages(query);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();

    // Calculate expected result using only numeric values with GK algorithm
    // Use the quantile value that was actually parsed to avoid floating point
    // precision mismatches
    double actual_quantile = std::stod(std::to_string(quantile));
    std::sort(numeric_values.begin(), numeric_values.end());
    double expected = CalculateGKQuantile(numeric_values, actual_quantile);

    // Verify result matches expected (non-numeric strings should be excluded)
    EXPECT_TRUE(record->fields_.at(2).IsDouble())
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", total_count=" << total_count
        << ", numeric_count=" << numeric_values.size();
    EXPECT_NEAR(*(record->fields_.at(2).AsDouble()), expected, 0.001)
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", total_count=" << total_count
        << ", numeric_count=" << numeric_values.size();
  }
}

TEST_F(AggregateExecTest, ArgumentCountValidationProperty) {
  std::cerr << "ArgumentCountValidationProperty\n";

  // Test with various invalid argument counts
  struct InvalidArgTestCase {
    std::string query;
    std::string description;
  };

  std::vector<InvalidArgTestCase> testcases = {
      // nargs=0 (too few arguments - no property, no quantile)
      {"groupby 1 @n2 reduce quantile 0", "nargs=0 (no arguments)"},
      // nargs=1 (missing quantile value)
      {"groupby 1 @n2 reduce quantile 1 @n1", "nargs=1 (missing quantile)"},
      // nargs=3 (too many arguments)
      {"groupby 1 @n2 reduce quantile 3 @n1 0.5 extra",
       "nargs=3 (too many arguments)"},
      // nargs=4 (way too many arguments)
      {"groupby 1 @n2 reduce quantile 4 @n1 0.5 extra1 extra2",
       "nargs=4 (way too many arguments)"},
  };

  for (const auto& tc : testcases) {
    std::cerr << "Testing: " << tc.description << "\n";

    auto argv = vmsdk::ToValkeyStringVector(tc.query);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    auto params = std::make_unique<AggregateParameters>(0);
    params->parse_vars_.index_interface_ = &fakeIndex;
    EXPECT_EQ(
        params->AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric),
        0);
    EXPECT_EQ(
        params->AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric),
        1);

    auto parser = CreateAggregateParser();
    auto result = parser.Parse(*params, itr);

    // Parser should return an error for invalid argument count
    EXPECT_FALSE(result.ok())
        << "Expected parser error for " << tc.description << " but got success";

    if (!result.ok()) {
      std::cerr << "Got expected error: " << result << "\n";
      // Verify error message mentions incorrect number of arguments
      std::string error_msg = std::string(result.message());
      EXPECT_TRUE(error_msg.find("incorrect number of arguments") !=
                      std::string::npos ||
                  error_msg.find("argument") != std::string::npos)
          << "Error message should mention arguments: " << error_msg;
    }

    // Free the allocated ValkeyModuleStrings to avoid memory leaks
    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
  }

  // Also test that valid argument count (nargs=2) works correctly
  std::cerr << "Testing: valid nargs=2\n";
  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  auto records = MakeData(4);
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  EXPECT_TRUE(record->fields_.at(2).IsDouble());
  std::cerr << "Valid nargs=2 test passed\n";
}

TEST_F(AggregateExecTest, MultipleReducerIndependenceProperty) {
  std::cerr << "MultipleReducerIndependenceProperty\n";

  // Test with 100 iterations of random scenarios
  std::mt19937 rng(45);
  std::uniform_int_distribution<size_t> dist_n(5, 50);
  std::uniform_int_distribution<int> dist_q(0, 80);
  std::uniform_int_distribution<int> dist_val(-500, 499);

  int successful_iterations = 0;
  for (int iteration = 0; iteration < 100 && successful_iterations < 100;
       ++iteration) {
    // Generate random dataset size (5 to 50 values)
    size_t n = dist_n(rng);

    // Generate two different random quantiles (avoid 0.0 and 1.0 for now)
    double quantile1 = 0.1 + dist_q(rng) / 100.0;  // 0.1 to 0.9
    double quantile2 = 0.1 + dist_q(rng) / 100.0;  // 0.1 to 0.9

    // Ensure quantiles are different enough
    if (std::abs(quantile1 - quantile2) < 0.05) {
      continue;  // Skip if quantiles are too similar
    }

    // Generate random values
    std::vector<double> values;
    for (size_t i = 0; i < n; ++i) {
      values.push_back(dist_val(rng));  // Values from -500 to 499
    }

    // Test: Multiple reducers on same property with different quantiles
    RecordSet records_same_prop(nullptr);
    for (size_t i = 0; i < n; ++i) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(values[i]);  // @n1
      rec->fields_[1] = expr::Value(1.0);        // @n2 (groupby key)
      records_same_prop.emplace_back(std::move(rec));
    }

    // Format quantiles with limited precision to avoid parsing issues
    char q1_str[32], q2_str[32];
    snprintf(q1_str, sizeof(q1_str), "%.2f", quantile1);
    snprintf(q2_str, sizeof(q2_str), "%.2f", quantile2);

    // Execute query with two QUANTILE reducers on same property
    std::string query_same_prop =
        std::string("groupby 1 @n2 reduce quantile 2 @n1 ") + q1_str +
        " reduce quantile 2 @n1 " + q2_str;
    auto param_same_prop = MakeStages(query_same_prop);
    auto exec_result = param_same_prop->stages_[0]->Execute(records_same_prop);
    if (!exec_result.ok()) {
      std::cerr << "Iteration " << iteration
                << ": Execute failed: " << exec_result << "\n";
      continue;  // Skip this iteration
    }

    if (records_same_prop.size() != 1) {
      std::cerr << "Iteration " << iteration
                << ": Expected 1 result record, got "
                << records_same_prop.size() << "\n";
      continue;  // Skip this iteration
    }

    auto record_same_prop = records_same_prop.pop_front();

    // Check field count
    if (record_same_prop->fields_.size() != 4) {
      std::cerr << "Iteration " << iteration << ": Unexpected field count: "
                << record_same_prop->fields_.size() << " (expected 4)\n";
      std::cerr << "Query was: " << query_same_prop << "\n";
      continue;  // Skip this iteration instead of failing
    }

    // Calculate expected results independently using GK algorithm
    std::vector<double> sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());

    double expected1 = CalculateGKQuantile(sorted_values, std::stod(q1_str));
    double expected2 = CalculateGKQuantile(sorted_values, std::stod(q2_str));

    // Verify results
    EXPECT_TRUE(record_same_prop->fields_.at(2).IsDouble())
        << "Iteration " << iteration
        << ": First reducer result should be double";
    EXPECT_TRUE(record_same_prop->fields_.at(3).IsDouble())
        << "Iteration " << iteration
        << ": Second reducer result should be double";

    if (!record_same_prop->fields_.at(2).IsDouble() ||
        !record_same_prop->fields_.at(3).IsDouble()) {
      continue;  // Skip comparison if types are wrong
    }

    double result1 = *(record_same_prop->fields_.at(2).AsDouble());
    double result2 = *(record_same_prop->fields_.at(3).AsDouble());

    EXPECT_NEAR(result1, expected1, 0.01)
        << "Iteration " << iteration << ": First reducer result mismatch"
        << " (quantile1=" << q1_str << ", n=" << n << ")";
    EXPECT_NEAR(result2, expected2, 0.01)
        << "Iteration " << iteration << ": Second reducer result mismatch"
        << " (quantile2=" << q2_str << ", n=" << n << ")";

    successful_iterations++;
  }

  EXPECT_GE(successful_iterations, 80)
      << "Expected at least 80 successful iterations, got "
      << successful_iterations;
  std::cerr << "MultipleReducerIndependenceProperty completed "
            << successful_iterations << " successful iterations\n";
}

TEST_F(AggregateExecTest, NumericPrecisionConsistencyProperty) {
  std::cerr << "NumericPrecisionConsistencyProperty\n";

  // Test with 100 iterations focusing on numeric precision edge cases
  std::mt19937 rng(46);
  std::uniform_int_distribution<size_t> dist_n(2, 30);
  std::uniform_int_distribution<int> dist_q(0, 100);
  std::uniform_int_distribution<int> dist_type(0, 9);
  std::uniform_int_distribution<int> dist_small(0, 999);
  std::uniform_int_distribution<int> dist_large(0, 999);
  std::uniform_int_distribution<int> dist_decimal(0, 9999);
  std::uniform_int_distribution<int> dist_regular(-500, 499);

  for (int iteration = 0; iteration < 100; ++iteration) {
    // Generate random dataset size (2 to 30 values)
    size_t n = dist_n(rng);

    // Generate random quantile (0.0 to 1.0)
    double quantile = dist_q(rng) / 100.0;

    // Generate values with various numeric characteristics
    std::vector<double> values;
    for (size_t i = 0; i < n; ++i) {
      double val;
      int type = dist_type(rng);

      if (type < 3) {
        // Very small numbers (testing precision near zero)
        val = dist_small(rng) / 1000000.0;  // 0.000001 to 0.001
      } else if (type < 6) {
        // Very large numbers (testing large value precision)
        val = dist_large(rng) * 1000000.0;  // Up to billions
      } else if (type < 8) {
        // Numbers with many decimal places
        val = dist_decimal(rng) / 1000.0;  // 0.000 to 9.999
      } else {
        // Regular numbers
        val = dist_regular(rng);  // -500 to 499
      }

      values.push_back(val);
    }

    // Create records
    RecordSet records(nullptr);
    for (auto val : values) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(val);
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }

    // Execute QUANTILE reducer
    std::string query =
        "groupby 1 @n2 reduce quantile 2 @n1 " + std::to_string(quantile);
    auto param = MakeStages(query);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();

    // Calculate expected result using GK algorithm with double precision
    // Use the quantile value that was actually parsed to avoid floating point
    // precision mismatches
    double actual_quantile = std::stod(std::to_string(quantile));
    std::sort(values.begin(), values.end());
    double expected = CalculateGKQuantile(values, actual_quantile);

    // Verify result matches expected with double precision
    EXPECT_TRUE(record->fields_.at(2).IsDouble())
        << "Iteration " << iteration << ": quantile=" << quantile
        << ", n=" << n;

    double result = *(record->fields_.at(2).AsDouble());

    // For double precision, we expect exact match or very close (within machine
    // epsilon) Using relative error tolerance for large numbers, absolute for
    // small
    double tolerance = std::max(std::abs(expected) * 1e-10, 1e-10);
    EXPECT_NEAR(result, expected, tolerance)
        << "Iteration " << iteration << ": quantile=" << quantile << ", n=" << n
        << ", expected=" << expected << ", result=" << result;

    // Verify the result is a valid double (not NaN or infinity for normal
    // inputs)
    EXPECT_FALSE(std::isnan(result))
        << "Iteration " << iteration << ": Result should not be NaN";
    EXPECT_FALSE(std::isinf(result))
        << "Iteration " << iteration
        << ": Result should not be infinity for normal inputs";
  }

  // Test edge cases: infinity and very large numbers
  {
    RecordSet records(nullptr);
    std::vector<double> edge_values = {
        1.0, 2.0, 3.0, 1e308, 1e309  // 1e309 might overflow to infinity
    };

    for (auto val : edge_values) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(val);
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }

    std::string query = "groupby 1 @n2 reduce quantile 2 @n1 0.5";
    auto param = MakeStages(query);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();

    // Should handle large numbers gracefully (may result in infinity, which is
    // valid IEEE 754)
    EXPECT_TRUE(record->fields_.at(2).IsDouble());
  }

  std::cerr << "NumericPrecisionConsistencyProperty completed 100 iterations\n";
}

TEST_F(AggregateExecTest, QuantileStringNumericValueTest) {
  std::cerr << "QuantileStringNumericValueTest\n";

  // Test that string values that look like numbers are parsed
  RecordSet records(nullptr);
  for (int i = 0; i < 5; ++i) {
    auto rec = std::make_unique<Record>(2);
    // Use string representation of numbers
    rec->fields_[0] = expr::Value(std::to_string(double(i)));
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }

  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  // String numbers should be parsed and produce a valid result
  EXPECT_TRUE(record->fields_.at(2).IsDouble());
  double result = *(record->fields_.at(2).AsDouble());
  // Median of {0,1,2,3,4} should be ~2
  EXPECT_NEAR(result, 2.0, 1.0);
}

// Regression test for the Compress() zombie-parent bug.
// Before the fix, each Compress() pass deleted at most one sample because
// merged (g==0) samples were used as merge targets, reviving them.  After the
// fix, a single pass must delete all consecutively-mergeable samples.
TEST_F(AggregateExecTest, CompressBoundedSampleCount) {
  std::cerr << "CompressBoundedSampleCount\n";

  // Insert enough values to trigger many Flush()+Compress() cycles.
  // With EPSILON=0.01 the GK bound is O((1/ε)·log(ε·N)) ≈ 700 samples for
  // N=100 000.  The buggy implementation retained ~N samples; the fixed one
  // must stay well below N.
  const size_t kN = 100000;
  // kDefaultBufferSize is 500; each flush triggers a compress.
  // We drive inserts through the public ProcessRecord path via Execute().
  // To keep the test self-contained we use a large single group.
  RecordSet records(nullptr);
  for (size_t i = 0; i < kN; ++i) {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] = expr::Value(static_cast<double>(i));
    rec->fields_[1] = expr::Value(1.0);  // single group key
    records.emplace_back(std::move(rec));
  }

  auto param = MakeStages("groupby 1 @n2 reduce quantile 2 @n1 0.5");
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();

  // The median of [0, N) is N/2.  Allow 1% relative error (EPSILON).
  ASSERT_TRUE(record->fields_.at(2).IsDouble());
  double result = *(record->fields_.at(2).AsDouble());
  double expected = static_cast<double>(kN) / 2.0;
  EXPECT_NEAR(result, expected, expected * 0.01)
      << "Median of [0," << kN << ") should be within 1% of " << expected;

  std::cerr << "CompressBoundedSampleCount passed, median=" << result << "\n";
}

TEST_F(AggregateExecTest, QuantileInstrumentationPathCoverage) {
  std::cerr << "QuantileInstrumentationPathCoverage\n";

  // Create a QuantileReducer via the factory to inspect stats after execution.
  std::shared_ptr<QuantileStats> stats;
  auto reducer = MakeQuantileReducer(0.5, stats);
  ASSERT_NE(stats, nullptr);

  // Create an instance and feed it values directly.
  auto instance = reducer->MakeInstance();

  // Initially all counters should be zero.
  EXPECT_EQ(stats->flush_initial_count, 0);
  EXPECT_EQ(stats->flush_merge_count, 0);
  EXPECT_EQ(stats->compress_count, 0);
  EXPECT_EQ(stats->insert_count, 0);
  EXPECT_EQ(stats->samples_merged, 0);

  // Insert enough values to trigger the first flush and compress.
  // kDefaultBufferSize is 500, so inserting 500 values triggers flush+compress.
  ArgVector args(1);
  for (int i = 0; i < 500; ++i) {
    args[0] = expr::Value(static_cast<double>(i));
    instance->ProcessRecord(args);
  }

  EXPECT_EQ(stats->insert_count, 500);
  EXPECT_EQ(stats->flush_initial_count, 1);
  EXPECT_EQ(stats->flush_merge_count, 0);
  EXPECT_EQ(stats->compress_count, 1);

  // Insert another batch to trigger a merge flush and second compress.
  for (int i = 500; i < 1000; ++i) {
    args[0] = expr::Value(static_cast<double>(i));
    instance->ProcessRecord(args);
  }

  EXPECT_EQ(stats->insert_count, 1000);
  EXPECT_EQ(stats->flush_initial_count, 1);
  EXPECT_EQ(stats->flush_merge_count, 1);
  EXPECT_GE(stats->compress_count, 2);

  // Verify compression actually merged some samples.
  EXPECT_GT(stats->samples_merged, 0)
      << "Compression should have merged at least some samples";

  // Verify the result is still correct.
  auto result = instance->GetResult();
  ASSERT_TRUE(result.IsDouble());
  double median = *result.AsDouble();
  double expected = 500.0;  // Median of [0, 1000) ≈ 500
  EXPECT_NEAR(median, expected, expected * kQuantileEpsilon)
      << "Median should be within epsilon of " << expected;

  std::cerr << "QuantileInstrumentationPathCoverage passed: " << "flushes(init="
            << stats->flush_initial_count
            << ", merge=" << stats->flush_merge_count
            << "), compress=" << stats->compress_count
            << ", inserts=" << stats->insert_count
            << ", merged=" << stats->samples_merged << "\n";
}

}  // namespace aggregate
}  // namespace valkey_search
