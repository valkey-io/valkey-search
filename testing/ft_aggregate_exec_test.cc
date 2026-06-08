/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/commands/ft_aggregate_exec.h"

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

  // Variant that returns the parse status instead of asserting success, for
  // exercising error paths.
  absl::Status TryParseStages(absl::string_view test) {
    auto argv = vmsdk::ToValkeyStringVector(test);
    vmsdk::ArgsIterator itr(argv.data(), argv.size());

    AggregateParameters params(0);
    params.parse_vars_.index_interface_ = &fakeIndex;
    params.AddRecordAttribute("n1", "n1", indexes::IndexerType::kNumeric);
    params.AddRecordAttribute("n2", "n1", indexes::IndexerType::kNumeric);

    auto parser = CreateAggregateParser();
    auto status = parser.Parse(params, itr);

    for (auto* str : argv) {
      ValkeyModule_FreeString(nullptr, str);
    }
    return status;
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

// Extracts the elements from a RANDOM_SAMPLE reducer result (Value::Array).
static std::vector<expr::Value> GetSampleArray(const expr::Value& value) {
  EXPECT_TRUE(value.IsArray()) << "Expected vector Value";
  if (!value.IsArray()) {
    return {};
  }
  auto vec = value.GetArray();
  return std::vector<expr::Value>(vec->begin(), vec->end());
}

// Checks that every element in `sample` appears in `allowed`.
static void ExpectAllElementsIn(const std::vector<expr::Value>& sample,
                                const std::vector<std::string>& allowed) {
  for (const auto& elem : sample) {
    std::string elem_str = elem.AsString();
    EXPECT_TRUE(std::find(allowed.begin(), allowed.end(), elem_str) !=
                allowed.end())
        << "Sample element \"" << elem_str << "\" not in allowed set";
  }
}

TEST_F(AggregateExecTest, RandomSampleBasicTest) {
  // Edge case: empty record set produces no groups
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 5");
    RecordSet records(nullptr);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 0);
  }

  // Edge case: single value in group
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 5");
    auto records = MakeData(1);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample = GetSampleArray(record->fields_.at(2));
    EXPECT_EQ(sample.size(), 1);
    EXPECT_EQ(sample[0].AsString(), expr::Value(0.0).AsString());
  }

  // Sampled values must come from the input set
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 3");
    auto records = MakeData(5);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample = GetSampleArray(record->fields_.at(2));
    std::vector<std::string> allowed;
    for (int i = 0; i < 5; ++i) {
      allowed.push_back(expr::Value(double(i)).AsString());
    }
    ExpectAllElementsIn(sample, allowed);
  }

  // Sample size == group size: all elements selected
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 4");
    auto records = MakeData(4);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample = GetSampleArray(record->fields_.at(2));
    EXPECT_EQ(sample.size(), 4);
    std::vector<std::string> all_values;
    for (int i = 0; i < 4; ++i) {
      all_values.push_back(expr::Value(double(i)).AsString());
    }
    ExpectAllElementsIn(sample, all_values);
  }

  // Sample size > group size: all elements selected, no duplicates
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 10");
    auto records = MakeData(4);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample = GetSampleArray(record->fields_.at(2));
    EXPECT_EQ(sample.size(), 4);
    std::vector<std::string> all_values;
    for (int i = 0; i < 4; ++i) {
      all_values.push_back(expr::Value(double(i)).AsString());
    }
    ExpectAllElementsIn(sample, all_values);
  }
}

TEST_F(AggregateExecTest, RandomSampleNilHandlingTest) {
  // Mixed nil and non-nil: only non-nil values should be sampled
  auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 3");
  RecordSet records(nullptr);
  // 3 non-nil values at indices 0, 2, 4
  for (int i = 0; i < 5; ++i) {
    auto rec = std::make_unique<Record>(2);
    rec->fields_[0] =
        (i % 2 == 0) ? expr::Value(double(i)) : expr::Value();  // nil
    rec->fields_[1] = expr::Value(1.0);
    records.emplace_back(std::move(rec));
  }
  EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
  EXPECT_EQ(records.size(), 1);
  auto record = records.pop_front();
  auto sample = GetSampleArray(record->fields_.at(2));
  EXPECT_EQ(sample.size(), 3);
  std::vector<std::string> allowed;
  for (int i : {0, 2, 4}) {
    allowed.push_back(expr::Value(double(i)).AsString());
  }
  ExpectAllElementsIn(sample, allowed);
}

TEST_F(AggregateExecTest, RandomSampleTypeHandlingTest) {
  // String values: type is preserved in output
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 3");
    RecordSet records(nullptr);
    std::vector<std::string> allowed;
    for (int i = 0; i < 5; ++i) {
      auto rec = std::make_unique<Record>(2);
      std::string val = std::string("str") + std::to_string(i);
      allowed.push_back(val);
      rec->fields_[0] = expr::Value(std::move(val));
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample = GetSampleArray(record->fields_.at(2));
    EXPECT_EQ(sample.size(), 3);
    ExpectAllElementsIn(sample, allowed);
  }

  // Mixed types: no nil in output, types preserved
  {
    auto param = MakeStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 4");
    RecordSet records(nullptr);
    std::vector<std::string> allowed;
    for (int i = 0; i < 6; ++i) {
      auto rec = std::make_unique<Record>(2);
      if (i % 2 == 0) {
        rec->fields_[0] = expr::Value(double(i));
        allowed.push_back(expr::Value(double(i)).AsString());
      } else {
        std::string val = std::string("str") + std::to_string(i);
        allowed.push_back(val);
        rec->fields_[0] = expr::Value(std::move(val));
      }
      rec->fields_[1] = expr::Value(1.0);
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample = GetSampleArray(record->fields_.at(2));
    EXPECT_EQ(sample.size(), 4);
    ExpectAllElementsIn(sample, allowed);
  }
}

TEST_F(AggregateExecTest, RandomSampleMultipleReducersTest) {
  {
    auto param = MakeStages(
        "groupby 1 @n2 "
        "reduce RANDOM_SAMPLE 2 @n1 3 "
        "reduce RANDOM_SAMPLE 2 @n1 2");
    auto records = MakeData(5);
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 1);
    auto record = records.pop_front();
    auto sample1 = GetSampleArray(record->fields_.at(2));
    auto sample2 = GetSampleArray(record->fields_.at(3));
    EXPECT_EQ(sample1.size(), 3);
    EXPECT_EQ(sample2.size(), 2);
    std::vector<std::string> allowed;
    for (int i = 0; i < 5; ++i) {
      allowed.push_back(expr::Value(double(i)).AsString());
    }
    ExpectAllElementsIn(sample1, allowed);
    ExpectAllElementsIn(sample2, allowed);
  }
}

TEST_F(AggregateExecTest, RandomSampleGroupByTest) {
  {
    auto param = MakeStages("groupby 1 @n1 reduce RANDOM_SAMPLE 2 @n2 2");
    RecordSet records(nullptr);
    // 6 records, 3 groups (n1 values 0,1,2), each group has 2 records
    for (int i = 0; i < 6; ++i) {
      auto rec = std::make_unique<Record>(2);
      rec->fields_[0] = expr::Value(double(i % 3));  // 3 groups
      rec->fields_[1] = expr::Value(double(i));
      records.emplace_back(std::move(rec));
    }
    EXPECT_TRUE((param->stages_[0]->Execute(records)).ok());
    EXPECT_EQ(records.size(), 3);
    for (auto& rec : records) {
      auto sample = GetSampleArray(rec->fields_.at(2));
      EXPECT_EQ(sample.size(), 2);
    }
  }
}

TEST_F(AggregateExecTest, RandomSampleParseErrorsTest) {
  // Negative sample size is rejected at parse time.
  {
    auto status = TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 -5");
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  }

  // Sample size above kMaxSampleSize is rejected.
  {
    auto status =
        TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 1001");
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), absl::StatusCode::kOutOfRange);
  }

  // Non-integer sample size is rejected.
  {
    auto status =
        TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 1.5");
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  }

  // Wrong argument count (1 instead of 2) is rejected.
  {
    auto status = TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 1 @n1");
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), absl::StatusCode::kOutOfRange);
  }

  // Sample size zero is accepted (empty sample is a valid result).
  {
    auto status = TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 0");
    EXPECT_TRUE(status.ok()) << status;
  }

  // Sample size at the upper bound is accepted.
  {
    auto status =
        TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 2 @n1 1000");
    EXPECT_TRUE(status.ok()) << status;
  }

  // Extra declared arguments beyond 2 are silently consumed (Redis compat).
  {
    auto status =
        TryParseStages("groupby 1 @n2 reduce RANDOM_SAMPLE 3 @n1 5 extra");
    EXPECT_TRUE(status.ok()) << status;
  }
}

}  // namespace aggregate
}  // namespace valkey_search
