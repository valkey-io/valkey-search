#include "src/commands/ft_aggregate_parser.h"

#include "gtest/gtest.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

#include <map>

std::ostream& operator<<(std::ostream& os, RedisModuleString *s) {
    return os << "S=" << *(std::string *)s;
}

namespace valkey_search {
namespace aggregate {


struct FakeIndexInterface : public IndexInterface {
  std::map<std::string, indexes::IndexerType> fields_;
  absl::StatusOr<indexes::IndexerType> GetFieldType(absl::string_view fld_name) const {
    std::string field_name(fld_name);
    std::cout << "Fake make reference " << field_name << "\n";
    auto itr = fields_.find(field_name);
    if (itr == fields_.end()) {
      return absl::NotFoundError(absl::StrCat("Unknown field ", fld_name, " in index."));
    } else {
      return itr->second;
    }
  }
};

struct AggregateTest : public vmsdk::RedisTest {
  void SetUp() {
    fakeIndex.fields_ = {
        { "n1", indexes::IndexerType::kNumeric},
        { "n2", indexes::IndexerType::kNumeric},
    };
    vmsdk::RedisTest::SetUp();
  }
  void TearDown() {
    vmsdk::RedisTest::TearDown();
  }
  FakeIndexInterface fakeIndex;
};

static struct TimeoutTestValue {
  std::string text_;
  std::optional<size_t> value_;
} TimeoutCases[] {
  {"", kTimeoutDefault },
  {"TIMEOUT", std::nullopt}, 
  {"TimeOut 1", 1},
  {"Timeout 0", 0},
  {"Timeout 60000", 60000},
  {"Timeout 60001", 60001},
  {"Timeout fred", std::nullopt}
};

static struct DialectTestValue {
  std::string text_;
  std::optional<size_t> value_;
} DialectCases[] {
  {"", kDialectDefault },
  {"DIALecT", std::nullopt},
  {"Dialect 0", 0},
  {"Dialect 3", 3},
  {"Dialect 5", 5},
  {"Dialect fred", std::nullopt}
};

static struct LoadsTestValue {
  std::string text_;
  std::optional<std::vector<std::string>> value_;
} LoadCases[] {
  {"", std::vector<std::string>{} },
  {"load *", std::vector<std::string>{"*"} },
  {"LOAD 55", std::nullopt },
  {"LOAD 0", std::vector<std::string>{}},
  {"LOAD 1 x", std::vector<std::string>{"x"}},
  {"LOAD 2 x", std::nullopt },
  {"LOAD 2 x y", std::vector<std::string>{"x", "y"}},
};

static void do_preface_test_case(
  FakeIndexInterface *fakeIndex,
  std::string test,
  TimeoutTestValue timeout_test,
  DialectTestValue dialect_test,
  LoadsTestValue loads_test) {
  std::cerr << "Running test: '" << test << "'\n";
  auto argv = vmsdk::ToRedisStringVector(test);
  vmsdk::ArgsIterator itr(argv.data(), argv.size()); 

  AggregateParameters params(fakeIndex);

  auto parser = CreateAggregateParser();

  auto result = parser.Parse(params, itr);
  if (timeout_test.value_ && dialect_test.value_ && loads_test.value_) {
    EXPECT_TRUE(result.ok()) << " Status: " << result;
    EXPECT_EQ(params.timeout_ms_, *timeout_test.value_);
    EXPECT_EQ(params.dialect_, *dialect_test.value_);
    EXPECT_TRUE(loads_test.value_);
    if (loads_test.value_ == std::vector<std::string>{"*"}) {
      EXPECT_TRUE(params.loadall_);
      EXPECT_TRUE(params.loads_.empty());
    } else {
      EXPECT_FALSE(params.loadall_);
      EXPECT_EQ(params.loads_.size(), loads_test.value_->size());
      for (auto i = 0; i < loads_test.value_->size(); ++i) {
        EXPECT_EQ(loads_test.value_->at(i), vmsdk::ToStringView(params.loads_[i].get()));
      }
    }
  } else {
    if (!timeout_test.value_) {
      EXPECT_EQ(params.timeout_ms_, kTimeoutDefault);
    }
    if (!dialect_test.value_) {
      EXPECT_EQ(params.dialect_, kDialectDefault);
    }
  }
}

TEST_F(AggregateTest, PrefaceParserTest) {
  for (auto timeout_test : TimeoutCases) {
    for (auto dialect_test : DialectCases) {
      for (auto loads_test : LoadCases) {
        std::vector<std::string> choices{timeout_test.text_, dialect_test.text_, loads_test.text_};
        for (size_t first_choice : {0, 1, 2}) {
          for (size_t second_choice : {0, 1}) {
            std::vector<std::string> these_choices = choices;
            std::string test;
            test = these_choices[first_choice];
            these_choices.erase(these_choices.begin() + first_choice);
            test += " ";
            test += these_choices[second_choice];
            these_choices.erase(these_choices.begin() + second_choice);
            test += " ";
            test += these_choices[0];
            ASSERT_EQ(these_choices.size(), 1);
            do_preface_test_case(&fakeIndex, test, timeout_test, dialect_test, loads_test);
          }
        }
      }
    }
  }
}

struct teststage {
  const char *stage_in_;
  const char *stage_out_;
};
static std::vector<teststage> teststages {
  {"bogus", nullptr},
  {"LiMiT", nullptr },
  {"LIMIT 10", nullptr },
  {"LIMIT fred", nullptr },
  {"LIMIT 0 10", "LIMIT: 0 10"},
  {"LIMIT 0 10 fred", nullptr},
  {"FiLTER", nullptr },
  {"FILTER fred", nullptr},
  {"FILTER @fred", nullptr},
  {"FILTER @n1 + @n2", nullptr},
  {"FILTER @n1", "FILTER: @n1"},
  {"SORtBY 1 @n1", "SORTBY: ASC:@n1"},
  {"SORTBY 2 @n1 ASC", "SORTBY: ASC:@n1"},
  {"SORTBY 2 @n1 DESC", "SORTBY: DESC:@n1"},
  {"SORTBY", nullptr },
  {"SORTBY 1", nullptr },
  {"SOrTBY 2 @n1", nullptr },
  {"SORTBY 1 @n1 MAX", nullptr},
  {"SORTBY 1 @n1 max 5", "SORTBY: ASC:@n1 MAX:5"},
  {"SOrTBY 2 @n1 max", nullptr },
  {"GrOUPBY 0", nullptr },
  {"GROUPBY 1", nullptr },
  {"GROUPBY 1 fred", nullptr },
  {"GROUPBY 1 @n1", "GROUPBY @n1" },
  {"GROUPBY 1 @n1 REDUCE", nullptr},
  {"GROUPBY 1 @n1 REDUCE COUNT", nullptr },
  {"GROUPBY 1 @n1 REDUCE COUNT 0", "GROUPBY @n1 COUNT() => COUNT()"},
  {"GROUPBY 1 @n1 REDUCE COUNT 0 AS Y", "GROUPBY @n1 COUNT() => Y"},
  {"GROUPBY 1 @n1 REDUCE MIN 1 @n2 as Z", "GROUPBY @n1 MIN(@n2) => Z"},
  {"apply", nullptr},
  {"apply x", nullptr},
  {"apply @n1", nullptr},
  {"apply @n1 xx", nullptr},
  {"APPLY @n1 as ferd", "APPLY: ferd := @n1"},
};


static void do_stage_test(FakeIndexInterface *fakeIndex, std::vector<size_t> indexes) {
  std::string text;
  bool any_bad = false;
  for (auto ix : indexes) {
    text += " ";
    text += teststages[ix].stage_in_;
    any_bad |= teststages[ix].stage_out_ == nullptr;
  }
  std::cout << "Doing case " << text << "\n";
  auto argv = vmsdk::ToRedisStringVector(text);
  vmsdk::ArgsIterator itr(argv.data(), argv.size()); 

  AggregateParameters params(fakeIndex);

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
      EXPECT_EQ(os.str(), teststages[indexes[i]].stage_out_);
    }
  }
}

TEST_F(AggregateTest, StageParserTest) {
  for (size_t i = 0; i < teststages.size(); ++i) {
    do_stage_test(&fakeIndex, std::vector<size_t>{i});
    for (size_t j = 0; j < teststages.size(); ++j) {
      do_stage_test(&fakeIndex, std::vector<size_t>{i, j});
      for (size_t k = 0; k < teststages.size(); ++k) {
        do_stage_test(&fakeIndex, std::vector<size_t>{i, j, k});
      }
    }
  }
}




}
}
