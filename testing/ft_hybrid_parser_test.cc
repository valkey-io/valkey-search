/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Parse-level tests for the FT.HYBRID VSIM clause.
//
// These sit at the parser rather than at the integration level because what
// they check is what the parse *produced* -- the K it settled on, and whether
// it left EF_RUNTIME unset so the index's own value applies. Neither is
// visible in a reply.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/commands/ft_create_parser.h"
#include "src/commands/ft_search_parser.h"
#include "src/indexes/numeric.h"
#include "src/indexes/scoring/scorer.h"
#include "src/query/multi_search.h"
#include "testing/common.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/module_config.h"
#include "vmsdk/src/testing_infra/utils.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search {
namespace query {
namespace {

// CreateVectorHNSWSchema builds a 100-dimension FLOAT32 HNSW index named
// `vector`, so a query blob has to be this many bytes to be accepted.
constexpr size_t kVectorDimensions = 100;

class FTHybridParserTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    index_schema_ =
        CreateVectorHNSWSchema("index_schema_key", &fake_ctx_).value();
    // A numeric field so the SEARCH arm has something to match on. The vector
    // schema helper supplies only the vector field.
    auto numeric =
        std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
    VMSDK_EXPECT_OK(index_schema_->AddIndex("n", "n", numeric));
    EXPECT_CALL(*index_schema_, GetIdentifier(::testing::_))
        .Times(::testing::AnyNumber());
  }

  void TearDown() override {
    // The schema unsubscribes from the KeyspaceEventManager as it dies, so it
    // has to go before the base tears that manager down.
    index_schema_.reset();
    ValkeySearchTest::TearDown();
  }

  // Parses `FT.HYBRID <index> <args...>` and returns the populated
  // parameters. `itr` is positioned where ParseAfterIndex expects it: just
  // past the index name.
  absl::StatusOr<std::unique_ptr<MultiSearchParameters>> Parse(
      const std::vector<std::string> &args) {
    std::vector<std::string> full(args);
    // Every command needs the vector blob bound to $q.
    full.push_back("PARAMS");
    full.push_back("2");
    full.push_back("q");
    full.push_back(std::string(kVectorDimensions * sizeof(float), '\0'));
    return ParseExact(full);
  }

  // The same, with nothing appended. Only for the cases that turn on what
  // follows the last token -- a trailing `POLICY` has a value when a PARAMS
  // clause follows it.
  absl::StatusOr<std::unique_ptr<MultiSearchParameters>> ParseExact(
      const std::vector<std::string> &full) {
    argv_.clear();
    for (const auto &a : full) {
      argv_.push_back(vmsdk::MakeUniqueValkeyString(a));
    }
    std::vector<ValkeyModuleString *> raw;
    raw.reserve(argv_.size());
    for (auto &a : argv_) {
      raw.push_back(a.get());
    }

    auto params = MakeMultiSearchParameters();
    params->index_schema = index_schema_;
    params->index_schema_name = "index_schema_key";
    params->db_num = 0;
    vmsdk::ArgsIterator itr{raw.data(), static_cast<int>(raw.size())};
    VMSDK_RETURN_IF_ERROR(ParseFtHybridCommand(*params, itr));
    return params;
  }

  // The VSIM arm is always the second one.
  static const MultiArmShim &VsimArm(const MultiSearchParameters &p) {
    return *p.arms.at(1);
  }

  ValkeyModuleCtx fake_ctx_;
  std::shared_ptr<MockIndexSchema> index_schema_;
  std::vector<vmsdk::UniqueValkeyString> argv_;
};

// ---------------------------------------------------------------------
// K
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, KnnBlockOmittedDefaultsKToTen) {
  // No KNN block at all. Redis applies the same default for this spelling.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
}

TEST_F(FTHybridParserTest, KnnBlockOmittedBeforeAnotherClauseDefaultsKToTen) {
  // The token after `$q` belongs to an enclosing clause, so there is no block
  // to read and the default still has to apply.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q",
                       "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "hs"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
}

TEST_F(FTHybridParserTest, KnnBlockOmittedBeforeYieldScoreAsDefaultsKToTen) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "YIELD_SCORE_AS", "vs"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
  EXPECT_EQ(vmsdk::ToStringView(VsimArm(**params).score_as.get()), "vs");
}

TEST_F(FTHybridParserTest, EmptyKnnBlockDefaultsKToTen) {
  // `KNN 0` is a block with no sub-arguments; it means what omitting the
  // block means.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "0"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
}

TEST_F(FTHybridParserTest, KnnBlockWithoutKDefaultsKToTen) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "EF_RUNTIME", "40"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
}

TEST_F(FTHybridParserTest, ExplicitKOverridesTheDefault) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "37"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 37);
}

// ---------------------------------------------------------------------
// EF_RUNTIME
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, EfRuntimeOmittedLeavesTheIndexDefaultInPlace) {
  // An unset `ef` is what makes VectorBase::Search fall through to the
  // EF_RUNTIME the index was created with. Setting it to some number here
  // would silently override the index definition for every query.
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_FALSE(VsimArm(**params).ef.has_value());
}

TEST_F(FTHybridParserTest, EfRuntimeOverridesTheIndexDefault) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "5", "EF_RUNTIME", "250"});
  VMSDK_EXPECT_OK(params);
  ASSERT_TRUE(VsimArm(**params).ef.has_value());
  EXPECT_EQ(*VsimArm(**params).ef, 250u);
}

TEST_F(FTHybridParserTest, EfRuntimeIsIndependentOfK) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "EF_RUNTIME", "64"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
  ASSERT_TRUE(VsimArm(**params).ef.has_value());
  EXPECT_EQ(*VsimArm(**params).ef, 64u);
}

// ---------------------------------------------------------------------
// SHARD_K_RATIO -- accepted and discarded
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, ShardKRatioIsAcceptedAndDoesNotDisturbK) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "5", "SHARD_K_RATIO", "0.25"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
  EXPECT_FALSE(VsimArm(**params).ef.has_value());
}

TEST_F(FTHybridParserTest, ShardKRatioAloneStillLeavesTheKDefault) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "SHARD_K_RATIO", "1.0"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
}

TEST_F(FTHybridParserTest, ShardKRatioRejectsANonNumericValue) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "5", "SHARD_K_RATIO", "banana"});
  EXPECT_FALSE(params.ok());
}

// ---------------------------------------------------------------------
// YIELD_SCORE_AS placement
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, YieldScoreAsInsideTheKnnBlockIsRejected) {
  // It names the arm, not the KNN search. Redis rejects this spelling too.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "5", "YIELD_SCORE_AS", "vs"});
  ASSERT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("Unknown VSIM KNN sub-arg"));
}

TEST_F(FTHybridParserTest, YieldScoreAsAfterTheKnnBlockNamesTheArm) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "YIELD_SCORE_AS", "vs"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
  EXPECT_EQ(vmsdk::ToStringView(VsimArm(**params).score_as.get()), "vs");
}

TEST_F(FTHybridParserTest, YieldScoreAsInsideTheRangeBlockIsRejected) {
  // Same rule for RANGE. The clause is reported unimplemented only after the
  // shape is validated, so an unknown sub-arg is what surfaces here.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "RANGE",
                       "4", "RADIUS", "5", "YIELD_SCORE_AS", "vs"});
  ASSERT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("Unknown VSIM RANGE sub-arg"));
}

// ---------------------------------------------------------------------
// Mode token
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, UnknownModeTokenIsRejected) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "BOGUS", "2", "K", "5"});
  ASSERT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("VSIM expects KNN or RANGE"));
}

TEST_F(FTHybridParserTest, RangeIsParsedThenReportedUnimplemented) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "RANGE",
                       "2", "RADIUS", "5"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().code(), absl::StatusCode::kUnimplemented);
}

// ---------------------------------------------------------------------
// A score alias naming a column LOAD also emits
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, ArmScoreAliasCollidingWithALoadedFieldIsRejected) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "n", "VSIM", "@vector",
             "$q", "KNN", "2", "K", "5", "LOAD", "1", "@n"});
  ASSERT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("collides with a column loaded by LOAD"));
}

TEST_F(FTHybridParserTest, FusedScoreAliasCollidingWithALoadedFieldIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                       "n", "LOAD", "1", "@n"});
  ASSERT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("collides with a column loaded by LOAD"));
}

TEST_F(FTHybridParserTest,
       ScoreAliasCollidingWithLoadAllIsAcceptedAndResolvedAtRuntime) {
  // `LOAD *` names no fields; it emits whatever each document happens to
  // carry, which the query cannot be expected to know. Rejecting it refused
  // commands that are perfectly serviceable, so the clash is resolved at
  // runtime instead: the named score wins over the database field.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "n", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "LOAD", "*"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, ScoreAliasNotNamingALoadedColumnIsAccepted) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "text_score", "VSIM",
             "@vector", "$q", "KNN", "2", "K", "5", "LOAD", "1", "@n"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, ScoreAliasCollidingWithNoLoadClauseIsAccepted) {
  // Nothing is loaded, so nothing collides -- the alias is the only thing
  // claiming that column name.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "n", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
}

// ---------------------------------------------------------------------
// VSIM FILTER: a pre-filter on the vector search
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, VsimFilterBecomesAPrefilteredVectorQuery) {
  // The arm is handed to the ordinary query parser as
  // `<filter>=>[KNN k @field $param]`, so what proves the filter took effect is
  // that the arm now carries a predicate -- a pure VSIM arm has none.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "FILTER", "@n:[0 3]"});
  VMSDK_EXPECT_OK(params);
  const auto &arm = VsimArm(**params);
  EXPECT_EQ(arm.k, 5);
  EXPECT_NE(arm.filter_parse_results.root_predicate, nullptr);
  // And its score stays the vector distance whatever the filter contains.
  EXPECT_TRUE(arm.vector_score_only);
  EXPECT_TRUE((*params)->per_arm_score_is_distance.at(1));
}

TEST_F(FTHybridParserTest, VsimFilterAcceptsAnOptionalCount) {
  auto without = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                        "2", "K", "5", "FILTER", "@n:[0 3]"});
  VMSDK_EXPECT_OK(without);
  auto with = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2",
                     "K", "5", "FILTER", "1", "@n:[0 3]"});
  VMSDK_EXPECT_OK(with);
  EXPECT_EQ(VsimArm(**without).k, VsimArm(**with).k);
}

TEST_F(FTHybridParserTest, VsimFilterCountSwallowsItsPolicyOptions) {
  // POLICY and BATCH_SIZE tune how the pre-filter runs rather than what it
  // answers, so they are consumed and discarded. The count is a raw token
  // count, as on the reference engine.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "FILTER", "5", "@n:[0 3]", "POLICY",
                       "BATCHES", "BATCH_SIZE", "10"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
}

TEST_F(FTHybridParserTest, VsimFilterAcceptsPolicyWithoutACount) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "FILTER", "@n:[0 3]", "POLICY", "ADHOC_BF"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
}

TEST_F(FTHybridParserTest, VsimFilterCountCannotRunPastTheArguments) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "FILTER", "9", "@n:[0 3]"});
  EXPECT_FALSE(params.ok());
}

TEST_F(FTHybridParserTest, VsimFilterBeforeTheKnnBlockIsRejected) {
  // The reference rejects this order too: the block comes first.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "FILTER",
                       "@n:[0 3]", "KNN", "2", "K", "5"});
  EXPECT_FALSE(params.ok());
}

TEST_F(FTHybridParserTest, AVsimArmWithNoFilterIsNotAPrefilteredQuery) {
  // The control: without a FILTER the arm keeps the direct path, carrying no
  // predicate and no score override.
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).filter_parse_results.root_predicate, nullptr);
  EXPECT_FALSE(VsimArm(**params).vector_score_only);
}

// ---------------------------------------------------------------------
// The name the fused score is generated under, and whether it reaches the
// reply. Measured against the reference: with no LOAD clause the default
// projection is the key and the score, any LOAD replaces that projection, and
// a COMBINE ... YIELD_SCORE_AS name is an explicit request that survives it.
// None of this is visible in the parse result other than here -- the reply
// path reads `agg->score_as` and `agg->suppressed_reply_column_`.
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, NoCombineAliasNamesTheScoreScoreAndEmitsIt) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->output_score_name, "__score");
  EXPECT_FALSE((*params)->output_score_name_explicit);
  EXPECT_EQ(vmsdk::ToStringView((*params)->agg->score_as.get()), "__score");
  EXPECT_FALSE((*params)->agg->suppressed_reply_column_.has_value());
}

TEST_F(FTHybridParserTest, CombineYieldScoreAsRenamesTheScore) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "hs"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->output_score_name, "hs");
  EXPECT_TRUE((*params)->output_score_name_explicit);
  EXPECT_EQ(vmsdk::ToStringView((*params)->agg->score_as.get()), "hs");
  EXPECT_FALSE((*params)->agg->suppressed_reply_column_.has_value());
}

TEST_F(FTHybridParserTest, LoadAllHidesTheDefaultScore) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "LOAD", "*"});
  VMSDK_EXPECT_OK(params);
  // The column still exists -- a SORTBY on it has to resolve -- it just does
  // not reach the caller.
  EXPECT_EQ(vmsdk::ToStringView((*params)->agg->score_as.get()), "__score");
  EXPECT_EQ((*params)->agg->suppressed_reply_column_,
            aggregate::AggregateParameters::kScoreColumn);
}

TEST_F(FTHybridParserTest, ANamedLoadHidesTheDefaultScoreToo) {
  // The case that separates "LOAD *" from "a LOAD clause": the reference drops
  // the default score for either.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "LOAD", "1", "@n"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->agg->suppressed_reply_column_,
            aggregate::AggregateParameters::kScoreColumn);
}

TEST_F(FTHybridParserTest, AnExplicitScoreNameSurvivesALoadClause) {
  // `LOAD *` emits every schema field, so the alias is rejected if the schema
  // claims to have a field by that name. The mock answers GetIdentifier for
  // anything unless told otherwise, so say that `hs` is not a field -- which
  // is what a real schema would say.
  EXPECT_CALL(*index_schema_, GetIdentifier(absl::string_view("hs")))
      .WillRepeatedly(::testing::Return(absl::NotFoundError("no such field")));
  for (const std::vector<std::string> &load :
       {std::vector<std::string>{"LOAD", "*"},
        std::vector<std::string>{"LOAD", "1", "@n"}}) {
    std::vector<std::string> args{"SEARCH",
                                  "@n:[0 10]",
                                  "VSIM",
                                  "@vector",
                                  "$q",
                                  "KNN",
                                  "2",
                                  "K",
                                  "5",
                                  "COMBINE",
                                  "RRF",
                                  "2",
                                  "YIELD_SCORE_AS",
                                  "hs"};
    args.insert(args.end(), load.begin(), load.end());
    auto params = Parse(args);
    VMSDK_EXPECT_OK(params);
    EXPECT_EQ((*params)->output_score_name, "hs");
    EXPECT_FALSE((*params)->agg->suppressed_reply_column_.has_value());
  }
}

TEST_F(FTHybridParserTest, ALoadThatNamesTheScoreKeepsIt) {
  // A LOAD replaces the default projection the score would otherwise come
  // from, so naming it in the clause is the only way to get the score
  // alongside a chosen set of fields.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "LOAD", "1", "@__score"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(vmsdk::ToStringView((*params)->agg->score_as.get()), "__score");
  EXPECT_FALSE((*params)->agg->suppressed_reply_column_.has_value());
}

TEST_F(FTHybridParserTest, ALoadThatNamesTheScoreBesideAFieldKeepsIt) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "LOAD", "2", "@n", "@__score"});
  VMSDK_EXPECT_OK(params);
  EXPECT_FALSE((*params)->agg->suppressed_reply_column_.has_value());
}

TEST_F(FTHybridParserTest, ALoadThatDoesNotNameTheScoreStillHidesIt) {
  // The control for the two above: it is naming the column that keeps it, not
  // the mere presence of a LOAD entry that happens to resolve.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "LOAD", "1", "@n"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->agg->suppressed_reply_column_,
            aggregate::AggregateParameters::kScoreColumn);
}

TEST_F(FTHybridParserTest, YieldingScoreAsScoreWithNoLoadIsRejected) {
  // It names the column the default projection already generates. The
  // reference refuses it rather than emitting one column twice.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "__score"});
  EXPECT_FALSE(params.ok());
}

TEST_F(FTHybridParserTest, YieldingScoreAsScoreIsFineOnceALoadClauseExists) {
  EXPECT_CALL(*index_schema_, GetIdentifier(absl::string_view("__score")))
      .WillRepeatedly(::testing::Return(absl::NotFoundError("no such field")));
  // With a LOAD clause there is no default projection to collide with, so the
  // same alias is accepted -- as the reference accepts it.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                       "__score", "LOAD", "*"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->output_score_name, "__score");
  EXPECT_FALSE((*params)->agg->suppressed_reply_column_.has_value());
}

TEST_F(FTHybridParserTest, CombineFunctionTakesAYieldScoreAsToo) {
  // FUNCTION is ours alone, so nothing outside this repo pins it. It has to
  // follow the same naming rule as RRF and LINEAR.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "FUNCTION", "4", "EXPR",
             "@__search_score + @__vector_score", "YIELD_SCORE_AS", "fx"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->output_score_name, "fx");
  EXPECT_TRUE((*params)->output_score_name_explicit);
}

TEST_F(FTHybridParserTest, CombineFunctionWithNoAliasKeepsTheDefaultName) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "FUNCTION", "2", "EXPR",
                       "@__search_score + @__vector_score"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->output_score_name, "__score");
  EXPECT_FALSE((*params)->output_score_name_explicit);
}

// ---------------------------------------------------------------------
// COMBINE scalar arguments.
//
// The shared `ParseParamValue` reaches `std::from_chars`, which succeeds on
// whatever prefix of the token looks numeric, so every one of these was
// previously accepted and silently acted on as a different number than the
// caller wrote. The reference rejects them.
// ---------------------------------------------------------------------

// The `<count>` after the method is a token span, and it used to reach
// std::from_chars, which reports success on a numeric prefix: `RRF 2abc` read
// as 2. The sub-args were given whole-token parsing for exactly this reason;
// the count two positions earlier was missed.
TEST_F(FTHybridParserTest, CombineCountRejectsAnythingButAWholeNumber) {
  for (absl::string_view bad :
       {"2abc", "4.5", "1e3", "0x10", "-1", "", "abc"}) {
    auto params =
        Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
               "5", "COMBINE", "RRF", std::string(bad), "CONSTANT", "60"});
    EXPECT_FALSE(params.ok()) << "COMBINE count accepted `" << bad << "`";
  }
}

TEST_F(FTHybridParserTest, VsimBlockCountRejectsAnythingButAWholeNumber) {
  for (absl::string_view bad :
       {"2abc", "4.5", "1e3", "0x10", "-1", "", "abc"}) {
    auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                         std::string(bad), "K", "5"});
    EXPECT_FALSE(params.ok()) << "KNN count accepted `" << bad << "`";
  }
}

// Every other sub-arg rejects an empty token; the alias did not, and an empty
// name would become the score column's output name.
TEST_F(FTHybridParserTest, YieldScoreAsRejectsAnEmptyAlias) {
  auto search_arm = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "", "VSIM",
                           "@vector", "$q", "KNN", "2", "K", "5"});
  EXPECT_FALSE(search_arm.ok());
  auto vsim_arm = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                         "2", "K", "5", "YIELD_SCORE_AS", ""});
  EXPECT_FALSE(vsim_arm.ok());
  auto combine =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS", ""});
  EXPECT_FALSE(combine.ok());
}

// The defaults a command-schema entry has to state. Nothing pinned these
// before: they lived only in FusionConfig's member initialisers.
TEST_F(FTHybridParserTest, OmittedCombineSubArgsTakeTheDocumentedDefaults) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "0"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.method, query::FusionConfig::Method::kRRF);
  EXPECT_EQ((*params)->fusion.window, 20u);
  EXPECT_DOUBLE_EQ((*params)->fusion.rrf_constant, 60.0);
  EXPECT_FALSE((*params)->fusion.alpha.has_value());
  EXPECT_FALSE((*params)->fusion.beta.has_value());
}

TEST_F(FTHybridParserTest, NoCombineClauseIsRrfWithTheSameDefaults) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.method, query::FusionConfig::Method::kRRF);
  EXPECT_EQ((*params)->fusion.window, 20u);
  EXPECT_DOUBLE_EQ((*params)->fusion.rrf_constant, 60.0);
}

// LINEAR's weights go together or not at all. The reference is asymmetric
// about this -- neither weight takes 0.3/0.7, exactly one is
// `SEARCH_SYNTAX Missing value for BETA` -- and these pin both halves.
TEST_F(FTHybridParserTest, LinearWithNeitherWeightTakesTheDefaultWeights) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "LINEAR", "0"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.method, query::FusionConfig::Method::kLinear);
  ASSERT_TRUE((*params)->fusion.alpha.has_value());
  ASSERT_TRUE((*params)->fusion.beta.has_value());
  EXPECT_DOUBLE_EQ(*(*params)->fusion.alpha, 0.3);
  EXPECT_DOUBLE_EQ(*(*params)->fusion.beta, 0.7);
  // The default weights are not a special case downstream: WINDOW still takes
  // the shared RRF/LINEAR default rather than FUNCTION's widest-allowed one.
  EXPECT_EQ((*params)->fusion.window, 20u);
}

// The other sub-arguments do not make the weights written, so a block that
// carries only WINDOW or only YIELD_SCORE_AS still defaults them.
TEST_F(FTHybridParserTest, LinearDefaultWeightsSurviveOtherSubArgs) {
  const std::vector<std::vector<std::string>> tails = {
      {"LINEAR", "2", "WINDOW", "5"},
      {"LINEAR", "2", "YIELD_SCORE_AS", "fs"},
      {"LINEAR", "4", "WINDOW", "5", "YIELD_SCORE_AS", "fs"},
  };
  for (const auto &tail : tails) {
    std::vector<std::string> argv = {"SEARCH", "@n:[0 10]", "VSIM", "@vector",
                                     "$q",     "KNN",       "2",    "K",
                                     "5",      "COMBINE"};
    argv.insert(argv.end(), tail.begin(), tail.end());
    auto params = Parse(argv);
    VMSDK_EXPECT_OK(params) << "rejected `" << absl::StrJoin(tail, " ") << "`";
    ASSERT_TRUE((*params)->fusion.alpha.has_value());
    ASSERT_TRUE((*params)->fusion.beta.has_value());
    EXPECT_DOUBLE_EQ(*(*params)->fusion.alpha, 0.3);
    EXPECT_DOUBLE_EQ(*(*params)->fusion.beta, 0.7);
  }
}

TEST_F(FTHybridParserTest, LinearWithBothWeightsKeepsWhatWasWritten) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "LINEAR", "4", "ALPHA", "0.25", "BETA", "1.5"});
  VMSDK_EXPECT_OK(params);
  ASSERT_TRUE((*params)->fusion.alpha.has_value());
  ASSERT_TRUE((*params)->fusion.beta.has_value());
  EXPECT_DOUBLE_EQ(*(*params)->fusion.alpha, 0.25);
  EXPECT_DOUBLE_EQ(*(*params)->fusion.beta, 1.5);
}

// Writing exactly one weight is an error, and stays one even when the written
// value equals the default it would otherwise have taken -- the rule is about
// which weights were written, not about what they say.
TEST_F(FTHybridParserTest, LinearWithExactlyOneWeightIsRejected) {
  const std::vector<std::vector<std::string>> tails = {
      {"LINEAR", "2", "ALPHA", "0.5"},
      {"LINEAR", "2", "BETA", "0.5"},
      {"LINEAR", "2", "ALPHA", "0.3"},
      {"LINEAR", "2", "BETA", "0.7"},
      {"LINEAR", "4", "ALPHA", "0.5", "WINDOW", "5"},
      {"LINEAR", "4", "BETA", "0.5", "YIELD_SCORE_AS", "fs"},
  };
  for (const auto &tail : tails) {
    std::vector<std::string> argv = {"SEARCH", "@n:[0 10]", "VSIM", "@vector",
                                     "$q",     "KNN",       "2",    "K",
                                     "5",      "COMBINE"};
    argv.insert(argv.end(), tail.begin(), tail.end());
    EXPECT_FALSE(Parse(argv).ok())
        << "accepted `" << absl::StrJoin(tail, " ") << "`";
  }
}

// A sub-arg belonging to another method is refused rather than ignored. Worth
// pinning: a review thread claimed `COMBINE RRF 1 ALPHA 0.5` parsed silently,
// and nothing in the suite contradicted it.
TEST_F(FTHybridParserTest, CrossMethodCombineSubArgsAreRejected) {
  const std::vector<std::vector<std::string>> cases = {
      {"RRF", "2", "ALPHA", "0.5"},
      {"RRF", "2", "BETA", "0.5"},
      {"RRF", "2", "EXPR", "@__search_score"},
      {"LINEAR", "6", "ALPHA", "0.5", "BETA", "0.5", "CONSTANT", "60"},
      {"FUNCTION", "4", "EXPR", "@__search_score", "CONSTANT", "60"},
  };
  for (const auto &tail : cases) {
    std::vector<std::string> argv = {"SEARCH", "@n:[0 10]", "VSIM", "@vector",
                                     "$q",     "KNN",       "2",    "K",
                                     "5",      "COMBINE"};
    argv.insert(argv.end(), tail.begin(), tail.end());
    auto params = Parse(argv);
    EXPECT_FALSE(params.ok()) << "COMBINE accepted a cross-method sub-arg: "
                              << absl::StrJoin(tail, " ");
  }
}

TEST_F(FTHybridParserTest, WindowRejectsAnythingButAWholeNumber) {
  for (absl::string_view bad :
       {"1.5", "20abc", "abc", "", "-1", "1e3", "0x10", "4294967296"}) {
    auto params =
        Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
               "5", "COMBINE", "RRF", "2", "WINDOW", std::string(bad)});
    EXPECT_FALSE(params.ok()) << "WINDOW accepted `" << bad << "`";
  }
}

TEST_F(FTHybridParserTest, WindowTakesAWholeNumber) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "WINDOW", "7"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 7u);
}

TEST_F(FTHybridParserTest, WindowAcceptsTheConfiguredMaximum) {
  // max-combine-window defaults to 1,000,000; the boundary itself is legal.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "WINDOW", "1000000"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 1000000u);
}

TEST_F(FTHybridParserTest, WindowRejectsAboveTheConfiguredMaximum) {
  // One past max-combine-window. WINDOW sizes both the fusion stage and, in
  // cluster mode, every shard's fetch, so it is bounded rather than trusted.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "WINDOW", "1000001"});
  EXPECT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("COMBINE WINDOW is out of range"));
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("maximum is 1000000"));
}

TEST_F(FTHybridParserTest, WindowZeroResolvesToTheMaximum) {
  // Not a pass-through of the reference, which rejects `WINDOW 0` outright.
  // Zero is this engine's "do not cap the arms", and it is resolved here
  // rather than carried: a zero reaching the cluster fanout would be turned
  // into a per-shard fetch limit of 10 by std::max(window, 10), which is the
  // opposite of what the user asked for.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "WINDOW", "0"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 1000000u);
}

TEST_F(FTHybridParserTest, FunctionDefaultsToTheWidestWindowAllowed) {
  // A user expression is expected to see every candidate, so FUNCTION with no
  // WINDOW must not inherit the RRF/LINEAR default of 20. It gets the same
  // resolved ceiling an explicit `WINDOW 0` does, for the same reason.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "FUNCTION", "2", "EXPR",
                       "@__search_score + @__vector_score"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 1000000u);
}

TEST_F(FTHybridParserTest, RrfConstantIsAFractionalNumber) {
  // The reference honours fractional constants -- `CONSTANT 1.5` ranks
  // strictly between 1 and 2 -- so truncating to an integer is wrong.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "CONSTANT", "1.5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_DOUBLE_EQ((*params)->fusion.rrf_constant, 1.5);
}

TEST_F(FTHybridParserTest, RrfConstantRejectsJunkAndNonFiniteAndNegative) {
  for (absl::string_view bad : {"60abc", "abc", "", "nan", "inf", "-inf",
                                "1e400", "-1", "-0.5", "0.5.5"}) {
    auto params =
        Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
               "5", "COMBINE", "RRF", "2", "CONSTANT", std::string(bad)});
    EXPECT_FALSE(params.ok()) << "CONSTANT accepted `" << bad << "`";
  }
}

TEST_F(FTHybridParserTest, RrfConstantAcceptsZeroAndLargeValues) {
  for (absl::string_view good : {"0", "0.5", "60", "4294967296", "1e3"}) {
    auto params =
        Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
               "5", "COMBINE", "RRF", "2", "CONSTANT", std::string(good)});
    VMSDK_EXPECT_OK(params) << "CONSTANT rejected `" << good << "`";
  }
}

TEST_F(FTHybridParserTest, AlphaAndBetaMustBeFiniteNumbers) {
  // `nan` and `inf` reach a fused score and poison every comparison against
  // it. std::isfinite cannot catch them here: the project builds with
  // -ffast-math, which folds that call to true, so the parser tests the IEEE
  // bit pattern instead. These cases are what prove it.
  for (absl::string_view bad :
       {"abc", "", "nan", "inf", "-inf", "1e400", "0.5.5", "1,5", "1/2"}) {
    auto alpha = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                        "2", "K", "5", "COMBINE", "LINEAR", "4", "ALPHA",
                        std::string(bad), "BETA", "0.5"});
    EXPECT_FALSE(alpha.ok()) << "ALPHA accepted `" << bad << "`";
    auto beta = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "LINEAR", "4", "ALPHA", "0.5",
                       "BETA", std::string(bad)});
    EXPECT_FALSE(beta.ok()) << "BETA accepted `" << bad << "`";
  }
}

TEST_F(FTHybridParserTest, AlphaAndBetaAcceptAnyFiniteWeight) {
  // Negative and greater-than-one weights are meaningful -- they subtract an
  // arm, or amplify it -- and the reference accepts both.
  for (absl::string_view good :
       {"0", "0.5", "1", "-0.5", "2.5", "1e3", "+0.5"}) {
    auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                         "2", "K", "5", "COMBINE", "LINEAR", "4", "ALPHA",
                         std::string(good), "BETA", "0.5"});
    VMSDK_EXPECT_OK(params) << "ALPHA rejected `" << good << "`";
  }
}

// ---------------------------------------------------------------------
// Per-arm score aliases in a pipeline stage.
//
// Fusion writes each arm's score into the fused record's attribute map, but
// nothing declares it a pipeline column, so a stage naming it used to be
// rejected against the index schema. FT.HYBRID's index-interface shim now
// answers for these names; what that produces is only visible in the parse
// result, which is why these sit here.
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, ASortByAPerArmAliasResolves) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM", "@vector",
             "$q", "KNN", "2", "K", "5", "SORTBY", "2", "@ts", "DESC"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, APerArmAliasColumnIsNumeric) {
  // Not cosmetic: the column is filled by parsing the text fusion wrote, and
  // only a numeric column parses it. A string column would order 0.9 above
  // 0.53 while still looking like a successful sort.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM", "@vector",
             "$q", "KNN", "2", "K", "5", "SORTBY", "2", "@ts", "DESC"});
  VMSDK_EXPECT_OK(params);
  const auto &info = (*params)->agg->record_info_by_index_;
  auto it = std::find_if(info.begin(), info.end(),
                         [](const auto &i) { return i.output_name_ == "ts"; });
  ASSERT_NE(it, info.end()) << "no column was created for the alias";
  EXPECT_EQ(it->data_type_, indexes::IndexerType::kNumeric);
  // Its identifier has to be the alias itself: that is the key fusion used,
  // and it is what the record-population step matches a column against.
  EXPECT_EQ(it->identifier_, "ts");
}

TEST_F(FTHybridParserTest, TheVsimArmsAliasResolvesToo) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "YIELD_SCORE_AS", "vs", "SORTBY", "2", "@vs", "ASC"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, AGroupByAPerArmAliasResolves) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "GROUPBY", "1",
                       "@ts", "REDUCE", "COUNT", "0", "AS", "cnt"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, APerArmAliasIsNotReachableUnderLoadAll) {
  // `LOAD *` projects the document's own fields; a fused score is not one of
  // them, and the reference refuses the reference there.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "LOAD", "*",
                       "SORTBY", "2", "@ts", "DESC"});
  EXPECT_FALSE(params.ok());
}

TEST_F(FTHybridParserTest, APerArmAliasIsReachableUnderANamedLoad) {
  // The control for the case above: a LOAD clause as such does not hide it.
  EXPECT_CALL(*index_schema_, GetIdentifier(absl::string_view("n")))
      .WillRepeatedly(::testing::Return(std::string("n")));
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "LOAD", "1", "@n",
                       "SORTBY", "2", "@ts", "DESC"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, NoColumnIsCreatedForAnAliasNoStageNames) {
  // The alias still reaches the reply, by the same path as before; creating a
  // column for it unasked would move it out of that path for every query.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  const auto &info = (*params)->agg->record_info_by_index_;
  EXPECT_TRUE(std::none_of(info.begin(), info.end(), [](const auto &i) {
    return i.output_name_ == "ts";
  }));
}

TEST_F(FTHybridParserTest, AnArmAliasNamingTheFusedScoreIsRejected) {
  // It would write the arm's score into the fused score's column, and the
  // caller could not tell which they were reading.
  auto by_default = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "__score",
                           "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  EXPECT_FALSE(by_default.ok());
  auto renamed = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "hs", "VSIM",
                        "@vector", "$q", "KNN", "2", "K", "5", "COMBINE", "RRF",
                        "2", "YIELD_SCORE_AS", "hs"});
  EXPECT_FALSE(renamed.ok());
  // And the VSIM arm, which is parsed by a different function.
  auto vsim = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2",
                     "K", "5", "YIELD_SCORE_AS", "__score"});
  EXPECT_FALSE(vsim.ok());
}

// ---------------------------------------------------------------------
// Range limits -- the same ones FT.SEARCH enforces in VerifyQueryString
// ---------------------------------------------------------------------

// Holds a configurable limit at `value` for the duration of a test, then puts
// back whatever it was, so one test's limit cannot leak into the next.
class ScopedLimit {
 public:
  ScopedLimit(vmsdk::config::Number &option, long long value)
      : option_(option), saved_(option.GetValue()) {
    VMSDK_EXPECT_OK(option.SetValue(value));
  }
  ~ScopedLimit() { VMSDK_EXPECT_OK(option_.SetValue(saved_)); }

 private:
  vmsdk::config::Number &option_;
  long long saved_;
};

TEST_F(FTHybridParserTest, KAboveTheMaxIsRejected) {
  ScopedLimit limit(options::GetMaxKnn(), 5);
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "6"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "Invalid range: Value above maximum; KNN parameter must be a "
            "positive integer greater than 0 and cannot exceed 5.");
}

TEST_F(FTHybridParserTest, KAtTheMaxIsAccepted) {
  // The control for the above: it is the limit that rejects, not the clause.
  ScopedLimit limit(options::GetMaxKnn(), 5);
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
}

TEST_F(FTHybridParserTest, KOfZeroIsRejected) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "0"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            absl::StrCat("Invalid range: Value below minimum; KNN parameter "
                         "must be a positive integer greater than 0 and cannot "
                         "exceed ",
                         options::GetMaxKnn().GetValue(), "."));
}

TEST_F(FTHybridParserTest, TheDefaultKIsBoundedByTheMaxToo) {
  // With no KNN block the default K applies, and a max below it has to refuse
  // the command rather than let the default through unchecked.
  ScopedLimit limit(options::GetMaxKnn(), 5);
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "Invalid range: Value above maximum; KNN parameter must be a "
            "positive integer greater than 0 and cannot exceed 5.");
}

TEST_F(FTHybridParserTest, EfRuntimeAboveTheMaxIsRejected) {
  ScopedLimit limit(options::GetMaxEfRuntime(), 5);
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "3", "EF_RUNTIME", "6"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "Invalid range: Value above maximum; `EF_RUNTIME` must be a "
            "positive integer greater than 0 and cannot exceed 5.");
}

TEST_F(FTHybridParserTest, EfRuntimeAtTheMaxIsAccepted) {
  ScopedLimit limit(options::GetMaxEfRuntime(), 5);
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "3", "EF_RUNTIME", "5"});
  VMSDK_EXPECT_OK(params);
  ASSERT_TRUE(VsimArm(**params).ef.has_value());
  EXPECT_EQ(*VsimArm(**params).ef, 5u);
}

TEST_F(FTHybridParserTest, EfRuntimeOfZeroIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "4", "K", "3", "EF_RUNTIME", "0"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            absl::StrCat("Invalid range: Value below minimum; `EF_RUNTIME` "
                         "must be a positive integer greater than 0 and cannot "
                         "exceed ",
                         options::GetMaxEfRuntime().GetValue(), "."));
}

TEST_F(FTHybridParserTest, TimeoutAboveTheMaxIsRejected) {
  const auto max_timeout_ms = options::GetMaxTimeoutMs().GetValue();
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "TIMEOUT", absl::StrCat(max_timeout_ms + 1)});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            absl::StrCat("TIMEOUT must be a positive integer greater than 0 "
                         "and cannot exceed ",
                         max_timeout_ms, "."));
}

TEST_F(FTHybridParserTest, TimeoutAtTheMaxIsAccepted) {
  const auto max_timeout_ms = options::GetMaxTimeoutMs().GetValue();
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "TIMEOUT", absl::StrCat(max_timeout_ms)});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->timeout_ms, static_cast<uint64_t>(max_timeout_ms));
}

// ---------------------------------------------------------------------
// POLICY / BATCH_SIZE inside the VSIM clause
//
// Both tune how the vector search runs rather than what it answers, so the
// value is read and discarded. What matters here is that reading it does not
// end the VSIM clause -- the token after it still belongs to VSIM.
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, PolicyInsideVsimDoesNotSwallowTheNextToken) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "POLICY", "BATCHES", "YIELD_SCORE_AS", "vs"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
  EXPECT_EQ(vmsdk::ToStringView(VsimArm(**params).score_as.get()), "vs");
}

TEST_F(FTHybridParserTest, BatchSizeInsideVsimDoesNotSwallowTheNextToken) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "BATCH_SIZE", "10", "YIELD_SCORE_AS", "vs"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
  EXPECT_EQ(vmsdk::ToStringView(VsimArm(**params).score_as.get()), "vs");
}

TEST_F(FTHybridParserTest, PolicyInsideVsimWithNoValueIsRejected) {
  // Parsed without the usual PARAMS suffix: with one, the clause would read
  // `PARAMS` as POLICY's value.
  auto params = ParseExact({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q",
                            "KNN", "2", "K", "5", "POLICY"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), "POLICY requires a value");
}

TEST_F(FTHybridParserTest, BatchSizeInsideVsimWithNoValueIsRejected) {
  auto params = ParseExact({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q",
                            "KNN", "2", "K", "5", "BATCH_SIZE"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), "BATCH_SIZE requires a value");
}

TEST_F(FTHybridParserTest, PolicyWithNoModeBlockStillParses) {
  // The regression guard for the spelling that reaches POLICY with no KNN
  // block at all: the clause has to accept it and keep the default K.
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "POLICY", "local"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 10);
}

TEST_F(FTHybridParserTest, PolicyBeforeAnAggregateStageStillParses) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "POLICY", "local", "LIMIT", "0", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ(VsimArm(**params).k, 5);
}

TEST_F(FTHybridParserTest, PolicyInsideTheSearchClauseIsRejected) {
  // POLICY is a top-level clause; inside SEARCH it used to end the arm and
  // leave the next token looking like a missing VSIM clause.
  auto params = Parse({"SEARCH", "@n:[0 10]", "POLICY", "local", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "POLICY is not supported in the SEARCH clause");
}

// ---------------------------------------------------------------------
// A vector query belongs in VSIM, not in SEARCH
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, AVectorQueryInTheSearchArmIsRejected) {
  auto params = Parse({"SEARCH", "*=>[KNN 5 @vector $q]", "VSIM", "@vector",
                       "$q", "KNN", "2", "K", "5"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "A vector query is not supported in the SEARCH clause; use VSIM");
}

TEST_F(FTHybridParserTest, ATextQueryInTheSearchArmIsAccepted) {
  // The control for the above: it is the vector clause that is refused, not
  // the SEARCH arm's query.
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_FALSE((*params)->arms.at(0)->IsVectorQuery());
}

// ---------------------------------------------------------------------
// DIALECT -- range-checked, as FT.SEARCH and FT.AGGREGATE check it, except
// that FT.HYBRID supports the one dialect.
// ---------------------------------------------------------------------

TEST_F(FTHybridParserTest, DialectTwoIsAccepted) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "DIALECT", "2"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->agg->dialect, 2);
}

TEST_F(FTHybridParserTest, DialectBelowTwoIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "DIALECT", "1"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "DIALECT requires a non negative integer >=2 and <= 2");
}

TEST_F(FTHybridParserTest, DialectAboveTwoIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "DIALECT", "3"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(),
            "DIALECT requires a non negative integer >=2 and <= 2");
}

// ---------------------------------------------------------------------
// YIELD_SCORE_AS __key -- the reserved key column
//
// `__key` is the aggregate's column 0, seeded before any clause is parsed.
// An alias naming it is refused in every clause that can carry one, so the
// answer does not depend on which arm asked or on whether a LOAD clause is
// present.
// ---------------------------------------------------------------------

constexpr absl::string_view kKeyRedefinition =
    "YIELD_SCORE_AS `__key` attempts to redefine the reserved `__key` field";

TEST_F(FTHybridParserTest, SearchArmYieldScoreAsKeyIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "__key", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), kKeyRedefinition);
}

TEST_F(FTHybridParserTest, VsimArmYieldScoreAsKeyIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "YIELD_SCORE_AS", "__key"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), kKeyRedefinition);
}

TEST_F(FTHybridParserTest, CombineYieldScoreAsKeyWithNoLoadIsRejected) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "__key"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), kKeyRedefinition);
}

TEST_F(FTHybridParserTest, CombineYieldScoreAsKeyWithLoadAllIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                       "__key", "LOAD", "*"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), kKeyRedefinition);
}

TEST_F(FTHybridParserTest, CombineYieldScoreAsKeyWithLoadFieldIsRejected) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                       "__key", "LOAD", "1", "@n"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), kKeyRedefinition);
}

TEST_F(FTHybridParserTest, SearchArmYieldScoreAsNonReservedIsAccepted) {
  // The control: it is the `__key` name that is refused, not the clause.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "text_score",
                       "VSIM", "@vector", "$q", "KNN", "2", "K", "5"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, VsimArmYieldScoreAsNonReservedIsAccepted) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "YIELD_SCORE_AS", "vec_score"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, CombineYieldScoreAsNonReservedIsAccepted) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "fused"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->output_score_name, "fused");
}

// ---------------------------------------------------------------------
// Vector blob size
// ---------------------------------------------------------------------
//
// FT.SEARCH rejects a query blob whose size is not the index's vector data
// size, in PostParseVectorParameters. A VSIM arm carrying a FILTER is
// rewritten into a query string and reaches that check through
// PostParseQueryString; a pure VSIM arm resolves its $param by hand and does
// not, so the parser checks it there. Both spellings must produce the one
// message, which is FT.SEARCH's.

// Builds a PARAMS clause binding $q to a blob of `bytes` bytes.
std::vector<std::string> ParamsWithBlobOf(size_t bytes) {
  return {"PARAMS", "2", "q", std::string(bytes, '\0')};
}

std::string BlobSizeError(size_t got) {
  return absl::StrCat(
      "Error parsing vector similarity parameters: query vector blob size (",
      got, ") does not match index's expected size (",
      kVectorDimensions * sizeof(float), ").");
}

TEST_F(FTHybridParserTest, PureVsimArmRejectsOverLongVectorBlob) {
  constexpr size_t kTooLong = kVectorDimensions * sizeof(float) + 4;
  std::vector<std::string> args{"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q",
                                "KNN",    "2",         "K",    "5"};
  auto extra = ParamsWithBlobOf(kTooLong);
  args.insert(args.end(), extra.begin(), extra.end());
  auto params = ParseExact(args);
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), BlobSizeError(kTooLong));
}

TEST_F(FTHybridParserTest, PureVsimArmRejectsUnderLongVectorBlob) {
  constexpr size_t kTooShort = kVectorDimensions * sizeof(float) - 4;
  std::vector<std::string> args{"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q",
                                "KNN",    "2",         "K",    "5"};
  auto extra = ParamsWithBlobOf(kTooShort);
  args.insert(args.end(), extra.begin(), extra.end());
  auto params = ParseExact(args);
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), BlobSizeError(kTooShort));
}

TEST_F(FTHybridParserTest, PureVsimArmAcceptsCorrectlySizedVectorBlob) {
  // The control: it is the size that is refused, not the clause.
  std::vector<std::string> args{"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q",
                                "KNN",    "2",         "K",    "5"};
  auto extra = ParamsWithBlobOf(kVectorDimensions * sizeof(float));
  args.insert(args.end(), extra.begin(), extra.end());
  auto params = ParseExact(args);
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, VsimArmWithFilterRejectsWrongSizedVectorBlob) {
  // The FILTER rewrites the arm into a query string, so this one reaches the
  // check inside PostParseQueryString. Same message as the pure-VSIM arm.
  constexpr size_t kTooLong = kVectorDimensions * sizeof(float) + 4;
  std::vector<std::string> args{"SEARCH", "@n:[0 10]", "VSIM",    "@vector",
                                "$q",     "KNN",       "2",       "K",
                                "5",      "FILTER",    "@n:[0 3]"};
  auto extra = ParamsWithBlobOf(kTooLong);
  args.insert(args.end(), extra.begin(), extra.end());
  auto params = ParseExact(args);
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), BlobSizeError(kTooLong));
}

// ---------------------------------------------------------------------
// SCORER
// ---------------------------------------------------------------------
//
// The SEARCH arm shares FT.SEARCH's validator
// (indexes::scoring::ParseScorerType), so the names the two commands take and
// the message they produce for a name they do not take are the same by
// construction. The VSIM arm has no scorer at all -- its score is a distance.

TEST_F(FTHybridParserTest, ScorerNamesTheSearchArmsScorer) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "SCORER", "BM25STD", "VSIM", "@vector", "$q"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->arms.at(0)->scorer,
            indexes::scoring::ScorerType::kBm25Std);
}

TEST_F(FTHybridParserTest, ScorerNameIsCaseInsensitive) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "SCORER", "bm25std", "VSIM", "@vector", "$q"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->arms.at(0)->scorer,
            indexes::scoring::ScorerType::kBm25Std);
}

TEST_F(FTHybridParserTest, ScorerRejectsTfidf) {
  // TFIDF is in the ScorerType enum but is not registered in kScorerByStr, so
  // it is not selectable -- by FT.SEARCH either.
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "SCORER", "TFIDF", "VSIM", "@vector", "$q"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), "Unknown argument `TFIDF`");
}

TEST_F(FTHybridParserTest, ScorerRejectsAnUnknownName) {
  auto params = Parse(
      {"SEARCH", "@n:[0 10]", "SCORER", "banana", "VSIM", "@vector", "$q"});
  ASSERT_FALSE(params.ok());
  EXPECT_EQ(params.status().message(), "Unknown argument `banana`");
}

TEST_F(FTHybridParserTest, ScorerOnTheVsimArmIsRejected) {
  // A vector arm's score is a distance; there is nothing to score.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "SCORER", "BM25STD"});
  ASSERT_FALSE(params.ok());
}

// ---------------------------------------------------------------------
// Synchronous local dispatch
// ---------------------------------------------------------------------

// ExecuteCommand<MultiSearchParameters> routes to this hook whenever the
// command runs inside MULTI/EXEC or Lua, or the reader thread pool is
// disabled. FT.HYBRID has no correct synchronous implementation, so the hook
// is the refusal itself: calling it is the dispatch decision.
TEST_F(FTHybridParserTest, ExecuteSyncLocalIsRefused) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q"});
  VMSDK_EXPECT_OK(params);
  auto status =
      MultiSearchParameters::ExecuteSyncLocal(&fake_ctx_, std::move(*params));
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(status.message(), kSyncLocalUnsupportedMsg);
  // Both triggers have to be visible in what the caller is told.
  EXPECT_THAT(std::string(status.message()),
              ::testing::AllOf(::testing::HasSubstr("MULTI/EXEC"),
                               ::testing::HasSubstr("reader thread pool")));
}

}  // namespace
}  // namespace query
}  // namespace valkey_search
