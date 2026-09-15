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

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/ft_aggregate_parser.h"
#include "src/indexes/numeric.h"
#include "src/query/multi_search.h"
#include "testing/common.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/testing_infra/utils.h"

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

TEST_F(FTHybridParserTest, ScoreAliasCollidingWithLoadAllIsRejected) {
  // `LOAD *` names no fields but emits every one the document carries, so a
  // score alias that is a schema field collides just the same.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "n", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "LOAD", "*"});
  ASSERT_FALSE(params.ok());
  EXPECT_THAT(params.status().message(),
              ::testing::HasSubstr("collides with a column loaded by LOAD"));
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
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                       "hs"});
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
      .WillRepeatedly(::testing::Return(
          absl::NotFoundError("no such field")));
  for (const std::vector<std::string> &load :
       {std::vector<std::string>{"LOAD", "*"},
        std::vector<std::string>{"LOAD", "1", "@n"}}) {
    std::vector<std::string> args{"SEARCH", "@n:[0 10]", "VSIM", "@vector",
                                  "$q",     "KNN",       "2",    "K",
                                  "5",      "COMBINE",   "RRF",  "2",
                                  "YIELD_SCORE_AS",      "hs"};
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
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                       "__score"});
  EXPECT_FALSE(params.ok());
}

TEST_F(FTHybridParserTest, YieldingScoreAsScoreIsFineOnceALoadClauseExists) {
  EXPECT_CALL(*index_schema_, GetIdentifier(absl::string_view("__score")))
      .WillRepeatedly(::testing::Return(
          absl::NotFoundError("no such field")));
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
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "FUNCTION", "2", "EXPR",
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

TEST_F(FTHybridParserTest, WindowRejectsAnythingButAWholeNumber) {
  for (absl::string_view bad : {"1.5", "20abc", "abc", "", "-1", "1e3", "0x10",
                                "4294967296"}) {
    auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                         "2", "K", "5", "COMBINE", "RRF", "2", "WINDOW",
                         std::string(bad)});
    EXPECT_FALSE(params.ok()) << "WINDOW accepted `" << bad << "`";
  }
}

TEST_F(FTHybridParserTest, WindowTakesAWholeNumber) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "WINDOW", "7"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 7u);
}

TEST_F(FTHybridParserTest, WindowZeroMeansUnlimited) {
  // Not a pass-through of the reference, which rejects `WINDOW 0` outright.
  // Zero is this engine's "do not cap the arms", and COMBINE FUNCTION relies
  // on it -- see the FunctionDefaultsToAnUnlimitedWindow case below.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "WINDOW", "0"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 0u);
}

TEST_F(FTHybridParserTest, FunctionDefaultsToAnUnlimitedWindow) {
  // A user expression is expected to see every candidate, so FUNCTION with no
  // WINDOW must not inherit the RRF/LINEAR default of 20.
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN", "2", "K",
             "5", "COMBINE", "FUNCTION", "2", "EXPR",
             "@__search_score + @__vector_score"});
  VMSDK_EXPECT_OK(params);
  EXPECT_EQ((*params)->fusion.window, 0u);
}

TEST_F(FTHybridParserTest, RrfConstantIsAFractionalNumber) {
  // The reference honours fractional constants -- `CONSTANT 1.5` ranks
  // strictly between 1 and 2 -- so truncating to an integer is wrong.
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "COMBINE", "RRF", "2", "CONSTANT",
                       "1.5"});
  VMSDK_EXPECT_OK(params);
  EXPECT_DOUBLE_EQ((*params)->fusion.rrf_constant, 1.5);
}

TEST_F(FTHybridParserTest, RrfConstantRejectsJunkAndNonFiniteAndNegative) {
  for (absl::string_view bad : {"60abc", "abc", "", "nan", "inf", "-inf",
                                "1e400", "-1", "-0.5", "0.5.5"}) {
    auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                         "2", "K", "5", "COMBINE", "RRF", "2", "CONSTANT",
                         std::string(bad)});
    EXPECT_FALSE(params.ok()) << "CONSTANT accepted `" << bad << "`";
  }
}

TEST_F(FTHybridParserTest, RrfConstantAcceptsZeroAndLargeValues) {
  for (absl::string_view good : {"0", "0.5", "60", "4294967296", "1e3"}) {
    auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                         "2", "K", "5", "COMBINE", "RRF", "2", "CONSTANT",
                         std::string(good)});
    VMSDK_EXPECT_OK(params) << "CONSTANT rejected `" << good << "`";
  }
}

TEST_F(FTHybridParserTest, AlphaAndBetaMustBeFiniteNumbers) {
  // `nan` and `inf` reach a fused score and poison every comparison against
  // it. std::isfinite cannot catch them here: the project builds with
  // -ffast-math, which folds that call to true, so the parser tests the IEEE
  // bit pattern instead. These cases are what prove it.
  for (absl::string_view bad : {"abc", "", "nan", "inf", "-inf", "1e400",
                                "0.5.5", "1,5", "1/2"}) {
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
  for (absl::string_view good : {"0", "0.5", "1", "-0.5", "2.5", "1e3",
                                 "+0.5"}) {
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
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "SORTBY", "2",
                       "@ts", "DESC"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, APerArmAliasColumnIsNumeric) {
  // Not cosmetic: the column is filled by parsing the text fusion wrote, and
  // only a numeric column parses it. A string column would order 0.9 above
  // 0.53 while still looking like a successful sort.
  auto params = Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM",
                       "@vector", "$q", "KNN", "2", "K", "5", "SORTBY", "2",
                       "@ts", "DESC"});
  VMSDK_EXPECT_OK(params);
  const auto &info = (*params)->agg->record_info_by_index_;
  auto it = std::find_if(info.begin(), info.end(), [](const auto &i) {
    return i.output_name_ == "ts";
  });
  ASSERT_NE(it, info.end()) << "no column was created for the alias";
  EXPECT_EQ(it->data_type_, indexes::IndexerType::kNumeric);
  // Its identifier has to be the alias itself: that is the key fusion used,
  // and it is what the record-population step matches a column against.
  EXPECT_EQ(it->identifier_, "ts");
}

TEST_F(FTHybridParserTest, TheVsimArmsAliasResolvesToo) {
  auto params = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                       "2", "K", "5", "YIELD_SCORE_AS", "vs", "SORTBY", "2",
                       "@vs", "ASC"});
  VMSDK_EXPECT_OK(params);
}

TEST_F(FTHybridParserTest, AGroupByAPerArmAliasResolves) {
  auto params =
      Parse({"SEARCH", "@n:[0 10]", "YIELD_SCORE_AS", "ts", "VSIM", "@vector",
             "$q", "KNN", "2", "K", "5", "GROUPBY", "1", "@ts", "REDUCE",
             "COUNT", "0", "AS", "cnt"});
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
                       "@vector", "$q", "KNN", "2", "K", "5", "LOAD", "1",
                       "@n", "SORTBY", "2", "@ts", "DESC"});
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
                        "@vector", "$q", "KNN", "2", "K", "5", "COMBINE",
                        "RRF", "2", "YIELD_SCORE_AS", "hs"});
  EXPECT_FALSE(renamed.ok());
  // And the VSIM arm, which is parsed by a different function.
  auto vsim = Parse({"SEARCH", "@n:[0 10]", "VSIM", "@vector", "$q", "KNN",
                     "2", "K", "5", "YIELD_SCORE_AS", "__score"});
  EXPECT_FALSE(vsim.ok());
}

}  // namespace
}  // namespace query
}  // namespace valkey_search
