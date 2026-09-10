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
#include "src/indexes/numeric.h"
#include "src/query/multi_search.h"
#include "testing/common.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/managed_pointers.h"
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

}  // namespace
}  // namespace query
}  // namespace valkey_search
