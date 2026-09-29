/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/search_converter.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/index_schema.h"
#include "src/indexes/numeric.h"
#include "src/indexes/text.h"
#include "src/query/predicate.h"
#include "testing/common.h"

namespace valkey_search::coordinator {

namespace {

using ::testing::UnorderedElementsAre;

// Field masks follow the order in which the indexes::Text objects are
// constructed, since each one allocates the next text field number.
constexpr FieldMaskPredicate kMaskA = 1ULL << 0;  // "text_a"
constexpr FieldMaskPredicate kMaskB = 1ULL << 1;  // "text_b"
constexpr FieldMaskPredicate kMaskC = 1ULL << 2;  // "text_c"

class SearchConverterTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    index_schema_ = CreateIndexSchema("index_schema_name").value();
    EXPECT_CALL(*index_schema_, GetIdentifier(::testing::_))
        .Times(::testing::AnyNumber());

    index_schema_->CreateTextIndexSchema();
    text_index_schema_ = index_schema_->GetTextIndexSchema();
    ASSERT_NE(text_index_schema_, nullptr);

    // Allocation order fixes the field numbers, hence kMaskA/B/C above.
    for (absl::string_view name : {"text_a", "text_b", "text_c"}) {
      auto proto = CreateTextIndexProto(/*with_suffix_trie=*/true,
                                        /*no_stem=*/false, /*weight=*/1.0);
      auto text_index =
          std::make_shared<indexes::Text>(proto, text_index_schema_);
      VMSDK_EXPECT_OK(
          index_schema_->AddIndex(name, std::string(name), text_index));
    }

    data_model::NumericIndex numeric_index_proto;
    auto numeric_index = std::make_shared<
        IndexTeser<indexes::Numeric, data_model::NumericIndex>>(
        numeric_index_proto);
    VMSDK_EXPECT_OK(
        index_schema_->AddIndex("num_field", "num_field", numeric_index));
  }

  void TearDown() override {
    // The schema unsubscribes from the KeyspaceEventManager as it is destroyed,
    // so it must go before the base TearDown tears that manager down.
    text_index_schema_.reset();
    index_schema_.reset();
    ValkeySearchTest::TearDown();
  }

  // The seam under test: serialize an in-process predicate onto the wire and
  // deserialize it, exactly as the fanout coordinator and the receiving shard
  // do. Returns the identifier set the shard would use for re-verification.
  absl::flat_hash_set<std::string> RoundTripIdentifiers(
      const query::Predicate& predicate,
      std::unique_ptr<query::Predicate>* out = nullptr) {
    auto proto = PredicateToGRPCPredicate(predicate);
    EXPECT_NE(proto, nullptr);
    absl::flat_hash_set<std::string> identifiers;
    auto result = GRPCPredicateToPredicate(*proto, index_schema_, identifiers);
    EXPECT_TRUE(result.ok()) << result.status();
    if (result.ok() && out != nullptr) {
      *out = std::move(result).value();
    }
    return identifiers;
  }

  std::shared_ptr<MockIndexSchema> index_schema_;
  std::shared_ptr<indexes::text::TextIndexSchema> text_index_schema_;
};

// Each of the five text predicate kinds must carry its own field mask across
// the wire *and* report the identifiers that mask selects. Reading the mask off
// the wrong oneof arm yields the default instance (mask 0), which selects
// nothing -- that empty set silently disables the shard-side re-filter and
// re-score in response_generator.cc. A distinct mask per case keeps each
// assertion independently meaningful.

TEST_F(SearchConverterTest, TermRoundTripCarriesIdentifiers) {
  query::TermPredicate predicate(text_index_schema_, kMaskA, "hello",
                                 /*exact=*/false);
  std::unique_ptr<query::Predicate> decoded;
  EXPECT_THAT(RoundTripIdentifiers(predicate, &decoded),
              UnorderedElementsAre("text_a"));
  auto* term = dynamic_cast<query::TermPredicate*>(decoded.get());
  ASSERT_NE(term, nullptr);
  EXPECT_EQ(term->GetFieldMask(), kMaskA);
  EXPECT_EQ(term->GetTextString(), "hello");
}

TEST_F(SearchConverterTest, PrefixRoundTripCarriesIdentifiers) {
  query::PrefixPredicate predicate(text_index_schema_, kMaskB, "hel");
  std::unique_ptr<query::Predicate> decoded;
  EXPECT_THAT(RoundTripIdentifiers(predicate, &decoded),
              UnorderedElementsAre("text_b"));
  auto* prefix = dynamic_cast<query::PrefixPredicate*>(decoded.get());
  ASSERT_NE(prefix, nullptr);
  EXPECT_EQ(prefix->GetFieldMask(), kMaskB);
  EXPECT_EQ(prefix->GetTextString(), "hel");
}

TEST_F(SearchConverterTest, SuffixRoundTripCarriesIdentifiers) {
  query::SuffixPredicate predicate(text_index_schema_, kMaskC, "llo");
  std::unique_ptr<query::Predicate> decoded;
  EXPECT_THAT(RoundTripIdentifiers(predicate, &decoded),
              UnorderedElementsAre("text_c"));
  auto* suffix = dynamic_cast<query::SuffixPredicate*>(decoded.get());
  ASSERT_NE(suffix, nullptr);
  EXPECT_EQ(suffix->GetFieldMask(), kMaskC);
  EXPECT_EQ(suffix->GetTextString(), "llo");
}

TEST_F(SearchConverterTest, InfixRoundTripCarriesIdentifiers) {
  query::InfixPredicate predicate(text_index_schema_, kMaskA | kMaskC, "ell");
  std::unique_ptr<query::Predicate> decoded;
  EXPECT_THAT(RoundTripIdentifiers(predicate, &decoded),
              UnorderedElementsAre("text_a", "text_c"));
  auto* infix = dynamic_cast<query::InfixPredicate*>(decoded.get());
  ASSERT_NE(infix, nullptr);
  EXPECT_EQ(infix->GetFieldMask(), kMaskA | kMaskC);
  EXPECT_EQ(infix->GetTextString(), "ell");
}

TEST_F(SearchConverterTest, FuzzyRoundTripCarriesIdentifiers) {
  query::FuzzyPredicate predicate(text_index_schema_, kMaskA | kMaskB, "helo",
                                  /*distance=*/1);
  std::unique_ptr<query::Predicate> decoded;
  EXPECT_THAT(RoundTripIdentifiers(predicate, &decoded),
              UnorderedElementsAre("text_a", "text_b"));
  auto* fuzzy = dynamic_cast<query::FuzzyPredicate*>(decoded.get());
  ASSERT_NE(fuzzy, nullptr);
  EXPECT_EQ(fuzzy->GetFieldMask(), kMaskA | kMaskB);
  EXPECT_EQ(fuzzy->GetTextString(), "helo");
  EXPECT_EQ(fuzzy->GetDistance(), 1u);
}

// A tree with one non-text leaf produced a non-empty identifier set even while
// the text arms contributed nothing, so mixed trees masked the bug. The union
// must contain both contributions.
TEST_F(SearchConverterTest, PrefixAndNumericRoundTripUnionsIdentifiers) {
  auto numeric_index = index_schema_->GetIndex("num_field").value();
  std::vector<std::unique_ptr<query::Predicate>> children;
  children.push_back(std::make_unique<query::PrefixPredicate>(
      text_index_schema_, kMaskC, "hel"));
  children.push_back(std::make_unique<query::NumericPredicate>(
      dynamic_cast<indexes::Numeric*>(numeric_index.get()), "num_field",
      "num_field", 0.0, true, 10.0, true));
  query::ComposedPredicate predicate(query::LogicalOperator::kAnd,
                                     std::move(children));

  EXPECT_THAT(RoundTripIdentifiers(predicate),
              UnorderedElementsAre("text_c", "num_field"));
}

// All text fields selected at once -- the shape a bare (unqualified) text term
// produces.
TEST_F(SearchConverterTest, PrefixWithAllFieldsMaskSelectsEveryTextField) {
  query::PrefixPredicate predicate(text_index_schema_, kMaskA | kMaskB | kMaskC,
                                   "hel");
  EXPECT_THAT(RoundTripIdentifiers(predicate),
              UnorderedElementsAre("text_a", "text_b", "text_c"));
}

// The FT.HYBRID parser sets SearchParameters::vector_score_only on a VSIM arm
// carrying a FILTER, rewriting the arm into `(<filter>)=>[KNN ...]`. In a
// cluster that arm is rebuilt on the shard from this wire request, and the
// rewritten query string is indistinguishable from a `text=>[KNN ...]` hybrid
// query -- so the flag itself has to cross. Without it the shard overwrites the
// KNN distance in Neighbor::score with BM25 relevance (ApplyHybridTextScore),
// sorts the arm by that relevance (TrimResults), and leaves a post-mutation
// refreshed distance out of the score (ProcessNeighborsForReply).
TEST_F(SearchConverterTest, VectorScoreOnlyRoundTrips) {
  for (bool value : {false, true}) {
    UnitTestSearchParameters parameters;
    parameters.index_schema_name = "index_schema_name";
    parameters.score_as = vmsdk::MakeUniqueValkeyString("__score");
    parameters.vector_score_only = value;

    auto request = ParametersToGRPCSearchRequest(parameters);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(request->vector_score_only(), value);

    UnitTestSearchParameters decoded;
    VMSDK_EXPECT_OK(
        GRPCSearchRequestToParameters(*request, /*context=*/nullptr, &decoded));
    EXPECT_EQ(decoded.vector_score_only, value);
  }
}

// A bare `*` parses to a FilterParseResults with is_match_all set, a null
// root_predicate and query_operations at kNone. The shard never re-parses the
// query string, so if the flag does not cross the wire the shard sees exactly
// what an absent filter looks like and hands a null predicate to
// EvaluateFilterAsPrimary, which dereferences it (search.cc:335).
TEST_F(SearchConverterTest, IsMatchAllRoundTrips) {
  for (bool value : {false, true}) {
    UnitTestSearchParameters parameters;
    parameters.index_schema_name = "index_schema_name";
    parameters.score_as = vmsdk::MakeUniqueValkeyString("__score");
    parameters.filter_parse_results.is_match_all = value;
    // Match-all carries no predicate and no operations; the flag is the only
    // thing distinguishing it from an absent filter.
    ASSERT_EQ(parameters.filter_parse_results.root_predicate, nullptr);

    auto request = ParametersToGRPCSearchRequest(parameters);
    ASSERT_NE(request, nullptr);
    EXPECT_EQ(request->is_match_all(), value);
    EXPECT_FALSE(request->has_root_filter_predicate());

    UnitTestSearchParameters decoded;
    VMSDK_EXPECT_OK(
        GRPCSearchRequestToParameters(*request, /*context=*/nullptr, &decoded));
    EXPECT_EQ(decoded.filter_parse_results.is_match_all, value);
  }
}

}  // namespace

}  // namespace valkey_search::coordinator
