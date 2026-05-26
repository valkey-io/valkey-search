/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Parser-focused tests for the GEO field type. Two surfaces are exercised:
 *   1) FT.CREATE schema parsing — that GEO fields produce a proto with the
 *      geo_index oneof set and the correct IndexerType.
 *   2) FT.SEARCH filter expression parsing —
 *      `@field:[lon lat radius unit]` for a GEO-typed field, including the
 *      error paths (bad units, out-of-range coordinates, malformed syntax,
 *      and the parse-time rejection of negated geo predicates).
 *
 * These tests avoid touching the runtime by stopping at parse-result level —
 * predicate-tree shape, error messages, and proto contents.
 */

#include <memory>
#include <string>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/commands/filter_parser.h"
#include "src/commands/ft_create_parser.h"
#include "src/index_schema.pb.h"
#include "src/indexes/geo.h"
#include "src/indexes/tag.h"
#include "src/utils/string_interning.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

using ::testing::TestParamInfo;
using ::testing::ValuesIn;

// =====================================================================
// 1) FT.CREATE schema parsing for the GEO field type.
// =====================================================================

class FTCreateGeoFieldTest : public vmsdk::ValkeyTest {};

TEST_F(FTCreateGeoFieldTest, SingleGeoField) {
  auto args = vmsdk::ToValkeyStringVector(
      "places ON HASH PREFIX 1 place: SCHEMA loc GEO");
  auto schema = ParseFTCreateArgs(nullptr, args.data(), args.size());
  ASSERT_TRUE(schema.ok()) << schema.status();
  ASSERT_EQ(schema->attributes().size(), 1);
  const auto& attr = schema->attributes(0);
  EXPECT_EQ(attr.identifier(), "loc");
  EXPECT_EQ(attr.alias(), "loc");
  EXPECT_EQ(attr.index().index_type_case(),
            data_model::Index::IndexTypeCase::kGeoIndex);
}

TEST_F(FTCreateGeoFieldTest, GeoAliasedWithAS) {
  auto args = vmsdk::ToValkeyStringVector(
      "places ON HASH PREFIX 1 place: SCHEMA $.location AS loc GEO");
  auto schema = ParseFTCreateArgs(nullptr, args.data(), args.size());
  ASSERT_TRUE(schema.ok()) << schema.status();
  ASSERT_EQ(schema->attributes().size(), 1);
  const auto& attr = schema->attributes(0);
  EXPECT_EQ(attr.identifier(), "$.location");
  EXPECT_EQ(attr.alias(), "loc");
  EXPECT_EQ(attr.index().index_type_case(),
            data_model::Index::IndexTypeCase::kGeoIndex);
}

TEST_F(FTCreateGeoFieldTest, MultipleFieldsWithGeo) {
  auto args = vmsdk::ToValkeyStringVector(
      "places ON HASH PREFIX 1 place: SCHEMA "
      "loc GEO category TAG visits NUMERIC");
  auto schema = ParseFTCreateArgs(nullptr, args.data(), args.size());
  ASSERT_TRUE(schema.ok()) << schema.status();
  ASSERT_EQ(schema->attributes().size(), 3);
  EXPECT_EQ(schema->attributes(0).index().index_type_case(),
            data_model::Index::IndexTypeCase::kGeoIndex);
  EXPECT_EQ(schema->attributes(1).index().index_type_case(),
            data_model::Index::IndexTypeCase::kTagIndex);
  EXPECT_EQ(schema->attributes(2).index().index_type_case(),
            data_model::Index::IndexTypeCase::kNumericIndex);
}

// =====================================================================
// 2) FT.SEARCH filter expression parsing for GEO.
//    Reuses the ValkeySearchTestWithParam infra so we get a real
//    IndexSchema we can register a GEO index on.
// =====================================================================

struct GeoFilterTestCase {
  std::string test_name;
  std::string filter;
  bool create_success{false};
  std::string create_expected_error_message;
  std::string expected_tree_structure;
};

class GeoFilterTest : public ValkeySearchTestWithParam<GeoFilterTestCase> {};

void InitGeoIndexSchema(MockIndexSchema* index_schema) {
  data_model::GeoIndex geo_index_proto;
  auto loc =
      std::make_shared<IndexTeser<indexes::Geo, data_model::GeoIndex>>(
          geo_index_proto);
  // Single anchor point so the index is non-empty (lon, lat).
  // IndexTeser::AddRecord takes a plain string_view and interns internally.
  VMSDK_EXPECT_OK(loc->AddRecord("anchor", "-122.4194,37.7749"));
  VMSDK_EXPECT_OK(index_schema->AddIndex("loc", "loc", loc));

  // Also register a TAG field so we can test composition.
  data_model::TagIndex tag_proto;
  tag_proto.set_separator(",");
  tag_proto.set_case_sensitive(true);
  auto category =
      std::make_shared<IndexTeser<indexes::Tag, data_model::TagIndex>>(
          tag_proto);
  VMSDK_EXPECT_OK(
      index_schema->AddIndex("category", "category", category));
}

TEST_P(GeoFilterTest, ParseParams) {
  const GeoFilterTestCase& tc = GetParam();
  auto index_schema = CreateIndexSchema("idx").value();
  InitGeoIndexSchema(index_schema.get());
  EXPECT_CALL(*index_schema, GetIdentifier(::testing::_))
      .Times(::testing::AnyNumber());
  TextParsingOptions options{};
  FilterParser parser(*index_schema, tc.filter, options);
  auto parse_results = parser.Parse();
  EXPECT_EQ(parse_results.ok(), tc.create_success)
      << "filter: " << tc.filter << "\nstatus: " << parse_results.status();
  if (!tc.create_success) {
    EXPECT_EQ(parse_results.status().message(),
              tc.create_expected_error_message);
    return;
  }
  std::string actual =
      PrintPredicateTree(parse_results.value().root_predicate.get());
  if (!tc.expected_tree_structure.empty()) {
    EXPECT_EQ(actual, tc.expected_tree_structure)
        << "tree mismatch for filter: " << tc.filter;
  }
  EXPECT_TRUE(parse_results.value().query_operations &
              QueryOperations::kContainsGeo);
}

INSTANTIATE_TEST_SUITE_P(
    GeoFilterParseTests, GeoFilterTest,
    ValuesIn<GeoFilterTestCase>({
        // ---------- Happy paths ----------
        {
            .test_name = "km_units",
            .filter = "@loc:[-122.4 37.7 100 km]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "m_units",
            .filter = "@loc:[-122.4 37.7 5000 m]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "mi_units",
            .filter = "@loc:[-122.4 37.7 50 mi]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "ft_units",
            .filter = "@loc:[-122.4 37.7 1000 ft]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "case_insensitive_unit_KM",
            .filter = "@loc:[-122.4 37.7 100 KM]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "case_insensitive_unit_Mi",
            .filter = "@loc:[-122.4 37.7 50 Mi]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "negative_longitude_zero_lat",
            .filter = "@loc:[-179.9 0 100 km]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "near_north_latitude_limit",
            .filter = "@loc:[0 85.0 100 km]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        {
            .test_name = "near_south_latitude_limit",
            .filter = "@loc:[0 -85.0 100 km]",
            .create_success = true,
            .expected_tree_structure = "GEO(loc)\n",
        },
        // ---------- Composition ----------
        {
            .test_name = "and_with_tag",
            .filter = "@loc:[-122.4 37.7 100 km] @category:{food}",
            .create_success = true,
            .expected_tree_structure = "AND{\n"
                                       "  GEO(loc)\n"
                                       "  TAG(category)\n"
                                       "}\n",
        },
        {
            .test_name = "or_with_tag",
            .filter = "@loc:[-122.4 37.7 100 km] | @category:{food}",
            .create_success = true,
            .expected_tree_structure = "OR{\n"
                                       "  GEO(loc)\n"
                                       "  TAG(category)\n"
                                       "}\n",
        },
        // ---------- Error: bad units ----------
        {
            .test_name = "unknown_unit",
            .filter = "@loc:[-122.4 37.7 100 yards]",
            .create_success = false,
            .create_expected_error_message =
                "Unknown distance unit `yards`",
        },
        {
            .test_name = "missing_unit",
            .filter = "@loc:[-122.4 37.7 100]",
            .create_success = false,
            .create_expected_error_message =
                "Missing distance unit at position 21",
        },
        // ---------- Error: out-of-range coords ----------
        {
            .test_name = "longitude_too_high",
            .filter = "@loc:[181 37.7 100 km]",
            .create_success = false,
            .create_expected_error_message =
                "Geo longitude out of range: 181",
        },
        {
            .test_name = "longitude_too_low",
            .filter = "@loc:[-181 37.7 100 km]",
            .create_success = false,
            .create_expected_error_message =
                "Geo longitude out of range: -181",
        },
        {
            .test_name = "latitude_above_mercator_strip",
            .filter = "@loc:[0 89 100 km]",
            .create_success = false,
            .create_expected_error_message =
                "Geo latitude out of range: 89 (supported range is "
                "[-85.05, 85.05])",
        },
        {
            .test_name = "latitude_below_mercator_strip",
            .filter = "@loc:[0 -89 100 km]",
            .create_success = false,
            .create_expected_error_message =
                "Geo latitude out of range: -89 (supported range is "
                "[-85.05, 85.05])",
        },
        // ---------- Error: zero / negative radius ----------
        {
            .test_name = "zero_radius",
            .filter = "@loc:[-122.4 37.7 0 km]",
            .create_success = false,
            .create_expected_error_message = "Geo radius must be > 0",
        },
        {
            .test_name = "negative_radius",
            .filter = "@loc:[-122.4 37.7 -5 km]",
            .create_success = false,
            .create_expected_error_message = "Geo radius must be > 0",
        },
        // ---------- Error: structural / missing parts ----------
        {
            .test_name = "missing_radius_and_unit",
            .filter = "@loc:[-122.4 37.7]",
            .create_success = false,
            // The number parser fails when it hits the closing `]` while
            // looking for the radius.
            .create_expected_error_message = "Invalid number: ",
        },
        // ---------- Error: parse-time negation rejection ----------
        {
            .test_name = "negated_geo_rejected",
            .filter = "-@loc:[-122.4 37.7 100 km]",
            .create_success = false,
            .create_expected_error_message =
                "Negated geo predicates are not supported",
        },
    }),
    [](const TestParamInfo<GeoFilterTestCase>& info) {
      return info.param.test_name;
    });

}  // namespace
}  // namespace valkey_search
