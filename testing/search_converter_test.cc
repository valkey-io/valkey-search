/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/search_converter.h"

#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_set.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/search.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

using coordinator::InfieldsFromGRPC;
using coordinator::ParametersToGRPCSearchRequest;

class SearchConverterInfieldsTest : public vmsdk::ValkeyTest {};

// Helper: serialize engaged infields to proto, decode via InfieldsFromGRPC.
absl::flat_hash_set<std::string> SerializeAndReadBackInfields(
    const absl::flat_hash_set<std::string>& original) {
  UnitTestSearchParameters params;
  params.infields = original;

  auto request = ParametersToGRPCSearchRequest(params);
  auto decoded = InfieldsFromGRPC(*request);
  return decoded.value_or(absl::flat_hash_set<std::string>{});
}

TEST_F(SearchConverterInfieldsTest, SerializeSingleField) {
  absl::flat_hash_set<std::string> original = {"title"};
  EXPECT_EQ(SerializeAndReadBackInfields(original), original);
}

// Unset infields (nullopt) emits no bytes and decodes back to nullopt.
TEST_F(SearchConverterInfieldsTest, UnsetIsNotSerialized) {
  UnitTestSearchParameters params;
  EXPECT_FALSE(params.infields.has_value());

  auto request = ParametersToGRPCSearchRequest(params);
  EXPECT_EQ(request->infields_size(), 0);
  EXPECT_FALSE(InfieldsFromGRPC(*request).has_value());
}

// An engaged-but-empty set collapses to the same wire bytes as unset. This is
// acceptable because INFIELDS 0 means "search all text fields" (Redis parity),
// which is also what unset means.
TEST_F(SearchConverterInfieldsTest, EngagedEmptyCollapsesToUnset) {
  UnitTestSearchParameters params;
  params.infields.emplace();  // engaged but empty

  auto request = ParametersToGRPCSearchRequest(params);
  EXPECT_EQ(request->infields_size(), 0);
  EXPECT_FALSE(InfieldsFromGRPC(*request).has_value());
}

TEST_F(SearchConverterInfieldsTest, WireRoundTrip) {
  UnitTestSearchParameters params;
  params.infields = absl::flat_hash_set<std::string>{"f1", "f2", "f3"};

  auto request = ParametersToGRPCSearchRequest(params);

  std::string wire;
  ASSERT_TRUE(request->SerializeToString(&wire));
  coordinator::SearchIndexPartitionRequest parsed;
  ASSERT_TRUE(parsed.ParseFromString(wire));

  absl::flat_hash_set<std::string> deserialized(parsed.infields().begin(),
                                                parsed.infields().end());
  EXPECT_EQ(deserialized, *params.infields);
}

}  // namespace
}  // namespace valkey_search
