/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/search_converter.h"

#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/search.h"
#include "testing/common.h"

namespace valkey_search {

namespace {

// Non-empty inkeys round-trip through the proto preserving exact key sets.
TEST_F(ValkeySearchTest, InkeysProtoSerializationRoundTrip) {
  const std::vector<absl::flat_hash_set<std::string>> key_sets = {
      {"key1"},
      {"k1", "k2", "k3"},
  };

  for (const auto &original_inkeys : key_sets) {
    SCOPED_TRACE("inkeys size: " + std::to_string(original_inkeys.size()));

    UnitTestSearchParameters params;
    params.inkeys = original_inkeys;

    auto request = coordinator::ParametersToGRPCSearchRequest(params);
    ASSERT_NE(request, nullptr);

    std::string wire;
    ASSERT_TRUE(request->SerializeToString(&wire));
    coordinator::SearchIndexPartitionRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(wire));

    ASSERT_TRUE(parsed.has_inkeys());
    absl::flat_hash_set<std::string> deserialized_inkeys(
        parsed.inkeys().keys().begin(), parsed.inkeys().keys().end());
    EXPECT_EQ(deserialized_inkeys, original_inkeys);
  }
}

// Engaged empty inkeys (INKEYS 0) is distinguishable from unset on the wire.
// This ensures the shard can correctly apply the empty-set filter.
TEST_F(ValkeySearchTest, InkeysEmptySetPresenceRoundTrip) {
  // Engaged empty set — has_inkeys() must be true, keys empty.
  {
    UnitTestSearchParameters params;
    params.inkeys.emplace();  // engaged, empty

    auto request = coordinator::ParametersToGRPCSearchRequest(params);
    ASSERT_NE(request, nullptr);

    std::string wire;
    ASSERT_TRUE(request->SerializeToString(&wire));
    coordinator::SearchIndexPartitionRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(wire));

    EXPECT_TRUE(parsed.has_inkeys());
    EXPECT_EQ(parsed.inkeys().keys_size(), 0);
  }

  // Disengaged (no INKEYS clause) — has_inkeys() must be false.
  {
    UnitTestSearchParameters params;
    // params.inkeys left as std::nullopt

    auto request = coordinator::ParametersToGRPCSearchRequest(params);
    ASSERT_NE(request, nullptr);

    std::string wire;
    ASSERT_TRUE(request->SerializeToString(&wire));
    coordinator::SearchIndexPartitionRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(wire));

    EXPECT_FALSE(parsed.has_inkeys());
  }
}

}  // namespace

}  // namespace valkey_search
