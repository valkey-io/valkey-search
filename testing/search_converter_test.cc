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

// Inkeys survive a serialize/deserialize round-trip through the proto.
TEST_F(ValkeySearchTest, InkeysProtoSerializationRoundTrip) {
  const std::vector<absl::flat_hash_set<std::string>> key_sets = {
      {},
      {"key1"},
      {"k1", "k2", "k3"},
  };

  for (const auto &original_inkeys : key_sets) {
    SCOPED_TRACE("inkeys size: " + std::to_string(original_inkeys.size()));

    UnitTestSearchParameters params;
    params.inkeys = original_inkeys;

    auto request = coordinator::ParametersToGRPCSearchRequest(params);
    ASSERT_NE(request, nullptr);

    // Verify serialization: read inkeys directly from the proto message.
    absl::flat_hash_set<std::string> serialized_inkeys(
        request->inkeys().begin(), request->inkeys().end());
    EXPECT_EQ(serialized_inkeys, original_inkeys);

    // Verify round-trip: serialize proto to bytes, parse back, read inkeys.
    std::string wire;
    ASSERT_TRUE(request->SerializeToString(&wire));
    coordinator::SearchIndexPartitionRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(wire));

    absl::flat_hash_set<std::string> deserialized_inkeys;
    for (const auto &key : parsed.inkeys()) {
      deserialized_inkeys.insert(key);
    }
    EXPECT_EQ(deserialized_inkeys, original_inkeys);
  }
}

}  // namespace

}  // namespace valkey_search
