/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/coordinator/search_converter.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/query/fanout_merge.h"
#include "src/query/search.h"
#include "testing/common.h"
#include "vmsdk/src/type_conversions.h"

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

// content_limit (bounded content fan-out) crosses the wire only when set, so
// an unbounded request is unchanged and a shard can tell the two apart.
TEST_F(ValkeySearchTest, ContentLimitPresenceRoundTrip) {
  for (const std::optional<uint64_t> &content_limit :
       {std::optional<uint64_t>(), std::optional<uint64_t>(0),
        std::optional<uint64_t>(7)}) {
    SCOPED_TRACE(content_limit.has_value()
                     ? "content_limit " + std::to_string(*content_limit)
                     : std::string("content_limit unset"));
    UnitTestSearchParameters params;
    params.content_limit = content_limit;

    auto request = coordinator::ParametersToGRPCSearchRequest(params);
    ASSERT_NE(request, nullptr);
    std::string wire;
    ASSERT_TRUE(request->SerializeToString(&wire));
    coordinator::SearchIndexPartitionRequest parsed;
    ASSERT_TRUE(parsed.ParseFromString(wire));

    ASSERT_EQ(parsed.has_content_limit(), content_limit.has_value());
    if (content_limit.has_value()) {
      EXPECT_EQ(parsed.content_limit(), *content_limit);
    }
  }
}

// A key-only entry decodes without content. An entry with no attributes and
// no omission mark is valid empty content, and is what a shard predating
// content_limit always sends.
TEST_F(ValkeySearchTest, DecodeNeighborEntryHonorsContentOmitted) {
  coordinator::NeighborEntry omitted;
  omitted.set_key("a");
  omitted.set_score(2.0f);
  omitted.set_content_omitted(true);
  auto key_only = query::fanout::DecodeNeighborEntry(omitted);
  EXPECT_FALSE(key_only.attribute_contents.has_value());
  EXPECT_EQ(key_only.score, 2.0f);

  coordinator::NeighborEntry empty;
  empty.set_key("b");
  auto with_content = query::fanout::DecodeNeighborEntry(empty);
  ASSERT_TRUE(with_content.attribute_contents.has_value());
  EXPECT_TRUE(with_content.attribute_contents->empty());

  coordinator::NeighborEntry full;
  full.set_key("c");
  auto *content = full.add_attribute_contents();
  content->set_identifier("f");
  content->set_content("v");
  auto decoded = query::fanout::DecodeNeighborEntry(full);
  ASSERT_TRUE(decoded.attribute_contents.has_value());
  ASSERT_EQ(decoded.attribute_contents->size(), 1u);
  EXPECT_EQ(
      vmsdk::ToStringView(decoded.attribute_contents->at("f").value.get()),
      "v");
}

}  // namespace

}  // namespace valkey_search
