/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/commands/ft_debug_indexstats.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/text_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/index_schema.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "vmsdk/src/command_parser.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {

using ::testing::AnyOf;

// ----------------------------------------------------------------------------
// Helpers to walk a StatPairs tree.
// ----------------------------------------------------------------------------
const StatValue* FindKey(const StatPairs& kv, absl::string_view key) {
  for (const auto& entry : kv) {
    if (entry.first == key) {
      return &entry.second;
    }
  }
  return nullptr;
}

const StatPairs* AsPairs(const StatValue* v) {
  if (v == nullptr) {
    return nullptr;
  }
  return std::get_if<StatPairs>(&v->v);
}

const std::string* AsString(const StatValue* v) {
  if (v == nullptr) {
    return nullptr;
  }
  return std::get_if<std::string>(&v->v);
}

const int64_t* AsInt(const StatValue* v) {
  if (v == nullptr) {
    return nullptr;
  }
  return std::get_if<int64_t>(&v->v);
}

// ----------------------------------------------------------------------------
// EmitResp / BuildLogLine: tests on a hand-built tree (no IndexSchema needed).
// ----------------------------------------------------------------------------
class StatPairsEmitterTest : public ValkeySearchTest {};

TEST_F(StatPairsEmitterTest, EmitsExpectedKeysAndValues) {
  StatPairs root;
  root.emplace_back("indexName", StatValue{std::string("idx")});
  root.emplace_back("count", StatValue{int64_t{42}});
  StatPairs nested;
  nested.emplace_back("a", StatValue{int64_t{1}});
  nested.emplace_back("b", StatValue{int64_t{2}});
  root.emplace_back("nested", StatValue{std::move(nested)});

  EmitResp(&fake_ctx_, root);
  const std::string reply = fake_ctx_.reply_capture.GetReply();

  // Outer array of 6 elements (3 key/value pairs).
  EXPECT_THAT(reply, ::testing::HasSubstr("*6\r\n"));
  EXPECT_THAT(reply, ::testing::HasSubstr("indexName"));
  EXPECT_THAT(reply, ::testing::HasSubstr("idx"));
  EXPECT_THAT(reply, ::testing::HasSubstr("count"));
  EXPECT_THAT(reply, ::testing::HasSubstr(":42\r\n"));
  EXPECT_THAT(reply, ::testing::HasSubstr("nested"));
  // Inner array of 4 elements (2 key/value pairs).
  EXPECT_THAT(reply, ::testing::HasSubstr("*4\r\n"));
}

TEST_F(StatPairsEmitterTest, BuildLogLineRendersIndentedTree) {
  StatPairs root;
  root.emplace_back("name", StatValue{std::string("idx")});
  StatPairs sub;
  sub.emplace_back("count", StatValue{int64_t{7}});
  root.emplace_back("section", StatValue{std::move(sub)});

  const std::string log = BuildLogLine(root);
  EXPECT_THAT(log, ::testing::HasSubstr("name: \"idx\""));
  EXPECT_THAT(log, ::testing::HasSubstr("section:"));
  EXPECT_THAT(log, ::testing::HasSubstr("  count: 7"));
}

// ----------------------------------------------------------------------------
// CollectIndexStats: exercise the full collector pipeline against an
// IndexSchema with concrete attribute types.
// ----------------------------------------------------------------------------
class CollectIndexStatsTest : public ValkeySearchTest {
 protected:
  void CreateIndexFromProto(absl::string_view pbtxt) {
    data_model::IndexSchema proto;
    ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
        std::string(pbtxt), &proto));
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, false));
    VMSDK_EXPECT_OK(
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto));
  }

  std::shared_ptr<IndexSchema> GetSchema(absl::string_view name) {
    auto schema_or = SchemaManager::Instance().GetIndexSchema(0, name);
    EXPECT_TRUE(schema_or.ok());
    return schema_or.ok() ? *schema_or : nullptr;
  }
};

TEST_F(CollectIndexStatsTest, EmptyTagAndNumericIndexProducesStableShape) {
  CreateIndexFromProto(R"pb(
    name: "idx"
    db_num: 0
    subscribed_key_prefixes: "p:"
    attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
    attributes: {
      alias: "tag_alias"
      identifier: "tag_id"
      index: { tag_index: { separator: "," case_sensitive: false } }
    }
    attributes: {
      alias: "num_alias"
      identifier: "num_id"
      index: { numeric_index: {} }
    }
  )pb");
  auto schema = GetSchema("idx");
  ASSERT_NE(schema, nullptr);

  StatPairs stats = CollectIndexStats(*schema, /*fields=*/{});

  // Top-level keys: indexName, indexLevel, attributes.
  ASSERT_NE(FindKey(stats, "indexName"), nullptr);
  ASSERT_NE(FindKey(stats, "indexLevel"), nullptr);
  ASSERT_NE(FindKey(stats, "attributes"), nullptr);

  const std::string* idx_name = AsString(FindKey(stats, "indexName"));
  ASSERT_NE(idx_name, nullptr);
  EXPECT_EQ(*idx_name, "idx");

  // Attributes block has both attributes.
  const StatPairs* attrs = AsPairs(FindKey(stats, "attributes"));
  ASSERT_NE(attrs, nullptr);
  EXPECT_EQ(attrs->size(), 2);

  // Each attribute body has attributeIdentifier + attributeType.
  for (const auto& attr_entry : *attrs) {
    const StatPairs* body = AsPairs(&attr_entry.second);
    ASSERT_NE(body, nullptr);
    EXPECT_NE(FindKey(*body, "attributeIdentifier"), nullptr);
    const std::string* type = AsString(FindKey(*body, "attributeType"));
    ASSERT_NE(type, nullptr);
    EXPECT_THAT(*type, AnyOf("TAG", "NUMERIC"));
  }
}

TEST_F(CollectIndexStatsTest, FieldFilterReturnsOnlyRequestedAttributes) {
  CreateIndexFromProto(R"pb(
    name: "idx"
    db_num: 0
    subscribed_key_prefixes: "p:"
    attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
    attributes: {
      alias: "tag_alias"
      identifier: "tag_id"
      index: { tag_index: { separator: "," case_sensitive: false } }
    }
    attributes: {
      alias: "num_alias"
      identifier: "num_id"
      index: { numeric_index: {} }
    }
  )pb");
  auto schema = GetSchema("idx");
  ASSERT_NE(schema, nullptr);

  StatPairs stats =
      CollectIndexStats(*schema, /*fields=*/{"num_alias"});
  const StatPairs* attrs = AsPairs(FindKey(stats, "attributes"));
  ASSERT_NE(attrs, nullptr);
  ASSERT_EQ(attrs->size(), 1);
  EXPECT_EQ(attrs->front().first, "num_alias");
}

TEST_F(CollectIndexStatsTest, IndexLevelStatsPresentEvenWithoutText) {
  CreateIndexFromProto(R"pb(
    name: "idx"
    db_num: 0
    subscribed_key_prefixes: "p:"
    attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
    attributes: {
      alias: "n"
      identifier: "n"
      index: { numeric_index: {} }
    }
  )pb");
  auto schema = GetSchema("idx");
  ASSERT_NE(schema, nullptr);

  StatPairs stats = CollectIndexStats(*schema, /*fields=*/{});
  const StatPairs* level = AsPairs(FindKey(stats, "indexLevel"));
  ASSERT_NE(level, nullptr);
  EXPECT_NE(FindKey(*level, "numUniqueWords"), nullptr);
  EXPECT_NE(FindKey(*level, "keysPerWordHistogram"), nullptr);
  EXPECT_NE(FindKey(*level, "wordWithMostKeys"), nullptr);
  const int64_t* nuw = AsInt(FindKey(*level, "numUniqueWords"));
  ASSERT_NE(nuw, nullptr);
  EXPECT_EQ(*nuw, 0);
}

// ----------------------------------------------------------------------------
// IndexStatsCmd: unknown-attribute name returns InvalidArgumentError with no
// partial reply.
// ----------------------------------------------------------------------------
class IndexStatsCmdTest : public CollectIndexStatsTest {};

TEST_F(IndexStatsCmdTest, UnknownAttributeReturnsError) {
  CreateIndexFromProto(R"pb(
    name: "idx"
    db_num: 0
    subscribed_key_prefixes: "p:"
    attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
    attributes: {
      alias: "tag_alias"
      identifier: "tag_id"
      index: { tag_index: { separator: "," case_sensitive: false } }
    }
  )pb");

  std::vector<std::string> argv{"INDEXSTATS", "idx", "no_such_field"};
  std::vector<ValkeyModuleString*> cmd_argv;
  cmd_argv.reserve(argv.size());
  for (const auto& s : argv) {
    cmd_argv.push_back(
        TestValkeyModule_CreateStringPrintf(&fake_ctx_, "%s", s.data()));
  }
  vmsdk::ArgsIterator itr{cmd_argv.data(),
                          static_cast<int>(cmd_argv.size())};
  itr.Next();  // skip "INDEXSTATS"

  absl::Status st = IndexStatsCmd(&fake_ctx_, itr);
  EXPECT_FALSE(st.ok());
  EXPECT_EQ(st.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(std::string(st.message()),
              ::testing::HasSubstr("no_such_field"));

  for (auto* s : cmd_argv) {
    TestValkeyModule_FreeString(&fake_ctx_, s);
  }
}

}  // namespace
}  // namespace valkey_search
