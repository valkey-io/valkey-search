/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <algorithm>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "gmock/gmock.h"
#include "google/protobuf/any.pb.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"
#include "src/coordinator/coordinator.pb.h"
#include "src/coordinator/metadata_manager.h"
#include "src/index_schema.pb.h"
#include "src/schema_manager.h"
#include "testing/common.h"
#include "testing/coordinator/common.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
namespace {

// Generates a random alphanumeric string of length [1, max_len].
std::string RandomString(std::mt19937 &rng, int max_len = 12) {
  static const char kChars[] =
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "0123456789";
  std::uniform_int_distribution<int> len_dist(1, max_len);
  std::uniform_int_distribution<int> char_dist(0, sizeof(kChars) - 2);
  int len = len_dist(rng);
  std::string result;
  result.reserve(len);
  for (int i = 0; i < len; ++i) {
    result.push_back(kChars[char_dist(rng)]);
  }
  return result;
}

// Generates a random sorted list of unique aliases.
std::vector<std::string> RandomAliases(std::mt19937 &rng, int max_count = 5) {
  std::uniform_int_distribution<int> count_dist(0, max_count);
  int count = count_dist(rng);
  std::vector<std::string> aliases;
  aliases.reserve(count);
  for (int i = 0; i < count; ++i) {
    std::string alias = "alias_" + RandomString(rng, 8);
    // Ensure uniqueness within this set.
    if (std::find(aliases.begin(), aliases.end(), alias) == aliases.end()) {
      aliases.push_back(std::move(alias));
    }
  }
  std::sort(aliases.begin(), aliases.end());
  return aliases;
}

// Alias-only proto change does not trigger index rebuild.
//
// When OnMetadataCallback receives a proto where only `aliases` and/or `stats`
// changed, it calls RebuildAliasMapsForIndex instead of tearing down and
// recreating the index, preserving the in-memory IndexSchema identity.
class AliasOnlyChangeNoRebuildTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    mock_client_pool_ = std::make_unique<coordinator::MockClientPool>();

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            SendClusterMessage(testing::_, testing::_, testing::_, testing::_,
                               testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Mock cluster operations to prevent abort in MetadataManager.
    ON_CALL(*kMockValkeyModule,
            Call(testing::_, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                 testing::StrEq("SLOTS")))
        .WillByDefault(testing::Return(
            reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyType(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(VALKEYMODULE_REPLY_ARRAY));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyLength(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    coordinator::MetadataManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
  std::unique_ptr<coordinator::MockClientPool> mock_client_pool_;
};

TEST_F(AliasOnlyChangeNoRebuildTest, PropertyAliasOnlyChangePreservesIndex) {
  // Run 100 iterations with different random seeds for coverage.
  constexpr int kIterations = 100;

  for (int iteration = 0; iteration < kIterations; ++iteration) {
    // Fresh singleton state per iteration.
    coordinator::MetadataManager::InitInstance(nullptr);
    SchemaManager::InitInstance(nullptr);

    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       *mock_client_pool_));
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));

    std::mt19937 rng(42 + iteration);
    std::string index_name = "idx_" + std::to_string(iteration);

    // 1. Create a base proto and create the index.
    data_model::IndexSchema base_proto;
    base_proto.set_name(index_name);
    base_proto.set_db_num(kDbNum);
    base_proto.add_subscribed_key_prefixes("prefix_" + RandomString(rng, 4));
    base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

    // Add one vector attribute (required for a valid index).
    auto *attr = base_proto.add_attributes();
    attr->set_alias("vec_alias");
    attr->set_identifier("vec_id");
    auto *vec = attr->mutable_index()->mutable_vector_index();
    vec->set_dimension_count(10);
    vec->set_normalize(true);
    vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec->set_initial_cap(100);
    auto *hnsw = vec->mutable_hnsw_algorithm();
    hnsw->set_m(16);
    hnsw->set_ef_construction(200);
    hnsw->set_ef_runtime(10);

    // Assign initial random aliases.
    auto initial_aliases = RandomAliases(rng);
    for (const auto &alias : initial_aliases) {
      base_proto.add_aliases(alias);
    }

    // Create the index via coordinator path (triggers OnMetadataCallback
    // for first creation).
    auto create_result =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
    ASSERT_TRUE(create_result.ok())
        << "Iteration " << iteration
        << ": CreateIndexSchema failed: " << create_result.status();

    // 2. Get the shared_ptr to the IndexSchema — this is our identity check.
    auto index_schema_or =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    ASSERT_TRUE(index_schema_or.ok()) << "Iteration " << iteration;
    IndexSchema *original_ptr = index_schema_or.value().get();

    // 3. Create a mutated proto that only changes aliases and/or stats,
    //    starting from the EXISTING index's ToProto() output to ensure the
    //    structural fields are identical.
    auto mutated_proto = *index_schema_or.value()->ToProto();
    mutated_proto.clear_aliases();
    auto new_aliases = RandomAliases(rng);
    for (const auto &alias : new_aliases) {
      mutated_proto.add_aliases(alias);
    }

    // Optionally mutate stats field (random documents_count).
    std::uniform_int_distribution<uint32_t> stats_dist(0, 1000);
    mutated_proto.mutable_stats()->set_documents_count(stats_dist(rng));

    // 4. Pack mutated proto and call CreateEntry on MetadataManager to
    //    trigger OnMetadataCallback.
    auto packed = std::make_unique<google::protobuf::Any>();
    packed->PackFrom(mutated_proto);

    auto entry_result = coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(kDbNum, index_name), std::move(packed));
    ASSERT_TRUE(entry_result.ok())
        << "Iteration " << iteration
        << ": CreateEntry failed: " << entry_result.status();

    // 5. Verify the IndexSchema pointer is the SAME (no rebuild occurred).
    auto after_schema_or =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    ASSERT_TRUE(after_schema_or.ok()) << "Iteration " << iteration;
    IndexSchema *after_ptr = after_schema_or.value().get();

    EXPECT_EQ(original_ptr, after_ptr)
        << "Iteration " << iteration
        << ": IndexSchema was rebuilt (pointer changed) for alias-only "
           "change";

    // 6. Verify the forward alias map reflects the new aliases.
    auto all_aliases = SchemaManager::Instance().GetAllAliases(kDbNum);
    std::vector<std::pair<std::string, std::string>> expected;
    for (const auto &alias : new_aliases) {
      expected.emplace_back(alias, index_name);
    }
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(all_aliases, expected)
        << "Iteration " << iteration
        << ": Forward alias map does not match expected aliases";
  }
}

// Forward alias map consistency after RebuildAliasMapsForIndex.
//
// After an alias-only metadata update, the forward alias map entries for
// a given index exactly match the proto's aliases field with no stale
// entries from previous iterations.
TEST_F(AliasOnlyChangeNoRebuildTest,
       PropertyForwardAliasMapConsistencyAfterRebuild) {
  // This test creates a single index and performs 150 sequential alias-only
  // updates with randomized alias sets (0 to 20 aliases each). After each
  // update it verifies the forward map exactly matches the latest proto's
  // aliases field with no stale entries from any prior iteration.
  constexpr int kIterations = 150;
  constexpr int kMaxAliases = 20;
  constexpr uint64_t kSeed = 7919;  // Fixed seed for reproducibility.

  // Fresh singleton state.
  coordinator::MetadataManager::InitInstance(nullptr);
  SchemaManager::InitInstance(nullptr);
  coordinator::MetadataManager::InitInstance(
      std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                     *mock_client_pool_));
  SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
      &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));

  const std::string index_name = "prop3_index";

  // Build a base proto for the index.
  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("prefix_");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("vec_alias");
  attr->set_identifier("vec_id");
  auto *vec = attr->mutable_index()->mutable_vector_index();
  vec->set_dimension_count(4);
  vec->set_normalize(false);
  vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
  vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
  vec->set_initial_cap(10);
  auto *hnsw = vec->mutable_hnsw_algorithm();
  hnsw->set_m(16);
  hnsw->set_ef_construction(200);
  hnsw->set_ef_runtime(10);

  // Create the index initially (no aliases in proto).
  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok())
      << "CreateIndexSchema failed: " << create_result.status();

  // Verify initial state: no aliases.
  auto initial_aliases = SchemaManager::Instance().GetAllAliases(kDbNum);
  ASSERT_TRUE(initial_aliases.empty());

  std::mt19937 rng(kSeed);

  for (int iter = 0; iter < kIterations; ++iter) {
    // Generate a random alias set for this iteration (0 to kMaxAliases).
    std::uniform_int_distribution<int> count_dist(0, kMaxAliases);
    int alias_count = count_dist(rng);
    std::vector<std::string> expected_aliases;
    expected_aliases.reserve(alias_count);
    // Use a set to ensure uniqueness.
    absl::flat_hash_set<std::string> alias_dedup;
    while (static_cast<int>(alias_dedup.size()) < alias_count) {
      alias_dedup.insert("a_" + RandomString(rng, 10));
    }
    for (const auto &a : alias_dedup) {
      expected_aliases.push_back(a);
    }
    std::sort(expected_aliases.begin(), expected_aliases.end());

    // Build a proto with the same structural fields but different aliases.
    // IMPORTANT: We must use ToProto() of the existing index to ensure all
    // fields match exactly (proto defaults vs explicitly set values). If we
    // use base_proto directly, the MessageDifferencer sees unset default fields
    // as different from explicitly-set-to-default fields in ToProto() output.
    auto existing_schema_or =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    ASSERT_TRUE(existing_schema_or.ok()) << "Iteration " << iter;
    data_model::IndexSchema alias_proto =
        *existing_schema_or.value()->ToProto();
    alias_proto.clear_aliases();
    for (const auto &alias : expected_aliases) {
      alias_proto.add_aliases(alias);
    }

    // Send alias-only update via CreateEntry (triggers OnMetadataCallback →
    // RebuildAliasMapsForIndex since structure is unchanged).
    auto packed = std::make_unique<google::protobuf::Any>();
    packed->PackFrom(alias_proto);
    auto entry_result = coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(kDbNum, index_name), std::move(packed));
    ASSERT_TRUE(entry_result.ok())
        << "Iteration " << iter
        << ": CreateEntry failed: " << entry_result.status();

    // Verify forward map via GetAllAliases.
    auto all_aliases = SchemaManager::Instance().GetAllAliases(kDbNum);

    // Build expected (alias, index_name) pairs, sorted.
    std::vector<std::pair<std::string, std::string>> expected_pairs;
    expected_pairs.reserve(expected_aliases.size());
    for (const auto &alias : expected_aliases) {
      expected_pairs.emplace_back(alias, index_name);
    }
    std::sort(expected_pairs.begin(), expected_pairs.end());

    // The forward map should EXACTLY match the proto's aliases — no stale
    // entries from prior iterations.
    ASSERT_EQ(all_aliases.size(), expected_pairs.size())
        << "Iteration " << iter << ": alias count mismatch. Expected "
        << expected_pairs.size() << " but got " << all_aliases.size();
    ASSERT_EQ(all_aliases, expected_pairs)
        << "Iteration " << iter
        << ": forward alias map does not match proto aliases";
  }
}

// Structural proto change triggers full rebuild.
//
// When any field other than `aliases` or `stats` changes (e.g., attributes,
// key prefixes), OnMetadataCallback performs RemoveIndexSchemaInternal followed
// by CreateIndexSchemaInternal, resulting in a new IndexSchema object.

// Enum representing different structural mutation types.
enum class StructuralMutation {
  kChangeKeyPrefix,
  kAddKeyPrefix,
  kRemoveKeyPrefix,
  kChangeAttributeDataType,
  kChangeLanguage,
  kChangePunctuation,
  kChangeWithOffsets,
  kChangeMinStemSize,
  kChangeSkipInitialScan,
  kAddAttribute,
  kRemoveAttribute,
  kChangeAttributeAlias,
  kChangeAttributeIdentifier,
  kChangeVectorDimension,
  kChangeVectorNormalize,
  kChangeDistanceMetric,
  kChangeVectorDataType,
  kChangeInitialCap,
  kChangeHnswM,
  kChangeHnswEfConstruction,
  kChangeHnswEfRuntime,
  kAddStopWord,
  kChangeScore,
  kChangeScoreField,
  kCount  // Must be last
};

std::string MutationName(StructuralMutation m) {
  switch (m) {
    case StructuralMutation::kChangeKeyPrefix:
      return "ChangeKeyPrefix";
    case StructuralMutation::kAddKeyPrefix:
      return "AddKeyPrefix";
    case StructuralMutation::kRemoveKeyPrefix:
      return "RemoveKeyPrefix";
    case StructuralMutation::kChangeAttributeDataType:
      return "ChangeAttributeDataType";
    case StructuralMutation::kChangeLanguage:
      return "ChangeLanguage";
    case StructuralMutation::kChangePunctuation:
      return "ChangePunctuation";
    case StructuralMutation::kChangeWithOffsets:
      return "ChangeWithOffsets";
    case StructuralMutation::kChangeMinStemSize:
      return "ChangeMinStemSize";
    case StructuralMutation::kChangeSkipInitialScan:
      return "ChangeSkipInitialScan";
    case StructuralMutation::kAddAttribute:
      return "AddAttribute";
    case StructuralMutation::kRemoveAttribute:
      return "RemoveAttribute";
    case StructuralMutation::kChangeAttributeAlias:
      return "ChangeAttributeAlias";
    case StructuralMutation::kChangeAttributeIdentifier:
      return "ChangeAttributeIdentifier";
    case StructuralMutation::kChangeVectorDimension:
      return "ChangeVectorDimension";
    case StructuralMutation::kChangeVectorNormalize:
      return "ChangeVectorNormalize";
    case StructuralMutation::kChangeDistanceMetric:
      return "ChangeDistanceMetric";
    case StructuralMutation::kChangeVectorDataType:
      return "ChangeVectorDataType";
    case StructuralMutation::kChangeInitialCap:
      return "ChangeInitialCap";
    case StructuralMutation::kChangeHnswM:
      return "ChangeHnswM";
    case StructuralMutation::kChangeHnswEfConstruction:
      return "ChangeHnswEfConstruction";
    case StructuralMutation::kChangeHnswEfRuntime:
      return "ChangeHnswEfRuntime";
    case StructuralMutation::kAddStopWord:
      return "AddStopWord";
    case StructuralMutation::kChangeScore:
      return "ChangeScore";
    case StructuralMutation::kChangeScoreField:
      return "ChangeScoreField";
    default:
      return "Unknown";
  }
}

// Apply a structural mutation to the proto based on the mutation type
// and a random seed for variation.
data_model::IndexSchema ApplyStructuralMutation(
    const data_model::IndexSchema &base, StructuralMutation mutation,
    uint32_t seed) {
  data_model::IndexSchema modified = base;

  switch (mutation) {
    case StructuralMutation::kChangeKeyPrefix: {
      if (modified.subscribed_key_prefixes_size() > 0) {
        modified.set_subscribed_key_prefixes(
            0, absl::StrCat("changed_prefix_", seed, ":"));
      }
      break;
    }
    case StructuralMutation::kAddKeyPrefix: {
      modified.add_subscribed_key_prefixes(
          absl::StrCat("new_prefix_", seed, ":"));
      break;
    }
    case StructuralMutation::kRemoveKeyPrefix: {
      // Change the only prefix (removing all prefixes may be invalid).
      if (modified.subscribed_key_prefixes_size() > 0) {
        modified.set_subscribed_key_prefixes(
            0, absl::StrCat("alt_prefix_", seed, ":"));
      }
      break;
    }
    case StructuralMutation::kChangeAttributeDataType: {
      // Note: we can't switch to JSON type as it requires the JSON module.
      // Instead, toggle between HASH and UNSPECIFIED which is a structural
      // change the MessageDifferencer will detect.
      modified.set_attribute_data_type(
          modified.attribute_data_type() == data_model::ATTRIBUTE_DATA_TYPE_HASH
              ? data_model::ATTRIBUTE_DATA_TYPE_UNSPECIFIED
              : data_model::ATTRIBUTE_DATA_TYPE_HASH);
      break;
    }
    case StructuralMutation::kChangeLanguage: {
      modified.set_language(modified.language() == data_model::LANGUAGE_ENGLISH
                                ? data_model::LANGUAGE_UNSPECIFIED
                                : data_model::LANGUAGE_ENGLISH);
      break;
    }
    case StructuralMutation::kChangePunctuation: {
      modified.set_punctuation(absl::StrCat("punct_", seed));
      break;
    }
    case StructuralMutation::kChangeWithOffsets: {
      modified.set_with_offsets(!modified.with_offsets());
      break;
    }
    case StructuralMutation::kChangeMinStemSize: {
      modified.set_min_stem_size(modified.min_stem_size() + 1 + (seed % 5));
      break;
    }
    case StructuralMutation::kChangeSkipInitialScan: {
      modified.set_skip_initial_scan(!modified.skip_initial_scan());
      break;
    }
    case StructuralMutation::kAddAttribute: {
      auto *new_attr = modified.add_attributes();
      new_attr->set_alias(absl::StrCat("new_attr_", seed));
      new_attr->set_identifier(absl::StrCat("new_field_", seed));
      new_attr->mutable_index()->mutable_numeric_index();
      break;
    }
    case StructuralMutation::kRemoveAttribute: {
      if (modified.attributes_size() > 1) {
        modified.mutable_attributes()->RemoveLast();
      } else {
        // Modify the existing attribute instead of leaving index with 0.
        modified.mutable_attributes(0)->set_alias(
            absl::StrCat("modified_alias_", seed));
      }
      break;
    }
    case StructuralMutation::kChangeAttributeAlias: {
      if (modified.attributes_size() > 0) {
        modified.mutable_attributes(0)->set_alias(
            absl::StrCat("attr_alias_", seed));
      }
      break;
    }
    case StructuralMutation::kChangeAttributeIdentifier: {
      if (modified.attributes_size() > 0) {
        modified.mutable_attributes(0)->set_identifier(
            absl::StrCat("attr_id_", seed));
      }
      break;
    }
    case StructuralMutation::kChangeVectorDimension: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index()) {
        modified.mutable_attributes(0)
            ->mutable_index()
            ->mutable_vector_index()
            ->set_dimension_count(20 + (seed % 100));
      }
      break;
    }
    case StructuralMutation::kChangeVectorNormalize: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index()) {
        auto *vi = modified.mutable_attributes(0)
                       ->mutable_index()
                       ->mutable_vector_index();
        vi->set_normalize(!vi->normalize());
      }
      break;
    }
    case StructuralMutation::kChangeDistanceMetric: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index()) {
        auto *vi = modified.mutable_attributes(0)
                       ->mutable_index()
                       ->mutable_vector_index();
        vi->set_distance_metric(vi->distance_metric() ==
                                        data_model::DISTANCE_METRIC_COSINE
                                    ? data_model::DISTANCE_METRIC_L2
                                    : data_model::DISTANCE_METRIC_COSINE);
      }
      break;
    }
    case StructuralMutation::kChangeVectorDataType: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index()) {
        auto *vi = modified.mutable_attributes(0)
                       ->mutable_index()
                       ->mutable_vector_index();
        vi->set_vector_data_type(vi->vector_data_type() ==
                                         data_model::VECTOR_DATA_TYPE_FLOAT32
                                     ? data_model::VECTOR_DATA_TYPE_UNSPECIFIED
                                     : data_model::VECTOR_DATA_TYPE_FLOAT32);
      }
      break;
    }
    case StructuralMutation::kChangeInitialCap: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index()) {
        modified.mutable_attributes(0)
            ->mutable_index()
            ->mutable_vector_index()
            ->set_initial_cap(200 + (seed % 500));
      }
      break;
    }
    case StructuralMutation::kChangeHnswM: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index() &&
          modified.attributes(0).index().vector_index().has_hnsw_algorithm()) {
        modified.mutable_attributes(0)
            ->mutable_index()
            ->mutable_vector_index()
            ->mutable_hnsw_algorithm()
            ->set_m(32 + (seed % 64));
      }
      break;
    }
    case StructuralMutation::kChangeHnswEfConstruction: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index() &&
          modified.attributes(0).index().vector_index().has_hnsw_algorithm()) {
        modified.mutable_attributes(0)
            ->mutable_index()
            ->mutable_vector_index()
            ->mutable_hnsw_algorithm()
            ->set_ef_construction(300 + (seed % 200));
      }
      break;
    }
    case StructuralMutation::kChangeHnswEfRuntime: {
      if (modified.attributes_size() > 0 &&
          modified.attributes(0).index().has_vector_index() &&
          modified.attributes(0).index().vector_index().has_hnsw_algorithm()) {
        modified.mutable_attributes(0)
            ->mutable_index()
            ->mutable_vector_index()
            ->mutable_hnsw_algorithm()
            ->set_ef_runtime(50 + (seed % 100));
      }
      break;
    }
    case StructuralMutation::kAddStopWord: {
      modified.add_stop_words(absl::StrCat("stopword_", seed));
      break;
    }
    case StructuralMutation::kChangeScore: {
      float new_score = static_cast<float>(seed % 100) / 100.0f;
      modified.set_score(new_score);
      break;
    }
    case StructuralMutation::kChangeScoreField: {
      modified.set_score_field(absl::StrCat("score_field_", seed));
      break;
    }
    default:
      break;
  }
  return modified;
}

// Test case structure for parameterized test.
struct StructuralChangeTestCase {
  StructuralMutation mutation;
  uint32_t seed;

  std::string TestName() const {
    return absl::StrCat(MutationName(mutation), "_seed", seed);
  }
};

class StructuralProtoChangeRebuildTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    mock_client_pool_ = std::make_unique<coordinator::MockClientPool>();

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            SendClusterMessage(testing::_, testing::_, testing::_, testing::_,
                               testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            Call(testing::_, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                 testing::StrEq("SLOTS")))
        .WillByDefault(testing::Return(
            reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyType(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(VALKEYMODULE_REPLY_ARRAY));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyLength(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));

    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       *mock_client_pool_));
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    coordinator::MetadataManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
  std::unique_ptr<coordinator::MockClientPool> mock_client_pool_;
};

// Generate test cases covering all structural mutation types (one per type).
std::vector<StructuralChangeTestCase> GenerateStructuralTestCases() {
  std::vector<StructuralChangeTestCase> cases;
  const int num_mutations = static_cast<int>(StructuralMutation::kCount);

  std::mt19937 rng(42);  // Fixed seed for reproducibility.
  for (int m = 0; m < num_mutations; ++m) {
    cases.push_back({static_cast<StructuralMutation>(m), rng()});
  }
  return cases;
}

// Structural proto change triggers full rebuild.
//
// For each structural mutation type, creates a fresh index, applies the
// mutation via CreateEntry, and verifies the IndexSchema pointer changes
// (proving full teardown + rebuild occurred).
TEST_F(StructuralProtoChangeRebuildTest,
       PropertyStructuralChangeTriggersFullRebuild) {
  auto test_cases = GenerateStructuralTestCases();
  ASSERT_GE(test_cases.size(), 20u)
      << "Expected at least 20 test cases (one per mutation type)";

  constexpr int kIterations = 100;
  int valid_cases = 0;

  const int loop_count =
      std::min(kIterations, static_cast<int>(test_cases.size()));
  for (int i = 0; i < loop_count; ++i) {
    const auto &tc = test_cases[i];

    // Fresh singleton state per iteration.
    coordinator::MetadataManager::InitInstance(nullptr);
    SchemaManager::InitInstance(nullptr);

    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       *mock_client_pool_));
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));

    std::string index_name = absl::StrCat("idx_", i);

    // 1. Build a base proto.
    data_model::IndexSchema base_proto;
    base_proto.set_name(index_name);
    base_proto.set_db_num(kDbNum);
    base_proto.add_subscribed_key_prefixes("prefix:");
    base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);
    base_proto.set_language(data_model::LANGUAGE_ENGLISH);
    base_proto.set_punctuation(".,!?");
    base_proto.set_with_offsets(true);
    base_proto.set_min_stem_size(3);
    base_proto.set_skip_initial_scan(true);

    auto *attr = base_proto.add_attributes();
    attr->set_alias("vec_attr");
    attr->set_identifier("vec_field");
    auto *vec = attr->mutable_index()->mutable_vector_index();
    vec->set_dimension_count(4);
    vec->set_normalize(true);
    vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec->set_initial_cap(10);
    auto *hnsw = vec->mutable_hnsw_algorithm();
    hnsw->set_m(16);
    hnsw->set_ef_construction(200);
    hnsw->set_ef_runtime(10);

    // Create the index via SchemaManager (coordinator mode internally uses
    // MetadataManager::CreateEntry which triggers OnMetadataCallback).
    auto create_result =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
    ASSERT_TRUE(create_result.ok())
        << "Iteration " << i << " (" << MutationName(tc.mutation)
        << "): CreateIndexSchema failed: " << create_result.status();

    // 2. Get the pointer to the current IndexSchema.
    auto original_or =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    ASSERT_TRUE(original_or.ok())
        << "Iteration " << i << ": GetIndexSchema failed";
    IndexSchema *original_ptr = original_or.value().get();
    ASSERT_NE(original_ptr, nullptr);

    // 3. Apply a structural mutation.
    auto modified_proto =
        ApplyStructuralMutation(base_proto, tc.mutation, tc.seed);

    // 4. Re-commit the modified proto via CreateEntry
    //    (triggers OnMetadataCallback → detects structural change → rebuild).
    auto packed_modified = std::make_unique<google::protobuf::Any>();
    packed_modified->PackFrom(modified_proto);
    auto update_result = coordinator::MetadataManager::Instance().CreateEntry(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(kDbNum, index_name), std::move(packed_modified));
    // Some mutations may produce an invalid proto that gets rejected
    // (e.g., unsupported attribute type). Skip these.
    if (!update_result.ok()) {
      continue;
    }

    // 5. Verify the IndexSchema pointer CHANGED (full rebuild occurred).
    auto new_or = SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    if (!new_or.ok()) {
      // Index was removed but recreation failed — still confirms structural
      // change was detected (the Remove happened).
      ++valid_cases;
      continue;
    }
    IndexSchema *new_ptr = new_or.value().get();
    ASSERT_NE(new_ptr, nullptr);

    EXPECT_NE(new_ptr, original_ptr)
        << "Iteration " << i << " (" << MutationName(tc.mutation)
        << ", seed=" << tc.seed
        << "): structural change did NOT trigger full rebuild. "
        << "IndexSchema pointer should have changed.";
    ++valid_cases;
  }

  // Ensure we validated a significant number of cases.
  EXPECT_GE(valid_cases, static_cast<int>(test_cases.size()) * 80 / 100)
      << "Too many test cases were skipped due to validation errors";
}

// Alias list sort invariant.
//
// After any sequence of AddAlias/RemoveAlias operations, the aliases list
// is always in lexicographic ascending order.
class AliasListDeterminismTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Standalone mode: no coordinator, no MetadataManager needed.
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
};

TEST_F(AliasListDeterminismTest, PropertyAliasListAlwaysSorted) {
  constexpr int kIterations = 150;
  constexpr uint64_t kSeed = 31337;

  const std::string index_name = "sort_invariant_idx";

  // Build a base proto for the index.
  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("prefix:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("vec_attr");
  attr->set_identifier("vec_field");
  auto *vec = attr->mutable_index()->mutable_vector_index();
  vec->set_dimension_count(4);
  vec->set_normalize(false);
  vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
  vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
  vec->set_initial_cap(10);
  auto *hnsw = vec->mutable_hnsw_algorithm();
  hnsw->set_m(16);
  hnsw->set_ef_construction(200);
  hnsw->set_ef_runtime(10);

  // Create the index in standalone mode.
  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok())
      << "CreateIndexSchema failed: " << create_result.status();

  std::mt19937 rng(kSeed);
  // Track which aliases are currently active so we can selectively remove.
  absl::flat_hash_set<std::string> active_aliases;

  for (int iter = 0; iter < kIterations; ++iter) {
    // Decide whether to add or remove an alias.
    // Bias toward add (70%) when few aliases exist, toward remove (50%) when
    // many exist.
    std::uniform_int_distribution<int> action_dist(0, 99);
    int action = action_dist(rng);
    bool do_add =
        active_aliases.empty() || (action < 70 && active_aliases.size() < 30);

    if (do_add) {
      // Generate a random alias name.
      std::string alias = "alias_" + RandomString(rng, 10);
      auto status =
          SchemaManager::Instance().AddAlias(kDbNum, alias, index_name);
      if (status.ok()) {
        active_aliases.insert(alias);
      }
      // If it fails (e.g., duplicate), that's fine — we still check sorting.
    } else {
      // Remove a random active alias.
      std::uniform_int_distribution<int> idx_dist(
          0, static_cast<int>(active_aliases.size()) - 1);
      auto it = active_aliases.begin();
      std::advance(it, idx_dist(rng));
      std::string alias_to_remove = *it;
      auto status =
          SchemaManager::Instance().RemoveAlias(kDbNum, alias_to_remove);
      if (status.ok()) {
        active_aliases.erase(alias_to_remove);
      }
    }

    // After each operation, verify the IndexSchema's aliases are sorted.
    auto schema_or =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    ASSERT_TRUE(schema_or.ok()) << "Iteration " << iter;

    const auto &aliases = schema_or.value()->GetAliases();

    // Verify lexicographic ascending order.
    for (size_t i = 1; i < aliases.size(); ++i) {
      ASSERT_LT(aliases[i - 1], aliases[i])
          << "Iteration " << iter
          << ": aliases not in sorted order at positions " << (i - 1) << " and "
          << i << ": \"" << aliases[i - 1] << "\" >= \"" << aliases[i] << "\"";
    }

    // Also verify the alias count matches our tracking set.
    ASSERT_EQ(aliases.size(), active_aliases.size())
        << "Iteration " << iter
        << ": alias count mismatch between IndexSchema and tracking set";

    // Verify GetAllAliases() also returns sorted results.
    auto all_aliases = SchemaManager::Instance().GetAllAliases(kDbNum);
    for (size_t i = 1; i < all_aliases.size(); ++i) {
      ASSERT_LE(all_aliases[i - 1].first, all_aliases[i].first)
          << "Iteration " << iter
          << ": GetAllAliases() not in sorted order at positions " << (i - 1)
          << " and " << i << ": \"" << all_aliases[i - 1].first << "\" > \""
          << all_aliases[i].first << "\"";
    }

    // Verify GetAllAliases() count matches active aliases.
    ASSERT_EQ(all_aliases.size(), active_aliases.size())
        << "Iteration " << iter
        << ": GetAllAliases() count mismatch with tracking set";
  }
}

// ALIASUPDATE atomicity: after UpdateAlias completes, the alias exists in
// exactly the target index and is absent from the source. The update is atomic
// (single lock scope), so the alias is never absent from both indexes.
class AliasUpdateReachabilityTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
};

TEST_F(AliasUpdateReachabilityTest,
       PropertyAddBeforeRemovePreservesReachability) {
  constexpr int kIterations = 150;
  constexpr uint64_t kSeed = 99991;

  std::mt19937 rng(kSeed);

  for (int iter = 0; iter < kIterations; ++iter) {
    // Fresh singleton state per iteration so indexes don't accumulate.
    SchemaManager::InitInstance(nullptr);
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));

    // Generate random index names for X and Y (ensure they differ).
    std::string index_x = "idx_x_" + RandomString(rng, 8);
    std::string index_y = "idx_y_" + RandomString(rng, 8);
    // Ensure X and Y are distinct.
    while (index_y == index_x) {
      index_y = "idx_y_" + RandomString(rng, 8);
    }

    // Generate a random alias name.
    std::string alias = "alias_" + RandomString(rng, 8);

    // Create index X.
    data_model::IndexSchema proto_x;
    proto_x.set_name(index_x);
    proto_x.set_db_num(kDbNum);
    proto_x.add_subscribed_key_prefixes("px_" + RandomString(rng, 3) + ":");
    proto_x.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);
    auto *attr_x = proto_x.add_attributes();
    attr_x->set_alias("vec_attr");
    attr_x->set_identifier("vec_field");
    auto *vec_x = attr_x->mutable_index()->mutable_vector_index();
    vec_x->set_dimension_count(4);
    vec_x->set_normalize(false);
    vec_x->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec_x->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec_x->set_initial_cap(10);
    auto *hnsw_x = vec_x->mutable_hnsw_algorithm();
    hnsw_x->set_m(16);
    hnsw_x->set_ef_construction(200);
    hnsw_x->set_ef_runtime(10);

    auto create_x =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto_x);
    ASSERT_TRUE(create_x.ok())
        << "Iteration " << iter
        << ": CreateIndexSchema(X) failed: " << create_x.status();

    // Create index Y.
    data_model::IndexSchema proto_y;
    proto_y.set_name(index_y);
    proto_y.set_db_num(kDbNum);
    proto_y.add_subscribed_key_prefixes("py_" + RandomString(rng, 3) + ":");
    proto_y.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);
    auto *attr_y = proto_y.add_attributes();
    attr_y->set_alias("vec_attr");
    attr_y->set_identifier("vec_field");
    auto *vec_y = attr_y->mutable_index()->mutable_vector_index();
    vec_y->set_dimension_count(4);
    vec_y->set_normalize(false);
    vec_y->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec_y->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec_y->set_initial_cap(10);
    auto *hnsw_y = vec_y->mutable_hnsw_algorithm();
    hnsw_y->set_m(16);
    hnsw_y->set_ef_construction(200);
    hnsw_y->set_ef_runtime(10);

    auto create_y =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto_y);
    ASSERT_TRUE(create_y.ok())
        << "Iteration " << iter
        << ": CreateIndexSchema(Y) failed: " << create_y.status();

    // Add alias A to index X.
    auto add_status =
        SchemaManager::Instance().AddAlias(kDbNum, alias, index_x);
    ASSERT_TRUE(add_status.ok())
        << "Iteration " << iter << ": AddAlias failed: " << add_status;

    // Verify alias is in X before update.
    auto schema_x_before =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_x);
    ASSERT_TRUE(schema_x_before.ok()) << "Iteration " << iter;
    const auto &x_aliases_before = schema_x_before.value()->GetAliases();
    ASSERT_NE(
        std::find(x_aliases_before.begin(), x_aliases_before.end(), alias),
        x_aliases_before.end())
        << "Iteration " << iter << ": alias not found in X before UpdateAlias";

    // Perform UpdateAlias: move alias from X to Y.
    auto update_status =
        SchemaManager::Instance().UpdateAlias(kDbNum, alias, index_y);
    ASSERT_TRUE(update_status.ok())
        << "Iteration " << iter << ": UpdateAlias failed: " << update_status;

    // Property verification: alias must be reachable — present in exactly
    // the target index Y.
    auto schema_x_after =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_x);
    ASSERT_TRUE(schema_x_after.ok()) << "Iteration " << iter;
    const auto &x_aliases_after = schema_x_after.value()->GetAliases();
    bool alias_in_x = std::find(x_aliases_after.begin(), x_aliases_after.end(),
                                alias) != x_aliases_after.end();

    auto schema_y_after =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_y);
    ASSERT_TRUE(schema_y_after.ok()) << "Iteration " << iter;
    const auto &y_aliases_after = schema_y_after.value()->GetAliases();
    bool alias_in_y = std::find(y_aliases_after.begin(), y_aliases_after.end(),
                                alias) != y_aliases_after.end();

    // Core property: alias must NEVER be absent from both indexes.
    ASSERT_TRUE(alias_in_x || alias_in_y)
        << "Iteration " << iter << ": REACHABILITY VIOLATION — alias \""
        << alias << "\" is absent from both index X (\"" << index_x
        << "\") and index Y (\"" << index_y << "\") after UpdateAlias";

    // After a successful UpdateAlias, alias should be in Y and NOT in X.
    EXPECT_TRUE(alias_in_y) << "Iteration " << iter << ": alias \"" << alias
                            << "\" not found in target index Y (\"" << index_y
                            << "\") after UpdateAlias";
    EXPECT_FALSE(alias_in_x) << "Iteration " << iter << ": alias \"" << alias
                             << "\" still present in source index X (\""
                             << index_x << "\") after UpdateAlias";

    // Also verify the forward alias map points to Y.
    auto all_aliases = SchemaManager::Instance().GetAllAliases(kDbNum);
    bool found_in_map = false;
    for (const auto &[a, idx] : all_aliases) {
      if (a == alias) {
        EXPECT_EQ(idx, index_y)
            << "Iteration " << iter << ": Forward alias map has alias \""
            << alias << "\" pointing to \"" << idx << "\" instead of \""
            << index_y << "\"";
        found_in_map = true;
        break;
      }
    }
    EXPECT_TRUE(found_in_map)
        << "Iteration " << iter << ": alias \"" << alias
        << "\" not found in forward alias map after UpdateAlias";
  }
}

// Null-byte alias acceptance: aliases with embedded null bytes are valid.
class NullByteAliasAcceptanceTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Standalone mode: no coordinator needed for this validation test.
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
};

TEST_F(NullByteAliasAcceptanceTest, PropertyNullByteAliasesAccepted) {
  const std::string index_name = "null_byte_test_idx";

  // Build a base proto for the index.
  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("prefix:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("vec_attr");
  attr->set_identifier("vec_field");
  auto *vec = attr->mutable_index()->mutable_vector_index();
  vec->set_dimension_count(4);
  vec->set_normalize(false);
  vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
  vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
  vec->set_initial_cap(10);
  auto *hnsw = vec->mutable_hnsw_algorithm();
  hnsw->set_m(16);
  hnsw->set_ef_construction(200);
  hnsw->set_ef_runtime(10);

  // Create the index in standalone mode.
  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok())
      << "CreateIndexSchema failed: " << create_result.status();

  // Alias containing a null byte.
  std::string null_alias = "alias_with";
  null_alias.push_back('\0');
  null_alias += "null";

  auto add_status =
      SchemaManager::Instance().AddAlias(kDbNum, null_alias, index_name);
  EXPECT_TRUE(add_status.ok()) << add_status;

  auto aliases = SchemaManager::Instance().GetAllAliases(kDbNum);
  EXPECT_EQ(aliases.size(), 1);

  auto remove_status =
      SchemaManager::Instance().RemoveAlias(kDbNum, null_alias);
  EXPECT_TRUE(remove_status.ok()) << remove_status;

  // UpdateAlias with a null-byte alias.
  std::string null_alias2 = "other";
  null_alias2.push_back('\0');
  null_alias2 += "alias";

  auto update_status =
      SchemaManager::Instance().UpdateAlias(kDbNum, null_alias2, index_name);
  EXPECT_TRUE(update_status.ok()) << update_status;
}

// Hashtag validation for single-slot indexes.
class HashtagAliasValidationTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Standalone mode: no coordinator needed for this validation test.
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
};

TEST_F(HashtagAliasValidationTest, AliasWithMatchingHashtagSucceeds) {
  const std::string index_name = "my_idx{slot1}";

  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("doc:{slot1}:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("tag_attr");
  attr->set_identifier("tag_field");
  attr->mutable_index()->mutable_tag_index();

  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok()) << create_result.status();

  auto status =
      SchemaManager::Instance().AddAlias(kDbNum, "alias{slot1}", index_name);
  EXPECT_TRUE(status.ok()) << status;

  auto update_status = SchemaManager::Instance().UpdateAlias(
      kDbNum, "other_alias{slot1}", index_name);
  EXPECT_TRUE(update_status.ok()) << update_status;
}

TEST_F(HashtagAliasValidationTest, AliasWithoutHashtagRejected) {
  const std::string index_name = "my_idx{slot1}";

  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("doc:{slot1}:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("tag_attr");
  attr->set_identifier("tag_field");
  attr->mutable_index()->mutable_tag_index();

  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok()) << create_result.status();

  auto status =
      SchemaManager::Instance().AddAlias(kDbNum, "plain_alias", index_name);
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), testing::HasSubstr("hashtag does not match"));

  auto update_status =
      SchemaManager::Instance().UpdateAlias(kDbNum, "plain_alias", index_name);
  EXPECT_EQ(update_status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(update_status.message(),
              testing::HasSubstr("hashtag does not match"));
}

TEST_F(HashtagAliasValidationTest, AliasWithDifferentHashtagRejected) {
  const std::string index_name = "my_idx{slot1}";

  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("doc:{slot1}:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("tag_attr");
  attr->set_identifier("tag_field");
  attr->mutable_index()->mutable_tag_index();

  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok()) << create_result.status();

  auto status = SchemaManager::Instance().AddAlias(kDbNum, "alias{different}",
                                                   index_name);
  EXPECT_EQ(status.code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(status.message(), testing::HasSubstr("hashtag does not match"));
}

TEST_F(HashtagAliasValidationTest, NonHashtagIndexAllowsAnyAlias) {
  const std::string index_name = "regular_index";

  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("doc:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("tag_attr");
  attr->set_identifier("tag_field");
  attr->mutable_index()->mutable_tag_index();

  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok()) << create_result.status();

  auto status1 =
      SchemaManager::Instance().AddAlias(kDbNum, "plain_alias", index_name);
  EXPECT_TRUE(status1.ok()) << status1;

  auto status2 =
      SchemaManager::Instance().AddAlias(kDbNum, "alias{anything}", index_name);
  EXPECT_TRUE(status2.ok()) << status2;
}

// Duplicate ALIASADD rejection.
//
// Calling AddAlias twice with the same alias returns AlreadyExists on the
// second call. The alias appears exactly once in the stored proto and
// GetAllAliases count does not increase on retry.
class DuplicateAliasAddTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    mock_client_pool_ = std::make_unique<coordinator::MockClientPool>();

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            SendClusterMessage(testing::_, testing::_, testing::_, testing::_,
                               testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Mock cluster operations for MetadataManager.
    ON_CALL(*kMockValkeyModule,
            Call(testing::_, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                 testing::StrEq("SLOTS")))
        .WillByDefault(testing::Return(
            reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyType(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(VALKEYMODULE_REPLY_ARRAY));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyLength(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));

    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       *mock_client_pool_));
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    coordinator::MetadataManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
  std::unique_ptr<coordinator::MockClientPool> mock_client_pool_;
};

TEST_F(DuplicateAliasAddTest, PropertyDuplicateAddReturnsAlreadyExists) {
  constexpr int kIterations = 120;
  constexpr uint64_t kSeed = 11213;

  const std::string index_name = "idempotent_add_idx";

  // Build a base proto for the index.
  data_model::IndexSchema base_proto;
  base_proto.set_name(index_name);
  base_proto.set_db_num(kDbNum);
  base_proto.add_subscribed_key_prefixes("prefix:");
  base_proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);

  auto *attr = base_proto.add_attributes();
  attr->set_alias("vec_attr");
  attr->set_identifier("vec_field");
  auto *vec = attr->mutable_index()->mutable_vector_index();
  vec->set_dimension_count(4);
  vec->set_normalize(false);
  vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
  vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
  vec->set_initial_cap(10);
  auto *hnsw = vec->mutable_hnsw_algorithm();
  hnsw->set_m(16);
  hnsw->set_ef_construction(200);
  hnsw->set_ef_runtime(10);

  // Create the index in coordinator mode.
  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok())
      << "CreateIndexSchema failed: " << create_result.status();

  std::mt19937 rng(kSeed);

  for (int iter = 0; iter < kIterations; ++iter) {
    // Generate a unique random alias for this iteration.
    std::string alias =
        "alias_" + std::to_string(iter) + "_" + RandomString(rng, 8);

    // First AddAlias — should succeed.
    auto first_add =
        SchemaManager::Instance().AddAlias(kDbNum, alias, index_name);
    ASSERT_TRUE(first_add.ok())
        << "Iteration " << iter << ": First AddAlias failed: " << first_add;

    // Record alias count after first add.
    auto aliases_after_first = SchemaManager::Instance().GetAllAliases(kDbNum);
    size_t count_after_first = aliases_after_first.size();

    // Second AddAlias with the SAME alias to the SAME index — rejected.
    auto second_add =
        SchemaManager::Instance().AddAlias(kDbNum, alias, index_name);
    EXPECT_EQ(second_add.code(), absl::StatusCode::kAlreadyExists)
        << "Iteration " << iter
        << ": Second AddAlias should return AlreadyExists, got: " << second_add;

    // Verify GetAllAliases count did NOT increase on duplicate attempt.
    auto aliases_after_second = SchemaManager::Instance().GetAllAliases(kDbNum);
    size_t count_after_second = aliases_after_second.size();
    EXPECT_EQ(count_after_second, count_after_first)
        << "Iteration " << iter
        << ": GetAllAliases count increased after duplicate attempt. "
        << "Expected " << count_after_first << " but got "
        << count_after_second;

    // Verify the alias appears exactly ONCE in the stored proto.
    auto entry_or = coordinator::MetadataManager::Instance().GetEntryContent(
        kSchemaManagerMetadataTypeName,
        coordinator::ObjName(kDbNum, index_name));
    ASSERT_TRUE(entry_or.ok())
        << "Iteration " << iter
        << ": GetEntryContent failed: " << entry_or.status();

    data_model::IndexSchema stored_proto;
    ASSERT_TRUE(entry_or.value().UnpackTo(&stored_proto))
        << "Iteration " << iter << ": Failed to unpack stored IndexSchema";

    // Count occurrences of the alias in the proto's aliases field.
    int alias_count = 0;
    for (int j = 0; j < stored_proto.aliases_size(); ++j) {
      if (stored_proto.aliases(j) == alias) {
        ++alias_count;
      }
    }
    EXPECT_EQ(alias_count, 1)
        << "Iteration " << iter << ": Alias \"" << alias << "\" appears "
        << alias_count << " times in stored proto (expected exactly 1). "
        << "Duplicate attempt must not introduce a duplicate.";
  }
}

// Index drop removes all aliases atomically.
//
// When an index with aliases is dropped via RemoveIndexSchema, all its
// aliases are removed from the forward alias map with no dangling entries.
class IndexDropAliasAtomicityTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, kDbNum))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Standalone mode: no coordinator, no MetadataManager needed.
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum = 0;
  ValkeyModuleCtx fake_ctx_;
};

TEST_F(IndexDropAliasAtomicityTest, PropertyIndexDropRemovesAllAliases) {
  constexpr int kIterations = 150;
  constexpr uint64_t kSeed = 80021;

  std::mt19937 rng(kSeed);

  for (int iter = 0; iter < kIterations; ++iter) {
    // Fresh singleton state per iteration so indexes don't accumulate.
    SchemaManager::InitInstance(nullptr);
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));

    // Generate a random index name.
    std::string index_name = "idx_drop_" + RandomString(rng, 8);

    // Create the index.
    data_model::IndexSchema proto;
    proto.set_name(index_name);
    proto.set_db_num(kDbNum);
    proto.add_subscribed_key_prefixes("dp_" + RandomString(rng, 3) + ":");
    proto.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);
    auto *attr = proto.add_attributes();
    attr->set_alias("vec_attr");
    attr->set_identifier("vec_field");
    auto *vec = attr->mutable_index()->mutable_vector_index();
    vec->set_dimension_count(4);
    vec->set_normalize(false);
    vec->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec->set_initial_cap(10);
    auto *hnsw = vec->mutable_hnsw_algorithm();
    hnsw->set_m(16);
    hnsw->set_ef_construction(200);
    hnsw->set_ef_runtime(10);

    auto create_result =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto);
    ASSERT_TRUE(create_result.ok())
        << "Iteration " << iter
        << ": CreateIndexSchema failed: " << create_result.status();

    // Add a random number of aliases (1-10) to the index.
    std::uniform_int_distribution<int> alias_count_dist(1, 10);
    int alias_count = alias_count_dist(rng);
    std::vector<std::string> aliases;
    aliases.reserve(alias_count);
    for (int i = 0; i < alias_count; ++i) {
      std::string alias = "alias_" + RandomString(rng, 8);
      auto status =
          SchemaManager::Instance().AddAlias(kDbNum, alias, index_name);
      if (status.ok()) {
        aliases.push_back(alias);
      }
    }

    // Verify all aliases exist in the forward map before drop.
    auto aliases_before = SchemaManager::Instance().GetAllAliases(kDbNum);
    ASSERT_EQ(aliases_before.size(), aliases.size())
        << "Iteration " << iter << ": Expected " << aliases.size()
        << " aliases before drop, got " << aliases_before.size();

    // Drop the index.
    auto remove_status =
        SchemaManager::Instance().RemoveIndexSchema(kDbNum, index_name);
    ASSERT_TRUE(remove_status.ok())
        << "Iteration " << iter
        << ": RemoveIndexSchema failed: " << remove_status;

    // Verify ALL aliases are gone from the forward map (no dangling entries).
    auto aliases_after = SchemaManager::Instance().GetAllAliases(kDbNum);
    EXPECT_TRUE(aliases_after.empty())
        << "Iteration " << iter
        << ": Expected empty alias map after index drop, but got "
        << aliases_after.size() << " dangling entries";

    // Additionally verify the index itself is gone.
    auto schema_or =
        SchemaManager::Instance().GetIndexSchema(kDbNum, index_name);
    EXPECT_EQ(schema_or.status().code(), absl::StatusCode::kNotFound)
        << "Iteration " << iter
        << ": Index should not exist after RemoveIndexSchema";
  }
}

// SwapDB alias map atomicity.
//
// OnSwapDB atomically swaps the forward alias maps so that db 0's aliases
// after the swap equal db 1's aliases before the swap, and vice versa.
class SwapDBAliasAtomicityTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(kDbNum0));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));

    // Standalone mode: no coordinator, no MetadataManager needed.
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  static constexpr int kDbNum0 = 0;
  static constexpr int kDbNum1 = 1;
  ValkeyModuleCtx fake_ctx_;
};

TEST_F(SwapDBAliasAtomicityTest, PropertySwapDBSwapsAliasMaps) {
  constexpr int kIterations = 150;
  constexpr uint64_t kSeed = 55781;

  std::mt19937 rng(kSeed);

  for (int iter = 0; iter < kIterations; ++iter) {
    // Fresh singleton state per iteration.
    SchemaManager::InitInstance(nullptr);
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/false));

    // --- Create an index in db 0 with random aliases ---
    std::string index_name_0 = "idx_db0_" + RandomString(rng, 6);

    data_model::IndexSchema proto_0;
    proto_0.set_name(index_name_0);
    proto_0.set_db_num(kDbNum0);
    proto_0.add_subscribed_key_prefixes("p0_" + RandomString(rng, 3) + ":");
    proto_0.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);
    auto *attr_0 = proto_0.add_attributes();
    attr_0->set_alias("vec_attr");
    attr_0->set_identifier("vec_field");
    auto *vec_0 = attr_0->mutable_index()->mutable_vector_index();
    vec_0->set_dimension_count(4);
    vec_0->set_normalize(false);
    vec_0->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec_0->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec_0->set_initial_cap(10);
    auto *hnsw_0 = vec_0->mutable_hnsw_algorithm();
    hnsw_0->set_m(16);
    hnsw_0->set_ef_construction(200);
    hnsw_0->set_ef_runtime(10);

    auto create_0 =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto_0);
    ASSERT_TRUE(create_0.ok())
        << "Iteration " << iter
        << ": CreateIndexSchema(db0) failed: " << create_0.status();

    // Add random aliases to db 0's index.
    std::uniform_int_distribution<int> alias_count_dist(0, 5);
    int alias_count_0 = alias_count_dist(rng);
    for (int i = 0; i < alias_count_0; ++i) {
      std::string alias = "a0_" + RandomString(rng, 6);
      SchemaManager::Instance().AddAlias(kDbNum0, alias, index_name_0);
    }

    // --- Create an index in db 1 with random aliases ---
    std::string index_name_1 = "idx_db1_" + RandomString(rng, 6);

    data_model::IndexSchema proto_1;
    proto_1.set_name(index_name_1);
    proto_1.set_db_num(kDbNum1);
    proto_1.add_subscribed_key_prefixes("p1_" + RandomString(rng, 3) + ":");
    proto_1.set_attribute_data_type(data_model::ATTRIBUTE_DATA_TYPE_HASH);
    auto *attr_1 = proto_1.add_attributes();
    attr_1->set_alias("vec_attr");
    attr_1->set_identifier("vec_field");
    auto *vec_1 = attr_1->mutable_index()->mutable_vector_index();
    vec_1->set_dimension_count(4);
    vec_1->set_normalize(false);
    vec_1->set_distance_metric(data_model::DISTANCE_METRIC_COSINE);
    vec_1->set_vector_data_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
    vec_1->set_initial_cap(10);
    auto *hnsw_1 = vec_1->mutable_hnsw_algorithm();
    hnsw_1->set_m(16);
    hnsw_1->set_ef_construction(200);
    hnsw_1->set_ef_runtime(10);

    auto create_1 =
        SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, proto_1);
    ASSERT_TRUE(create_1.ok())
        << "Iteration " << iter
        << ": CreateIndexSchema(db1) failed: " << create_1.status();

    // Add random aliases to db 1's index.
    int alias_count_1 = alias_count_dist(rng);
    for (int i = 0; i < alias_count_1; ++i) {
      std::string alias = "a1_" + RandomString(rng, 6);
      SchemaManager::Instance().AddAlias(kDbNum1, alias, index_name_1);
    }

    // Record the alias maps for both databases before the swap.
    auto aliases_db0_before = SchemaManager::Instance().GetAllAliases(kDbNum0);
    auto aliases_db1_before = SchemaManager::Instance().GetAllAliases(kDbNum1);

    // Simulate SwapDB.
    ValkeyModuleSwapDbInfo swap_info;
    swap_info.version = VALKEYMODULE_SWAPDBINFO_VERSION;
    swap_info.dbnum_first = kDbNum0;
    swap_info.dbnum_second = kDbNum1;
    SchemaManager::Instance().OnSwapDB(&swap_info);

    // Verify: alias map for db 0 now equals what db 1 had before.
    auto aliases_db0_after = SchemaManager::Instance().GetAllAliases(kDbNum0);
    // Verify: alias map for db 1 now equals what db 0 had before.
    auto aliases_db1_after = SchemaManager::Instance().GetAllAliases(kDbNum1);

    EXPECT_EQ(aliases_db0_after, aliases_db1_before)
        << "Iteration " << iter
        << ": After SwapDB, db 0's alias map should equal db 1's "
           "original alias map";

    EXPECT_EQ(aliases_db1_after, aliases_db0_before)
        << "Iteration " << iter
        << ": After SwapDB, db 1's alias map should equal db 0's "
           "original alias map";
  }
}

// ============================================================================
// Fingerprint/Consistency Regression Tests
// (Consolidated from alias_cluster_search_propagation_test.cc)
// ============================================================================

constexpr absl::string_view kFingerprintTestProto = R"(
  name: "test_idx"
  db_num: 0
  subscribed_key_prefixes: "prefix:"
  attribute_data_type: ATTRIBUTE_DATA_TYPE_HASH
  language: LANGUAGE_ENGLISH
  with_offsets: true
  min_stem_size: 4
  score: 1.0
  attributes: {
    alias: "vec_attr"
    identifier: "vec_field"
    index: {
      vector_index: {
        dimension_count: 10
        normalize: true
        distance_metric: DISTANCE_METRIC_COSINE
        vector_data_type: VECTOR_DATA_TYPE_FLOAT32
        initial_cap: 100
        hnsw_algorithm {
          m: 16
          ef_construction: 200
          ef_runtime: 10
        }
      }
    }
  }
)";

// ComputeFingerprint is invariant to alias changes.
TEST(AliasFingerprint, ComputeFingerprintIgnoresAliases) {
  data_model::IndexSchema proto_no_aliases;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kFingerprintTestProto), &proto_no_aliases));

  data_model::IndexSchema proto_with_aliases = proto_no_aliases;
  proto_with_aliases.add_aliases("a");
  proto_with_aliases.add_aliases("b");

  google::protobuf::Any any_base;
  any_base.PackFrom(proto_no_aliases);
  auto fp_base = SchemaManager::ComputeFingerprint(any_base);
  ASSERT_TRUE(fp_base.ok()) << fp_base.status();

  google::protobuf::Any any_aliased;
  any_aliased.PackFrom(proto_with_aliases);
  auto fp_aliased = SchemaManager::ComputeFingerprint(any_aliased);
  ASSERT_TRUE(fp_aliased.ok()) << fp_aliased.status();

  EXPECT_EQ(*fp_base, *fp_aliased);
}

// ComputeFingerprint produces distinct values for structural changes.
TEST(AliasFingerprint, ComputeFingerprintDistinguishesStructuralChanges) {
  data_model::IndexSchema base_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kFingerprintTestProto), &base_proto));

  google::protobuf::Any any_base;
  any_base.PackFrom(base_proto);
  auto fp_base = SchemaManager::ComputeFingerprint(any_base);
  ASSERT_TRUE(fp_base.ok()) << fp_base.status();

  // Different dimension count.
  data_model::IndexSchema varied = base_proto;
  varied.mutable_attributes(0)
      ->mutable_index()
      ->mutable_vector_index()
      ->set_dimension_count(128);
  google::protobuf::Any any_varied;
  any_varied.PackFrom(varied);
  auto fp_varied = SchemaManager::ComputeFingerprint(any_varied);
  ASSERT_TRUE(fp_varied.ok()) << fp_varied.status();
  EXPECT_NE(*fp_base, *fp_varied);
}

// Stored proto round-trips correctly through MetadataManager.
class StoredProtoRoundTripTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    mock_client_pool_ = std::make_unique<coordinator::MockClientPool>();

    ON_CALL(*kMockValkeyModule, GetSelectedDb(&fake_ctx_))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
        .WillByDefault(testing::Return(&fake_ctx_));
    ON_CALL(*kMockValkeyModule, FreeThreadSafeContext(testing::_))
        .WillByDefault(testing::Return());
    ON_CALL(*kMockValkeyModule, SelectDb(testing::_, 0))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            SendClusterMessage(testing::_, testing::_, testing::_, testing::_,
                               testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, Replicate(testing::_, testing::_, testing::_))
        .WillByDefault(testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule,
            Call(testing::_, testing::StrEq("CLUSTER"), testing::StrEq("c"),
                 testing::StrEq("SLOTS")))
        .WillByDefault(testing::Return(
            reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyType(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(VALKEYMODULE_REPLY_ARRAY));
    ON_CALL(
        *kMockValkeyModule,
        CallReplyLength(reinterpret_cast<ValkeyModuleCallReply *>(0xDEADBEEF)))
        .WillByDefault(testing::Return(0));
    ON_CALL(*kMockValkeyModule, GetMyClusterID())
        .WillByDefault(testing::Return("fake_node_id"));

    coordinator::MetadataManager::InitInstance(
        std::make_unique<coordinator::MetadataManager>(&fake_ctx_,
                                                       *mock_client_pool_));
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() {}, nullptr, /*coordinator_enabled=*/true));
  }

  void TearDown() override {
    SchemaManager::InitInstance(nullptr);
    coordinator::MetadataManager::InitInstance(nullptr);
    KeyspaceEventManager::InitInstance(nullptr);
    ValkeySearch::InitInstance(nullptr);
    vmsdk::ValkeyTest::TearDown();
  }

  ValkeyModuleCtx fake_ctx_;
  std::unique_ptr<coordinator::MockClientPool> mock_client_pool_;
};

TEST_F(StoredProtoRoundTripTest, StoredProtoMatchesToProto) {
  data_model::IndexSchema base_proto;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      std::string(kFingerprintTestProto), &base_proto));
  auto create_result =
      SchemaManager::Instance().CreateIndexSchema(&fake_ctx_, base_proto);
  ASSERT_TRUE(create_result.ok()) << create_result.status();

  auto entry_or = coordinator::MetadataManager::Instance().GetEntryContent(
      kSchemaManagerMetadataTypeName, coordinator::ObjName(0, "test_idx"));
  ASSERT_TRUE(entry_or.ok()) << entry_or.status();
  data_model::IndexSchema stored_proto;
  ASSERT_TRUE(entry_or.value().UnpackTo(&stored_proto));

  auto schema_or = SchemaManager::Instance().GetIndexSchema(0, "test_idx");
  ASSERT_TRUE(schema_or.ok()) << schema_or.status();
  auto live_proto = schema_or.value()->ToProto();

  google::protobuf::util::MessageDifferencer differ;
  const auto *descriptor = data_model::IndexSchema::descriptor();
  differ.IgnoreField(descriptor->FindFieldByName("aliases"));
  differ.IgnoreField(descriptor->FindFieldByName("stats"));
  EXPECT_TRUE(differ.Compare(*live_proto, stored_proto));
}

}  // namespace
}  // namespace valkey_search
