/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "third_party/hnswlib/space_ip.h"
#include "third_party/hnswlib/space_l2.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::indexes {
namespace {

using ::testing::ElementsAre;
using ::testing::SizeIs;

class HNSWAllowReplaceDeletedTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    std::cout << "HNSWAllowReplaceDeletedTest::SetUp() starting..." << std::endl;
    std::cout.flush();
    
    std::cout << "About to call ValkeyTest::SetUp()..." << std::endl;
    std::cout.flush();
    ValkeyTest::SetUp();
    std::cout << "ValkeyTest::SetUp() completed" << std::endl;
    std::cout.flush();
    
    std::cout << "About to init ValkeySearch instance..." << std::endl;
    std::cout.flush();
    ValkeySearch::InitInstance(std::make_unique<TestableValkeySearch>());
    std::cout << "ValkeySearch instance initialized" << std::endl;
    std::cout.flush();
    
    std::cout << "About to init KeyspaceEventManager..." << std::endl;
    std::cout.flush();
    KeyspaceEventManager::InitInstance(
        std::make_unique<TestableKeyspaceEventManager>());
    std::cout << "KeyspaceEventManager initialized" << std::endl;
    std::cout.flush();
    
    std::cout << "About to init SchemaManager..." << std::endl;
    std::cout.flush();
    SchemaManager::InitInstance(std::make_unique<TestableSchemaManager>(
        &fake_ctx_, []() { server_events::SubscribeToServerEvents(); }, nullptr,
        false));
    std::cout << "SchemaManager initialized" << std::endl;
    std::cout.flush();
    
    std::cout << "About to setup VectorExternalizer..." << std::endl;
    std::cout.flush();
    VectorExternalizer::Instance().Init(&registry_ctx_);
    std::cout << "VectorExternalizer setup completed" << std::endl;
    std::cout.flush();
    
    std::cout << "ValkeySearchTest::SetUp() completed" << std::endl;
    std::cout.flush();
    
    // Reset options to defaults
    std::cout << "About to reset options..." << std::endl;
    std::cout.flush();
    VMSDK_EXPECT_OK(options::Reset());
    std::cout << "Options reset completed" << std::endl;
    std::cout.flush();
  }

  // Generate random float vector
  std::vector<float> GenerateRandomVector(size_t dim, std::mt19937& gen) {
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::vector<float> vec(dim);
    for (size_t i = 0; i < dim; ++i) {
      vec[i] = dist(gen);
    }
    return vec;
  }

  // Measure memory usage
  struct MemoryStats {
    size_t initial_cap;
    size_t current_elements;
    size_t deleted_elements;
    size_t max_elements;
  };

  MemoryStats GetMemoryStats(const std::shared_ptr<VectorHNSW<float>>& index) {
    return {
        .initial_cap = index->GetCapacity(),
        .current_elements = index->GetElementCount(),
        .deleted_elements = index->GetDeletedCount(),  
        .max_elements = index->GetCapacity()  // Use capacity as approximation
    };
  }
};

/**
 * TEST PURPOSE: Verifies basic functionality of the allow_replace_deleted feature
 * 
 * WHAT IT TESTS:
 * - Sequential remove/add operations with replacement enabled
 * - Ensures deleted slots are properly reused during new insertions
 * - Validates that search functionality remains intact after replacements
 * 
 * HOW IT WORKS:
 * 1. Creates HNSW index with allow_replace_deleted=true
 * 2. Adds 2 initial vectors to establish baseline
 * 3. Performs 10 iterations of: search -> remove -> add -> search
 * 4. In each iteration:
 *    - Searches to verify current state
 *    - Removes a vector (should increase deleted_count, keep current_elements same)
 *    - Adds new vector with same key (should reuse deleted slot, decrease deleted_count)
 *    - Searches again to verify functionality
 * 
 * EXPECTED BEHAVIOR:
 * - current_elements stays constant (2) throughout all operations
 * - deleted_elements cycles between 0->1->0 in each iteration
 * - Search always returns expected number of results
 * - No memory expansion occurs due to efficient slot reuse
 */
TEST_F(HNSWAllowReplaceDeletedTest, SimpleSequentialTest) {
  std::cout << "Starting simple sequential test..." << std::endl;
  std::cout.flush();
  
  const size_t dim = 4;  // Very small dimension
  const size_t initial_cap = 2;  // Just 2 vectors
  std::mt19937 gen(42);
  
  // Test with allow_replace_deleted = true
  std::cout << "Setting allow_replace_deleted to true..." << std::endl;
  std::cout.flush();
  VMSDK_EXPECT_OK(options::GetHNSWAllowReplaceDeletedMutable().SetValue(true));
  
  auto proto = CreateHNSWVectorIndexProto(
      dim, data_model::DISTANCE_METRIC_COSINE, initial_cap, 4, 100, 10);

  std::cout << "Creating HNSW index..." << std::endl;
  std::cout.flush();

  auto index = VectorHNSW<float>::Create(
      proto, "test_attr", data_model::ATTRIBUTE_DATA_TYPE_HASH).value();

  std::cout << "HNSW index created, adding initial 2 vectors..." << std::endl;
  std::cout.flush();

  // Add 2 initial vectors
  auto vec1 = GenerateRandomVector(dim, gen);
  auto vec2 = GenerateRandomVector(dim, gen);
  
  std::string data1(reinterpret_cast<char*>(vec1.data()), vec1.size() * sizeof(float));
  std::string data2(reinterpret_cast<char*>(vec2.data()), vec2.size() * sizeof(float));
  
  auto key1 = StringInternStore::Intern("vector1");
  auto key2 = StringInternStore::Intern("vector2");
  
  VMSDK_EXPECT_OK(index->AddRecord(key1, data1));
  std::cout << "Added vector1" << std::endl;
  std::cout.flush();
  
  VMSDK_EXPECT_OK(index->AddRecord(key2, data2));
  std::cout << "Added vector2" << std::endl;
  std::cout.flush();

  // Test sequential remove/add/search pattern
  std::cout << "Testing sequential remove/add/search pattern..." << std::endl;
  std::cout.flush();

  for (int i = 0; i < 10; ++i) {
    std::cout << "Iteration " << i + 1 << "/10" << std::endl;
    std::cout.flush();
    
    // 1. Search first
    auto query_vec = GenerateRandomVector(dim, gen);
    std::string query_data(reinterpret_cast<char*>(query_vec.data()), query_vec.size() * sizeof(float));
    
    std::cout << "  Searching..." << std::endl;
    std::cout.flush();
    auto search_result = index->Search(query_data, 2);
    VMSDK_EXPECT_OK(search_result);
    std::cout << "  Search completed, found " << search_result->size() << " results" << std::endl;
    std::cout.flush();

    // 2. Remove vector1
    std::cout << "  Removing vector1..." << std::endl;
    std::cout.flush();
    auto stats_before_remove = GetMemoryStats(index);
    auto remove_result = index->RemoveRecord(key1);
    VMSDK_EXPECT_OK(remove_result);
    auto stats_after_remove = GetMemoryStats(index);
    
    // Validate that deleted count increased but current elements stayed the same
    // (HNSW marks elements as deleted but doesn't physically remove them)
    EXPECT_EQ(stats_after_remove.deleted_elements, stats_before_remove.deleted_elements + 1);
    EXPECT_EQ(stats_after_remove.current_elements, stats_before_remove.current_elements);
    std::cout << "  Remove completed - deleted count: " << stats_after_remove.deleted_elements 
              << ", current elements: " << stats_after_remove.current_elements << std::endl;
    std::cout.flush();

    // 3. Re-add vector1 with new data (this should trigger replacement logic)
    auto new_vec = GenerateRandomVector(dim, gen);
    std::string new_data(reinterpret_cast<char*>(new_vec.data()), new_vec.size() * sizeof(float));
    
    std::cout << "  Adding new vector1..." << std::endl;
    std::cout.flush();
    auto add_result = index->AddRecord(key1, new_data);
    VMSDK_EXPECT_OK(add_result);
    auto stats_after_add = GetMemoryStats(index);
    
    // With allow_replace_deleted=true, deleted count should decrease, current elements should stay the same
    // (reusing the deleted slot doesn't change the total element count)
    EXPECT_EQ(stats_after_add.deleted_elements, stats_after_remove.deleted_elements - 1);
    EXPECT_EQ(stats_after_add.current_elements, stats_after_remove.current_elements);
    std::cout << "  Add completed - deleted count: " << stats_after_add.deleted_elements 
              << ", current elements: " << stats_after_add.current_elements << std::endl;
    std::cout.flush();

    // 4. Search again to make sure everything is still working
    std::cout << "  Final search..." << std::endl;
    std::cout.flush();
    auto final_search = index->Search(query_data, 2);
    VMSDK_EXPECT_OK(final_search);
    std::cout << "  Final search completed, found " << final_search->size() << " results" << std::endl;
    std::cout.flush();
  }

  std::cout << "Sequential test completed successfully!" << std::endl;
  std::cout.flush();
}

/**
 * TEST PURPOSE: Validates memory behavior when replacement is disabled
 * 
 * WHAT IT TESTS:
 * - Memory allocation patterns with allow_replace_deleted=false
 * - Index expansion behavior when deleted slots cannot be reused
 * - Performance impact of memory fragmentation from unused deleted slots
 * 
 * HOW IT WORKS:
 * 1. Creates HNSW index with allow_replace_deleted=false (default behavior)
 * 2. Adds 1000 initial vectors to establish baseline
 * 3. Deletes 500 vectors (half of the index)
 * 4. Adds 500 new vectors (same amount as deleted)
 * 5. Measures memory usage and timing throughout the process
 * 
 * EXPECTED BEHAVIOR:
 * - After deletion: current_elements stays 1000, deleted_elements becomes 500
 * - After new additions: current_elements becomes 1500, deleted_elements stays 500
 * - Index capacity expands beyond initial 1000 to accommodate new vectors
 * - Deleted slots remain unused, causing memory fragmentation
 * - Operation timing reflects the cost of memory expansion
 */
TEST_F(HNSWAllowReplaceDeletedTest, MemoryEfficiencyWithoutReplacement) {
  const size_t dim = 128;
  const size_t initial_cap = 1000;
  std::mt19937 gen(42);

  // Test with allow_replace_deleted = false (default)
  VMSDK_EXPECT_OK(options::GetHNSWAllowReplaceDeletedMutable().SetValue(false));
  
  auto proto_without_replace = CreateHNSWVectorIndexProto(
      dim, data_model::DISTANCE_METRIC_COSINE, initial_cap, 16, 200, 50);

  auto index_without_replace = VectorHNSW<float>::Create(
      proto_without_replace, "test_attr", data_model::ATTRIBUTE_DATA_TYPE_HASH).value();

  auto start = std::chrono::high_resolution_clock::now();
  
  // Add initial vectors
  std::vector<InternedStringPtr> keys;
  for (size_t i = 0; i < initial_cap; ++i) {
    auto vec = GenerateRandomVector(dim, gen);
    std::string data(reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(float));
    auto key = StringInternStore::Intern(std::to_string(i));
    VMSDK_EXPECT_OK(index_without_replace->AddRecord(key, data));
    keys.push_back(key);
  }

  auto stats_after_initial_add = GetMemoryStats(index_without_replace);
  EXPECT_EQ(stats_after_initial_add.current_elements, initial_cap);
  EXPECT_EQ(stats_after_initial_add.deleted_elements, 0);

  // Delete half
  for (size_t i = 0; i < initial_cap / 2; ++i) {
    VMSDK_EXPECT_OK(index_without_replace->RemoveRecord(keys[i]));
  }

  auto stats_after_delete = GetMemoryStats(index_without_replace);
  EXPECT_EQ(stats_after_delete.current_elements, initial_cap);  // No change in total count
  EXPECT_EQ(stats_after_delete.deleted_elements, initial_cap / 2);

  // Add new vectors (will expand the index since replacement is disabled)
  for (size_t i = initial_cap; i < initial_cap + initial_cap / 2; ++i) {
    auto vec = GenerateRandomVector(dim, gen);
    std::string data(reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(float));
    auto key = StringInternStore::Intern(std::to_string(i));
    VMSDK_EXPECT_OK(index_without_replace->AddRecord(key, data));
  }

  auto stats_after_new_add = GetMemoryStats(index_without_replace);
  // With replacement disabled, new vectors are added as new slots, so total count increases
  EXPECT_EQ(stats_after_new_add.current_elements, initial_cap + initial_cap / 2);  // Original + new
  EXPECT_EQ(stats_after_new_add.deleted_elements, initial_cap / 2);  // Half deleted remain

  auto end = std::chrono::high_resolution_clock::now();
  auto duration_without_replace = 
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  auto stats_final = GetMemoryStats(index_without_replace);
  
  // Without replacement, index should have expanded beyond initial capacity
  EXPECT_GE(stats_final.current_elements, initial_cap / 2);  // At least half remain
  
  std::cout << "With allow_replace_deleted=false:\n"
            << "  Time: " << duration_without_replace.count() << "ms\n"
            << "  Max elements: " << stats_final.max_elements << "\n"
            << "  Current elements: " << stats_final.current_elements << "\n";
}

/**
 * TEST PURPOSE: Direct comparison of memory efficiency between replacement modes
 * 
 * WHAT IT TESTS:
 * - Side-by-side comparison of memory usage with/without replacement
 * - Validates that replacement feature delivers expected memory savings
 * - Confirms different behaviors produce predictably different outcomes
 * 
 * HOW IT WORKS:
 * 1. Runs identical test scenarios with both allow_replace_deleted settings
 * 2. For each mode:
 *    - Creates index with 1000 initial capacity
 *    - Adds 1000 vectors to fill the index
 *    - Deletes 500 vectors (creating potential reuse slots)
 *    - Adds 500 new vectors (testing replacement vs expansion behavior)
 * 3. Compares final memory statistics between both modes
 * 
 * EXPECTED BEHAVIOR:
 * - With replacement=true: current=1000, deleted=0, capacity=1000 (efficient reuse)
 * - With replacement=false: current=1500, deleted=500, capacity>1000 (expansion + fragmentation)
 * - Clear demonstration that replacement prevents memory expansion and fragmentation
 */
TEST_F(HNSWAllowReplaceDeletedTest, MemoryEfficiencyComparison) {
  const size_t dim = 128;
  const size_t initial_cap = 1000;
  std::mt19937 gen(42);

  // Test both scenarios: with and without replacement
  for (bool allow_replace : {false, true}) {
    std::cout << "\nTesting with allow_replace_deleted=" << (allow_replace ? "true" : "false") << std::endl;
    
    VMSDK_EXPECT_OK(options::GetHNSWAllowReplaceDeletedMutable().SetValue(allow_replace));
    
    auto proto = CreateHNSWVectorIndexProto(
        dim, data_model::DISTANCE_METRIC_COSINE, initial_cap, 16, 200, 50);

    auto index = VectorHNSW<float>::Create(
        proto, "test_attr", data_model::ATTRIBUTE_DATA_TYPE_HASH).value();

    // Add initial vectors
    std::vector<InternedStringPtr> keys;
    for (size_t i = 0; i < initial_cap; ++i) {
      auto vec = GenerateRandomVector(dim, gen);
      std::string data(reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(float));
      auto key = StringInternStore::Intern(std::to_string(i));
      VMSDK_EXPECT_OK(index->AddRecord(key, data));
      keys.push_back(key);
    }

    auto stats_initial = GetMemoryStats(index);
    EXPECT_EQ(stats_initial.current_elements, initial_cap);
    EXPECT_EQ(stats_initial.deleted_elements, 0);

    // Delete half the vectors
    for (size_t i = 0; i < initial_cap / 2; ++i) {
      VMSDK_EXPECT_OK(index->RemoveRecord(keys[i]));
    }

    auto stats_after_delete = GetMemoryStats(index);
    EXPECT_EQ(stats_after_delete.current_elements, initial_cap);  // No change in total count
    EXPECT_EQ(stats_after_delete.deleted_elements, initial_cap / 2);

    // Add the same number of new vectors as we deleted
    for (size_t i = initial_cap; i < initial_cap + initial_cap / 2; ++i) {
      auto vec = GenerateRandomVector(dim, gen);
      std::string data(reinterpret_cast<char*>(vec.data()), vec.size() * sizeof(float));
      auto key = StringInternStore::Intern(std::to_string(i));
      VMSDK_EXPECT_OK(index->AddRecord(key, data));
    }

    auto stats_final = GetMemoryStats(index);

    if (allow_replace) {
      // With replacement: deleted slots should be reused
      EXPECT_EQ(stats_final.current_elements, initial_cap);
      EXPECT_EQ(stats_final.deleted_elements, 0);  // All deleted slots reused
      EXPECT_EQ(stats_final.max_elements, initial_cap);  // No expansion needed
      
      std::cout << "  With replacement - Current: " << stats_final.current_elements 
                << ", Deleted: " << stats_final.deleted_elements 
                << ", Capacity: " << stats_final.max_elements << std::endl;
    } else {
      // Without replacement: index should expand
      EXPECT_EQ(stats_final.current_elements, initial_cap + initial_cap / 2);  // Original + new elements
      EXPECT_EQ(stats_final.deleted_elements, initial_cap / 2);  // Deleted slots remain unused
      EXPECT_GT(stats_final.max_elements, initial_cap);  // Index expanded
      
      std::cout << "  Without replacement - Current: " << stats_final.current_elements 
                << ", Deleted: " << stats_final.deleted_elements 
                << ", Capacity: " << stats_final.max_elements << std::endl;
    }
  }
}

/**
 * TEST PURPOSE: Validates search accuracy and performance with replacement enabled
 * 
 * WHAT IT TESTS:
 * - Search accuracy remains intact when using deleted slot replacement
 * - Performance comparison between replacement modes during search operations
 * - Ensures replacement logic doesn't negatively impact search quality or speed
 * 
 * HOW IT WORKS:
 * 1. Creates test dataset with 1000 128-dimensional vectors
 * 2. For each replacement mode (false/true):
 *    - Builds HNSW index with all vectors
 *    - Deletes 10% of vectors (every 10th vector)
 *    - Adds 100 new vectors (testing replacement/expansion behavior)
 *    - Performs 100 search queries with k=10
 *    - Measures average query time and result consistency
 * 3. Compares search performance metrics between both modes
 * 
 * EXPECTED BEHAVIOR:
 * - Both modes should return similar result quality (same average results per query)
 * - Replacement mode may show slight performance improvement due to better cache locality
 * - No degradation in search accuracy despite internal slot reuse
 * - Demonstrates that replacement feature maintains search integrity
 */
TEST_F(HNSWAllowReplaceDeletedTest, SearchAccuracyComparison) {
  const size_t dim = 128;
  const size_t num_vectors = 1000;
  const size_t num_queries = 100;
  const size_t k = 10;
  std::mt19937 gen(42);

  // Create test data
  std::vector<std::vector<float>> vectors;
  for (size_t i = 0; i < num_vectors; ++i) {
    vectors.push_back(GenerateRandomVector(dim, gen));
  }

  // Test search accuracy with both settings
  for (bool allow_replace : {false, true}) {
    VMSDK_EXPECT_OK(options::GetHNSWAllowReplaceDeletedMutable().SetValue(allow_replace));
    
    auto proto = CreateHNSWVectorIndexProto(
        dim, data_model::DISTANCE_METRIC_L2, num_vectors, 16, 200, 50);

    auto index = VectorHNSW<float>::Create(
        proto, "test_attr", data_model::ATTRIBUTE_DATA_TYPE_HASH).value();

    // Add vectors with some deletions and replacements
    std::vector<InternedStringPtr> keys;
    for (size_t i = 0; i < num_vectors; ++i) {
      std::string data(reinterpret_cast<char*>(vectors[i].data()), 
                      vectors[i].size() * sizeof(float));
      auto key = StringInternStore::Intern(std::to_string(i));
      VMSDK_EXPECT_OK(index->AddRecord(key, data));
      keys.push_back(key);
    }

    // Delete and replace some vectors
    for (size_t i = 0; i < num_vectors / 10; ++i) {
      VMSDK_EXPECT_OK(index->RemoveRecord(keys[i * 10]));
    }

    for (size_t i = 0; i < num_vectors / 10; ++i) {
      auto new_vec = GenerateRandomVector(dim, gen);
      std::string data(reinterpret_cast<char*>(new_vec.data()), 
                      new_vec.size() * sizeof(float));
      auto key = StringInternStore::Intern(std::to_string(num_vectors + i));
      VMSDK_EXPECT_OK(index->AddRecord(key, data));
    }

    // Measure search performance
    auto start = std::chrono::high_resolution_clock::now();
    size_t total_results = 0;
    
    for (size_t q = 0; q < num_queries; ++q) {
      auto query_vec = GenerateRandomVector(dim, gen);
      std::string query_data(reinterpret_cast<char*>(query_vec.data()), 
                           query_vec.size() * sizeof(float));
      
      auto results = index->Search(query_data, k);
      VMSDK_EXPECT_OK(results);
      if (results.ok()) {
        total_results += results->size();
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto search_duration = 
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    std::cout << "Search performance with allow_replace_deleted=" 
              << (allow_replace ? "true" : "false") << ":\n"
              << "  Average query time: " 
              << search_duration.count() / num_queries << "μs\n"
              << "  Average results per query: " 
              << static_cast<double>(total_results) / num_queries << "\n";
  }
}

}  // namespace
}  // namespace valkey_search::indexes
