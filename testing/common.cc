/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "testing/common.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/blocking_counter.h"
#include "absl/synchronization/mutex.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_hnsw.h"
#include "src/query/search.h"
#include "src/schema_manager.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/thread_pool.h"
#include "vmsdk/src/type_conversions.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {

std::vector<std::vector<float>> DeterministicallyGenerateVectors(
    int size, int dimensions, float max_value) {
  std::vector<std::vector<float>> result(size, std::vector<float>(dimensions));

  for (int i = 0; i < size; ++i) {
    for (int j = 0; j < dimensions; ++j) {
      result[i][j] = max_value * (static_cast<float>(i + j) /
                                  static_cast<float>(size + dimensions));
    }
  }
  return result;
}

void TestableValkeySearch::InitThreadPools(std::optional<size_t> readers,
                                           std::optional<size_t> writers) {
  if (readers) {
    reader_thread_pool_ =
        std::make_unique<vmsdk::ThreadPool>("reader-pool", *readers);
    reader_thread_pool_->StartWorkers();
  }
  if (writers) {
    writer_thread_pool_ =
        std::make_unique<vmsdk::ThreadPool>("writer-pool", *writers);
    writer_thread_pool_->StartWorkers();
  }
}

absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateVectorHNSWSchema(
    std::string index_schema_key, ValkeyModuleCtx *fake_ctx,
    vmsdk::ThreadPool *writer_thread_pool,
    const std::vector<absl::string_view> *key_prefixes,
    int32_t index_schema_db_num) {
  VMSDK_ASSIGN_OR_RETURN(
      auto test_index_schema,
      CreateIndexSchema(index_schema_key, fake_ctx, writer_thread_pool,
                        key_prefixes, index_schema_db_num));

  auto dimensions = 100;
  auto index = indexes::VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(dimensions, data_model::DISTANCE_METRIC_COSINE,
                                 1000, 10, 300, 30),
      "vector_identifier",
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  VMSDK_EXPECT_OK(index);
  VMSDK_EXPECT_OK(test_index_schema->AddIndex("vector", "vector", *index));
  return test_index_schema;
}

absl::StatusOr<std::shared_ptr<MockIndexSchema>> CreateIndexSchema(
    std::string index_schema_key, ValkeyModuleCtx *fake_ctx,
    vmsdk::ThreadPool *writer_thread_pool,
    const std::vector<absl::string_view> *key_prefixes,
    int32_t index_schema_db_num) {
  ValkeyModuleCtx local_fake_ctx;
  if (fake_ctx == nullptr) {
    fake_ctx = &local_fake_ctx;
  }
  std::vector<absl::string_view> local_key_prefixes = {"prefix:"};
  if (key_prefixes == nullptr) {
    key_prefixes = &local_key_prefixes;
  }
  ON_CALL(*kMockValkeyModule, GetSelectedDb(fake_ctx))
      .WillByDefault(testing::Return(index_schema_db_num));
  EXPECT_CALL(*kMockValkeyModule, GetDetachedThreadSafeContext(testing::_))
      .WillRepeatedly(testing::Return(fake_ctx));
  VMSDK_ASSIGN_OR_RETURN(
      auto test_index_schema,
      valkey_search::MockIndexSchema::Create(
          fake_ctx, index_schema_key, *key_prefixes,
          std::make_unique<valkey_search::HashAttributeDataType>(),
          writer_thread_pool));
  VMSDK_RETURN_IF_ERROR(
      SchemaManager::Instance().ImportIndexSchema(test_index_schema));
  return test_index_schema;
}

data_model::VectorIndex CreateHNSWVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric, int initial_cap,
    int m, int ef_construction, size_t ef_runtime) {
  data_model::VectorIndex vector_index_proto;
  vector_index_proto.set_dimension_count(dimensions);
  vector_index_proto.set_distance_metric(distance_metric);
  vector_index_proto.set_initial_cap(initial_cap);
  auto hnsw_algorithm = std::make_unique<data_model::HNSWAlgorithm>();
  hnsw_algorithm->set_m(m);
  hnsw_algorithm->set_ef_construction(ef_construction);
  hnsw_algorithm->set_ef_runtime(ef_runtime);
  vector_index_proto.set_allocated_hnsw_algorithm(hnsw_algorithm.release());
  return vector_index_proto;
}

data_model::VectorIndex CreateFlatVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric, int initial_cap,
    uint32_t block_size) {
  data_model::VectorIndex vector_index_proto;
  vector_index_proto.set_dimension_count(dimensions);
  vector_index_proto.set_distance_metric(distance_metric);
  vector_index_proto.set_initial_cap(initial_cap);
  auto flat_algorithm = std::make_unique<data_model::FlatAlgorithm>();
  flat_algorithm->set_block_size(block_size);
  vector_index_proto.set_allocated_flat_algorithm(flat_algorithm.release());
  return vector_index_proto;
}

data_model::NumericIndex CreateNumericIndexProto() { return {}; }

data_model::TagIndex CreateTagIndexProto(const std::string &separator,
                                         bool case_sensitive) {
  data_model::TagIndex tag_index_proto;
  tag_index_proto.set_separator(separator);
  tag_index_proto.set_case_sensitive(case_sensitive);
  return tag_index_proto;
}

data_model::TextIndex CreateTextIndexProto(bool with_suffix_trie,
                                           bool no_stem,
                                           uint32_t min_stem_size) {
  data_model::TextIndex text_index_proto;
  text_index_proto.set_with_suffix_trie(with_suffix_trie);
  text_index_proto.set_no_stem(no_stem);
  text_index_proto.set_min_stem_size(min_stem_size);
  return text_index_proto;
}

indexes::Neighbor ToIndexesNeighbor(const NeighborTest &neighbor_test) {
  auto string_interned_external_id =
      StringInternStore::Intern(neighbor_test.external_id);
  indexes::Neighbor neighbor(string_interned_external_id,
                             neighbor_test.distance);
  if (neighbor_test.attribute_contents.has_value()) {
    std::optional<RecordsMap> attribute_contents;
    neighbor.attribute_contents.value() = RecordsMap();
    for (const auto &attribute_contents :
         neighbor_test.attribute_contents.value()) {
      auto attribute_alias =
          vmsdk::MakeUniqueValkeyString(attribute_contents.first);
      neighbor.attribute_contents.value().emplace(
          vmsdk::ToStringView(attribute_alias.get()),
          RecordsMapValue(
              std::move(attribute_alias),
              vmsdk::MakeUniqueValkeyString(attribute_contents.second)));
    }
  }
  return neighbor;
}

NeighborTest ToNeighborTest(const indexes::Neighbor &neighbor) {
  NeighborTest neighbor_test;
  neighbor_test.external_id = *neighbor.external_id;
  neighbor_test.distance = neighbor.distance;
  if (neighbor.attribute_contents.has_value()) {
    std::unordered_map<std::string, std::string> attribute_contents;
    for (const auto &attribute_content : neighbor.attribute_contents.value()) {
      attribute_contents[std::string(attribute_content.first)] =
          vmsdk::ToStringView(attribute_content.second.value.get());
    }
    neighbor_test.attribute_contents = std::move(attribute_contents);
  }
  return neighbor_test;
}

query::ReturnAttribute ToReturnAttribute(
    const TestReturnAttribute &test_return_attribute) {
  return query::ReturnAttribute{
      .identifier =
          vmsdk::MakeUniqueValkeyString(test_return_attribute.identifier),
      .alias = vmsdk::MakeUniqueValkeyString(test_return_attribute.alias)};
}

std::unordered_map<std::string, std::string> ToStringMap(
    const RecordsMap &map) {
  std::unordered_map<std::string, std::string> result;
  for (const auto &itr : map) {
    result[std::string(itr.first)] =
        vmsdk::ToStringView(itr.second.value.get());
  }
  return result;
}

bool RespReply::CompareArrays(const RespArray &array1,
                              const RespArray &array2) {
  if (array1.size() != array2.size()) {
    return false;
  }
  return std::is_permutation(array1.begin(), array1.end(), array2.begin());
}

// Equality operator to compare two RespReply objects
bool RespReply::operator==(const RespReply &other) const {
  if (value.index() != other.value.index()) {
    return false;  // Different types in the variant
  }

  if (std::holds_alternative<std::string>(value)) {
    return std::get<std::string>(value) == std::get<std::string>(other.value);
  }
  if (std::holds_alternative<int64_t>(value)) {
    return std::get<int64_t>(value) == std::get<int64_t>(other.value);
  }
  if (std::holds_alternative<RespArray>(value)) {
    const RespArray &array1 = std::get<RespArray>(value);
    const RespArray &array2 = std::get<RespArray>(other.value);
    return CompareArrays(array1, array2);
  }
  return false;
}

// Helper function to parse an integer from RESP
int64_t ParseRespReplyInteger(absl::string_view input, size_t &pos) {
  int64_t result = 0;
  size_t next_pos = input.find("\r\n", pos);
  result = std::stoll(std::string(input.substr(pos, next_pos - pos)));
  pos = next_pos + 2;  // Skip the CRLF
  return result;
}

RespReply ParseRespReply(absl::string_view input, size_t &pos) {
  char prefix = input[pos++];
  RespReply reply;

  if (prefix == '+') {
    // Simple string
    size_t next_pos = input.find("\r\n", pos);
    reply.value = std::string(input.substr(pos, next_pos - pos));
    pos = next_pos + 2;  // Skip the CRLF
  } else if (prefix == '-') {
    // Error string
    size_t next_pos = input.find("\r\n", pos);
    reply.value = "ERROR: " + std::string(input.substr(pos, next_pos - pos));
    pos = next_pos + 2;
  } else if (prefix == ':') {
    // Integer
    reply.value = ParseRespReplyInteger(input, pos);
  } else if (prefix == '$') {
    // Bulk string
    int64_t len = ParseRespReplyInteger(input, pos);
    if (len == -1) {
      reply.value = std::string("NULL");
    } else {
      reply.value = std::string(input.substr(pos, len));
      pos += len + 2;  // Skip the CRLF
    }
  } else if (prefix == '*') {
    // Array
    int64_t array_size = ParseRespReplyInteger(input, pos);
    std::vector<RespReply> array;
    array.reserve(array_size);
    for (int64_t i = 0; i < array_size; ++i) {
      array.push_back(ParseRespReply(input, pos));
    }
    reply.value = array;
  }
  return reply;
}

RespReply ParseRespReply(absl::string_view input) {
  size_t pos = 0;
  return ParseRespReply(input, pos);
}

void WaitWorkerTasksAreCompleted(vmsdk::ThreadPool &thread_pool) {
  auto mutex = std::make_shared<absl::Mutex>();
  auto is_completed = std::make_shared<bool>();
  auto blocking_refcount =
      std::make_shared<absl::BlockingCounter>(thread_pool.Size());
  for (size_t i = 0; i < thread_pool.Size(); ++i) {
    thread_pool.Schedule(
        [blocking_refcount = blocking_refcount, mutex = mutex,
         is_completed = is_completed]() {
          blocking_refcount->DecrementCount();
          absl::MutexLock lock(mutex.get());
          mutex->Await(absl::Condition(is_completed.get()));
        },
        vmsdk::ThreadPool::Priority::kLow);
  }
  blocking_refcount->Wait();
  absl::MutexLock lock(mutex.get());
  *is_completed = true;
}

}  // namespace valkey_search
