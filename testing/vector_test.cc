/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
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
#include "testing/common.h"
#include "third_party/hnswlib/space_ip.h"
#include "third_party/hnswlib/space_l2.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/type_conversions.h"

namespace valkey_search::indexes {

namespace {
constexpr static int kDimensions = 100;
constexpr static int kInitialCap = 15000;
constexpr static uint32_t kBlockSize = 250;
constexpr static int kM = 16;
constexpr static int kEFConstruction = 20;
constexpr static int kEFRuntime = 20;
const hnswlib::InnerProductSpace kInnerProductSpace{kDimensions};
const hnswlib::L2Space kL2Space{kDimensions};
const absl::flat_hash_map<data_model::DistanceMetric, std::string>
    kExpectedSpaces = {
        {data_model::DISTANCE_METRIC_COSINE, typeid(kInnerProductSpace).name()},
        {data_model::DISTANCE_METRIC_IP, typeid(kInnerProductSpace).name()},
        {data_model::DISTANCE_METRIC_L2, typeid(kL2Space).name()},
};

class VectorIndexTest : public ValkeySearchTest {
 public:
  HashAttributeDataType hash_attribute_data_type_;
};

void TestInitializationHNSW(int dimensions,
                            data_model::DistanceMetric distance_metric,
                            const std::string& distance_metric_name,
                            int initial_cap, int m, int ef_construction,
                            size_t ef_runtime) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  auto index = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(dimensions, distance_metric, initial_cap, m,
                                 ef_construction, ef_runtime),
      "attribute_identifier_1",
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  auto* space = index.value()->GetSpace();
  EXPECT_EQ(distance_metric_name, typeid(*space).name());
  EXPECT_EQ(index.value()->GetDimensions(), dimensions);
  EXPECT_EQ(index.value()->GetNormalize(),
            distance_metric == data_model::DISTANCE_METRIC_COSINE);
  EXPECT_EQ(index.value()->GetCapacity(), initial_cap);
  EXPECT_EQ(index.value()->GetM(), m);
  EXPECT_EQ(index.value()->GetEfConstruction(), ef_construction);
  EXPECT_EQ(index.value()->GetEfRuntime(), ef_runtime);
}

TEST_F(VectorIndexTest, InitializationHNSW) {
  for (auto& distance_metric : kExpectedSpaces) {
    TestInitializationHNSW(kDimensions, distance_metric.first,
                           distance_metric.second, kInitialCap, kM,
                           kEFConstruction, kEFRuntime);
  }
}
TEST_F(VectorIndexTest, InitializationFlat) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  for (auto& distance_metric : kExpectedSpaces) {
    auto index = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric.first,
                                   kInitialCap, kBlockSize),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    auto* space = index.value()->GetSpace();
    EXPECT_EQ(distance_metric.second, typeid(*space).name());
    EXPECT_EQ(index.value()->GetDimensions(), kDimensions);
    EXPECT_EQ(index.value()->GetNormalize(),
              distance_metric.first == data_model::DISTANCE_METRIC_COSINE);
    EXPECT_EQ(index.value()->GetCapacity(), kInitialCap);
    EXPECT_EQ(index.value()->GetBlockSize(), kBlockSize);
  }
}

enum class ExpectedResults { kSuccess, kSkipped, kError };

auto IndexToKey = [](int i) {
  return StringInternStore::Intern(std::to_string(i) + "_key");
};

void VerifyResult(const absl::StatusOr<bool>& res,
                  ExpectedResults expected_result) {
  if (expected_result == ExpectedResults::kSuccess) {
    VMSDK_EXPECT_OK(res);
    EXPECT_TRUE(res.value());
  } else if (expected_result == ExpectedResults::kSkipped) {
    VMSDK_EXPECT_OK(res);
    EXPECT_FALSE(res.value());
  } else {
    EXPECT_FALSE(res.status().ok());
  }
}

void VerifyAdd(IndexBase* index, const std::vector<std::vector<float>>& vectors,
               int i, ExpectedResults expected_result) {
  auto id = IndexToKey(i);
  absl::string_view vector = VectorToStr(vectors[i]);
  bool alreadyExist = index->IsTracked(id);
  auto res = index->AddRecord(id, vector);
  if (res.ok()) {
    EXPECT_TRUE(index->IsTracked(id));
  } else if (!alreadyExist) {
    EXPECT_FALSE(index->IsTracked(id));
  }
  VerifyResult(res, expected_result);
}

void VerifyModify(IndexBase* index, const std::vector<float>& vector, int i,
                  ExpectedResults expected_result, bool expected_tracked) {
  auto id = IndexToKey(i);
  absl::string_view vector_str = VectorToStr(vector);
  auto res = index->ModifyRecord(id, vector_str);
  EXPECT_EQ(index->IsTracked(id), expected_tracked);
  VerifyResult(res, expected_result);
}
template <typename T>
void TestIndex(T* index, int dimensions, int vector_size) {
  auto vectors =
      DeterministicallyGenerateVectors(vector_size, dimensions, 10.0);
  for (size_t i = 0; i < vectors.size(); ++i) {
    VerifyAdd(index, vectors, i, ExpectedResults::kSuccess);
  }
  VerifyAdd(index, vectors, 0, ExpectedResults::kError);
  auto vectors_small_dim =
      DeterministicallyGenerateVectors(vectors.size(), dimensions - 1, 1.0);
  VerifyAdd(index, vectors_small_dim, 0, ExpectedResults::kSkipped);
  VerifyModify(index, vectors_small_dim[0], 0, ExpectedResults::kSkipped,
               false);

  VerifyModify(index, vectors[0], 0, ExpectedResults::kError, false);

  VerifyModify(index, vectors[0], vectors.size(), ExpectedResults::kError,
               false);

  auto vectors_same_dim =
      DeterministicallyGenerateVectors(vectors.size(), dimensions, 5.0);

  VerifyModify(index, vectors[vectors.size() - 2], vectors.size() - 1,
               ExpectedResults::kSuccess, true);

  for (size_t i = 1; i < vectors.size() - 1; ++i) {
    absl::string_view vector = VectorToStr(vectors[i]);
    auto res = index->Search(vector, 10);
    VMSDK_EXPECT_OK(res);
    if (res.ok()) {
      EXPECT_FALSE(res->empty());
      bool found = false;
      for (const auto& neighbors : res.value()) {
        if (neighbors.external_id == IndexToKey(i)) {
          EXPECT_LT(neighbors.distance - res.value()[0].distance, 0.0001);
          found = true;
          break;
        }
      }
      EXPECT_TRUE(found);
    }
  }
  VMSDK_EXPECT_OK(
      index->RemoveRecord(IndexToKey(vectors.size()), DeletionType::kNone));
  EXPECT_FALSE(
      index->RemoveRecord(IndexToKey(vectors.size()), DeletionType::kNone)
          .value());
  for (size_t i = 0; i < vectors.size(); ++i) {
    VMSDK_EXPECT_OK(index->RemoveRecord(IndexToKey(i), DeletionType::kNone));
    EXPECT_FALSE(index->IsTracked(IndexToKey(i)));
  }
  for (size_t i = 0; i < vectors.size(); ++i) {
    VerifyAdd(index, vectors, i, ExpectedResults::kSuccess);
  }
}

struct NormalizeStringRecordTestCase {
  std::string test_name;
  bool success{true};
  std::string record;
  std::vector<float> expected_norm_values;
};

class NormalizeStringRecordTest
    : public ValkeySearchTestWithParam<NormalizeStringRecordTestCase> {};

TEST_P(NormalizeStringRecordTest, NormalizeStringRecord) {
  auto& params = GetParam();

  auto index = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                 kInitialCap, kM, kEFConstruction, kEFRuntime),
      "attribute_identifier_1",
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
  auto record = vmsdk::MakeUniqueRedisString(params.record);
  auto norm_record = index.value()->NormalizeStringRecord(std::move(record));
  if (!params.success) {
    EXPECT_FALSE(norm_record.get());
    return;
  }
  auto norm_record_str = vmsdk::ToStringView(norm_record.get());
  for (size_t i = 0; i < params.expected_norm_values.size(); ++i) {
    float value = *(((float*)norm_record_str.data()) + i);
    EXPECT_FLOAT_EQ(value, params.expected_norm_values[i]);
  }
}

INSTANTIATE_TEST_SUITE_P(
    NormalizeStringRecordTests, NormalizeStringRecordTest,

    testing::ValuesIn<NormalizeStringRecordTestCase>({
        {
            .test_name = "cardinality_1",
            .record = "[ 0.1]",
            .expected_norm_values{0.1},
        },
        {
            .test_name = "cardinality_1_1",
            .record = "[,0.1]",
            .expected_norm_values{0.1},
        },
        {
            .test_name = "cardinality_3_1",
            .record = "[ 0.1, ,0.2,0.3,]",
            .expected_norm_values{0.1, 0.2, 0.3},
        },
        {
            .test_name = "cardinality_3_fail",
            .success = false,
            .record = "[ 0.1, ,0.2,a,]",
        },
    }),
    [](const testing::TestParamInfo<NormalizeStringRecordTestCase>& info) {
      return info.param.test_name;
    });

TEST_F(VectorIndexTest, BasicHNSW) {
  for (auto& distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    auto index = VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, kInitialCap,
                                   kM, kEFConstruction, kEFRuntime),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    TestIndex<VectorHNSW<float>>(index->get(), kDimensions, 100);
  }
}

TEST_F(VectorIndexTest, BasicFlat) {
  for (auto& distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    auto index = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric, kInitialCap,
                                   kBlockSize),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    TestIndex<VectorFlat<float>>(index->get(), kDimensions, 100);
  }
}

TEST_F(VectorIndexTest, ResizeHNSW) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  for (auto& distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    const int initial_cap = 10;
    auto index = VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kM, kEFConstruction, kEFRuntime),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    ValkeySearch::Instance().SetHNSWBlockSize(1024);
    uint32_t block_size = ValkeySearch::Instance().GetHNSWBlockSize();
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap);
    auto vectors = DeterministicallyGenerateVectors(
        initial_cap + block_size + 100, kDimensions, 10.0);

    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index->get(), vectors, i, ExpectedResults::kSuccess);
    }
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 2 * block_size);
    /*
    for (size_t i = 0; i < vectors.size(); ++i) {
      VMSDK_EXPECT_OK(
          index.value()->RemoveRecord(IndexToKey(i), DeletionType::kNone));
      EXPECT_FALSE(index.value()->IsTracked(IndexToKey(i)));
    }
    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index->get(), vectors, i, ExpectedResults::kSuccess);
    }

    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 3 * block_size);
     */
  }
}

TEST_F(VectorIndexTest, ResizeFlat) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  for (auto& distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    const int initial_cap = 10;
    auto index = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kBlockSize),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    auto vectors = DeterministicallyGenerateVectors(
        initial_cap + kBlockSize + 100, kDimensions, 10.0);
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index->get(), vectors, i, ExpectedResults::kSuccess);
    }
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 2 * kBlockSize);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VMSDK_EXPECT_OK(
          index.value()->RemoveRecord(IndexToKey(i), DeletionType::kNone));
      EXPECT_FALSE(index.value()->IsTracked(IndexToKey(i)));
    }
    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index->get(), vectors, i, ExpectedResults::kSuccess);
    }
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 2 * kBlockSize);
  }
}

float CalcRecall(VectorFlat<float>* flat_index, VectorHNSW<float>* hsw_index,
                 uint64_t k, int dimensions, std::optional<size_t> ef_runtime) {
  auto search_vectors = DeterministicallyGenerateVectors(50, dimensions, 1.5);
  int cnt = 0;
  for (const auto& search_vector : search_vectors) {
    absl::string_view vector = VectorToStr(search_vector);
    auto res_hnsw = hsw_index->Search(vector, k, nullptr, ef_runtime);
    auto res_flat = flat_index->Search(vector, k);
    for (auto& label : *res_hnsw) {
      for (auto& real_label : *res_flat) {
        if (label.external_id == real_label.external_id) {
          ++cnt;
          break;
        }
      }
    }
  }
  return ((float)(cnt)) / ((float)(k * search_vectors.size()));
}
// Note this test is expected to fail if run with `config=release`. This has to
// do with the usage of the optimization flag `-ffast-math`
TEST_F(VectorIndexTest, EfRuntimeRecall) {
  for (auto& distance_metric : {data_model::DISTANCE_METRIC_L2}) {
    // Use a large cap to make sure chunked array is properly exercised
    const int initial_cap = 31000;
    auto index_hnsw = VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kM, kEFConstruction, kEFRuntime),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    auto vectors = DeterministicallyGenerateVectors(1000, kDimensions, 2.2);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index_hnsw->get(), vectors, i, ExpectedResults::kSuccess);
    }

    auto index_flat = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kBlockSize),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index_flat->get(), vectors, i, ExpectedResults::kSuccess);
    }
    uint64_t k = 10;
    auto no_ef_runtime_recall = CalcRecall(index_flat->get(), index_hnsw->get(),
                                           k, kDimensions, std::nullopt);
    auto default_ef_runtime_recall = CalcRecall(
        index_flat->get(), index_hnsw->get(), k, kDimensions, kEFRuntime);
    auto ef_runtime_recall = CalcRecall(index_flat->get(), index_hnsw->get(), k,
                                        kDimensions, kEFRuntime * 8);
    EXPECT_LE(no_ef_runtime_recall, ef_runtime_recall);
    EXPECT_GE(ef_runtime_recall, 0.96f);
    EXPECT_EQ(default_ef_runtime_recall, no_ef_runtime_recall);
  }
}

TEST_F(VectorIndexTest, SaveAndLoadHnsw) {
  for (auto& distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    const int initial_cap = 1000;
    const uint64_t k = 10;
    FakeSafeRDB rdb;
    auto vectors = DeterministicallyGenerateVectors(1000, kDimensions, 2.2);
    // Load the vectors into a Flat index. This will be used for computing the
    // recall later
    auto index_flat = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kBlockSize),
        "attribute_identifier_1",
        data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
    VMSDK_EXPECT_OK(index_flat);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VerifyAdd(index_flat->get(), vectors, i, ExpectedResults::kSuccess);
    }

    data_model::VectorIndex hnsw_proto =
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kM, kEFConstruction, kEFRuntime);
    // Create and save empty HNSW index
    {
      auto index_hnsw = VectorHNSW<float>::Create(
          hnsw_proto, "attribute_identifier_2",
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
      VMSDK_EXPECT_OK(index_hnsw);
      if (distance_metric == data_model::DISTANCE_METRIC_COSINE) {
        EXPECT_TRUE((*index_hnsw)->GetNormalize());
      }
      VMSDK_EXPECT_OK((*index_hnsw)->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK(
          (*index_hnsw)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      hnsw_proto = (*index_hnsw)->ToProto()->vector_index();
    }

    // Load the HNSW index, populate data, validate recall, save again
    {
      auto loaded_index_hnsw = VectorHNSW<float>::LoadFromRDB(
          &fake_ctx_, &hash_attribute_data_type_, hnsw_proto,
          "attribute_identifier_3", SupplementalContentChunkIter(&rdb));
      VMSDK_EXPECT_OK(loaded_index_hnsw);
      VMSDK_EXPECT_OK(
          (*loaded_index_hnsw)
              ->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                SupplementalContentChunkIter(&rdb)));
      for (size_t i = 0; i < vectors.size(); ++i) {
        VerifyAdd(loaded_index_hnsw->get(), vectors, i,
                  ExpectedResults::kSuccess);
      }
      auto default_ef_runtime_recall =
          CalcRecall(index_flat->get(), loaded_index_hnsw->get(), k,
                     kDimensions, kEFRuntime);
      EXPECT_GE(default_ef_runtime_recall, 0.96f);
      VMSDK_EXPECT_OK(
          (*loaded_index_hnsw)->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK(
          (*loaded_index_hnsw)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      hnsw_proto = (*loaded_index_hnsw)->ToProto()->vector_index();
    }

    // Load the HNSW index, run search queries and validate recall
    {
      auto loaded_index_hnsw = VectorHNSW<float>::LoadFromRDB(
          &fake_ctx_, &hash_attribute_data_type_, hnsw_proto,
          "attribute_identifier_4", SupplementalContentChunkIter(&rdb));
      VMSDK_EXPECT_OK(loaded_index_hnsw);
      VMSDK_EXPECT_OK(
          (*loaded_index_hnsw)
              ->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                SupplementalContentChunkIter(&rdb)));
      auto default_ef_runtime_recall =
          CalcRecall(index_flat->get(), loaded_index_hnsw->get(), k,
                     kDimensions, kEFRuntime);
      EXPECT_GE(default_ef_runtime_recall, 0.96f);
    }
  }
}

TEST_F(VectorIndexTest, SaveAndLoadFlat) {
  for (auto& distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    const int initial_cap = 1000;
    const uint64_t k = 10;
    FakeSafeRDB rdb;
    auto vectors = DeterministicallyGenerateVectors(1000, kDimensions, 2.2);
    auto search_vectors =
        DeterministicallyGenerateVectors(50, kDimensions, 1.5);
    std::vector<std::deque<Neighbor>> expected_results;

    data_model::VectorIndex flat_proto = CreateFlatVectorIndexProto(
        kDimensions, distance_metric, initial_cap, kBlockSize);
    // Create and save empty Flat index
    {
      auto index = VectorFlat<float>::Create(
          flat_proto, "attribute_identifier_1",
          data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH);
      if (distance_metric == data_model::DISTANCE_METRIC_COSINE) {
        EXPECT_TRUE(index.value()->GetNormalize());
      }
      VMSDK_EXPECT_OK(index.value()->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK((*index)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      flat_proto = (*index)->ToProto()->vector_index();
    }

    // Load the index, populate data, perform search, save the index again
    {
      auto index_pr = VectorFlat<float>::LoadFromRDB(
          &fake_ctx_, &hash_attribute_data_type_, flat_proto,
          "attribute_identifier_2", SupplementalContentChunkIter(&rdb));
      VMSDK_EXPECT_OK(index_pr);
      auto index = std::move(index_pr.value());
      VMSDK_EXPECT_OK(
          index->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb)));
      for (size_t i = 0; i < vectors.size(); ++i) {
        VerifyAdd(index.get(), vectors, i, ExpectedResults::kSuccess);
      }
      for (const auto& search_vector : search_vectors) {
        absl::string_view vector = VectorToStr(search_vector);
        auto res = index->Search(vector, k);
        expected_results.push_back(std::move(*res));
      }
      VMSDK_EXPECT_OK(index->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK(index->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      flat_proto = index->ToProto()->vector_index();
    }

    // Load the index, run search queries and validate that the search results
    // match the previous results
    {
      auto index_pr = VectorFlat<float>::LoadFromRDB(
          &fake_ctx_, &hash_attribute_data_type_, flat_proto,
          "attribute_identifier_3", SupplementalContentChunkIter(&rdb));
      VMSDK_EXPECT_OK(index_pr);
      auto index = std::move(index_pr.value());
      VMSDK_EXPECT_OK(
          index->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb)));
      for (size_t i = 0; i < search_vectors.size(); ++i) {
        absl::string_view vector = VectorToStr(search_vectors[i]);
        auto res = index->Search(vector, k);
        EXPECT_EQ(ToVectorNeighborTest(*res),
                  ToVectorNeighborTest(expected_results[i]));
      }

      // Re-insert the vectors
      for (size_t i = 0; i < vectors.size(); ++i) {
        VerifyModify(index.get(), vectors[i], i, ExpectedResults::kSkipped,
                     true);
      }
    }
  }
}
// ---------------------------------------------------------------------------
// HNSW load-validation tests (corruption hardening).
//
// These exercise HierarchicalNSW::LoadIndex directly via in-memory chunk
// streams, so a golden serialization can be built deterministically and then
// surgically corrupted. A valid index must load; a corrupt one must be
// rejected (status error) without crashing.
// ---------------------------------------------------------------------------
namespace {

// A chunk store that is both an InputStream and an OutputStream. SaveIndex
// writes into `chunks`, tests mutate `chunks` in place, and LoadIndex replays
// it from the front -- so there is no copying between separate stream objects.
// LoadChunk mirrors the real stream's exhaustion behavior (NotFound) so a
// truncated input is realistic.
class ChunkStream : public hnswlib::InputStream, public hnswlib::OutputStream {
 public:
  std::vector<std::string> chunks;

  absl::Status SaveChunk(const char* data, size_t len) override {
    chunks.emplace_back(data, len);
    return absl::OkStatus();
  }
  absl::StatusOr<std::unique_ptr<std::string>> LoadChunk() override {
    if (read_idx_ >= chunks.size()) {
      return absl::NotFoundError("No more elements remaining");
    }
    return std::make_unique<std::string>(chunks[read_idx_++]);
  }
  // Reset the read cursor so the same store can be replayed again.
  void Rewind() { read_idx_ = 0; }

 private:
  size_t read_idx_ = 0;
};

// VectorTracker that copies each vector into stable storage and hands back a
// pointer to it (LoadIndex stores that pointer in the level-0 record).
class BufTracker : public hnswlib::VectorTracker {
 public:
  char* TrackVector(uint64_t /*id*/, char* vector, size_t len) override {
    storage_.emplace_back(vector, len);
    return storage_.back().data();
  }

 private:
  std::deque<std::string> storage_;
};

// On-disk geometry for kM / kDimensions, used to locate bytes for mutation.
constexpr size_t kU32 = sizeof(uint32_t);
constexpr size_t kStride = kM * kU32 + kU32;               // 68
constexpr size_t kLinks0 = 2 * kM * kU32 + kU32;           // 132
constexpr size_t kVecBytes = kDimensions * sizeof(float);  // 400
constexpr size_t kLabelOff = kLinks0 + kVecBytes;          // 532
constexpr size_t kElemChunkBytes = kLabelOff + sizeof(hnswlib::labeltype);

template <typename T>
T PeekAt(const std::string& c, size_t off) {
  CHECK_LE(off + sizeof(T), c.size()) << "PeekAt out of bounds";
  T v;
  std::memcpy(&v, c.data() + off, sizeof(T));
  return v;
}
template <typename T>
void PokeAt(std::string* c, size_t off, T v) {
  CHECK_LE(off + sizeof(T), c->size()) << "PokeAt out of bounds";
  std::memcpy(c->data() + off, &v, sizeof(T));
}

// Build a deterministic HNSW index with explicit per-element levels (a level of
// 0 falls back to the seeded RNG, since addPoint only forces level > 0) and
// serialize it into a golden ChunkStream.
ChunkStream BuildGoldenChunks(const std::vector<int>& force_levels,
                              size_t max_elements) {
  hnswlib::L2Space space{kDimensions};
  hnswlib::HierarchicalNSW<float> algo(&space, max_elements, kM,
                                       kEFConstruction,
                                       /*random_seed=*/100,
                                       /*allow_replace_deleted=*/false);
  // HNSW stores a POINTER to each vector (it does not copy). Both addPoint
  // (distance computations against earlier elements) and SaveIndex dereference
  // those pointers, so the source vectors must outlive SaveIndex. Own them all
  // here until the index is serialized; reserve() keeps the backing buffers
  // stable as the container grows. After SaveIndex the golden ChunkStream owns
  // its bytes by value, so this storage can safely be destroyed.
  std::vector<std::vector<float>> vectors;
  vectors.reserve(force_levels.size());
  for (size_t i = 0; i < force_levels.size(); i++) {
    std::vector<float> v(kDimensions, 0.1f);
    v[i % kDimensions] = static_cast<float>(i + 1);
    vectors.push_back(std::move(v));
    algo.addPoint(vectors.back().data(), /*label=*/i, force_levels[i]);
  }
  ChunkStream golden;
  EXPECT_TRUE(algo.SaveIndex(golden).ok());
  return golden;
}

// Load a golden ChunkStream, returning a Status (validation throws; mirror the
// real LoadFromRDB by converting the exception into an error status).
absl::Status LoadGolden(ChunkStream& golden, size_t max_elements,
                        bool validate) {
  golden.Rewind();
  hnswlib::L2Space space{kDimensions};
  hnswlib::HierarchicalNSW<float> algo(&space);
  BufTracker tracker;
  try {
    return algo.LoadIndex(golden, &space, max_elements, &tracker, kM, validate);
  } catch (const std::exception& e) {
    return absl::InternalError(e.what());
  }
}

// Walk the golden stream and record, per element, the index of its link-list
// size chunk and (if present) its upper-level data chunk. Robust to whatever
// levels the RNG assigned to the un-forced elements.
struct GoldenLayout {
  uint32_t enterpoint = 0;
  int32_t maxlevel = 0;
  size_t num_elements = 0;
  std::vector<size_t> size_chunk;
  std::vector<int> data_chunk;  // -1 if the element has no upper levels
};
GoldenLayout AnalyzeGolden(const ChunkStream& golden) {
  const std::vector<std::string>& chunks = golden.chunks;
  GoldenLayout g;
  hnswlib::data_model::HNSWIndexHeader h;
  EXPECT_TRUE(h.ParseFromString(chunks[0]));
  g.enterpoint = h.enterpoint_node();
  g.maxlevel = h.max_level();
  g.num_elements = h.curr_element_count();
  size_t idx = 1 + g.num_elements;  // first link-list size chunk
  for (size_t i = 0; i < g.num_elements; i++) {
    g.size_chunk.push_back(idx);
    // The size chunk holds the link-list BYTE size as a size_t (8 bytes); the
    // layer count is derived from it, not stored directly.
    uint64_t lls = PeekAt<uint64_t>(chunks[idx], 0);  // LE host == on-disk LE
    idx++;
    if (lls != 0) {
      g.data_chunk.push_back(static_cast<int>(idx));
      idx++;
    } else {
      g.data_chunk.push_back(-1);
    }
  }
  return g;
}

hnswlib::data_model::HNSWIndexHeader GetHeader(const ChunkStream& golden) {
  hnswlib::data_model::HNSWIndexHeader h;
  EXPECT_TRUE(h.ParseFromString(golden.chunks[0]));
  return h;
}
void SetHeader(ChunkStream* golden,
               const hnswlib::data_model::HNSWIndexHeader& h) {
  std::string s;
  EXPECT_TRUE(h.SerializeToString(&s));
  golden->chunks[0] = s;
}

// Multi-layer golden index reused across corruption tests: element 0 is forced
// to level 2 (the entry point), element 1 to level 1, the rest level 0.
constexpr size_t kGoldenMax = 32;
ChunkStream MultiLayerGolden() {
  return BuildGoldenChunks({2, 1, 0, 0, 0, 0, 0, 0}, kGoldenMax);
}

void ExpectReject(ChunkStream golden, absl::string_view substr) {
  auto status = LoadGolden(golden, kGoldenMax, /*validate=*/true);
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(std::string(status.message()), ::testing::HasSubstr(substr));
}

}  // namespace

// ---- Happy path ----------------------------------------------------------

TEST_F(VectorIndexTest, LoadValidatesEmptyIndex) {
  auto golden = BuildGoldenChunks({}, kGoldenMax);
  VMSDK_EXPECT_OK(LoadGolden(golden, kGoldenMax, /*validate=*/true));
}

TEST_F(VectorIndexTest, LoadValidatesSingleVector) {
  auto golden = BuildGoldenChunks({0}, kGoldenMax);
  VMSDK_EXPECT_OK(LoadGolden(golden, kGoldenMax, /*validate=*/true));
}

TEST_F(VectorIndexTest, LoadValidatesMultiLayerRoundTripIdentity) {
  auto golden = MultiLayerGolden();
  hnswlib::L2Space space{kDimensions};
  hnswlib::HierarchicalNSW<float> algo(&space);
  BufTracker tracker;
  golden.Rewind();
  VMSDK_EXPECT_OK(algo.LoadIndex(golden, &space, kGoldenMax, &tracker, kM,
                                 /*validate=*/true));
  EXPECT_EQ(algo.cur_element_count_, 8u);
  EXPECT_EQ(algo.maxlevel_, 2);
  EXPECT_EQ(algo.element_levels_[algo.enterpoint_node_], 2);
  // save -> load -> save must be byte-identical for a valid index.
  ChunkStream resaved;
  VMSDK_EXPECT_OK(algo.SaveIndex(resaved));
  EXPECT_EQ(resaved.chunks, golden.chunks);
}

// ---- Header corruption ---------------------------------------------------

TEST_F(VectorIndexTest, RejectHeaderMMismatch) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  h.set_m(kM + 1);
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "header M does not match");
}

TEST_F(VectorIndexTest, RejectHeaderMaxM0Mismatch) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  h.set_max_m_0(2 * kM + 1);
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "maxM0 does not equal 2*M");
}

TEST_F(VectorIndexTest, RejectHeaderEnterpointOutOfRange) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  h.set_enterpoint_node(h.curr_element_count());  // == cur, out of range
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "enterpoint_node is out of range");
}

TEST_F(VectorIndexTest, RejectHeaderMaxLevelTooLarge) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  h.set_max_level(1000);
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "max_level exceeds the element count");
}

TEST_F(VectorIndexTest, RejectHeaderSerializeSizeMismatch) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  h.set_serialize_size_data_per_element(h.serialize_size_data_per_element() +
                                        1);
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "serialized element size is inconsistent");
}

TEST_F(VectorIndexTest, RejectHeaderOffsetLevel0Nonzero) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  h.set_offset_level_0(8);
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "offset_level_0 must be 0");
}

TEST_F(VectorIndexTest, RejectHeaderMultInconsistentWithM) {
  auto golden = MultiLayerGolden();
  auto h = GetHeader(golden);
  // 0.5 is well-formed (0 < 0.5 <= 2) but != 1/log(kM); the tightened check
  // recomputes 1/log(M) and rejects it, where the old range check would not.
  h.set_mult(0.5);
  SetHeader(&golden, h);
  ExpectReject(std::move(golden), "mult is inconsistent with M");
}

// ---- Level-0 record corruption -------------------------------------------

TEST_F(VectorIndexTest, RejectLevel0ChunkWrongSize) {
  auto golden = MultiLayerGolden();
  golden.chunks[1].resize(kElemChunkBytes - 1);  // truncate element 0's chunk
  ExpectReject(std::move(golden), "level-0 element chunk has the wrong size");
}

TEST_F(VectorIndexTest, RejectLevel0CountTooLarge) {
  auto golden = MultiLayerGolden();
  PokeAt<uint16_t>(&golden.chunks[1], 0, 2 * kM + 1);  // count > maxM0_
  ExpectReject(std::move(golden), "level-0 neighbor count exceeds 2*M");
}

TEST_F(VectorIndexTest, RejectLevel0NeighborOutOfRange) {
  auto golden = MultiLayerGolden();
  PokeAt<uint16_t>(&golden.chunks[2], 0, 1);        // element 1: count = 1
  PokeAt<uint32_t>(&golden.chunks[2], kU32, 9999);  // neighbor[0] out of range
  ExpectReject(std::move(golden), "level-0 neighbor id out of range");
}

TEST_F(VectorIndexTest, RejectDuplicateLabel) {
  auto golden = MultiLayerGolden();
  auto label0 = PeekAt<hnswlib::labeltype>(golden.chunks[1], kLabelOff);
  PokeAt<hnswlib::labeltype>(&golden.chunks[3], kLabelOff,
                             label0);  // element 2 dup
  ExpectReject(std::move(golden), "duplicate label in index");
}

// ---- Upper-level record corruption ---------------------------------------

TEST_F(VectorIndexTest, RejectSizeChunkWrongSize) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  golden.chunks[g.size_chunk[g.enterpoint]].resize(4);  // not sizeof(size_t)
  ExpectReject(std::move(golden), "link-list size chunk has the wrong size");
}

TEST_F(VectorIndexTest, RejectLinkListSizeNotMultiple) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  PokeAt<uint64_t>(&golden.chunks[g.size_chunk[g.enterpoint]], 0,
                   2 * kStride + 1);
  ExpectReject(std::move(golden), "not a multiple of the stride");
}

TEST_F(VectorIndexTest, RejectElementLevelExceedsMaxLevel) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  // Declare level 3 (> maxlevel 2) for the entry point.
  PokeAt<uint64_t>(&golden.chunks[g.size_chunk[g.enterpoint]], 0, 3 * kStride);
  ExpectReject(std::move(golden), "element level exceeds max_level");
}

TEST_F(VectorIndexTest, RejectUpperChunkWrongSize) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  golden.chunks[g.data_chunk[g.enterpoint]].resize(
      kStride);  // declared 2 levels
  ExpectReject(std::move(golden), "upper-level link-list chunk has the wrong");
}

TEST_F(VectorIndexTest, RejectUpperCountTooLarge) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  PokeAt<uint16_t>(&golden.chunks[g.data_chunk[g.enterpoint]], 0,
                   kM + 1);  // level 1
  ExpectReject(std::move(golden), "upper-level neighbor count exceeds M");
}

TEST_F(VectorIndexTest, RejectUpperNeighborOutOfRange) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  PokeAt<uint16_t>(&golden.chunks[g.data_chunk[g.enterpoint]], 0, 1);
  PokeAt<uint32_t>(&golden.chunks[g.data_chunk[g.enterpoint]], kU32, 9999);
  ExpectReject(std::move(golden), "upper-level neighbor id out of range");
}

TEST_F(VectorIndexTest, RejectUpperNeighborAbsentAtLevel) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  // Entry point's level-2 list -> element 1, which exists only at level 1.
  PokeAt<uint16_t>(&golden.chunks[g.data_chunk[g.enterpoint]], kStride, 1);
  PokeAt<uint32_t>(&golden.chunks[g.data_chunk[g.enterpoint]], kStride + kU32,
                   1);
  ExpectReject(std::move(golden), "neighbor is absent at that level");
}

TEST_F(VectorIndexTest, RejectEntrypointNotMaxLevel) {
  auto golden = MultiLayerGolden();
  auto g = AnalyzeGolden(golden);
  // Demote the entry point to level 1 while the header still claims maxlevel 2.
  PokeAt<uint64_t>(&golden.chunks[g.size_chunk[g.enterpoint]], 0, kStride);
  golden.chunks[g.data_chunk[g.enterpoint]].resize(kStride);
  ExpectReject(std::move(golden), "enterpoint node is not at max_level");
}

// ---- Kill switch ---------------------------------------------------------

TEST_F(VectorIndexTest, ValidationDisabledBypassesChecks) {
  auto golden = MultiLayerGolden();
  PokeAt<uint16_t>(&golden.chunks[2], 0, 1);  // element 1: count = 1
  PokeAt<uint32_t>(&golden.chunks[2], kU32,
                   1);  // neighbor[0] == self (a self-loop)
  // Enabled: rejected. Disabled: loads (the self-loop is not memory-unsafe).
  EXPECT_FALSE(LoadGolden(golden, kGoldenMax, /*validate=*/true).ok());
  VMSDK_EXPECT_OK(LoadGolden(golden, kGoldenMax, /*validate=*/false));
}

}  // namespace

}  // namespace valkey_search::indexes
