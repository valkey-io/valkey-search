/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/indexes/vector_hnsw.h"
#include "src/metrics.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "testing/common.h"
#include "third_party/hnswlib/index.pb.h"
#include "third_party/hnswlib/iostream.h"
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
    kExpectedSpaces = {{
        {data_model::DISTANCE_METRIC_COSINE, typeid(kInnerProductSpace).name()},
        {data_model::DISTANCE_METRIC_IP, typeid(kInnerProductSpace).name()},
        {data_model::DISTANCE_METRIC_L2, typeid(kL2Space).name()},
    }};

static cancel::Token &CancelNever() {
  static cancel::Token cancel_never = cancel::Make(1000000, nullptr);
  return cancel_never;
}

static void ExpectNeighborsNear(const std::vector<NeighborTest> &act,
                                const std::vector<NeighborTest> &exp,
                                float tolerance = 1e-5f) {
  ASSERT_EQ(act.size(), exp.size());
  std::vector<NeighborTest> sorted_act = act;
  std::vector<NeighborTest> sorted_exp = exp;
  auto compare_by_id = [](const NeighborTest &a, const NeighborTest &b) {
    return a.external_id < b.external_id;
  };
  std::sort(sorted_act.begin(), sorted_act.end(), compare_by_id);
  std::sort(sorted_exp.begin(), sorted_exp.end(), compare_by_id);
  for (size_t j = 0; j < sorted_act.size(); ++j) {
    EXPECT_EQ(sorted_act[j].external_id, sorted_exp[j].external_id);
    EXPECT_NEAR(sorted_act[j].distance, sorted_exp[j].distance, tolerance);
  }
}

class VectorIndexTest : public ValkeySearchTest {
 public:
  HashAttributeDataType hash_attribute_data_type_;
  const char *attribute_identifier = "attribute_identifier_1";
  data_model::AttributeDataType attribute_data_type =
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
};

TEST_F(VectorIndexTest, InitializationHNSW) {
  for (auto &distance_metric : kExpectedSpaces) {
    auto index = VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDimensions, distance_metric.first,
                                   kInitialCap, kM, kEFConstruction,
                                   kEFRuntime),
        attribute_identifier, attribute_data_type);
    auto *space = index.value()->GetSpace();
    EXPECT_EQ(distance_metric.second, typeid(*space).name());
    EXPECT_EQ(index.value()->GetDimensions(), kDimensions);
    EXPECT_EQ(index.value()->GetNormalize(),
              distance_metric.first == data_model::DISTANCE_METRIC_COSINE);
    EXPECT_EQ(index.value()->GetCapacity(), kInitialCap);
    EXPECT_EQ(index.value()->GetM(), kM);
    EXPECT_EQ(index.value()->GetEfConstruction(), kEFConstruction);
    EXPECT_EQ(index.value()->GetEfRuntime(), kEFRuntime);
  }
}
TEST_F(VectorIndexTest, InitializationFlat) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  for (auto &distance_metric : kExpectedSpaces) {
    auto index = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric.first,
                                   kInitialCap, kBlockSize),
        attribute_identifier, attribute_data_type);
    auto *space = index.value()->GetSpace();
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

absl::Status VerifyResult(const absl::StatusOr<bool> &res,
                          ExpectedResults expected_result,
                          std::string error_prefix) {
  if (expected_result == ExpectedResults::kSuccess) {
    if (!res.ok()) {
      return absl::InternalError(
          absl::StrCat(error_prefix, "Expected success but res not ok"));
    }
    if (!res.value()) {
      return absl::InternalError(absl::StrCat(
          error_prefix, "Expected success but res value is false"));
    }
    return absl::OkStatus();
  }
  if (expected_result == ExpectedResults::kSkipped) {
    if (!res.ok()) {
      return absl::InternalError(
          absl::StrCat(error_prefix, "Expected skipped but res not ok"));
    }
    if (res.value()) {
      return absl::InternalError(
          absl::StrCat(error_prefix, "Expected skipped but res value is true"));
    }
    return absl::OkStatus();
  }
  if (res.status().ok()) {
    return absl::InternalError(
        absl::StrCat(error_prefix, "Expected kError but res status not ok"));
  }
  return absl::OkStatus();
}

absl::Status VerifyAdd(indexes::IndexBase *index,
                       const std::vector<std::vector<float>> &vectors, int i,
                       ExpectedResults expected_result) {
  auto id = IndexToKey(i);
  absl::string_view vector = VectorToStr(vectors[i]);
  bool alreadyExist = index->IsTracked(id);
  auto res = index->AddRecord(id, vector);
  if (res.ok()) {
    if (!index->IsTracked(id)) {
      return absl::InternalError(
          "From VerifyAdd - IsTracked is false after AddRecord");
    }
  } else if (!alreadyExist) {
    if (index->IsTracked(id)) {
      return absl::InternalError(
          "From VerifyAdd - IsTracked is true after AddRecord while "
          "alreadyExist is false");
    }
  }
  return VerifyResult(res, expected_result, "From VerifyAdd - ");
}

#define VERIFY_ADD(index, vectors, i, expected_result)                   \
  do {                                                                   \
    absl::Status status = VerifyAdd(index, vectors, i, expected_result); \
    if (!status.ok()) {                                                  \
      std::cout << status.message();                                     \
      EXPECT_TRUE(false);                                                \
    }                                                                    \
  } while (0)

absl::Status VerifyModify(indexes::IndexBase *index,
                          const std::vector<float> &vector, int i,
                          ExpectedResults expected_result,
                          bool expected_tracked) {
  auto id = IndexToKey(i);
  absl::string_view vector_str = VectorToStr(vector);
  auto res = index->ModifyRecord(id, vector_str);
  if (index->IsTracked(id) != expected_tracked) {
    return absl::InternalError(absl::StrCat(
        "From VerifyModify - IsTracked ,", index->IsTracked(id),
        ", does not match the expected_tracked, ", expected_tracked));
  }
  return VerifyResult(res, expected_result, "From VerifyModify - ");
}

#define VERIFY_MODIFY(index, vector, position, expected_result, flag) \
  do {                                                                \
    absl::Status status =                                             \
        VerifyModify(index, vector, position, expected_result, flag); \
    if (!status.ok()) {                                               \
      std::cout << status.message();                                  \
      EXPECT_TRUE(false);                                             \
    }                                                                 \
  } while (0)

template <typename T>
void TestIndex(T *index, int dimensions, int vector_size,
               const char *attribute_identifier,
               data_model::AttributeDataType attribute_data_type) {
  auto vectors =
      DeterministicallyGenerateVectors(vector_size, dimensions, 10.0);
  for (size_t i = 0; i < vectors.size(); ++i) {
    VERIFY_ADD(index, vectors, i, ExpectedResults::kSuccess);
  }
  VERIFY_ADD(index, vectors, 0, ExpectedResults::kError);
  auto vectors_small_dim =
      DeterministicallyGenerateVectors(vectors.size(), dimensions - 1, 1.0);
  VERIFY_ADD(index, vectors_small_dim, 0, ExpectedResults::kSkipped);
  VERIFY_MODIFY(index, vectors_small_dim[0], 0, ExpectedResults::kSkipped,
                false);

  VERIFY_MODIFY(index, vectors[0], 0, ExpectedResults::kError, false);

  VERIFY_MODIFY(index, vectors[0], vectors.size(), ExpectedResults::kError,
                false);

  auto vectors_same_dim =
      DeterministicallyGenerateVectors(vectors.size(), dimensions, 5.0);

  VERIFY_MODIFY(index, vectors[vectors.size() - 2], vectors.size() - 1,
                ExpectedResults::kSuccess, true);

  absl::string_view vector = VectorToStr(vectors_small_dim[0]);
  auto res = index->Search(vector, 10, CancelNever());
  EXPECT_FALSE(res.ok());
  EXPECT_EQ(
      res.status().message(),
      absl::StrCat(
          "Error parsing vector similarity query: query vector blob size (",
          vector.size(), ") does not match index's expected size (",
          dimensions * sizeof(float), ")."));
  for (size_t i = 1; i < vectors.size() - 1; ++i) {
    absl::string_view vector = VectorToStr(vectors[i]);
    auto res = index->Search(vector, 10, CancelNever());
    VMSDK_EXPECT_OK(res);
    if (res.ok()) {
      EXPECT_FALSE(res->empty());
      bool found = false;
      for (const auto &neighbors : res.value()) {
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
    VERIFY_ADD(index, vectors, i, ExpectedResults::kSuccess);
  }
}

struct NormalizeStringRecordTestCase {
  std::string test_name;
  bool success{true};
  std::string record;
  std::vector<float> expected_norm_values;
};

class NormalizeStringRecordTest
    : public ValkeySearchTestWithParam<NormalizeStringRecordTestCase> {
 public:
  const char *attribute_identifier = "attribute_identifier_1";
  data_model::AttributeDataType attribute_data_type =
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
};

TEST_P(NormalizeStringRecordTest, NormalizeStringRecord) {
  auto &params = GetParam();

  auto index = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                 kInitialCap, kM, kEFConstruction, kEFRuntime),
      attribute_identifier, attribute_data_type);
  auto record = vmsdk::MakeUniqueValkeyString(params.record);
  auto norm_record = index.value()->NormalizeStringRecord(std::move(record));
  if (!params.success) {
    EXPECT_FALSE(norm_record.get());
    return;
  }
  auto norm_record_str = vmsdk::ToStringView(norm_record.get());
  for (size_t i = 0; i < params.expected_norm_values.size(); ++i) {
    float value = *(((float *)norm_record_str.data()) + i);
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
    [](const testing::TestParamInfo<NormalizeStringRecordTestCase> &info) {
      return info.param.test_name;
    });

class VectorIndexParamTest
    : public VectorIndexTest,
      public ::testing::WithParamInterface<data_model::DistanceMetric> {};

TEST_P(VectorIndexParamTest, BasicHNSW) {
  const auto &distance_metric = GetParam();

  auto index = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDimensions, distance_metric, kInitialCap, kM,
                                 kEFConstruction, kEFRuntime),
      attribute_identifier, attribute_data_type);

  TestIndex<VectorHNSW<float>>(index->get(), kDimensions, 100,
                               attribute_identifier, attribute_data_type);
}

INSTANTIATE_TEST_SUITE_P(
    VectorIndexParamTests, VectorIndexParamTest,
    ::testing::Values(data_model::DISTANCE_METRIC_COSINE,
                      data_model::DISTANCE_METRIC_L2),
    [](const ::testing::TestParamInfo<VectorIndexParamTest::ParamType> &info) {
      switch (info.param) {
        case data_model::DISTANCE_METRIC_COSINE:
          return "Cosine";
        case data_model::DISTANCE_METRIC_L2:
          return "L2";
        default:
          return "Unknown";
      }
    });

TEST_P(VectorIndexParamTest, BasicFlat) {
  const auto &distance_metric = GetParam();
  auto index = VectorFlat<float>::Create(
      CreateFlatVectorIndexProto(kDimensions, distance_metric, kInitialCap,
                                 kBlockSize),
      attribute_identifier, attribute_data_type);

  TestIndex<VectorFlat<float>>(index->get(), kDimensions, 100,
                               attribute_identifier, attribute_data_type);
}

TEST_F(VectorIndexTest, ResizeHNSW) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  for (auto &distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    const int initial_cap = 10;
    auto index = VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kM, kEFConstruction, kEFRuntime),
        attribute_identifier, attribute_data_type);
    EXPECT_TRUE(ValkeySearch::Instance().SetHNSWBlockSize(1024).ok());
    uint32_t block_size = ValkeySearch::Instance().GetHNSWBlockSize();
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap);
    auto vectors = DeterministicallyGenerateVectors(
        initial_cap + block_size + 100, kDimensions, 10.0);

    for (size_t i = 0; i < vectors.size(); ++i) {
      VERIFY_ADD(index->get(), vectors, i, ExpectedResults::kSuccess);
    }
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 2 * block_size);
  }
}

TEST_F(VectorIndexTest, ResizeFlat) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  for (auto &distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    const int initial_cap = 10;
    auto index = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kBlockSize),
        attribute_identifier, attribute_data_type);
    auto vectors = DeterministicallyGenerateVectors(
        initial_cap + kBlockSize + 100, kDimensions, 10.0);
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VERIFY_ADD(index->get(), vectors, i, ExpectedResults::kSuccess);
    }
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 2 * kBlockSize);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VMSDK_EXPECT_OK(
          index.value()->RemoveRecord(IndexToKey(i), DeletionType::kNone));
      EXPECT_FALSE(index.value()->IsTracked(IndexToKey(i)));
    }
    for (size_t i = 0; i < vectors.size(); ++i) {
      VERIFY_ADD(index->get(), vectors, i, ExpectedResults::kSuccess);
    }
    EXPECT_EQ(index.value()->GetCapacity(), initial_cap + 2 * kBlockSize);
  }
}

TEST_F(VectorIndexTest, VectorFlatCosineNormalizationDistance)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  const int dimensions = 4;
  auto index = VectorFlat<float>::Create(
      CreateFlatVectorIndexProto(dimensions, data_model::DISTANCE_METRIC_COSINE,
                                 10, 10),
      attribute_identifier, attribute_data_type);
  ASSERT_TRUE(index.ok());

  // Non-unit vector [3.0, 0.0, 0.0, 0.0] with magnitude 3.0
  std::vector<float> vec1 = {3.0f, 0.0f, 0.0f, 0.0f};
  std::string vec1_bytes(reinterpret_cast<const char *>(vec1.data()),
                         vec1.size() * sizeof(float));

  auto key1 = IndexToKey(1);
  VMSDK_EXPECT_OK(index.value()->AddRecord(key1, vec1_bytes));

  // Search query [5.0, 0.0, 0.0, 0.0] pointing in exact same direction.
  // Cosine distance should be 0.0 (1 - (3*5)/(3*5) = 0).
  std::vector<float> query = {5.0f, 0.0f, 0.0f, 0.0f};
  std::string query_bytes(reinterpret_cast<const char *>(query.data()),
                          query.size() * sizeof(float));

  auto search_res = index.value()->Search(query_bytes, 1, CancelNever());
  ASSERT_TRUE(search_res.ok());
  ASSERT_EQ(search_res.value().size(), 1);
  EXPECT_NEAR(search_res.value()[0].distance, 0.0f, 1e-5f);
}

float CalcRecall(VectorFlat<float> *flat_index, VectorHNSW<float> *hnsw_index,
                 uint64_t k, int dimensions, std::optional<size_t> ef_runtime) {
  auto search_vectors = DeterministicallyGenerateVectors(50, dimensions, 1.5);
  int cnt = 0;
  for (const auto &search_vector : search_vectors) {
    absl::string_view vector = VectorToStr(search_vector);
    auto res_hnsw =
        hnsw_index->Search(vector, k, CancelNever(), nullptr, ef_runtime);
    auto res_flat = flat_index->Search(vector, k, CancelNever());
    for (auto &label : *res_hnsw) {
      for (auto &real_label : *res_flat) {
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
  for (auto &distance_metric : {data_model::DISTANCE_METRIC_L2}) {
    // Use a large cap to make sure chunked array is properly exercised
    const int initial_cap = 31000;
    auto index_hnsw = VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kM, kEFConstruction, kEFRuntime),
        attribute_identifier, attribute_data_type);
    auto vectors = DeterministicallyGenerateVectors(1000, kDimensions, 2.2);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VERIFY_ADD(index_hnsw->get(), vectors, i, ExpectedResults::kSuccess);
    }
    auto index_flat = VectorFlat<float>::Create(
        CreateFlatVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kBlockSize),
        attribute_identifier, attribute_data_type);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VERIFY_ADD(index_flat->get(), vectors, i, ExpectedResults::kSuccess);
    }
    uint64_t k = 10;
    auto no_ef_runtime_recall = CalcRecall(index_flat->get(), index_hnsw->get(),
                                           k, kDimensions, std::nullopt);
    auto default_ef_runtime_recall = CalcRecall(
        index_flat->get(), index_hnsw->get(), k, kDimensions, kEFRuntime);
    auto ef_runtime_recall = CalcRecall(index_flat->get(), index_hnsw->get(), k,
                                        kDimensions, kEFRuntime * 8);
    EXPECT_GE(ef_runtime_recall, 0.96f);
    EXPECT_EQ(default_ef_runtime_recall, no_ef_runtime_recall);
  }
}

TEST_F(VectorIndexTest, SaveAndLoadHnsw) {
  for (auto &distance_metric :
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
        attribute_identifier, attribute_data_type);
    VMSDK_EXPECT_OK(index_flat);
    for (size_t i = 0; i < vectors.size(); ++i) {
      VERIFY_ADD(index_flat->get(), vectors, i, ExpectedResults::kSuccess);
    }

    data_model::VectorIndex hnsw_proto =
        CreateHNSWVectorIndexProto(kDimensions, distance_metric, initial_cap,
                                   kM, kEFConstruction, kEFRuntime);
    // Create and save empty HNSW index
    {
      auto index_hnsw = VectorHNSW<float>::Create(
          hnsw_proto, "attribute_identifier_2", attribute_data_type);
      VMSDK_EXPECT_OK(index_hnsw);
      if (distance_metric == data_model::DISTANCE_METRIC_COSINE) {
        EXPECT_TRUE((*index_hnsw)->GetNormalize());
      }
      VMSDK_EXPECT_OK((*index_hnsw)->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK(
          (*index_hnsw)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      hnsw_proto = (*index_hnsw)->ToProto()->vector_index();
    }
    ValkeyModuleString *records[vectors.size()];
    for (size_t i = 0; i < vectors.size(); ++i) {
      records[i] = new ValkeyModuleString{
          std::string((char *)&vectors[i][0], kDimensions * sizeof(float))};
    }

    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([&records](ValkeyModuleKey *key, int, const char *,
                                   ValkeyModuleString **value_out, void *) {
          auto key_str = absl::string_view(key->key);
          CHECK(absl::ConsumeSuffix(&key_str, "_key"));
          int index;
          CHECK(absl::SimpleAtoi(key_str, &index));
          *value_out = records[index];
          ValkeyModule_RetainString(nullptr, records[index]);
          return VALKEYMODULE_OK;
        });
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
        VERIFY_ADD(loaded_index_hnsw->get(), vectors, i,
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
    for (size_t i = 0; i < vectors.size(); ++i) {
      delete records[i];
    }
  }
}

// Verify allow-replace-deleted replaces deleted HNSW elements
TEST_F(VectorIndexTest, AllowReplaceDeletedNoLabelReuse)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  VMSDK_EXPECT_OK(options::GetHNSWAllowReplaceDeletedMutable().SetValue(true));
  EXPECT_TRUE(options::GetHNSWAllowReplaceDeleted().GetValue());
  auto attribute_identifier = "attr_id";
  auto index = VectorHNSW<float>::Create(
      CreateHNSWVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                 kInitialCap, kM, kEFConstruction, kEFRuntime),
      attribute_identifier, attribute_data_type);
  VMSDK_EXPECT_OK(index);
  auto vectors = DeterministicallyGenerateVectors(10, kDimensions, 10.0);
  for (size_t i = 0; i < vectors.size(); ++i) {
    VERIFY_ADD(index->get(), vectors, i, ExpectedResults::kSuccess);
  }
  VectorBase *base = index->get();
  EXPECT_EQ(base->GetMaxInternalLabel(), 9u);
  VMSDK_EXPECT_OK((*index)->RemoveRecord(IndexToKey(8), DeletionType::kNone));
  VMSDK_EXPECT_OK((*index)->RemoveRecord(IndexToKey(9), DeletionType::kNone));
  EXPECT_EQ(base->GetMaxInternalLabel(), 9u);
  EXPECT_EQ(base->GetLabelCount(), 10u);
  EXPECT_EQ(base->GetTrackedKeyCount(), 8u);
  auto new_vectors = DeterministicallyGenerateVectors(5, kDimensions, 20.0);
  for (size_t i = 0; i < new_vectors.size(); ++i) {
    auto key = StringInternStore::Intern(absl::StrCat("new_", i, "_key"));
    absl::string_view vec_str = VectorToStr(new_vectors[i]);
    auto res = (*index)->AddRecord(key, vec_str);
    VMSDK_EXPECT_OK(res) << "AddRecord failed for new vector " << i;
    EXPECT_TRUE(res.value());
  }
  EXPECT_EQ(base->GetTrackedKeyCount(), 13u);
  // Verifies we reused tombstoned hnsw nodes
  EXPECT_EQ(base->GetLabelCount(), 13u);
  absl::string_view query = VectorToStr(new_vectors[0]);
  auto search_result = (*index)->Search(query, 13, CancelNever());
  VMSDK_EXPECT_OK(search_result);
  EXPECT_EQ(search_result->size(), 13u);
}

TEST_F(VectorIndexTest, SaveAndLoadFlat) {
  for (auto &distance_metric :
       {data_model::DISTANCE_METRIC_COSINE, data_model::DISTANCE_METRIC_L2}) {
    std::cout << "distance_metric: " << distance_metric << "\n";
    const int initial_cap = 1000;
    const uint64_t k = 10;
    FakeSafeRDB rdb;
    auto vectors = DeterministicallyGenerateVectors(1000, kDimensions, 2.2);
    auto search_vectors =
        DeterministicallyGenerateVectors(50, kDimensions, 1.5);
    std::vector<std::vector<Neighbor>> expected_results;

    data_model::VectorIndex flat_proto = CreateFlatVectorIndexProto(
        kDimensions, distance_metric, initial_cap, kBlockSize);
    // Create and save empty Flat index
    {
      auto index = VectorFlat<float>::Create(flat_proto, attribute_identifier,
                                             attribute_data_type);
      if (distance_metric == data_model::DISTANCE_METRIC_COSINE) {
        EXPECT_TRUE(index.value()->GetNormalize());
      }
      VMSDK_EXPECT_OK(index.value()->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK((*index)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      flat_proto = (*index)->ToProto()->vector_index();
    }
    ValkeyModuleString *records[vectors.size()];
    for (size_t i = 0; i < vectors.size(); ++i) {
      records[i] = new ValkeyModuleString{
          std::string((char *)&vectors[i][0], kDimensions * sizeof(float))};
    }

    EXPECT_CALL(*kMockValkeyModule, OpenKey(testing::_, testing::_, testing::_))
        .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
    EXPECT_CALL(*kMockValkeyModule,
                HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                        testing::An<ValkeyModuleString **>(),
                        testing::TypedEq<void *>(nullptr)))
        .WillRepeatedly([&records](ValkeyModuleKey *key, int, const char *,
                                   ValkeyModuleString **value_out, void *) {
          auto key_str = absl::string_view(key->key);
          CHECK(absl::ConsumeSuffix(&key_str, "_key"));
          int index;
          CHECK(absl::SimpleAtoi(key_str, &index));
          *value_out = records[index];
          ValkeyModule_RetainString(nullptr, records[index]);
          return VALKEYMODULE_OK;
        });
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
        VERIFY_ADD(index.get(), vectors, i, ExpectedResults::kSuccess);
      }
      for (const auto &search_vector : search_vectors) {
        absl::string_view vector = VectorToStr(search_vector);
        auto res = index->Search(vector, k, CancelNever());
        expected_results.push_back(std::move(*res));
      }
      VMSDK_EXPECT_OK(index->SaveIndex(RDBChunkOutputStream(&rdb)));
      VMSDK_EXPECT_OK(index->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
      flat_proto = index->ToProto()->vector_index();
    }

    // Load the index, run search queries and validate that the search results
    // match the previous results
    {
      ValkeyModuleString *records[vectors.size()];
      for (size_t i = 0; i < vectors.size(); ++i) {
        records[i] = new ValkeyModuleString{
            std::string((char *)&vectors[i][0], kDimensions * sizeof(float))};
      }

      EXPECT_CALL(*kMockValkeyModule,
                  OpenKey(testing::_, testing::_, testing::_))
          .WillRepeatedly(TestValkeyModule_OpenKeyDefaultImpl);
      EXPECT_CALL(*kMockValkeyModule,
                  HashGet(testing::_, VALKEYMODULE_HASH_CFIELDS, testing::_,
                          testing::An<ValkeyModuleString **>(),
                          testing::TypedEq<void *>(nullptr)))
          .WillRepeatedly([&records](ValkeyModuleKey *key, int, const char *,
                                     ValkeyModuleString **value_out, void *) {
            auto key_str = absl::string_view(key->key);
            CHECK(absl::ConsumeSuffix(&key_str, "_key"));
            int index;
            CHECK(absl::SimpleAtoi(key_str, &index));
            *value_out = records[index];
            ValkeyModule_RetainString(nullptr, records[index]);
            return VALKEYMODULE_OK;
          });

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
        auto res = index->Search(vector, k, CancelNever());
        auto act = ToVectorNeighborTest(*res);
        auto exp = ToVectorNeighborTest(expected_results[i]);
        ExpectNeighborsNear(act, exp);
      }

      // Re-insert the vectors
      for (size_t i = 0; i < vectors.size(); ++i) {
        VERIFY_MODIFY(index.get(), vectors[i], i, ExpectedResults::kSkipped,
                      true);
      }
    }
    for (size_t i = 0; i < vectors.size(); ++i) {
      delete records[i];
    }
  }
}

// verify reclaimable_memory is correctly synchronized and writes are not lost
// lost writes can lead to negative integer underflow issue
TEST_F(VectorIndexTest, ReclaimableMemoryRaceReturnsToBaseline)
ABSL_NO_THREAD_SAFETY_ANALYSIS {
  constexpr int kThreads = 8;
  constexpr int kIters = 50000;
  hnswlib::L2Space l2_space{kDimensions};
  VectorHNSW<float>::HNSWIndex algo(&l2_space, /*max_elements=*/kThreads,
                                    /*normalized=*/false, kM, kEFConstruction,
                                    /*allow_replace_deleted=*/false,
                                    /*random_seed=*/100);
  std::vector<float> v(kDimensions, 1.0f);
  absl::string_view v_bytes(reinterpret_cast<const char *>(v.data()),
                            v.size() * sizeof(float));
  auto vector_allocator = CREATE_UNIQUE_PTR(
      FixedSizeAllocator, kDimensions * sizeof(float) + 1, true);

  float magnitude = kDefaultMagnitude;
  for (int t = 0; t < kThreads; ++t) {
    algo.addPoint(InputVector(VectorRecord::Construct(v_bytes, magnitude,
                                                      vector_allocator.get()),
                              v_bytes.size(), false),
                  t);
  }

  // baseline is unsigned 64-bit integer, if goes to negative it underflows to a
  // large positive integer
  const uint64_t baseline = Metrics::GetStats().reclaimable_memory;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&algo, t]() {
      for (int i = 0; i < kIters; ++i) {
        algo.markDelete(t);    // += vector_size_
        algo.unmarkDelete(t);  // -= vector_size_  (net per cycle: 0)
      }
    });
  }
  for (auto &th : threads) {
    th.join();
  }

  // With atomic RMW ops, perfectly balanced mark/unmark cycles must net to
  // zero, so the counter must return to its pre-test baseline.
  EXPECT_EQ(Metrics::GetStats().reclaimable_memory, baseline);
}

// offsetData_ and the element stride must be 8-byte aligned so the vector
// pointer read/write is atomic on ARM64 and avoids a torn-pointer crash.
TEST_F(VectorIndexTest, OffsetDataIsPointerAlignedOnCreate) {
  hnswlib::L2Space l2_space{kDimensions};
  VectorHNSW<float>::HNSWIndex algo(
      &l2_space, /*max_elements=*/16, /*normalized=*/false, kM, kEFConstruction,
      /*allow_replace_deleted=*/false, /*random_seed=*/100);
  EXPECT_EQ(algo.offsetData_ % alignof(char *), 0u);
  EXPECT_EQ(algo.size_data_per_element_ % alignof(char *), 0u);
  EXPECT_GE(algo.offsetData_, algo.size_links_level0_);
}

namespace {
// InputStream that yields a single pre-built chunk (the index header).
class SingleChunkInputStream : public hnswlib::InputStream {
 public:
  explicit SingleChunkInputStream(std::string chunk)
      : chunk_(std::move(chunk)) {}
  absl::StatusOr<std::unique_ptr<std::string>> LoadChunk() override {
    return std::make_unique<std::string>(chunk_);
  }

 private:
  std::string chunk_;
};
}  // namespace

// An old snapshot stores the unpadded offset_data (132). LoadIndex must
// recompute the aligned offset rather than trust the header, else the restored
// index stays misaligned and exposed to the torn-pointer race.
TEST_F(VectorIndexTest, LoadRecomputesAlignedOffsetForOldSnapshot) {
  hnswlib::L2Space l2_space{kDimensions};
  const size_t unpadded_offset =
      kM * 2 * sizeof(unsigned int) + sizeof(unsigned int);  // 132
  ASSERT_NE(unpadded_offset % alignof(char *), 0u);

  hnswlib::data_model::HNSWIndexHeader header;
  header.set_offset_level_0(0);
  header.set_max_elements(16);
  header.set_curr_element_count(0);
  header.set_serialize_size_data_per_element(
      unpadded_offset + kDimensions * sizeof(float) + sizeof(size_t));
  header.set_label_offset(unpadded_offset + sizeof(char *));
  header.set_offset_data(unpadded_offset);
  header.set_max_level(-1);
  header.set_enterpoint_node(0);
  header.set_max_m(kM);
  header.set_max_m_0(kM * 2);
  header.set_m(kM);
  header.set_mult(1.0 / std::log(kM));
  header.set_ef_construction(kEFConstruction);
  std::string serialized;
  ASSERT_TRUE(header.SerializeToString(&serialized));

  VectorHNSW<float>::HNSWIndex algo;
  SingleChunkInputStream input(serialized);
  auto generator = [](absl::string_view vector_data, bool is_marked_deleted) {
    float magnitude =
        CalcMagnitude(reinterpret_cast<const float *>(vector_data.data()),
                      vector_data.size() / sizeof(float));
    return VectorRecord::Construct(vector_data, magnitude);
  };
  VMSDK_EXPECT_OK(algo.LoadIndex(input, &l2_space, /*max_elements_i=*/16,
                                 /*expected_m=*/kM, /*validate=*/true,
                                 generator));
  EXPECT_EQ(algo.offsetData_ % alignof(char *), 0u);
  EXPECT_EQ(algo.size_data_per_element_ % alignof(char *), 0u);
}

}  // namespace

}  // namespace valkey_search::indexes
