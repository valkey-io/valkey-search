/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Tests for the SVS_VAMANA threading model: the sequential threadpool vtable,
// the create-failure counter, the FINE_GRAIN build and concurrent writers.
// Built only where the SVS C API is (x86_64 Linux).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <thread>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/cleanup/cleanup.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/notification.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data.h"
#include "src/index_schema.pb.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_svs.h"
#include "src/metrics.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "svs/c/svs_c.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {

class VectorSVSTestPeer {
 public:
  static svs_index_h Handle(const VectorSVS<float> &index)
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return index.svs_index_;
  }
};

namespace {

using ::testing::HasSubstr;

constexpr int kDimensions = 8;
constexpr int kNumVectors = 100;
constexpr uint32_t kGraphMaxDegree = 16;
constexpr uint32_t kConstructionWindowSize = 32;
constexpr uint32_t kSearchWindowSize = 10;

data_model::VectorIndex CreateSVSVectorIndexProto(
    int dimensions, data_model::DistanceMetric distance_metric,
    data_model::SVSCompressionType compression) {
  data_model::VectorIndex vector_index_proto;
  vector_index_proto.set_dimension_count(dimensions);
  vector_index_proto.set_distance_metric(distance_metric);
  auto svs_algorithm = std::make_unique<data_model::SVSVamanaAlgorithm>();
  svs_algorithm->set_graph_max_degree(kGraphMaxDegree);
  svs_algorithm->set_construction_window_size(kConstructionWindowSize);
  svs_algorithm->set_search_window_size(kSearchWindowSize);
  svs_algorithm->set_alpha(0.0f);
  svs_algorithm->set_compression(compression);
  vector_index_proto.set_allocated_svs_vamana_algorithm(
      svs_algorithm.release());
  return vector_index_proto;
}

InternedStringPtr IndexToKey(int i) {
  return StringInternStore::Intern(absl::StrCat(i, "_key"));
}

cancel::Token &CancelNever() {
  static cancel::Token cancel_never = cancel::Make(1000000, nullptr);
  return cancel_never;
}

// VectorSVS re-declares GetLabelCount under protected:; call it via the base.
VectorBase *AsBase(const std::shared_ptr<VectorSVS<float>> &p) {
  return p.get();
}

// -- (a) vtable shape --------------------------------------------------------

TEST(SvsThreadpoolVtable, VersionAndStructSizeMatchPinnedHeader) {
  EXPECT_EQ(kSvsThreadpoolOps.version, SVS_C_API_VERSION);
  EXPECT_EQ(kSvsThreadpoolOps.struct_size,
            sizeof(svs_threadpool_interface_ops));
  ASSERT_NE(kSvsThreadpoolOps.size, nullptr);
  ASSERT_NE(kSvsThreadpoolOps.parallel_for, nullptr);
  EXPECT_EQ(kSvsThreadpoolIface.ops, &kSvsThreadpoolOps);
  EXPECT_EQ(kSvsThreadpoolIface.self, nullptr);
}

TEST(SvsThreadpoolVtable, SizeReturnsOne) {
  EXPECT_EQ(kSvsThreadpoolOps.size(kSvsThreadpoolIface.self), 1u);
}

TEST(SvsThreadpoolVtable, InstallsOnARealBuilder) {
  svs_error_h err = svs_error_create();
  svs_algorithm_h algo = svs_algorithm_create_vamana(
      kGraphMaxDegree, kConstructionWindowSize, kSearchWindowSize, err);
  ASSERT_NE(algo, nullptr) << svs_error_get_message(err);
  svs_index_builder_h builder = svs_index_builder_create(
      SVS_DISTANCE_METRIC_EUCLIDEAN, kDimensions, algo, err);
  ASSERT_NE(builder, nullptr) << svs_error_get_message(err);
  EXPECT_TRUE(svs_index_builder_set_threadpool_custom(
      builder, &kSvsThreadpoolIface, err))
      << svs_error_get_message(err);
  svs_index_builder_free(builder);
  svs_algorithm_free(algo);
  svs_error_free(err);
}

// -- (b) parallel_for contract -----------------------------------------------

struct ParallelForRecorder {
  std::vector<size_t> seen;
  std::thread::id caller;
  bool all_on_caller_thread = true;
};

void RecordInvocation(void *param, size_t i) {
  auto *rec = static_cast<ParallelForRecorder *>(param);
  rec->seen.push_back(i);
  if (std::this_thread::get_id() != rec->caller) {
    rec->all_on_caller_thread = false;
  }
}

class SvsThreadpoolParallelFor : public ::testing::TestWithParam<size_t> {};

TEST_P(SvsThreadpoolParallelFor, InvokesEveryIndexOnceInOrderOnCallerThread) {
  const size_t n = GetParam();
  ParallelForRecorder rec;
  rec.caller = std::this_thread::get_id();
  rec.seen.reserve(n);

  svs_error_h err = svs_error_create();
  EXPECT_TRUE(kSvsThreadpoolOps.parallel_for(kSvsThreadpoolIface.self,
                                             &RecordInvocation, &rec, n, err));
  svs_error_free(err);

  std::vector<size_t> expected(n);
  std::iota(expected.begin(), expected.end(), size_t{0});
  EXPECT_EQ(rec.seen, expected);
  EXPECT_TRUE(rec.all_on_caller_thread);
}

INSTANTIATE_TEST_SUITE_P(Sizes, SvsThreadpoolParallelFor,
                         ::testing::Values(size_t{0}, size_t{1}, size_t{7},
                                           size_t{1000}));

// -- (c) build, add and search through the installed pool --------------------

class VectorSVSThreadpoolTest : public ValkeySearchTest {
 public:
  const char *attribute_identifier = "attribute_identifier_1";
  data_model::AttributeDataType attribute_data_type =
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
};

class VectorSVSThreadpoolCompressionTest
    : public VectorSVSThreadpoolTest,
      public ::testing::WithParamInterface<data_model::SVSCompressionType> {};

TEST_P(VectorSVSThreadpoolCompressionTest,
       BootstrapAddAndSearchFindPlantedVector) {
  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                GetParam()),
      attribute_identifier, attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  ASSERT_TRUE(index.ok());

  // The first AddRecord bootstraps the index; the rest go through add_points.
  auto vectors =
      DeterministicallyGenerateVectors(kNumVectors, kDimensions, 10.0);
  for (int i = 0; i < kNumVectors; ++i) {
    VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                   VectorToStr(vectors[i])));
  }
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), static_cast<size_t>(kNumVectors));

  for (int planted : {0, 42, kNumVectors - 1}) {
    auto result =
        (*index)->Search(VectorToStr(vectors[planted]), 1, CancelNever());
    VMSDK_EXPECT_OK(result);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result->size(), 1u) << "planted " << planted;
    EXPECT_EQ((*result)[0].external_id, IndexToKey(planted));
    EXPECT_NEAR((*result)[0].distance, 0.0f, 1e-3f);
  }
}

// SQ8 is left out: its distances are a known open item (issue #13).
INSTANTIATE_TEST_SUITE_P(
    Compression, VectorSVSThreadpoolCompressionTest,
    ::testing::Values(data_model::SVS_COMPRESSION_NONE,
                      data_model::SVS_COMPRESSION_FP16),
    [](const ::testing::TestParamInfo<data_model::SVSCompressionType> &info) {
      return data_model::SVSCompressionType_Name(info.param);
    });

// -- (d) installer failure bumps svs_create_exceptions_cnt once --------------

size_t ZeroThreadpoolSize(void * /*self*/) { return 0; }

TEST_F(VectorSVSThreadpoolTest,
       RejectedThreadpoolInstallBumpsCreateCounterOnce) {
  // The C API rejects a custom pool whose size() returns 0 at install time,
  // so the first AddRecord fails inside BootstrapIndex.
  auto *saved_size = kSvsThreadpoolOps.size;
  kSvsThreadpoolOps.size = &ZeroThreadpoolSize;
  absl::Cleanup restore = [saved_size] { kSvsThreadpoolOps.size = saved_size; };

  auto &counter = Metrics::GetStats().svs_create_exceptions_cnt;
  const uint64_t before = counter.load();

  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE),
      attribute_identifier, attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  ASSERT_TRUE(index.ok());
  // Create() does not touch SVS; nothing counted yet.
  EXPECT_EQ(counter.load(), before);

  auto vectors = DeterministicallyGenerateVectors(2, kDimensions, 10.0);
  auto result = testing_infra::AddVectorRecord(**index, IndexToKey(0),
                                               VectorToStr(vectors[0]));
  ASSERT_FALSE(result.ok());
  EXPECT_THAT(result.status().message(),
              HasSubstr("index_builder_set_threadpool_custom"));
  EXPECT_EQ(counter.load(), before + 1);
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), 0u);

  // With the real vtable back the index bootstraps and the counter stays.
  std::move(restore).Invoke();
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(0),
                                                 VectorToStr(vectors[0])));
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(1),
                                                 VectorToStr(vectors[1])));
  EXPECT_EQ(counter.load(), before + 1);
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), 2u);
}

// -- (e) the index is built with FINE_GRAIN synchronization -----------------

TEST_F(VectorSVSThreadpoolTest, BootstrapBuildsWithFineGrainSyncKind) {
  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE),
      attribute_identifier, attribute_data_type, 0);
  ASSERT_TRUE(index.ok());
  EXPECT_EQ(VectorSVSTestPeer::Handle(**index), nullptr);

  auto vectors = DeterministicallyGenerateVectors(1, kDimensions, 10.0);
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(0),
                                                 VectorToStr(vectors[0])));
  svs_index_h handle = VectorSVSTestPeer::Handle(**index);
  ASSERT_NE(handle, nullptr);

  svs_error_h err = svs_error_create();
  svs_sync_kind_t sync_kind = SVS_SYNC_KIND_NONE;
  ASSERT_TRUE(svs_index_dynamic_get_sync_kind(handle, &sync_kind, err))
      << svs_error_get_message(err);
  svs_error_free(err);
  EXPECT_EQ(sync_kind, SVS_SYNC_KIND_FINE_GRAIN);
}

// -- (f) concurrent writers ---------------------------------------------------
//
// At the pinned SVS revision SVS_SYNC_KIND_FINE_GRAIN is an alias of GLOBAL:
// SVS serializes writers on one lock inside the index. These tests therefore
// exercise the module's label map, slot reservation and first-build paths
// under concurrency, not parallel SVS mutation; they become that acceptance
// when upstream implements FINE_GRAIN.

// Records are built on the test thread (VectorRegistry is not thread-safe).
struct PreparedRecord {
  InternedStringPtr key;
  AttributeData data;
};

PreparedRecord PrepareRecord(VectorBase &index, int i,
                             const std::vector<float> &vector) {
  auto key = IndexToKey(i);
  auto valkey_str = vmsdk::MakeUniqueValkeyString(VectorToStr(vector));
  auto vec = VectorRegistry::Instance().DedupOrConstruct(
      key, valkey_str.get(), data_model::ATTRIBUTE_DATA_TYPE_HASH,
      index.GetDBNum(), &index);
  return PreparedRecord{key, AttributeData(std::move(vec))};
}

// Runs work(w) on `writers` threads released together; returns their statuses.
template <typename Work>
std::vector<absl::Status> RunWriters(int writers, Work work) {
  absl::Notification go;
  std::vector<absl::Status> statuses(writers);
  std::vector<std::thread> threads;
  threads.reserve(writers);
  for (int w = 0; w < writers; ++w) {
    threads.emplace_back([&, w] {
      go.WaitForNotification();
      statuses[w] = work(w);
    });
  }
  go.Notify();
  for (auto &t : threads) t.join();
  return statuses;
}

// Each id must be its own nearest neighbour, window widened to the index.
void ExpectEachIsItsOwnNearest(VectorSVS<float> &index,
                               const std::vector<int> &ids,
                               const std::vector<std::vector<float>> &vectors,
                               size_t live) {
  for (int i : ids) {
    auto result = index.Search(VectorToStr(vectors[i]), 1, CancelNever(),
                               nullptr, /*ef_runtime=*/live);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->size(), 1u) << "id " << i;
    EXPECT_EQ((*result)[0].external_id, IndexToKey(i)) << "id " << i;
    EXPECT_NEAR((*result)[0].distance, 0.0f, 1e-3f) << "id " << i;
  }
}

constexpr int kWriters = 8;
constexpr int kPerWriter = 50;
constexpr int kConcurrentVectors = kWriters * kPerWriter;

TEST_F(VectorSVSThreadpoolTest, EightWritersAddConcurrentlyAndEveryIdIsFound) {
  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE),
      attribute_identifier, attribute_data_type, 0);
  ASSERT_TRUE(index.ok());
  // Bootstrap first so every writer below goes through add_points.
  auto vectors = DeterministicallyGenerateVectors(kConcurrentVectors + 1,
                                                  kDimensions, 10.0);
  VMSDK_EXPECT_OK(
      testing_infra::AddVectorRecord(**index, IndexToKey(kConcurrentVectors),
                                     VectorToStr(vectors[kConcurrentVectors])));

  std::vector<PreparedRecord> records;
  records.reserve(kConcurrentVectors);
  for (int i = 0; i < kConcurrentVectors; ++i) {
    records.push_back(PrepareRecord(**index, i, vectors[i]));
  }
  auto statuses = RunWriters(kWriters, [&](int w) {
    for (int i = w * kPerWriter; i < (w + 1) * kPerWriter; ++i) {
      auto result =
          (*index)->AddRecord(records[i].key, std::move(records[i].data));
      if (!result.ok()) return result.status();
    }
    return absl::OkStatus();
  });
  for (const auto &status : statuses) VMSDK_EXPECT_OK(status);

  EXPECT_EQ(AsBase(*index)->GetLabelCount(),
            static_cast<size_t>(kConcurrentVectors + 1));
  std::vector<int> ids(kConcurrentVectors + 1);
  std::iota(ids.begin(), ids.end(), 0);
  ExpectEachIsItsOwnNearest(**index, ids, vectors, kConcurrentVectors + 1);
}

TEST_F(VectorSVSThreadpoolTest, FirstAddRaceBuildsOnceAndLosersAdd) {
  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE),
      attribute_identifier, attribute_data_type, 0);
  ASSERT_TRUE(index.ok());
  auto &counter = Metrics::GetStats().svs_create_exceptions_cnt;
  const uint64_t before = counter.load();

  auto vectors = DeterministicallyGenerateVectors(kWriters, kDimensions, 10.0);
  std::vector<PreparedRecord> records;
  for (int i = 0; i < kWriters; ++i) {
    records.push_back(PrepareRecord(**index, i, vectors[i]));
  }
  // All writers start on an empty index: exactly one builds.
  auto statuses = RunWriters(kWriters, [&](int w) {
    return (*index)
        ->AddRecord(records[w].key, std::move(records[w].data))
        .status();
  });
  for (const auto &status : statuses) VMSDK_EXPECT_OK(status);

  EXPECT_EQ(counter.load(), before);
  EXPECT_NE(VectorSVSTestPeer::Handle(**index), nullptr);
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), static_cast<size_t>(kWriters));
  std::vector<int> ids(kWriters);
  std::iota(ids.begin(), ids.end(), 0);
  ExpectEachIsItsOwnNearest(**index, ids, vectors, kWriters);
}

TEST_F(VectorSVSThreadpoolTest, ConcurrentAddsAndRemovesStayConsistent) {
  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE),
      attribute_identifier, attribute_data_type, 0);
  ASSERT_TRUE(index.ok());
  // Prefill [0, N); four writers remove even ids of their slice, four add
  // [N, 2N).
  auto vectors = DeterministicallyGenerateVectors(2 * kConcurrentVectors,
                                                  kDimensions, 10.0);
  for (int i = 0; i < kConcurrentVectors; ++i) {
    VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                   VectorToStr(vectors[i])));
  }
  std::vector<PreparedRecord> records;
  for (int i = kConcurrentVectors; i < 2 * kConcurrentVectors; ++i) {
    records.push_back(PrepareRecord(**index, i, vectors[i]));
  }
  const int half = kWriters / 2;
  const int per_remover = kConcurrentVectors / half;
  const int per_adder = kConcurrentVectors / half;
  auto statuses = RunWriters(kWriters, [&](int w) -> absl::Status {
    if (w < half) {
      for (int i = w * per_remover; i < (w + 1) * per_remover; i += 2) {
        auto removed =
            (*index)->RemoveRecord(IndexToKey(i), DeletionType::kRecord);
        if (!removed.ok()) return removed.status();
        if (!*removed) {
          return absl::NotFoundError(absl::StrCat("id ", i, " not removed"));
        }
      }
    } else {
      const int a = w - half;
      for (int i = a * per_adder; i < (a + 1) * per_adder; ++i) {
        auto result =
            (*index)->AddRecord(records[i].key, std::move(records[i].data));
        if (!result.ok()) return result.status();
      }
    }
    return absl::OkStatus();
  });
  for (const auto &status : statuses) VMSDK_EXPECT_OK(status);

  std::vector<int> live_ids;
  for (int i = 1; i < kConcurrentVectors; i += 2) live_ids.push_back(i);
  for (int i = kConcurrentVectors; i < 2 * kConcurrentVectors; ++i) {
    live_ids.push_back(i);
  }
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), live_ids.size());
  ExpectEachIsItsOwnNearest(**index, live_ids, vectors, live_ids.size());
  // A removed vector's nearest neighbour is never the removed id.
  for (int i = 0; i < kConcurrentVectors; i += 2) {
    auto result = (*index)->Search(VectorToStr(vectors[i]), 1, CancelNever(),
                                   nullptr, /*ef_runtime=*/live_ids.size());
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->size(), 1u);
    EXPECT_NE((*result)[0].external_id, IndexToKey(i)) << "id " << i;
  }
}

TEST_F(VectorSVSThreadpoolTest,
       EightWritersModifyConcurrentlyAndEveryIdIsFound) {
  auto index = VectorSVS<float>::Create(
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE),
      attribute_identifier, attribute_data_type, 0);
  ASSERT_TRUE(index.ok());
  auto vectors =
      DeterministicallyGenerateVectors(kConcurrentVectors, kDimensions, 10.0);
  for (int i = 0; i < kConcurrentVectors; ++i) {
    VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                   VectorToStr(vectors[i])));
  }

  // Replace every vector from 8 threads at once (delete + add inside SVS,
  // under the shared lock); each id must end up on its new vector.
  std::vector<std::vector<float>> replaced(vectors);
  for (auto &v : replaced) {
    for (auto &x : v) x = -x - 1.0f;
  }
  std::vector<PreparedRecord> records;
  records.reserve(kConcurrentVectors);
  for (int i = 0; i < kConcurrentVectors; ++i) {
    records.push_back(PrepareRecord(**index, i, replaced[i]));
  }
  auto statuses = RunWriters(kWriters, [&](int w) {
    for (int i = w * kPerWriter; i < (w + 1) * kPerWriter; ++i) {
      auto result =
          (*index)->ModifyRecord(records[i].key, std::move(records[i].data));
      if (!result.ok()) return result.status();
    }
    return absl::OkStatus();
  });
  for (const auto &status : statuses) VMSDK_EXPECT_OK(status);

  EXPECT_EQ(AsBase(*index)->GetLabelCount(),
            static_cast<size_t>(kConcurrentVectors));
  std::vector<int> ids(kConcurrentVectors);
  std::iota(ids.begin(), ids.end(), 0);
  ExpectEachIsItsOwnNearest(**index, ids, replaced, kConcurrentVectors);
}

}  // namespace
}  // namespace valkey_search::indexes
