/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Unit tests for VectorSVS<T>::SaveIndexImpl / LoadFromRDB (src/indexes/
// vector_svs.cc). Covers docs/svs-rdb/design.md: empty index, populated
// round-trip per compression mode, the terminator invariant, header
// validation, and the duplicate-label metric (not the label-count gate,
// which design.md's "Corrected in round 2" paragraph says does not exist).

#include "src/indexes/vector_svs.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "absl/algorithm/container.h"
#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/synchronization/notification.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/index_base.h"
#include "src/indexes/svs_index.pb.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_flat.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "svs/c/svs_c.h"
#include "testing/common.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {

class VectorSVSTestPeer {
 public:
  static svs_index_h Handle(const VectorSVS<float> &index)
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return index.svs_index_;
  }
  static bool MutationsExclusive(const VectorSVS<float> &index) {
    return index.mutations_exclusive_.load();
  }
  static absl::Mutex &ResizeMutex(VectorSVS<float> &index) {
    return index.resize_mutex_;
  }
};

namespace {

constexpr int kDimensions = 8;

auto IndexToKey = [](int i) {
  return StringInternStore::Intern(absl::StrCat(i, "_key"));
};

// GetLabelCount / GetMaxLoadedLabel are public on VectorBase but VectorSVS
// re-declares its overrides under `protected:` (they're implementation
// detail there); access control resolves on the static type, so a caller
// outside the class needs the VectorBase view to reach them at all.
VectorBase *AsBase(const std::shared_ptr<VectorSVS<float>> &p) {
  return p.get();
}

class VectorSVSTest : public ValkeySearchTest {
 public:
  // Enabling a pause point flips a process-global switch that makes every
  // BACKGROUND_PAUSEPOINT take a mutex; put it back so later tests keep
  // their interleavings.
  void TearDown() override {
    vmsdk::debug::pause_points_enabled = false;
    ValkeySearchTest::TearDown();
  }
  HashAttributeDataType hash_attribute_data_type_;
  const char *attribute_identifier = "attribute_identifier_1";
  data_model::AttributeDataType attribute_data_type =
      data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH;
};

// Registers OpenKey / HashGet mocks that serve `records[i]` for key
// "<i>_key", as LoadTrackedKeys needs to re-read every vector from the live
// keyspace (design.md "Labels").
void ExpectHashGetsForRecords(std::vector<ValkeyModuleString *> &records) {
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
}

// -- Empty index -------------------------------------------------------------

TEST_F(VectorSVSTest, EmptyIndexRoundTrip) {
  FakeSafeRDB rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), 0u);
  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&rdb)));

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded);
  EXPECT_EQ(AsBase(*loaded)->GetLabelCount(), 0u);

  // Handle-less empty index: no SVS call happened on either side of the
  // round trip, and the loaded instance still bootstraps on the first HSET.
  auto vectors = DeterministicallyGenerateVectors(1, kDimensions, 1.0);
  auto result = testing_infra::AddVectorRecord(**loaded, IndexToKey(0),
                                               VectorToStr(vectors[0]));
  VMSDK_EXPECT_OK(result);
  EXPECT_EQ(AsBase(*loaded)->GetLabelCount(), 1u);
}

// -- Populated round trip, every compression mode ---------------------------

class VectorSVSCompressionTest
    : public VectorSVSTest,
      public testing::WithParamInterface<data_model::SVSCompressionType> {};

TEST_P(VectorSVSCompressionTest, PopulatedRoundTrip) {
  const size_t kNumVectors = 20;
  FakeSafeRDB rdb;
  auto vectors =
      DeterministicallyGenerateVectors(kNumVectors, kDimensions, 5.0);
  std::vector<ValkeyModuleString *> records(kNumVectors);
  for (size_t i = 0; i < kNumVectors; ++i) {
    records[i] = ValkeyModule_CreateString(
        nullptr, (const char *)vectors[i].data(), kDimensions * sizeof(float));
  }

  auto proto = CreateSVSVectorIndexProto(
      kDimensions, data_model::DISTANCE_METRIC_L2, GetParam());
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  for (size_t i = 0; i < kNumVectors; ++i) {
    auto result = testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                 VectorToStr(vectors[i]));
    VMSDK_EXPECT_OK(result);
  }
  ASSERT_EQ(AsBase(*index)->GetLabelCount(), kNumVectors);
  const uint64_t max_label = AsBase(*index)->GetMaxLoadedLabel();

  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&rdb)));
  VMSDK_EXPECT_OK((*index)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));

  ExpectHashGetsForRecords(records);

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded);
  VMSDK_EXPECT_OK(
      (*loaded)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb)));

  // Structural fidelity: the label set and the max-label watermark (which
  // gates inc_id_ on the next HSET, design.md "Labels") match the original.
  EXPECT_EQ(AsBase(*loaded)->GetLabelCount(), kNumVectors);
  EXPECT_EQ(AsBase(*loaded)->GetMaxLoadedLabel(), max_label);

  // The SVS stream itself round-tripped: querying with a saved vector finds
  // it as its own nearest neighbor through the *loaded* SVS index.
  auto query = VectorToStr(vectors[0]);
  auto search_result = (*loaded)->Search(query, 1, CancelNever());
  VMSDK_EXPECT_OK(search_result);
  ASSERT_EQ(search_result->size(), 1u);
  EXPECT_EQ((*search_result)[0].external_id, IndexToKey(0));

  for (auto *record : records) {
    ValkeyModule_FreeString(nullptr, record);
  }
}

INSTANTIATE_TEST_SUITE_P(
    CompressionModes, VectorSVSCompressionTest,
    testing::Values(data_model::SVS_COMPRESSION_NONE,
                    data_model::SVS_COMPRESSION_FP16,
                    data_model::SVS_COMPRESSION_SQ8),
    [](const testing::TestParamInfo<data_model::SVSCompressionType> &info) {
      return data_model::SVSCompressionType_Name(info.param);
    });

// -- Save after deletes -------------------------------------------------------

// Deletes only mark SVS slots, and svs_index_ stays non-null even with no
// live entries, so the save path's consolidate/compact runs over the deleted
// slots. Covers the entry point being deleted and zero survivors.
struct DeleteCase {
  const char *name;
  std::vector<size_t> deleted;
};

class VectorSVSDeleteBeforeSaveTest
    : public VectorSVSTest,
      public testing::WithParamInterface<DeleteCase> {};

TEST_P(VectorSVSDeleteBeforeSaveTest, SaveAndLoadAfterDeletes) {
  const size_t kNumVectors = 10;
  FakeSafeRDB rdb;
  auto vectors =
      DeterministicallyGenerateVectors(kNumVectors + 1, kDimensions, 5.0);
  std::vector<ValkeyModuleString *> records(kNumVectors + 1);
  for (size_t i = 0; i <= kNumVectors; ++i) {
    records[i] = ValkeyModule_CreateString(
        nullptr, (const char *)vectors[i].data(), kDimensions * sizeof(float));
  }

  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  for (size_t i = 0; i < kNumVectors; ++i) {
    VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                   VectorToStr(vectors[i])));
  }
  for (size_t i : GetParam().deleted) {
    VMSDK_EXPECT_OK((*index)->RemoveRecord(IndexToKey(i), DeletionType::kNone));
  }
  const size_t survivors = kNumVectors - GetParam().deleted.size();
  ASSERT_EQ(AsBase(*index)->GetLabelCount(), survivors);

  auto save_status = (*index)->SaveIndex(RDBChunkOutputStream(&rdb));
  ASSERT_TRUE(save_status.ok()) << save_status;
  auto keys_status = (*index)->SaveTrackedKeys(RDBChunkOutputStream(&rdb));
  ASSERT_TRUE(keys_status.ok()) << keys_status;

  ExpectHashGetsForRecords(records);
  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  ASSERT_TRUE(loaded.ok()) << loaded.status();
  auto load_keys_status =
      (*loaded)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb));
  ASSERT_TRUE(load_keys_status.ok()) << load_keys_status;
  EXPECT_EQ(AsBase(*loaded)->GetLabelCount(), survivors);

  // Every survivor is still its own nearest neighbour after the reload.
  for (size_t i = 0; i < kNumVectors; ++i) {
    if (absl::c_linear_search(GetParam().deleted, i)) continue;
    auto search_result =
        (*loaded)->Search(VectorToStr(vectors[i]), 1, CancelNever());
    ASSERT_TRUE(search_result.ok()) << search_result.status();
    ASSERT_EQ(search_result->size(), 1u);
    EXPECT_EQ((*search_result)[0].external_id, IndexToKey(i));
  }

  // The loaded index still accepts writes.
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(
      **loaded, IndexToKey(kNumVectors), VectorToStr(vectors[kNumVectors])));
  EXPECT_EQ(AsBase(*loaded)->GetLabelCount(), survivors + 1);

  for (auto *record : records) {
    ValkeyModule_FreeString(nullptr, record);
  }
}

INSTANTIATE_TEST_SUITE_P(
    DeleteCases, VectorSVSDeleteBeforeSaveTest,
    testing::Values(DeleteCase{"AllDeleted", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
                    DeleteCase{"FirstInsertedDeleted", {0}},
                    DeleteCase{"AllButOneDeleted",
                               {0, 1, 2, 3, 4, 5, 6, 7, 8}}),
    [](const testing::TestParamInfo<DeleteCase> &info) {
      return std::string(info.param.name);
    });

// A foreground SAVE / DEBUG RELOAD runs on the main thread while reader
// threads keep searching; SVS compacts the live index during the save, so
// the two must not overlap. Meaningful under TSAN; a plain build only checks
// that concurrent searches keep returning results.
TEST_F(VectorSVSTest, SaveWhileSearchingIsExclusive) {
  const size_t kNumVectors = 200;
  auto vectors =
      DeterministicallyGenerateVectors(kNumVectors, kDimensions, 5.0);
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  for (size_t i = 0; i < kNumVectors; ++i) {
    VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                   VectorToStr(vectors[i])));
  }
  // Deleted slots give consolidate() and compact() real work to do.
  for (size_t i = 0; i < kNumVectors; i += 4) {
    VMSDK_EXPECT_OK((*index)->RemoveRecord(IndexToKey(i), DeletionType::kNone));
  }

  std::atomic<bool> stop{false};
  std::atomic<size_t> failed_searches{0};
  std::vector<std::thread> readers;
  for (int t = 0; t < 4; ++t) {
    readers.emplace_back([&, t] {
      for (size_t q = t; !stop.load(); q = (q + 1) % kNumVectors) {
        auto result =
            (*index)->Search(VectorToStr(vectors[q]), 5, CancelNever());
        if (!result.ok() || result->empty()) ++failed_searches;
      }
    });
  }
  for (int round = 0; round < 5; ++round) {
    FakeSafeRDB rdb;
    auto status = (*index)->SaveIndex(RDBChunkOutputStream(&rdb));
    EXPECT_TRUE(status.ok()) << status;
  }
  stop = true;
  for (auto &reader : readers) reader.join();
  EXPECT_EQ(failed_searches.load(), 0u);
}

// -- Recall and payload span verification ------------------------------------

constexpr int kRecallTestDimensions = 128;
constexpr size_t kRecallTestVectorCount = 1000;

TEST_F(VectorSVSTest, SaveAndLoadSvsWithRecallAndMultipleChunks) {
  const uint64_t k = 10;
  FakeSafeRDB rdb;
  auto vectors = DeterministicallyGenerateVectors(kRecallTestVectorCount,
                                                  kRecallTestDimensions, 2.2);

  // Load vectors into a Flat index as ground truth
  auto index_flat = VectorFlat<float>::Create(
      CreateFlatVectorIndexProto(kRecallTestDimensions,
                                 data_model::DISTANCE_METRIC_L2,
                                 kRecallTestVectorCount + 100, 250),
      attribute_identifier, attribute_data_type, 0);
  VMSDK_EXPECT_OK(index_flat);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto result = testing_infra::AddVectorRecord(*(*index_flat), IndexToKey(i),
                                                 VectorToStr(vectors[i]));
    VMSDK_EXPECT_OK(result);
  }

  // Create and save SVS index
  auto proto = CreateSVSVectorIndexProto(kRecallTestDimensions,
                                         data_model::DISTANCE_METRIC_L2,
                                         data_model::SVS_COMPRESSION_NONE);
  auto index_svs = VectorSVS<float>::Create(proto, attribute_identifier,
                                            attribute_data_type, 0);
  VMSDK_EXPECT_OK(index_svs);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto result = testing_infra::AddVectorRecord(**index_svs, IndexToKey(i),
                                                 VectorToStr(vectors[i]));
    VMSDK_EXPECT_OK(result);
  }
  VMSDK_EXPECT_OK((*index_svs)->SaveIndex(RDBChunkOutputStream(&rdb)));
  VMSDK_EXPECT_OK((*index_svs)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));

  // Set up mocks for LoadTrackedKeys
  std::vector<ValkeyModuleString *> records(kRecallTestVectorCount);
  for (size_t i = 0; i < kRecallTestVectorCount; ++i) {
    records[i] =
        ValkeyModule_CreateString(nullptr, (const char *)vectors[i].data(),
                                  kRecallTestDimensions * sizeof(float));
  }
  ExpectHashGetsForRecords(records);

  // Load SVS index
  auto loaded_svs = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded_svs);
  VMSDK_EXPECT_OK((*loaded_svs)
                      ->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                        SupplementalContentChunkIter(&rdb)));

  // Verify recall against ground truth is >= 0.9
  auto recall = CalcRecall(index_flat->get(), loaded_svs->get(), k,
                           kRecallTestDimensions, std::nullopt);
  EXPECT_GE(recall, 0.9f);

  // Verify payload spans multiple chunks
  FakeSafeRDB count_rdb;
  VMSDK_EXPECT_OK((*index_svs)->SaveIndex(RDBChunkOutputStream(&count_rdb)));
  size_t payload_chunk_count = 0;
  {
    SupplementalContentChunkIter iter(&count_rdb);
    while (iter.HasNext()) {
      auto chunk = iter.Next();
      ASSERT_TRUE(chunk.ok());
      // Presence of the field, not its length, separates payload from the
      // sentinel; a zero-length chunk is ordinary payload and must be counted.
      if ((*chunk)->has_binary_content()) {
        ++payload_chunk_count;
      }
    }
  }
  EXPECT_GT(payload_chunk_count, 1);

  // Cleanup
  for (auto *record : records) {
    ValkeyModule_FreeString(nullptr, record);
  }
}

// -- Header validation --------------------------------------------------------
//
// LoadFromRDB hard-fails on five header/proto disagreements (OVERVIEW.md
// "Round 2"): format_version, svs_version, build_config, dimensionality, and
// element_type. Each case below builds one header that is otherwise valid
// and corrupts exactly one field, so a case can only pass by tripping the
// gate it names, not some earlier one.

// Known-good value for a header that must pass the format_version gate so a
// later gate in the same header can be exercised instead. Mirrors
// kSvsHeaderFormatVersion (vector_svs.cc, private to that file); update this
// if that constant ever bumps.
constexpr uint32_t kKnownGoodFormatVersion = 1;

// Builds a header that matches `proto` on every field the corrupt-* mutators
// below don't touch, with has_index=false so no real SVS stream is needed.
data_model::SVSIndexHeader ValidHeaderFor(
    const data_model::VectorIndex &proto) {
  data_model::SVSIndexHeader header;
  header.set_format_version(kKnownGoodFormatVersion);
  header.set_svs_version(svs_get_version());
  header.set_has_index(false);
  header.set_element_type(data_model::VECTOR_DATA_TYPE_FLOAT32);
  header.set_dimensionality(kDimensions);
  *header.mutable_build_config() = proto.svs_vamana_algorithm();
  return header;
}

struct HeaderGateCase {
  std::string name;
  void (*corrupt)(data_model::SVSIndexHeader &header);
  std::string expected_substring;
};

const HeaderGateCase kHeaderGateCases[] = {
    {"FormatVersion",
     [](data_model::SVSIndexHeader &h) {
       // Any value the real constant will never legitimately take proves
       // the gate without exporting kSvsHeaderFormatVersion for this test.
       h.set_format_version(0xFFFFFFFFu);
     },
     "format_version mismatch"},
    {"SvsVersion",
     [](data_model::SVSIndexHeader &h) {
       h.set_svs_version(h.svs_version() + 1);
     },
     "svs_version mismatch"},
    {"BuildConfig",
     [](data_model::SVSIndexHeader &h) {
       // design.md "Header": compression is the one build-config field that
       // genuinely has to match the payload, so corrupt that one.
       h.mutable_build_config()->set_compression(
           data_model::SVS_COMPRESSION_FP16);
     },
     "build_config does not match the index definition"},
    {"Dimensionality",
     [](data_model::SVSIndexHeader &h) {
       h.set_dimensionality(h.dimensionality() + 1);
     },
     "dimensionality mismatch"},
    {"ElementType",
     [](data_model::SVSIndexHeader &h) {
       h.set_element_type(data_model::VECTOR_DATA_TYPE_FLOAT16);
     },
     "element_type mismatch"},
};

class VectorSVSHeaderGateTest
    : public VectorSVSTest,
      public testing::WithParamInterface<HeaderGateCase> {};

TEST_P(VectorSVSHeaderGateTest, LoadFromRdbRejectsCorruptedField) {
  FakeSafeRDB rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);

  data_model::SVSIndexHeader header = ValidHeaderFor(proto);
  GetParam().corrupt(header);
  std::string serialized;
  ASSERT_TRUE(header.SerializeToString(&serialized));

  {
    RDBChunkOutputStream out(&rdb);
    VMSDK_EXPECT_OK(out.SaveString(serialized));
    VMSDK_EXPECT_OK(out.Close());
  }

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  EXPECT_EQ(loaded.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(loaded.status().message(),
              testing::HasSubstr(GetParam().expected_substring));
}

INSTANTIATE_TEST_SUITE_P(
    HeaderGates, VectorSVSHeaderGateTest, testing::ValuesIn(kHeaderGateCases),
    [](const testing::TestParamInfo<HeaderGateCase> &info) {
      return info.param.name;
    });

TEST_F(VectorSVSTest, LoadFromRdbRejectsCorruptHeader) {
  FakeSafeRDB rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  // An unterminated varint: a byte with the continuation bit set followed
  // by nothing valid to continue it. Guaranteed to fail ParseFromString.
  std::string garbage(10, '\xFF');
  {
    RDBChunkOutputStream out(&rdb);
    VMSDK_EXPECT_OK(out.SaveString(garbage));
    VMSDK_EXPECT_OK(out.Close());
  }

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  EXPECT_EQ(loaded.status().code(), absl::StatusCode::kInternal);
  EXPECT_THAT(loaded.status().message(),
              testing::HasSubstr("Could not deserialize SVS index header"));
}

// -- The terminator invariant -------------------------------------------------

TEST_F(VectorSVSTest, LoadFromRdbRejectsPayloadChunksRemainingAfterSvsStops) {
  FakeSafeRDB original_rdb;
  auto vectors = DeterministicallyGenerateVectors(5, kDimensions, 3.0);
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto result = testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                 VectorToStr(vectors[i]));
    VMSDK_EXPECT_OK(result);
  }
  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&original_rdb)));

  // Copy every real chunk (header plus SVS stream payload, stopping before
  // the sentinel) into a new RDB, then splice in one extra payload chunk
  // before the sentinel. SVS stops reading the instant its last component
  // is complete (design.md "The terminator invariant") and never asks for
  // the spliced-in chunk, so it is left unconsumed.
  FakeSafeRDB spliced_rdb;
  {
    SupplementalContentChunkIter in_iter(&original_rdb);
    RDBChunkOutputStream out(&spliced_rdb);
    while (in_iter.HasNext()) {
      auto chunk = in_iter.Next();
      ASSERT_TRUE(chunk.ok());
      VMSDK_EXPECT_OK(out.SaveString((*chunk)->binary_content()));
    }
    VMSDK_EXPECT_OK(out.SaveString("extra-unread-chunk"));
    VMSDK_EXPECT_OK(out.Close());
  }

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&spliced_rdb), 0);
  EXPECT_EQ(loaded.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(loaded.status().message(),
              testing::HasSubstr("payload chunks remain"));
}

TEST_F(VectorSVSTest, LoadFromRdbRejectsTrailingChunksForEmptyIndex) {
  FakeSafeRDB original_rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&original_rdb)));

  // An empty save writes exactly one real chunk, the header, so the iterator's
  // one-chunk lookahead is already done once the header is handed out.
  FakeSafeRDB spliced_rdb;
  {
    SupplementalContentChunkIter in_iter(&original_rdb);
    auto header = in_iter.Next();
    VMSDK_EXPECT_OK(header);
    EXPECT_FALSE(in_iter.HasNext());
    RDBChunkOutputStream out(&spliced_rdb);
    VMSDK_EXPECT_OK(out.SaveString((*header)->binary_content()));
    VMSDK_EXPECT_OK(out.SaveString("extra-unread-chunk"));
    VMSDK_EXPECT_OK(out.Close());
  }

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&spliced_rdb), 0);
  EXPECT_EQ(loaded.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_THAT(loaded.status().message(),
              testing::HasSubstr("payload chunks remain"));
}

TEST_F(VectorSVSTest, LoadFromRdbRejectsPayloadTruncatedBeforeSvsIsSatisfied) {
  FakeSafeRDB original_rdb;
  auto vectors = DeterministicallyGenerateVectors(5, kDimensions, 3.0);
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto result = testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                 VectorToStr(vectors[i]));
    VMSDK_EXPECT_OK(result);
  }
  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&original_rdb)));

  // Drop the tail of the last graph payload behind a clean terminator: only SVS
  // can tell the stream is short, and before failbit it loaded stale rows.
  FakeSafeRDB truncated_rdb;
  {
    SupplementalContentChunkIter in_iter(&original_rdb);
    RDBChunkOutputStream out(&truncated_rdb);
    auto header = in_iter.Next();
    ASSERT_TRUE(header.ok());
    VMSDK_EXPECT_OK(out.SaveString((*header)->binary_content()));
    std::string payload;
    while (in_iter.HasNext()) {
      auto chunk = in_iter.Next();
      ASSERT_TRUE(chunk.ok());
      payload += (*chunk)->binary_content();
    }
    VMSDK_EXPECT_OK(out.SaveString(payload.substr(0, payload.size() - 16)));
    VMSDK_EXPECT_OK(out.Close());
  }

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&truncated_rdb), 0);
  EXPECT_FALSE(loaded.ok());
}

// -- Duplicate-label metric ---------------------------------------------------

TEST_F(VectorSVSTest, DuplicateLabelOnLoadIncrementsMetric) {
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);

  // Two distinct tracked keys sharing one internal_id: not reachable through
  // the real SaveTrackedKeys path, but exactly what a duplicate label on
  // load looks like to GetOrCreateVectorLockFree, which is what the metric
  // (src/metrics.h:89) actually fires on (design.md "Corrected in round 2").
  auto vectors = DeterministicallyGenerateVectors(2, kDimensions, 1.0);
  std::vector<ValkeyModuleString *> records(2);
  for (int i = 0; i < 2; ++i) {
    records[i] = ValkeyModule_CreateString(
        nullptr, (const char *)vectors[i].data(), kDimensions * sizeof(float));
  }
  ExpectHashGetsForRecords(records);

  FakeSafeRDB rdb;
  {
    RDBChunkOutputStream out(&rdb);
    for (int i = 0; i < 2; ++i) {
      data_model::TrackedKeyMetadata metadata;
      metadata.set_key(absl::StrCat(i, "_key"));
      metadata.set_internal_id(1);  // same label for both keys
      VMSDK_EXPECT_OK(out.SaveString(metadata.SerializeAsString()));
    }
    VMSDK_EXPECT_OK(out.Close());
  }

  uint64_t baseline = Metrics::GetStats().svs_duplicate_label_on_load_cnt;
  VMSDK_EXPECT_OK(
      (*index)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                SupplementalContentChunkIter(&rdb)));
  EXPECT_EQ(Metrics::GetStats().svs_duplicate_label_on_load_cnt - baseline, 1u);
  // Only one slot exists: the duplicate never got its own map entry.
  EXPECT_EQ(AsBase(*index)->GetLabelCount(), 1u);

  for (auto *record : records) {
    ValkeyModule_FreeString(nullptr, record);
  }
}

// -- Custom SVS allocator parity between build and stream load --------------

std::atomic<int64_t> g_external_memory_net_bytes{0};

int StubIncrExternalMemory(size_t bytes) {
  g_external_memory_net_bytes.fetch_add(static_cast<int64_t>(bytes));
  return VALKEYMODULE_OK;
}

int StubDecrExternalMemory(size_t bytes) {
  g_external_memory_net_bytes.fetch_sub(static_cast<int64_t>(bytes));
  return VALKEYMODULE_OK;
}

// The hooks are process-global; leaving them set silently switches every later
// SVS test in the binary onto the mmap allocator.
class ScopedExternalMemoryCounter {
 public:
  ScopedExternalMemoryCounter()
      : saved_incr_(ValkeyModule_IncrExternalMemory),
        saved_decr_(ValkeyModule_DecrExternalMemory) {
    ValkeyModule_IncrExternalMemory = StubIncrExternalMemory;
    ValkeyModule_DecrExternalMemory = StubDecrExternalMemory;
    g_external_memory_net_bytes.store(0);
  }
  ScopedExternalMemoryCounter(const ScopedExternalMemoryCounter &) = delete;
  ScopedExternalMemoryCounter &operator=(const ScopedExternalMemoryCounter &) =
      delete;
  ~ScopedExternalMemoryCounter() {
    ValkeyModule_IncrExternalMemory = saved_incr_;
    ValkeyModule_DecrExternalMemory = saved_decr_;
  }

  int64_t net_bytes() const { return g_external_memory_net_bytes.load(); }

 private:
  int (*saved_incr_)(size_t);
  int (*saved_decr_)(size_t);
};

TEST_F(VectorSVSTest, StreamLoadAllocatesSameBytesAsBuild) {
  ScopedExternalMemoryCounter guard;

  constexpr int kAllocDimensions = 4;
  constexpr size_t kAllocVectorCount = 1000;
  FakeSafeRDB rdb;
  auto vectors = DeterministicallyGenerateVectors(kAllocVectorCount,
                                                  kAllocDimensions, 2.2);
  std::vector<ValkeyModuleString *> records(kAllocVectorCount);
  for (size_t i = 0; i < kAllocVectorCount; ++i) {
    records[i] =
        ValkeyModule_CreateString(nullptr, (const char *)vectors[i].data(),
                                  kAllocDimensions * sizeof(float));
  }

  auto proto = CreateSVSVectorIndexProto(kAllocDimensions,
                                         data_model::DISTANCE_METRIC_L2,
                                         data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  for (size_t i = 0; i < kAllocVectorCount; ++i) {
    auto result = testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                                 VectorToStr(vectors[i]));
    VMSDK_EXPECT_OK(result);
  }

  const int64_t built = guard.net_bytes();
  ASSERT_GT(built, 0) << "custom allocator was not installed on the builder; "
                         "check the ValkeyModule_Incr/DecrExternalMemory "
                         "gate in vector_svs.cc";

  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&rdb)));
  VMSDK_EXPECT_OK((*index)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));

  index->reset();
  ASSERT_EQ(guard.net_bytes(), 0)
      << "deallocating the built index did not return net bytes to zero; "
         "build and destroy are asymmetric on the custom allocator";

  ExpectHashGetsForRecords(records);

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded);
  VMSDK_EXPECT_OK(
      (*loaded)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb)));

  const int64_t loaded_bytes = guard.net_bytes();
  EXPECT_EQ(loaded_bytes, built)
      << "built=" << built << " loaded=" << loaded_bytes
      << " delta=" << (built - loaded_bytes);

  for (auto *record : records) {
    ValkeyModule_FreeString(nullptr, record);
  }
}

// -- Restored index vs built index: who guards concurrent writers -----------

svs_sync_kind_t SyncKindOf(svs_index_h handle) {
  svs_error_h err = svs_error_create();
  svs_sync_kind_t kind = SVS_SYNC_KIND_NONE;
  EXPECT_TRUE(svs_index_dynamic_get_sync_kind(handle, &kind, err))
      << svs_error_get_message(err);
  svs_error_free(err);
  return kind;
}

// A built index guards itself (FINE_GRAIN) and mutates under the shared
// lock. A restored one reports whatever the stream load gave it; the module
// keeps its mutations exclusive exactly when that is SVS_SYNC_KIND_NONE.
TEST_F(VectorSVSTest, RestoredIndexMutationsMatchItsSyncKind) {
  FakeSafeRDB rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  auto vectors = DeterministicallyGenerateVectors(1, kDimensions, 1.0);
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(0),
                                                 VectorToStr(vectors[0])));
  svs_index_h built = VectorSVSTestPeer::Handle(**index);
  ASSERT_NE(built, nullptr);
  EXPECT_EQ(SyncKindOf(built), SVS_SYNC_KIND_FINE_GRAIN);
  EXPECT_FALSE(VectorSVSTestPeer::MutationsExclusive(**index));

  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&rdb)));
  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded);
  svs_index_h restored = VectorSVSTestPeer::Handle(**loaded);
  ASSERT_NE(restored, nullptr);
  EXPECT_EQ(VectorSVSTestPeer::MutationsExclusive(**loaded),
            SyncKindOf(restored) == SVS_SYNC_KIND_NONE)
      << "an index SVS does not guard must mutate under the exclusive lock";
}

// -- Concurrent writers on a loaded index (as #19 does on a built one) -------

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

constexpr int kLoadWriters = 8;
constexpr int kLoadPerWriter = 50;
constexpr int kLoadConcurrentVectors = kLoadWriters * kLoadPerWriter;

TEST_F(VectorSVSTest, EightWritersAddConcurrentlyAfterLoad) {
  // Save an index holding key 0 (internal id 0), restore it the way the
  // server does (LoadFromRDB + LoadTrackedKeys, so inc_id_ resumes at 1),
  // then add 400 vectors from 8 threads; every id must be its own nearest.
  // Searches run after the writers: in the server the index-schema phase
  // gate keeps searches and mutations apart, and the base class reads its
  // label map lock-free on that basis.
  FakeSafeRDB rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(index);
  auto vectors = DeterministicallyGenerateVectors(kLoadConcurrentVectors + 1,
                                                  kDimensions, 10.0);
  std::vector<ValkeyModuleString *> records(1);
  records[0] = ValkeyModule_CreateString(
      nullptr, (const char *)vectors[0].data(), kDimensions * sizeof(float));
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(0),
                                                 VectorToStr(vectors[0])));
  VMSDK_EXPECT_OK((*index)->SaveIndex(RDBChunkOutputStream(&rdb)));
  VMSDK_EXPECT_OK((*index)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
  ExpectHashGetsForRecords(records);

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded);
  VMSDK_EXPECT_OK(
      (*loaded)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb)));
  ASSERT_EQ(AsBase(*loaded)->GetLabelCount(), 1u);

  std::vector<PreparedRecord> prepared;
  prepared.reserve(kLoadConcurrentVectors + 1);
  prepared.emplace_back();  // slot 0 unused: key 0 is the restored vector
  for (int i = 1; i <= kLoadConcurrentVectors; ++i) {
    prepared.push_back(PrepareRecord(**loaded, i, vectors[i]));
  }
  auto statuses = RunWriters(kLoadWriters, [&](int w) {
    for (int i = 1 + w * kLoadPerWriter; i <= (w + 1) * kLoadPerWriter; ++i) {
      auto result =
          (*loaded)->AddRecord(prepared[i].key, std::move(prepared[i].data));
      if (!result.ok()) return result.status();
    }
    return absl::OkStatus();
  });
  for (const auto &status : statuses) EXPECT_TRUE(status.ok()) << status;

  EXPECT_EQ(AsBase(*loaded)->GetLabelCount(),
            static_cast<size_t>(kLoadConcurrentVectors + 1));
  for (int i = 0; i <= kLoadConcurrentVectors; ++i) {
    auto result =
        (*loaded)->Search(VectorToStr(vectors[i]), 1, CancelNever(), nullptr,
                          /*ef_runtime=*/kLoadConcurrentVectors + 1);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(result->size(), 1u) << "id " << i;
    EXPECT_EQ((*result)[0].external_id, IndexToKey(i)) << "id " << i;
  }
  ValkeyModule_FreeString(nullptr, records[0]);
}

// A foreground SAVE runs on the main thread while writer threads mutate, and
// writers update the label map outside resize_mutex_. Each case parks a writer
// where the map and SVS disagree on whether any entry is live, then saves:
// the save must succeed and its RDB must load back.
struct SaveRaceCase {
  const char *name;
  const char *pause_point;
  bool remove;  // true: park a remove after its SVS delete; false: an add
};

class VectorSVSSaveRaceTest : public VectorSVSTest,
                              public testing::WithParamInterface<SaveRaceCase> {
};

TEST_P(VectorSVSSaveRaceTest, SaveWhileWriterIsBetweenMapAndSvs) {
  const char *pause_point = GetParam().pause_point;
  const bool remove_case = GetParam().remove;
  auto vectors = DeterministicallyGenerateVectors(2, kDimensions, 5.0);
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto index = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  ASSERT_TRUE(index.ok()) << index.status();
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**index, IndexToKey(0),
                                                 VectorToStr(vectors[0])));
  if (!remove_case) {
    // Leave an SVS handle with no live entries for the add to land in.
    VMSDK_EXPECT_OK((*index)->RemoveRecord(IndexToKey(0), DeletionType::kNone));
  }

  // The vector record is built on the main thread, as the writer pool's
  // callers do; only the mutation runs on the writer thread.
  PreparedRecord to_add = PrepareRecord(**index, 1, vectors[1]);
  vmsdk::debug::PausePointControl(pause_point, true);
  absl::Status writer_status;
  std::thread writer([&] {
    if (remove_case) {
      writer_status =
          (*index)->RemoveRecord(IndexToKey(0), DeletionType::kNone).status();
    } else {
      writer_status =
          (*index)->AddRecord(to_add.key, std::move(to_add.data)).status();
    }
  });
  const absl::Time deadline = absl::Now() + absl::Seconds(10);
  while (vmsdk::debug::PausePointWaiters(pause_point).value_or(0) == 0 &&
         absl::Now() < deadline) {
    absl::SleepFor(absl::Milliseconds(1));
  }
  const bool parked =
      vmsdk::debug::PausePointWaiters(pause_point).value_or(0) == 1;

  FakeSafeRDB rdb;
  auto save_status = (*index)->SaveIndex(RDBChunkOutputStream(&rdb));
  vmsdk::debug::PausePointControl(pause_point, false);
  writer.join();

  ASSERT_TRUE(parked) << "writer never reached " << pause_point;
  EXPECT_TRUE(writer_status.ok()) << writer_status;
  ASSERT_TRUE(save_status.ok()) << save_status;
  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  EXPECT_TRUE(loaded.ok()) << loaded.status();
}

INSTANTIATE_TEST_SUITE_P(
    SaveRaceCases, VectorSVSSaveRaceTest,
    testing::Values(SaveRaceCase{"RemoveOfLastEntry",
                                 "svs_remove_after_svs_delete",
                                 /*remove=*/true},
                    SaveRaceCase{"AddIntoEmptyHandle",
                                 "svs_add_after_map_reserve",
                                 /*remove=*/false}),
    [](const testing::TestParamInfo<SaveRaceCase> &info) {
      return std::string(info.param.name);
    });

// Observes the lock mode rather than the flag: with the test thread holding
// the shared side of resize_mutex_, an add on a built index proceeds (it
// takes the shared side too) while an add on a restored index waits for the
// exclusive side until the test lets go.
void HoldSharedWhile(absl::Mutex &mu, const std::function<void()> &body)
    ABSL_NO_THREAD_SAFETY_ANALYSIS {
  mu.ReaderLock();
  body();
  mu.ReaderUnlock();
}

TEST_F(VectorSVSTest, RestoredIndexMutatesUnderTheExclusiveLock) {
  FakeSafeRDB rdb;
  auto proto =
      CreateSVSVectorIndexProto(kDimensions, data_model::DISTANCE_METRIC_L2,
                                data_model::SVS_COMPRESSION_NONE);
  auto built = VectorSVS<float>::Create(proto, attribute_identifier,
                                        attribute_data_type, 0);
  VMSDK_EXPECT_OK(built);
  auto vectors = DeterministicallyGenerateVectors(3, kDimensions, 2.0);
  std::vector<ValkeyModuleString *> records(1);
  records[0] = ValkeyModule_CreateString(
      nullptr, (const char *)vectors[0].data(), kDimensions * sizeof(float));
  VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**built, IndexToKey(0),
                                                 VectorToStr(vectors[0])));
  VMSDK_EXPECT_OK((*built)->SaveIndex(RDBChunkOutputStream(&rdb)));
  VMSDK_EXPECT_OK((*built)->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
  ExpectHashGetsForRecords(records);
  auto restored = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_, proto, "attribute_identifier_2",
      SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(restored);
  VMSDK_EXPECT_OK(
      (*restored)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                   SupplementalContentChunkIter(&rdb)));

  // The writer is parked at svs_add_after_map_reserve, just before it takes
  // resize_mutex_, so it is demonstrably running when the test takes the
  // shared side and lets it go. `wait` bounds the add from there: generous
  // where it must finish (a slow runner is not a failure), short where it
  // must not (the writer is only ever blocked, never slow).
  constexpr absl::string_view kPark = "svs_add_after_map_reserve";
  auto add_while_shared_held = [&](VectorSVS<float> &index, int id,
                                   absl::Duration wait) {
    PreparedRecord record = PrepareRecord(index, id, vectors[id]);
    absl::Notification done;
    absl::Status status;
    bool finished_while_held = false;
    vmsdk::debug::PausePointControl(kPark, true);
    std::thread writer([&] {
      status = index.AddRecord(record.key, std::move(record.data)).status();
      done.Notify();
    });
    const absl::Time deadline = absl::Now() + absl::Seconds(10);
    while (vmsdk::debug::PausePointWaiters(kPark).value_or(0) == 0 &&
           absl::Now() < deadline) {
      absl::SleepFor(absl::Milliseconds(1));
    }
    EXPECT_EQ(vmsdk::debug::PausePointWaiters(kPark).value_or(0), 1u)
        << "writer never reached the pause point";
    HoldSharedWhile(VectorSVSTestPeer::ResizeMutex(index), [&] {
      vmsdk::debug::PausePointControl(kPark, false);  // release the writer
      finished_while_held = done.WaitForNotificationWithTimeout(wait);
    });
    writer.join();
    VMSDK_EXPECT_OK(status);
    return finished_while_held;
  };
  EXPECT_TRUE(add_while_shared_held(**built, 1, absl::Seconds(10)))
      << "a built index mutates under the shared lock";
  EXPECT_FALSE(add_while_shared_held(**restored, 1, absl::Milliseconds(300)))
      << "a restored index must wait for the exclusive lock";
  EXPECT_EQ(AsBase(*restored)->GetLabelCount(), 2u);
  ValkeyModule_FreeString(nullptr, records[0]);
}

// -- IP / COSINE scores -------------------------------------------------------

// SVS returns dot product as a similarity; __v_score must be the distance
// 1 - dot on every path: SVS's search with and without a filter, and the
// pre-filter path that scores records outside SVS (RecomputeDistance).
constexpr int kMetricDimensions = 4;

// Five documents whose expected scores against MetricTestQuery are
// {1.0, 0.2, 0.0, 0.4, 1.0}. IP uses unit vectors. COSINE scales them, so
// a missing normalization shows up as a wrong score, and makes the last
// document a zero vector, which must score 1.0 rather than NaN.
std::vector<std::vector<float>> MetricTestVectors(
    data_model::DistanceMetric metric) {
  if (metric == data_model::DISTANCE_METRIC_IP) {
    return {{0, 0, 1, 0},
            {0.8, 0.6, 0, 0},
            {1, 0, 0, 0},
            {0.6, 0.8, 0, 0},
            {0, 0, 0, 1}};
  }
  return {
      {0, 0, 5, 0}, {4, 3, 0, 0}, {2, 0, 0, 0}, {0.3, 0.4, 0, 0}, {0, 0, 0, 0}};
}

std::vector<float> MetricTestQuery(data_model::DistanceMetric metric) {
  if (metric == data_model::DISTANCE_METRIC_IP) {
    return {1, 0, 0, 0};
  }
  return {3, 0, 0, 0};
}

const std::vector<float> kMetricExpectedScores = {1.0f, 0.2f, 0.0f, 0.4f, 1.0f};

struct MetricCase {
  const char *name;
  data_model::DistanceMetric metric;
  data_model::SVSCompressionType compression;
  float tolerance;
};

class VectorSVSMetricTest : public VectorSVSTest,
                            public testing::WithParamInterface<MetricCase> {
 protected:
  std::shared_ptr<VectorSVS<float>> CreatePopulatedSvs() {
    const auto &param = GetParam();
    auto index = VectorSVS<float>::Create(
        CreateSVSVectorIndexProto(kMetricDimensions, param.metric,
                                  param.compression),
        attribute_identifier, attribute_data_type, 0);
    CHECK(index.ok()) << index.status();
    auto vectors = MetricTestVectors(param.metric);
    for (size_t i = 0; i < vectors.size(); ++i) {
      CHECK(testing_infra::AddVectorRecord(**index, IndexToKey(i),
                                           VectorToStr(vectors[i]))
                .ok());
    }
    return *index;
  }

  // Index of the test document a search result refers to.
  static size_t DocIndex(const Neighbor &neighbor) {
    for (size_t i = 0; i < kMetricExpectedScores.size(); ++i) {
      if (neighbor.external_id == IndexToKey(i)) return i;
    }
    ADD_FAILURE() << "unexpected key " << neighbor.external_id->Str();
    return 0;
  }
};

TEST_P(VectorSVSMetricTest, ScoresAreDistancesOnEveryPath) {
  const auto &param = GetParam();
  auto svs = CreatePopulatedSvs();
  auto flat = VectorFlat<float>::Create(
      CreateFlatVectorIndexProto(kMetricDimensions, param.metric,
                                 /*initial_cap=*/10, /*block_size=*/10),
      "attribute_identifier_2", attribute_data_type, 0);
  VMSDK_EXPECT_OK(flat);
  auto vectors = MetricTestVectors(param.metric);
  for (size_t i = 0; i < vectors.size(); ++i) {
    VMSDK_EXPECT_OK(testing_infra::AddVectorRecord(**flat, IndexToKey(i),
                                                   VectorToStr(vectors[i])));
  }
  const auto query_vector = MetricTestQuery(param.metric);
  const absl::string_view query = VectorToStr(query_vector);

  auto result = svs->Search(query, vectors.size(), CancelNever());
  VMSDK_EXPECT_OK(result);
  ASSERT_EQ(result->size(), vectors.size());
  auto flat_result = (*flat)->Search(query, vectors.size(), CancelNever());
  VMSDK_EXPECT_OK(flat_result);
  ASSERT_EQ(flat_result->size(), vectors.size());
  std::vector<float> flat_scores(vectors.size());
  for (const auto &neighbor : *flat_result) {
    flat_scores[DocIndex(neighbor)] = neighbor.distance;
  }

  // The exact match comes first, and scores ascend from there.
  EXPECT_EQ((*result)[0].external_id, IndexToKey(2));
  float previous = 0.0f;
  for (const auto &neighbor : *result) {
    const size_t doc = DocIndex(neighbor);
    EXPECT_NEAR(neighbor.distance, kMetricExpectedScores[doc], param.tolerance)
        << "doc " << doc;
    EXPECT_NEAR(neighbor.distance, flat_scores[doc], param.tolerance)
        << "doc " << doc;
    auto recomputed =
        AsBase(svs)->RecomputeDistance(VectorToStr(vectors[doc]), query);
    VMSDK_EXPECT_OK(recomputed);
    EXPECT_NEAR(neighbor.distance, *recomputed, param.tolerance)
        << "doc " << doc;
    EXPECT_GE(neighbor.distance, previous) << "doc " << doc;
    previous = neighbor.distance;
  }

  // A filter routes the query through SVS's filtered search, which must
  // report the same scores.
  auto filtered = svs->Search(query, vectors.size(), CancelNever(),
                              std::make_unique<hnswlib::BaseFilterFunctor>());
  VMSDK_EXPECT_OK(filtered);
  ASSERT_EQ(filtered->size(), result->size());
  for (size_t i = 0; i < result->size(); ++i) {
    EXPECT_FLOAT_EQ((*filtered)[i].distance, (*result)[i].distance)
        << "rank " << i;
  }
}

// Radius 0.3 holds docs 1 (0.2) and 2 (0.0). Under the default cap the SVS
// fetch is not full, so the radius filters it; with a cap of 1 the fetch is
// full and still in range, so the exhaustive scan answers. Both must agree.
TEST_P(VectorSVSMetricTest, SearchRangeKeepsDocsWithinRadius) {
  const auto &param = GetParam();
  auto svs = CreatePopulatedSvs();
  const auto query_vector = MetricTestQuery(param.metric);
  const absl::string_view query = VectorToStr(query_vector);
  auto &fetch_cap = options::GetMaxNonVectorSearchResultsFetched();
  const auto saved_cap = fetch_cap.GetValue();
  absl::Cleanup restore = [&] {
    VMSDK_EXPECT_OK(fetch_cap.SetValue(saved_cap));
  };

  for (const long long cap : {saved_cap, 1LL}) {
    VMSDK_EXPECT_OK(fetch_cap.SetValue(cap));
    auto result = svs->SearchRange(query, 0.3f, CancelNever());
    VMSDK_EXPECT_OK(result);
    std::vector<size_t> docs;
    for (const auto &neighbor : *result) {
      const size_t doc = DocIndex(neighbor);
      EXPECT_NEAR(neighbor.distance, kMetricExpectedScores[doc],
                  param.tolerance)
          << "cap " << cap << " doc " << doc;
      docs.push_back(doc);
    }
    EXPECT_THAT(docs, testing::UnorderedElementsAre(1, 2)) << "cap " << cap;
  }
}

TEST_P(VectorSVSMetricTest, ZeroQueryScoresOne) {
  const auto &param = GetParam();
  auto svs = CreatePopulatedSvs();
  auto result = svs->Search(VectorToStr(std::vector<float>(kMetricDimensions)),
                            kMetricExpectedScores.size(), CancelNever());
  VMSDK_EXPECT_OK(result);
  ASSERT_EQ(result->size(), kMetricExpectedScores.size());
  for (const auto &neighbor : *result) {
    EXPECT_NEAR(neighbor.distance, 1.0f, param.tolerance)
        << "doc " << DocIndex(neighbor);
  }
}

TEST_P(VectorSVSMetricTest, RoundTripKeepsScores) {
  const auto &param = GetParam();
  auto svs = CreatePopulatedSvs();
  auto vectors = MetricTestVectors(param.metric);
  std::vector<ValkeyModuleString *> records(vectors.size());
  for (size_t i = 0; i < vectors.size(); ++i) {
    records[i] =
        ValkeyModule_CreateString(nullptr, (const char *)vectors[i].data(),
                                  kMetricDimensions * sizeof(float));
  }
  FakeSafeRDB rdb;
  VMSDK_EXPECT_OK(svs->SaveIndex(RDBChunkOutputStream(&rdb)));
  VMSDK_EXPECT_OK(svs->SaveTrackedKeys(RDBChunkOutputStream(&rdb)));
  ExpectHashGetsForRecords(records);

  auto loaded = VectorSVS<float>::LoadFromRDB(
      &fake_ctx_, &hash_attribute_data_type_,
      CreateSVSVectorIndexProto(kMetricDimensions, param.metric,
                                param.compression),
      "attribute_identifier_2", SupplementalContentChunkIter(&rdb), 0);
  VMSDK_EXPECT_OK(loaded);
  VMSDK_EXPECT_OK(
      (*loaded)->LoadTrackedKeys(&fake_ctx_, &hash_attribute_data_type_,
                                 SupplementalContentChunkIter(&rdb)));

  const auto query_vector = MetricTestQuery(param.metric);
  const absl::string_view query = VectorToStr(query_vector);
  auto result = (*loaded)->Search(query, vectors.size(), CancelNever());
  VMSDK_EXPECT_OK(result);
  ASSERT_EQ(result->size(), vectors.size());
  EXPECT_EQ((*result)[0].external_id, IndexToKey(2));
  for (const auto &neighbor : *result) {
    const size_t doc = DocIndex(neighbor);
    EXPECT_NEAR(neighbor.distance, kMetricExpectedScores[doc], param.tolerance)
        << "doc " << doc;
  }

  for (auto *record : records) {
    ValkeyModule_FreeString(nullptr, record);
  }
}

INSTANTIATE_TEST_SUITE_P(
    Metrics, VectorSVSMetricTest,
    testing::Values(MetricCase{"IP_NONE", data_model::DISTANCE_METRIC_IP,
                               data_model::SVS_COMPRESSION_NONE, 1e-5f},
                    MetricCase{"IP_FP16", data_model::DISTANCE_METRIC_IP,
                               data_model::SVS_COMPRESSION_FP16, 2e-3f},
                    MetricCase{"COSINE_NONE",
                               data_model::DISTANCE_METRIC_COSINE,
                               data_model::SVS_COMPRESSION_NONE, 1e-5f},
                    MetricCase{"COSINE_FP16",
                               data_model::DISTANCE_METRIC_COSINE,
                               data_model::SVS_COMPRESSION_FP16, 2e-3f}),
    [](const testing::TestParamInfo<MetricCase> &info) {
      return info.param.name;
    });

}  // namespace
}  // namespace valkey_search::indexes
