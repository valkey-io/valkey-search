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
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
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
#include "svs/c/svs_c.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes {
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

}  // namespace
}  // namespace valkey_search::indexes
