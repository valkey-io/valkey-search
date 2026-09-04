/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

// Exercises the main-thread state machine that keeps the VectorRegistry in
// sync with the keys in the database: IndexSchema::ProcessKeyspaceNotification
// -> TrackRecord -> VectorRegistry::Track / erase / ShareWithValkeyHash, and
// the untrack side (VectorRegistry::UntrackIfUnused ->
// DetachFromValkeyHash).
//
// Unlike vector_registry_test.cc -- which calls VectorRegistry::Track()
// directly and therefore never sees the state machine that decides *whether*
// to call it -- every case here drives a real keyspace notification against a
// fake keyspace, and asserts an observable effect: the registry contents, the
// registry statistics, the state of the index, and (when sharing is on) the
// actual byte pointer that was handed to the engine via HashSetStringRef.
//
// The matrix is: {HASH, JSON} x {vector-only schema, vector+numeric+tag
// schema} x {sharing on, sharing off}, and for each combination every
// transition of the per-field state {valid, invalid, absent} on both key
// creation and key overwrite, plus whole-key deletion.

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/attribute_data_type.h"
#include "src/indexes/numeric.h"
#include "src/indexes/tag.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_hnsw.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "src/vector_registry.h"
#include "testing/common.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

using ::testing::_;
using ::testing::An;
using ::testing::TypedEq;

// Per-field state that the state machine has to cope with.
enum class FieldState : std::uint8_t {
  kValid,    // present, parses/sizes correctly
  kInvalid,  // present, but not usable for this index type
  kAbsent,   // not present on the key at all
};

struct NonVectorFields {
  FieldState numeric{FieldState::kValid};
  FieldState tag{FieldState::kValid};
};

struct StateMachineTestCase {
  bool json;          // JSON attribute data type instead of HASH
  bool mixed_schema;  // vector + numeric + tag instead of vector only
  bool sharing;       // search.enable-vector-sharing
  std::string name;
};

// A shared reference handed to the engine by ValkeyModule_HashSetStringRef.
struct SharedRef {
  const char *buf;
  size_t len;
};

constexpr int kDims = 4;
constexpr size_t kVectorBytes = kDims * sizeof(float);
constexpr absl::string_view kVectorAlias = "vec";
constexpr absl::string_view kNumericAlias = "num";
constexpr absl::string_view kTagAlias = "tag";
constexpr absl::string_view kKeyPrefix = "prefix:";

std::string VectorAtScale(float scale) {
  float v[kDims];
  for (int i = 0; i < kDims; ++i) {
    v[i] = scale + static_cast<float>(i);
  }
  return {reinterpret_cast<const char *>(v), kVectorBytes};
}

class VectorRegistryStateMachineTest
    : public ValkeySearchTestWithParam<StateMachineTestCase> {
 protected:
  // ---- fake keyspace -----------------------------------------------------
  // key -> (field identifier -> value). A key is absent from the map iff it
  // does not exist in the database.
  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<std::string, std::string>>
      keyspace_;
  // key -> (field identifier -> shared reference), populated by
  // HashSetStringRef and cleared by HashSet. This is the observable proof that
  // ShareWithValkeyHash / DetachFromValkeyHash actually ran.
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, SharedRef>>
      shared_refs_;

  // Keys whose stored type no longer matches the schema's data type.
  absl::flat_hash_set<std::string> wrong_type_keys_;

  std::shared_ptr<MockIndexSchema> index_schema_;
  std::shared_ptr<indexes::VectorBase> vector_index_;
  std::shared_ptr<indexes::Numeric> numeric_index_;
  std::shared_ptr<indexes::Tag> tag_index_;

  static std::string_view VectorIdentifier() {
    return GetParam().json ? "$.vec" : "vec_id";
  }
  static std::string_view NumericIdentifier() {
    return GetParam().json ? "$.num" : "num_id";
  }
  static std::string_view TagIdentifier() {
    return GetParam().json ? "$.tag" : "tag_id";
  }

  void SetUp() override {
    // The registry latches the option value at Construct() time, and the base
    // fixture constructs it, so the option has to be set first.
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(GetParam().sharing));
    ValkeySearchTestWithParam::SetUp();
    ASSERT_EQ(VectorRegistry::Instance().IsSharingActive(), GetParam().sharing);

    InstallKeyspaceMocks();
    BuildSchema();
  }

  void TearDown() override {
    index_schema_.reset();
    vector_index_.reset();
    numeric_index_.reset();
    tag_index_.reset();
    ValkeySearchTestWithParam::TearDown();
    auto &enable_sharing =
        const_cast<vmsdk::config::Boolean &>(options::GetEnableVectorSharing());
    VMSDK_EXPECT_OK(enable_sharing.SetValue(true));
  }

  // ---- schema ------------------------------------------------------------
  void BuildSchema() {
    std::vector<absl::string_view> key_prefixes = {kKeyPrefix};
    std::unique_ptr<AttributeDataType> data_type;
    if (GetParam().json) {
      data_type = std::make_unique<JsonAttributeDataType>();
    } else {
      data_type = std::make_unique<HashAttributeDataType>();
    }
    auto schema_or = MockIndexSchema::Create(&fake_ctx_, "state_machine_schema",
                                             key_prefixes, std::move(data_type),
                                             /*mutations_thread_pool=*/nullptr);
    ASSERT_TRUE(schema_or.ok()) << schema_or.status().message();
    index_schema_ = schema_or.value();

    auto hnsw = indexes::VectorHNSW<float>::Create(
        CreateHNSWVectorIndexProto(kDims, data_model::DISTANCE_METRIC_L2,
                                   /*initial_cap=*/64, /*m=*/8,
                                   /*ef_construction=*/64, /*ef_runtime=*/8),
        VectorIdentifier(),
        GetParam().json
            ? data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_JSON
            : data_model::AttributeDataType::ATTRIBUTE_DATA_TYPE_HASH,
        /*db_num=*/0);
    ASSERT_TRUE(hnsw.ok()) << hnsw.status().message();
    vector_index_ = *hnsw;
    VMSDK_EXPECT_OK(index_schema_->AddIndex(kVectorAlias, VectorIdentifier(),
                                            vector_index_));

    if (GetParam().mixed_schema) {
      numeric_index_ =
          std::make_shared<indexes::Numeric>(CreateNumericIndexProto());
      VMSDK_EXPECT_OK(index_schema_->AddIndex(
          kNumericAlias, NumericIdentifier(), numeric_index_));
      tag_index_ =
          std::make_shared<indexes::Tag>(CreateTagIndexProto(",", false));
      VMSDK_EXPECT_OK(
          index_schema_->AddIndex(kTagAlias, TagIdentifier(), tag_index_));
    }
  }

  // ---- fake engine -------------------------------------------------------
  static VectorRegistryStateMachineTest *current_test_;

  // Shared-API entry point used by JsonAttributeDataType::GetRecord. Returns
  // the raw JSON text for the requested path, exactly as the JSON module
  // would: a one-element array wrapping the value.
  static int JsonGetValue(ValkeyModuleKey *key, const char *path,
                          ValkeyModuleString **result) {
    auto key_it = current_test_->keyspace_.find(key->key);
    if (key_it == current_test_->keyspace_.end()) {
      return VALKEYMODULE_ERR;
    }
    auto field_it = key_it->second.find(path);
    if (field_it == key_it->second.end()) {
      return VALKEYMODULE_ERR;
    }
    *result =
        vmsdk::MakeUniqueValkeyString(absl::StrCat("[", field_it->second, "]"))
            .release();
    return VALKEYMODULE_OK;
  }

  bool KeyExists(absl::string_view key) const {
    return keyspace_.contains(key);
  }

  void InstallKeyspaceMocks() {
    current_test_ = this;

    ON_CALL(*kMockValkeyModule, SelectDb(_, _))
        .WillByDefault(::testing::Return(VALKEYMODULE_OK));
    ON_CALL(*kMockValkeyModule, GetSelectedDb(_))
        .WillByDefault(::testing::Return(0));

    // A read-only open of a nonexistent key yields nullptr; a write open
    // always yields a handle, as in the real engine.
    EXPECT_CALL(*kMockValkeyModule, OpenKey(_, _, _))
        .WillRepeatedly([this](ValkeyModuleCtx *ctx, ValkeyModuleString *key,
                               int flags) -> ValkeyModuleKey * {
          const std::string &name = key->data;
          if (!absl::string_view(name).starts_with(kKeyPrefix)) {
            return TestValkeyModule_OpenKeyDefaultImpl(ctx, key, flags);
          }
          if ((flags & VALKEYMODULE_WRITE) == 0 && !KeyExists(name)) {
            return nullptr;
          }
          return new ValkeyModuleKey{ctx, name};
        });

    EXPECT_CALL(*kMockValkeyModule, KeyType(_))
        .WillRepeatedly([this](ValkeyModuleKey *key) -> int {
          if (!absl::string_view(key->key).starts_with(kKeyPrefix)) {
            return TestValkeyModule_KeyTypeDefaultImpl(key);
          }
          if (!KeyExists(key->key)) {
            return VALKEYMODULE_KEYTYPE_EMPTY;
          }
          if (wrong_type_keys_.contains(key->key)) {
            return VALKEYMODULE_KEYTYPE_LIST;
          }
          return GetParam().json ? VALKEYMODULE_KEYTYPE_MODULE
                                 : VALKEYMODULE_KEYTYPE_HASH;
        });

    // HashGet is called two ways: with VALKEYMODULE_HASH_CFIELDS and a
    // `const char*` field (the ingestion path), and with
    // VALKEYMODULE_HASH_NONE and a `ValkeyModuleString*` field reinterpreted
    // as `const char*` by the varargs shim (the sharing path).
    EXPECT_CALL(
        *kMockValkeyModule,
        HashGet(_, _, _, An<ValkeyModuleString **>(), TypedEq<void *>(nullptr)))
        .WillRepeatedly([this](ValkeyModuleKey *key, int flags,
                               const char *field,
                               ValkeyModuleString **value_out, void *) -> int {
          std::string field_name =
              (flags & VALKEYMODULE_HASH_CFIELDS)
                  ? std::string(field)
                  : reinterpret_cast<const ValkeyModuleString *>(field)->data;
          *value_out = nullptr;
          auto key_it = keyspace_.find(key->key);
          if (key_it == keyspace_.end()) {
            return VALKEYMODULE_OK;
          }
          auto field_it = key_it->second.find(field_name);
          if (field_it == key_it->second.end()) {
            return VALKEYMODULE_OK;
          }
          *value_out =
              vmsdk::MakeUniqueValkeyString(field_it->second).release();
          return VALKEYMODULE_OK;
        });

    EXPECT_CALL(*kMockValkeyModule,
                HashSet(_, _, _, _, TypedEq<void *>(nullptr)))
        .WillRepeatedly([this](ValkeyModuleKey *key, int flags,
                               ValkeyModuleString *field,
                               ValkeyModuleString *value, void *) -> int {
          keyspace_[key->key][field->data] = value->data;
          // Writing a plain value drops any shared reference on the field.
          shared_refs_[key->key].erase(field->data);
          return VALKEYMODULE_OK;
        });

    // The engine reports whether the field is *eligible* to hold a string
    // reference. The fake says yes for every field of an existing hash.
    // Faithful to VM_HashHasStringRef: a *predicate* reporting whether the
    // field currently holds a shared string reference (1) or a plain value
    // (0), with VALKEYMODULE_ERR only when the key is not a hash. Share and
    // detach ask opposite questions of this predicate, which is why they test
    // its result in opposite directions.
    EXPECT_CALL(*kMockValkeyModule, HashHasStringRef(_, _))
        .WillRepeatedly(
            [this](ValkeyModuleKey *key, ValkeyModuleString *field) -> int {
              auto key_it = keyspace_.find(key->key);
              if (key_it == keyspace_.end() || GetParam().json ||
                  wrong_type_keys_.contains(key->key)) {
                return VALKEYMODULE_ERR;
              }
              auto ref_it = shared_refs_.find(key->key);
              return (ref_it != shared_refs_.end() &&
                      ref_it->second.contains(field->data))
                         ? 1
                         : 0;
            });

    EXPECT_CALL(*kMockValkeyModule, HashSetStringRef(_, _, _, _))
        .WillRepeatedly([this](ValkeyModuleKey *key, ValkeyModuleString *field,
                               const char *buf, size_t len) -> int {
          shared_refs_[key->key][field->data] = SharedRef{buf, len};
          keyspace_[key->key][field->data] = std::string(buf, len);
          return VALKEYMODULE_OK;
        });

    if (GetParam().json) {
      vmsdk::SetModuleLoaded("json");
      ResetJsonLoadedCache();
      ON_CALL(*kMockValkeyModule,
              GetSharedAPI(_, ::testing::StrEq("JSON_GetValue")))
          .WillByDefault([](ValkeyModuleCtx *, const char *) -> void * {
            return reinterpret_cast<void *>(&JsonGetValue);
          });
    }
  }

  // ---- test-case construction -------------------------------------------
  // Renders a field value for the requested state. Returns nullopt for
  // kAbsent, meaning "do not write this field at all".
  static std::optional<std::string> VectorValue(FieldState state, float scale) {
    switch (state) {
      case FieldState::kAbsent:
        return std::nullopt;
      case FieldState::kValid:
        if (GetParam().json) {
          // JSON stores the vector as a JSON array of kDims floats.
          std::string out = "[";
          for (int i = 0; i < kDims; ++i) {
            absl::StrAppend(&out, i ? "," : "", scale + static_cast<float>(i));
          }
          absl::StrAppend(&out, "]");
          return out;
        }
        return VectorAtScale(scale);
      case FieldState::kInvalid:
        // Wrong number of dimensions: rejected by IsValidSizeVector after
        // normalization, and by IsValidSizeVector directly for HASH.
        return GetParam().json ? std::string("[1,2]")
                               : std::string("\x01\x02\x03", 3);
    }
    return std::nullopt;
  }

  static std::optional<std::string> NumericValue(FieldState state) {
    switch (state) {
      case FieldState::kAbsent:
        return std::nullopt;
      case FieldState::kValid:
        return std::string("42");
      case FieldState::kInvalid:
        return std::string("not_a_number");
    }
    return std::nullopt;
  }

  static std::optional<std::string> TagValue(FieldState state) {
    switch (state) {
      case FieldState::kAbsent:
        return std::nullopt;
      case FieldState::kValid:
      case FieldState::kInvalid:
        // Every string is a legal tag; there is no invalid tag value.
        return std::string("electronics");
    }
    return std::nullopt;
  }

  // Writes the key into the fake keyspace and delivers the notification the
  // engine would deliver. `vector_scale` distinguishes otherwise-identical
  // valid vectors.
  void WriteKey(absl::string_view key, FieldState vector_state,
                float vector_scale = 1.0f,
                NonVectorFields non_vector_fields = {}) {
    auto &fields = keyspace_[std::string(key)];
    auto &refs = shared_refs_[std::string(key)];
    // Writing or deleting a hash field replaces the value object, drops
    // any string reference the engine was holding for it. Modelling this is
    // what makes the sharing assertions below meaningful.
    auto set = [&fields, &refs](std::string_view id,
                                const std::optional<std::string> &value) {
      std::string id_str(id);
      refs.erase(id_str);
      if (value.has_value()) {
        fields[id_str] = *value;
      } else {
        fields.erase(id_str);
      }
    };
    set(VectorIdentifier(), VectorValue(vector_state, vector_scale));
    if (GetParam().mixed_schema) {
      set(NumericIdentifier(), NumericValue(non_vector_fields.numeric));
      set(TagIdentifier(), TagValue(non_vector_fields.tag));
    }
    Notify(key);
  }

  void DeleteKey(absl::string_view key) {
    keyspace_.erase(std::string(key));
    shared_refs_.erase(std::string(key));
    Notify(key);
  }

  void Notify(absl::string_view key) {
    auto key_str = vmsdk::MakeUniqueValkeyString(key);
    index_schema_->OnKeyspaceNotification(
        &fake_ctx_,
        GetParam().json ? VALKEYMODULE_NOTIFY_MODULE : VALKEYMODULE_NOTIFY_HASH,
        "event", key_str.get());
    if (auto *pool = ValkeySearch::Instance().GetWriterThreadPool()) {
      WaitWorkerTasksAreCompleted(*pool);
    }
  }

  // ---- observation -------------------------------------------------------
  std::pair<std::shared_ptr<indexes::VectorRecord>, size_t> Lookup(
      absl::string_view key) const {
    return VectorRegistry::Instance().LookupRecord(
        StringInternStore::Intern(key),
        vector_index_->GetInternedAttributeIdentifier(), 0);
  }

  static size_t EntryCount() {
    return VectorRegistry::Instance().GetStats().entry_cnt;
  }
  static uint64_t SharingHits() {
    return VectorRegistry::Instance().GetStats().hash_sharing_hits.GetTotal();
  }

  bool IndexTracks(absl::string_view key) const {
    return vector_index_->IsTracked(StringInternStore::Intern(key));
  }

  // True when the engine currently holds a shared reference for the key's
  // vector field that points at `record`'s payload.
  bool SharesBuffer(absl::string_view key,
                    const indexes::VectorRecord *record) const {
    auto key_it = shared_refs_.find(std::string(key));
    if (key_it == shared_refs_.end()) {
      return false;
    }
    auto field_it = key_it->second.find(VectorIdentifier());
    if (field_it == key_it->second.end()) {
      return false;
    }
    return field_it->second.buf == record->GetRawVector() &&
           field_it->second.len == kVectorBytes;
  }

  bool HasSharedRef(absl::string_view key) const {
    auto key_it = shared_refs_.find(std::string(key));
    return key_it != shared_refs_.end() &&
           key_it->second.contains(VectorIdentifier());
  }

  // Sharing only happens for HASH keys, and only when the option is on.
  static bool SharingExpected() {
    return GetParam().sharing && !GetParam().json;
  }

  // The registry exists to make the buffer the engine points at and the buffer
  // the index holds be the same object. A JSON document stores the vector as
  // text, so there is no engine-side buffer to unify with and nothing for the
  // registry to do: JSON keys are deliberately never registered. (The registry
  // is keyed by {db, key, attribute}, so it performs no cross-key dedup that
  // JSON could otherwise benefit from.)
  static bool RegistrationExpected() { return !GetParam().json; }

  static size_t ExpectedEntries(size_t hash_entries) {
    return RegistrationExpected() ? hash_entries : 0;
  }

  // Asserts the index holds exactly the bytes of a valid vector at `scale` for
  // `key`, and that the registry and engine-side sharing state match what this
  // data type is supposed to produce. Returns the registered record, or nullptr
  // for data types that are not registered.
  std::shared_ptr<indexes::VectorRecord> ExpectTracked(absl::string_view key,
                                                       float scale) {
    // Index-side effect, asserted for every data type: the vector actually
    // reached the index with the right bytes.
    EXPECT_TRUE(IndexTracks(key)) << "index does not track " << key;
    auto indexed =
        vector_index_->GetVectorDuringSearch(StringInternStore::Intern(key));
    EXPECT_TRUE(indexed.ok()) << indexed.status().message();
    if (indexed.ok()) {
      EXPECT_EQ(absl::string_view(indexed->data(), indexed->size()),
                absl::string_view(VectorAtScale(scale)));
    }

    auto [record, size] = Lookup(key);
    if (!RegistrationExpected()) {
      EXPECT_EQ(record, nullptr)
          << key
          << " must not be registered: its vector cannot be shared "
             "with the engine, so a registry entry would only pin "
             "memory";
      EXPECT_FALSE(HasSharedRef(key));
      return nullptr;
    }
    EXPECT_NE(record, nullptr) << "key " << key << " is not in the registry";
    if (!record) {
      return nullptr;
    }
    EXPECT_EQ(size, kVectorBytes);
    EXPECT_EQ(absl::string_view(record->GetRawVector(), size),
              absl::string_view(VectorAtScale(scale)));
    if (SharingExpected()) {
      EXPECT_TRUE(SharesBuffer(key, record.get()))
          << "engine is not sharing the registry's buffer for " << key;
    } else {
      EXPECT_FALSE(HasSharedRef(key))
          << "engine unexpectedly holds a shared reference for " << key;
    }
    return record;
  }

  void ExpectNotTracked(absl::string_view key) {
    auto [record, size] = Lookup(key);
    EXPECT_EQ(record, nullptr) << "key " << key << " is still in the registry";
    EXPECT_EQ(size, 0u);
    EXPECT_FALSE(HasSharedRef(key))
        << "engine still holds a shared reference for " << key;
  }
};

VectorRegistryStateMachineTest *VectorRegistryStateMachineTest::current_test_ =
    nullptr;

constexpr absl::string_view kKey = "prefix:1";

// --- key creation -------------------------------------------------------

// Create: vector field present and valid.
TEST_P(VectorRegistryStateMachineTest, CreateWithValidVector) {
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kValid, 1.0f);

  auto record = ExpectTracked(kKey, 1.0f);
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
  EXPECT_EQ(SharingHits(), hits_before + (SharingExpected() ? 1u : 0u));
  if (GetParam().mixed_schema) {
    EXPECT_TRUE(numeric_index_->IsTracked(StringInternStore::Intern(kKey)));
    EXPECT_TRUE(tag_index_->IsTracked(StringInternStore::Intern(kKey)));
  }
}

// Create: vector field present but not a usable vector.
TEST_P(VectorRegistryStateMachineTest, CreateWithInvalidVector) {
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kInvalid);

  ExpectNotTracked(kKey);
  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
  EXPECT_EQ(SharingHits(), hits_before);
  EXPECT_FALSE(IndexTracks(kKey));
}

// Create: vector field not present on the key.
TEST_P(VectorRegistryStateMachineTest, CreateWithAbsentVector) {
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kAbsent);

  ExpectNotTracked(kKey);
  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
  EXPECT_EQ(SharingHits(), hits_before);
  EXPECT_FALSE(IndexTracks(kKey));
}

// --- overwrite of a key that already holds a valid vector ----------------

// valid -> different valid: the registry must hold a new record with the new
// payload, and the engine must be sharing the *new* buffer.
TEST_P(VectorRegistryStateMachineTest, OverwriteValidWithDifferentValid) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  auto first = ExpectTracked(kKey, 1.0f);
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kValid, 5.0f);

  auto second = ExpectTracked(kKey, 5.0f);
  if (RegistrationExpected()) {
    ASSERT_NE(second, nullptr);
    EXPECT_NE(first, second) << "changed payload must produce a new record";
  }
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
  EXPECT_EQ(SharingHits(), hits_before + (SharingExpected() ? 1u : 0u));
}

// valid -> byte-identical valid: the record must be reused (that is the whole
// point of the registry) and the engine re-shares the buffer with Valkey.
TEST_P(VectorRegistryStateMachineTest, OverwriteValidWithIdenticalValid) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  auto first = ExpectTracked(kKey, 1.0f);
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kValid, 1.0f);

  auto second = ExpectTracked(kKey, 1.0f);
  if (RegistrationExpected()) {
    EXPECT_EQ(first, second) << "identical payload must reuse the same record";
  }
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
  EXPECT_EQ(SharingHits(), hits_before + (SharingExpected() ? 1u : 0u))
      << "re-sharing an unchanged record";
}

// valid -> invalid: the key no longer has an indexable vector, so the registry
// must not keep serving the superseded payload.
TEST_P(VectorRegistryStateMachineTest, OverwriteValidWithInvalid) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  static_cast<void>(ExpectTracked(kKey, 1.0f));

  WriteKey(kKey, FieldState::kInvalid);

  EXPECT_FALSE(IndexTracks(kKey));
  ExpectNotTracked(kKey);
  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
}

// valid -> field deleted (key still exists).
TEST_P(VectorRegistryStateMachineTest, OverwriteValidWithAbsent) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  static_cast<void>(ExpectTracked(kKey, 1.0f));

  WriteKey(kKey, FieldState::kAbsent);

  EXPECT_FALSE(IndexTracks(kKey));
  ExpectNotTracked(kKey);
  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
}

// --- overwrite of a key that does not currently hold a valid vector ------

TEST_P(VectorRegistryStateMachineTest, OverwriteInvalidWithValid) {
  WriteKey(kKey, FieldState::kInvalid);
  ExpectNotTracked(kKey);
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kValid, 3.0f);

  static_cast<void>(ExpectTracked(kKey, 3.0f));
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
  EXPECT_EQ(SharingHits(), hits_before + (SharingExpected() ? 1u : 0u));
}

TEST_P(VectorRegistryStateMachineTest, OverwriteAbsentWithValid) {
  WriteKey(kKey, FieldState::kAbsent);
  ExpectNotTracked(kKey);
  const uint64_t hits_before = SharingHits();

  WriteKey(kKey, FieldState::kValid, 3.0f);

  static_cast<void>(ExpectTracked(kKey, 3.0f));
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
  EXPECT_EQ(SharingHits(), hits_before + (SharingExpected() ? 1u : 0u));
}

// --- whole-key deletion --------------------------------------------------

TEST_P(VectorRegistryStateMachineTest, DeleteKeyRemovesRegistryEntry) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  static_cast<void>(ExpectTracked(kKey, 1.0f));

  DeleteKey(kKey);

  EXPECT_FALSE(IndexTracks(kKey));
  ExpectNotTracked(kKey);
  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
}

// A second key must be untouched by the first key's deletion.
TEST_P(VectorRegistryStateMachineTest, DeleteKeyLeavesOtherKeysTracked) {
  constexpr absl::string_view kOther = "prefix:2";
  WriteKey(kKey, FieldState::kValid, 1.0f);
  WriteKey(kOther, FieldState::kValid, 9.0f);
  static_cast<void>(ExpectTracked(kKey, 1.0f));
  static_cast<void>(ExpectTracked(kOther, 9.0f));
  EXPECT_EQ(EntryCount(), ExpectedEntries(2));

  DeleteKey(kKey);

  ExpectNotTracked(kKey);
  static_cast<void>(ExpectTracked(kOther, 9.0f));
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
}

// --- non-vector fields ---------------------------------------------------

// Mutating only a non-vector field must leave the tracked vector record
// completely alone -- same instance, same sharing state, no extra share call.
TEST_P(VectorRegistryStateMachineTest, NonVectorFieldChangeLeavesVectorAlone) {
  if (!GetParam().mixed_schema) {
    GTEST_SKIP() << "requires a schema with non-vector fields";
  }
  WriteKey(kKey, FieldState::kValid, 1.0f);
  auto first = ExpectTracked(kKey, 1.0f);
  const uint64_t hits_before = SharingHits();

  // Same vector, different tag.
  keyspace_[std::string(kKey)][TagIdentifier()] = "furniture";
  Notify(kKey);

  auto second = ExpectTracked(kKey, 1.0f);
  if (RegistrationExpected()) {
    EXPECT_EQ(first, second);
  }
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));
  EXPECT_EQ(SharingHits(), hits_before);
}

// A valid vector alongside an invalid non-vector field. Whatever the
// key-level policy does to the index, the registry must not be left holding a
// record for a key that is no longer indexed.
TEST_P(VectorRegistryStateMachineTest, InvalidNonVectorFieldWithValidVector) {
  if (!GetParam().mixed_schema) {
    GTEST_SKIP() << "requires a schema with non-vector fields";
  }
  WriteKey(kKey, FieldState::kValid, 1.0f,
           {.numeric = FieldState::kInvalid, .tag = FieldState::kValid});

  if (IndexTracks(kKey)) {
    static_cast<void>(ExpectTracked(kKey, 1.0f));
  } else {
    ExpectNotTracked(kKey);
    EXPECT_EQ(EntryCount(), ExpectedEntries(0));
  }
}

// --- dropping the index --------------------------------------------------

// Destroying the vector index must drain its keys out of the registry and
// hand the engine back plain values.
TEST_P(VectorRegistryStateMachineTest, DroppingIndexUntracksAndDetaches) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  static_cast<void>(ExpectTracked(kKey, 1.0f));
  EXPECT_EQ(EntryCount(), ExpectedEntries(1));

  index_schema_.reset();
  vector_index_.reset();
  numeric_index_.reset();
  tag_index_.reset();

  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
  EXPECT_FALSE(HasSharedRef(kKey))
      << "engine still holds a reference into freed registry memory";
  // The value must survive the detach, as a plain (unshared) value.
  EXPECT_EQ(keyspace_[std::string(kKey)][VectorIdentifier()],
            *VectorValue(FieldState::kValid, 1.0f));
}

// --- key replaced by a value of the wrong type ---------------------------

// ProcessKeyspaceNotification bails out before TrackRecord when the key is not
// of the schema's data type, so a key whose type changes under an existing
// registry entry never reaches the untrack path.
TEST_P(VectorRegistryStateMachineTest, KeyReplacedByWrongTypeIsUntracked) {
  WriteKey(kKey, FieldState::kValid, 1.0f);
  static_cast<void>(ExpectTracked(kKey, 1.0f));

  // The key still exists but now holds a value of another type -- e.g. after
  // RENAME/RESTORE REPLACE/COPY REPLACE over an existing hash, which replaces
  // the value in place with no intervening `del` notification. The old hash
  // object (and any string references into it) is destroyed by that
  // replacement, so only the registry's own bookkeeping is at stake here.
  keyspace_[std::string(kKey)].clear();
  shared_refs_.erase(std::string(kKey));
  wrong_type_keys_.insert(std::string(kKey));
  Notify(kKey);

  ExpectNotTracked(kKey);
  EXPECT_EQ(EntryCount(), ExpectedEntries(0));
}

std::vector<StateMachineTestCase> AllCases() {
  std::vector<StateMachineTestCase> cases;
  for (bool json : {false, true}) {
    for (bool mixed : {false, true}) {
      for (bool sharing : {false, true}) {
        cases.push_back(StateMachineTestCase{
            .json = json,
            .mixed_schema = mixed,
            .sharing = sharing,
            .name = absl::StrCat(json ? "Json" : "Hash",
                                 mixed ? "_Mixed" : "_VectorOnly",
                                 sharing ? "_SharingOn" : "_SharingOff")});
      }
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    VectorRegistryStateMachineTests, VectorRegistryStateMachineTest,
    ::testing::ValuesIn(AllCases()),
    [](const ::testing::TestParamInfo<StateMachineTestCase> &info) {
      return info.param.name;
    });

}  // namespace
}  // namespace valkey_search