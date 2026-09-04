/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_BASE_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_BASE_H_

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/index_schema.pb.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/index_base.h"
#include "src/query/predicate.h"
#include "src/rdb_serialization.h"
#include "src/utils/allocator.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/managed_pointers.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search {
enum class QueryOperations : uint64_t;
}

namespace valkey_search::indexes {
constexpr float kDefaultMagnitude = 1.0f;

class VectorRecord {
 public:
  // Disallow copy and move because it is variable-sized and should only be
  // managed via std::shared_ptr.
  VectorRecord(const VectorRecord &) = delete;
  VectorRecord &operator=(const VectorRecord &) = delete;
  VectorRecord(VectorRecord &&) = delete;
  VectorRecord &operator=(VectorRecord &&) = delete;
  ~VectorRecord() = default;

  // Static factory method to construct a VectorRecord managed by
  // std::shared_ptr.
  static std::shared_ptr<VectorRecord> Construct(
      absl::string_view vector, float reciprocal_magnitude,
      Allocator *allocator = nullptr);

  inline const char *GetRawVector() const { return data_; }
  inline float GetReciprocalMagnitude() const { return reciprocal_magnitude_; }

 private:
  // Constructor is private, called via placement new in Construct.
  VectorRecord(absl::string_view vector, float reciprocal_magnitude);

  const float reciprocal_magnitude_;
  char data_[0];  // flexible array member
};

// Computes 1/||v|| over `size` elements of storage type T. Templated because
// FLOAT16/BFLOAT16 vectors are 2 bytes per element: reading them as float
// would both walk off the end of the buffer and produce garbage magnitudes.
// The accumulation is always done in float regardless of T.
template <typename T>
float CalcReciprocalMagnitude(const T *src, size_t size);

extern template float CalcReciprocalMagnitude<float>(const float *, size_t);
extern template float CalcReciprocalMagnitude<float16>(const float16 *, size_t);
extern template float CalcReciprocalMagnitude<bfloat16>(const bfloat16 *,
                                                        size_t);

// Verify the running CPU has a BF16 kernel that decodes bfloat16 correctly.
//
// This is a correctness gate, not a check for native BF16 silicon. What it
// rejects is falling back to simsimd's serial BF16 path, which is wrong when
// the build sets SIMSIMD_NATIVE_BF16=1: simsimd then typedefs simsimd_bf16_t
// to _Float16 (x86) or __fp16 (ARM), both of which are IEEE half -- 1/5/10
// bits, not bfloat16's 1/8/7 -- so the serial path reads the stored bits as
// half precision and yields wrong values.
//
// Which CPUs have a correct kernel differs by architecture, and neither
// requirement is "native BF16 instructions":
//   x86 -- AVX2 (Haswell, 2013) and newer qualify. Those kernels have no BF16
//          instructions; they upcast by shifting left 16 bits, which is exactly
//          correct. Genoa and Sapphire additionally have native avx512bf16.
//   ARM -- only FEAT_BF16 (NEON-BF16 / SVE-BF16) qualifies. simsimd ships no
//          shift-based ARM fallback, so pre-FEAT_BF16 cores such as Neoverse
//          N1 (AWS Graviton 2) have no correct kernel at all.
//
// Returns OkStatus when a correct kernel exists, or when the build leaves
// SIMSIMD_NATIVE_BF16=0 (the serial path is then bit-correct and usable).
// Otherwise returns FailedPreconditionError; callers refuse to create or load
// a BFLOAT16 index on this host. Called at index-create / RDB-load time rather
// than module load, so a host without a correct BF16 kernel can still serve
// FLOAT32 and FLOAT16 indexes.
absl::Status CheckSimsimdBf16Capability();

// Scales `record` by `reciprocal_magnitude`, interpreting it as elements of
// storage type T. The scale is applied in float and rounded once back into T.
template <typename T>
std::vector<char> NormalizeVector(absl::string_view record,
                                  float reciprocal_magnitude);

template <typename T>
std::vector<char> NormalizeVector(absl::string_view record,
                                  float *magnitude = nullptr);

// Runtime-dispatched variants, for the few call sites that hold only a
// data-type enum rather than a compile-time T (VectorRegistry, QueryVector).
float CalcReciprocalMagnitude(absl::string_view record,
                              data_model::VectorDataType data_type);

std::vector<char> NormalizeVector(absl::string_view record,
                                  data_model::VectorDataType data_type,
                                  float reciprocal_magnitude);

// Default score value used when no scorer has been applied yet.
inline constexpr float kDefaultScore = 0.0f;

// Lightweight result entry used during non-vector search collection.
// Trivially destructible — destroying a vector of 10K of these is a no-op.
struct BorrowedNeighbor {
  BorrowedInternedStringPtr key;
  float distance;
  float score;
};
static_assert(std::is_trivially_destructible_v<BorrowedNeighbor>,
              "BorrowedNeighbor must be trivially destructible");

struct Neighbor {
  InternedStringPtr external_id;
  float distance;
  float score;
  uint64_t sequence_number;
  std::optional<RecordsMap> attribute_contents;
  Neighbor() : distance(0.0f), score(kDefaultScore), sequence_number(0) {}
  Neighbor(const InternedStringPtr &external_id, float distance)
      : external_id(external_id),
        distance(distance),
        score(distance),
        sequence_number(0) {}
  Neighbor(const InternedStringPtr &external_id, float distance, float score)
      : external_id(external_id),
        distance(distance),
        score(score),
        sequence_number(0) {}
  Neighbor(const InternedStringPtr &external_id, float distance,
           std::optional<RecordsMap> &&attribute_contents)
      : external_id(external_id),
        distance(distance),
        score(distance),
        sequence_number(0),
        attribute_contents(std::move(attribute_contents)) {}
  Neighbor(Neighbor &&other) noexcept
      : external_id(std::move(other.external_id)),
        distance(other.distance),
        score(other.score),
        sequence_number(other.sequence_number),
        attribute_contents(std::move(other.attribute_contents)) {}
  Neighbor &operator=(Neighbor &&other) noexcept {
    if (this != &other) {
      external_id = std::move(other.external_id);
      distance = other.distance;
      score = other.score;
      sequence_number = other.sequence_number;
      attribute_contents = std::move(other.attribute_contents);
    }
    return *this;
  }
  friend std::ostream &operator<<(std::ostream &os, const Neighbor &n) {
    os << "Key: " << n.external_id->Str() << " Dist: " << n.distance
       << " Score: " << n.score << " Seq: " << n.sequence_number;
    if (n.attribute_contents.has_value()) {
      os << ' ' << *n.attribute_contents;
    } else {
      os << " [NoContents]";
    }
    return os;
  }
};

const absl::NoDestructor<absl::flat_hash_map<
    absl::string_view, data_model::VectorIndex::AlgorithmCase>>
    kVectorAlgoByStr({
        {"HNSW", data_model::VectorIndex::AlgorithmCase::kHnswAlgorithm},
        {"FLAT", data_model::VectorIndex::AlgorithmCase::kFlatAlgorithm},
    });

const absl::NoDestructor<
    absl::flat_hash_map<absl::string_view, data_model::DistanceMetric>>
    kDistanceMetricByStr(
        {{"L2", data_model::DistanceMetric::DISTANCE_METRIC_L2},
         {"IP", data_model::DistanceMetric::DISTANCE_METRIC_IP},
         {"COSINE", data_model::DistanceMetric::DISTANCE_METRIC_COSINE}});

const absl::NoDestructor<
    absl::flat_hash_map<absl::string_view, data_model::VectorDataType>>
    kVectorDataTypeByStr({{"FLOAT32", data_model::VECTOR_DATA_TYPE_FLOAT32},
                          {"FLOAT16", data_model::VECTOR_DATA_TYPE_FLOAT16},
                          {"BFLOAT16", data_model::VECTOR_DATA_TYPE_BFLOAT16}});

template <typename V>
absl::string_view LookupKeyByValue(
    const absl::flat_hash_map<absl::string_view, V> &map, const V &value) {
  auto it = std::find_if(map.begin(), map.end(), [&value](const auto &pair) {
    return pair.second == value;
  });
  if (it != map.end()) {
    return it->first;  // Return the key
  } else {
    return "";
  }
}

struct TrackedKeyMetadata {
  uint64_t internal_id;
  // If normalize_ is false, this will be 1.0f. Otherwise, it will be the
  // magnitude of the vector.
  float magnitude;
};

class VectorBase : public IndexBase {
 public:
  ~VectorBase() override;
  absl::StatusOr<indexes::RecordResult> AddRecord(
      const InternedStringPtr &key, absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<bool> RemoveRecord(const InternedStringPtr &key,
                                    indexes::DeletionType deletion_type =
                                        indexes::DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<indexes::RecordResult> ModifyRecord(
      const InternedStringPtr &key, absl::string_view record) override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  virtual size_t GetCapacity() const = 0;
  bool GetNormalize() const { return normalize_; }
  std::unique_ptr<data_model::Index> ToProto() const override;
  absl::Status SaveIndex(RDBChunkOutputStream chunked_out) const override;
  absl::Status SaveTrackedKeys(RDBChunkOutputStream chunked_out) const
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::Status LoadTrackedKeys(ValkeyModuleCtx *ctx,
                               const AttributeDataType *attribute_data_type,
                               SupplementalContentChunkIter &&iter);

  uint32_t GetMutationWeight() const override;

  size_t GetTrackedKeyCount() const override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  size_t GetUnTrackedKeyCount() const override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  bool IsTracked(const InternedStringPtr &key) const override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  bool IsUnTracked(const InternedStringPtr &key) const override;
  void UnTrack(const InternedStringPtr &key) override {}
  absl::Status ForEachTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn)
      const override ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::Status ForEachUnTrackedKey(
      absl::AnyInvocable<absl::Status(const InternedStringPtr &)> fn)
      const override ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);

  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  absl::StatusOr<InternedStringPtr> GetKeyDuringSearch(
      uint64_t internal_id) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  bool AddPrefilteredKey(
      absl::string_view query, float query_magnitude,
      const InternedStringPtr &key, uint64_t count,
      std::priority_queue<std::pair<float, hnswlib::labeltype>> &results,
      absl::flat_hash_set<const char *> &top_keys) const;
  template <typename T>
  absl::StatusOr<std::vector<Neighbor>> CreateReply(
      std::priority_queue<std::pair<T, hnswlib::labeltype>> &knn_res);
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  absl::StatusOr<std::vector<char>> GetVectorDuringSearch(
      const InternedStringPtr &key) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  size_t GetVectorDataSize() const { return GetDataTypeSize() * dimensions_; }

  virtual uint64_t GetMaxLoadedLabel() const { return 0; }
  virtual size_t GetLabelCount() const { return 0; }
  Allocator *GetVectorAllocator() const { return vector_allocator_.get(); }
  int GetDimensions() const { return dimensions_; }
  // Provided by VectorType<T> so the per-element byte width and format
  // conversion come from sizeof(T) + if-constexpr on T instead of a
  // runtime switch on GetVectorDataType().
  vmsdk::UniqueValkeyString NormalizeStringRecord(
      vmsdk::UniqueValkeyString record) const override = 0;
  // Returns the vector data type enum. Used to disambiguate FLOAT16 from
  // BFLOAT16 wherever a byte width alone is ambiguous -- both are 2 bytes, so
  // a size-based check would silently route the wrong format.
  //
  // Stored rather than dispatched: ~VectorBase() has to untrack this index's
  // records from the VectorRegistry, whose key includes the data type, and a
  // virtual call from a base destructor would resolve against the already
  // destroyed VectorType<T> part.
  data_model::VectorDataType GetVectorDataType() const {
    return vector_data_type_;
  }
  // KNN entry point, dispatched virtually so callers hold a VectorBase* and
  // never need to know the storage type or the ANN algorithm. `ef_runtime` is
  // meaningful only to HNSW; FLAT ignores it.
  //
  // The default arguments must stay identical here and in every override.
  // C++ binds default arguments statically, so differing defaults would make
  // the same call mean different things depending on the static type of the
  // pointer it went through.
  virtual absl::StatusOr<std::vector<Neighbor>> Search(
      absl::string_view query, uint64_t count,
      cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr,
      std::optional<size_t> ef_runtime = std::nullopt,
      bool enable_partial_results = false) = 0;
  bool IsValidSizeVector(absl::string_view record) const {
    return record.size() == GetVectorDataSize();
  }
  const InternedStringPtr &GetInternedAttributeIdentifier() const {
    return interned_attribute_identifier_;
  }

 protected:
  VectorBase(IndexerType indexer_type, int dimensions, size_t element_size,
             data_model::VectorDataType vector_data_type,
             data_model::AttributeDataType attribute_data_type,
             absl::string_view attribute_identifier, int db_num)
      : IndexBase(indexer_type),
        db_num_(db_num),
        dimensions_(dimensions),
        vector_data_type_(vector_data_type),
        attribute_identifier_(attribute_identifier),
        interned_attribute_identifier_(
            StringInternStore::Intern(attribute_identifier)),
        attribute_data_type_(attribute_data_type)
#ifndef SAN_BUILD
        ,
        vector_allocator_(CREATE_UNIQUE_PTR(
            FixedSizeAllocator,
            sizeof(VectorRecord) + dimensions * element_size, true))
#endif  // !SAN_BUILD
  {
  }
  void RemoveRecordDueToError(const InternedStringPtr &key,
                              std::optional<uint64_t> internal_id)
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);

  int RespondWithInfo(ValkeyModuleCtx *ctx) const override;

  virtual absl::Status AddRecordImpl(
      uint64_t internal_id,
      std::shared_ptr<const VectorRecord> &&vector_record) = 0;
  virtual absl::Status RemoveRecordImpl(uint64_t internal_id) = 0;
  virtual absl::Status ModifyRecordImpl(
      uint64_t internal_id,
      std::shared_ptr<const VectorRecord> &&vector_record) = 0;
  virtual int RespondWithInfoImpl(ValkeyModuleCtx *ctx) const = 0;

  virtual size_t GetDataTypeSize() const = 0;
  // Computes 1/||record|| interpreting `record` as elements of the concrete
  // storage type. Implemented by VectorType<T>; VectorBase cannot know the
  // element width.
  virtual float ComputeReciprocalMagnitude(absl::string_view record) const = 0;
  virtual void ToProtoImpl(
      data_model::VectorIndex *vector_index_proto) const = 0;
  virtual absl::Status SaveIndexImpl(
      RDBChunkOutputStream chunked_out) const = 0;

  virtual std::shared_ptr<const VectorRecord> &GetVectorLockFree(
      uint64_t internal_id) const = 0;
  virtual std::shared_ptr<const VectorRecord> &GetVector(
      uint64_t internal_id) const = 0;

  int db_num_;
  int dimensions_;
  const data_model::VectorDataType vector_data_type_;
  std::string attribute_identifier_;
  InternedStringPtr interned_attribute_identifier_;
  bool normalize_{false};
  data_model::AttributeDataType attribute_data_type_;
  data_model::DistanceMetric distance_metric_;
  virtual float ComputeDistance(absl::string_view query,
                                const VectorRecord *vector_record,
                                float query_magnitude) const = 0;
  virtual std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const = 0;
  mutable absl::Mutex resize_mutex_;

 private:
  absl::StatusOr<uint64_t> TrackKey(const InternedStringPtr &key,
                                    float magnitude)
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<std::optional<uint64_t>> UnTrackKey(
      const InternedStringPtr &key) ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<bool> UpdateMetadata(const InternedStringPtr &key,
                                      float magnitude,
                                      const VectorRecord *vector_record)
      ABSL_LOCKS_EXCLUDED(resize_mutex_, key_to_metadata_mutex_);
  absl::StatusOr<uint64_t> GetInternalId(const InternedStringPtr &key) const
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  absl::StatusOr<uint64_t> GetInternalIdDuringSearch(
      const InternedStringPtr &key) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  absl::flat_hash_map<uint64_t, InternedStringPtr> key_by_internal_id_
      ABSL_GUARDED_BY(key_to_metadata_mutex_);

  InternedStringHashMap<TrackedKeyMetadata> tracked_metadata_by_key_
      ABSL_GUARDED_BY(key_to_metadata_mutex_);
  uint64_t inc_id_ ABSL_GUARDED_BY(key_to_metadata_mutex_){0};
  mutable absl::Mutex key_to_metadata_mutex_;
  absl::StatusOr<std::pair<float, hnswlib::labeltype>>
  ComputeDistanceFromRecord(const InternedStringPtr &key,
                            absl::string_view query,
                            float query_magnitude) const;
  std::shared_ptr<const VectorRecord> GetOrConstructVectorRecord(
      const InternedStringPtr &key, absl::string_view record) const;
  UniqueFixedSizeAllocatorPtr vector_allocator_{nullptr, nullptr};
};

class PrefilterEvaluator : public query::Evaluator {
 public:
  explicit PrefilterEvaluator(
      const valkey_search::indexes::text::TextIndex *text_index,
      QueryOperations query_operations)
      : query::Evaluator(query_operations), text_index_(text_index) {}
  bool Evaluate(const query::Predicate &predicate,
                const InternedStringPtr &key);
  const InternedStringPtr &GetTargetKey() const override {
    CHECK(key_);
    return *key_;
  }
  bool IsPrefilterEvaluator() const override { return true; }

 private:
  query::EvaluationResult EvaluateTags(
      const query::TagPredicate &predicate) override;
  query::EvaluationResult EvaluateNumeric(
      const query::NumericPredicate &predicate) override;
  query::EvaluationResult EvaluateText(const query::TextPredicate &predicate,
                                       bool require_positions) override;
  const valkey_search::indexes::text::TextIndex *text_index_;
  const InternedStringPtr *key_{nullptr};
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_BASE_H_
