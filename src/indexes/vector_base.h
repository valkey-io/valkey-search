/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_VECTOR_BASE_H_
#define VALKEYSEARCH_SRC_INDEXES_VECTOR_BASE_H_

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <type_traits>
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
class IndexSchema;
}  // namespace valkey_search

namespace valkey_search::indexes {

constexpr float kDefaultMagnitude = 1.0f;
// Initial capacity of a range search's result vector.
constexpr size_t kRangeReserve = 128;

inline constexpr size_t UpperBoundToMultipleOf64(size_t num) {
  return (num + 63) & ~size_t(63);
}

// Storage header placed immediately before a VectorRecord's vector data. Its
// layout depends on the type the reciprocal magnitude is kept in. In every
// layout the reference count is the 4 bytes at data - 8, so type-erased code
// can always reach it.
template <typename MagnitudeT>
struct VectorRecordHeader;

template <>
struct VectorRecordHeader<float> {
  mutable std::atomic<uint32_t> ref_count_{1};
  const float reciprocal_magnitude_{1.0f};
};

template <>
struct VectorRecordHeader<double> {
  const double reciprocal_magnitude_{1.0};
  mutable std::atomic<uint32_t> ref_count_{1};
  const uint32_t padding_{0};
};

static_assert(sizeof(VectorRecordHeader<float>) == 8);
static_assert(sizeof(VectorRecordHeader<double>) == 16);
static_assert(sizeof(VectorRecordHeader<float>) -
                  offsetof(VectorRecordHeader<float>, ref_count_) ==
              8);
static_assert(sizeof(VectorRecordHeader<double>) -
                  offsetof(VectorRecordHeader<double>, ref_count_) ==
              8);

// Type-erased handle to vector data that carries a VectorRecordHeader. The
// header type is not part of the handle's type: the top bit of the reference
// count marks a VectorRecordHeader<double>, which is all that copying,
// releasing and GetReciprocalMagnitude() need to know. Code that knows the
// header type statically uses TypedVectorRecord instead.
class VectorRecord {
 public:
  VectorRecord() noexcept : data_(nullptr) {}
  VectorRecord(std::nullptr_t) noexcept : data_(nullptr) {}

  VectorRecord(const VectorRecord &other) noexcept
      : data_(other.data_.load(std::memory_order_relaxed)) {
    const char *ptr = data_.load(std::memory_order_relaxed);
    if (ptr) {
      GetRefCount(ptr)->fetch_add(1, std::memory_order_relaxed);
    }
  }

  VectorRecord(VectorRecord &&other) noexcept
      : data_(other.data_.exchange(nullptr, std::memory_order_relaxed)) {}

  VectorRecord &operator=(const VectorRecord &other) noexcept {
    if (this != &other) {
      const char *new_ptr = other.data_.load(std::memory_order_relaxed);
      if (new_ptr) {
        GetRefCount(new_ptr)->fetch_add(1, std::memory_order_relaxed);
      }
      const char *old_ptr = data_.exchange(new_ptr, std::memory_order_acq_rel);
      if (old_ptr) {
        Release(old_ptr);
      }
    }
    return *this;
  }

  VectorRecord &operator=(VectorRecord &&other) noexcept {
    if (this != &other) {
      const char *new_ptr =
          other.data_.exchange(nullptr, std::memory_order_relaxed);
      const char *old_ptr = data_.exchange(new_ptr, std::memory_order_acq_rel);
      if (old_ptr) {
        Release(old_ptr);
      }
    }
    return *this;
  }

  VectorRecord &operator=(std::nullptr_t) noexcept {
    reset();
    return *this;
  }

  ~VectorRecord() noexcept {
    const char *old_ptr = data_.exchange(nullptr, std::memory_order_relaxed);
    if (old_ptr) {
      Release(old_ptr);
    }
  }

  void reset() noexcept {
    const char *old_ptr = data_.exchange(nullptr, std::memory_order_acq_rel);
    if (old_ptr) {
      Release(old_ptr);
    }
  }

  // Builds a record whose header keeps the reciprocal magnitude as MagnitudeT.
  template <typename MagnitudeT = float>
  static VectorRecord Construct(
      absl::string_view vector,
      std::type_identity_t<MagnitudeT> reciprocal_magnitude,
      Allocator *allocator);

  const char *GetRawVector() const noexcept {
    return data_.load(std::memory_order_relaxed);
  }

  double GetReciprocalMagnitude() const noexcept {
    const char *ptr = data_.load(std::memory_order_relaxed);
    if (!ptr) {
      return 1.0;
    }
    return HasDoubleHeader(ptr) ? GetHeader<double>(ptr)->reciprocal_magnitude_
                                : GetHeader<float>(ptr)->reciprocal_magnitude_;
  }

  uint32_t RefCount() const noexcept {
    const char *ptr = data_.load(std::memory_order_relaxed);
    return ptr ? GetRefCount(ptr)->load(std::memory_order_relaxed) &
                     kRefCountMask
               : 0;
  }

  bool empty() const noexcept {
    return data_.load(std::memory_order_relaxed) == nullptr;
  }

  explicit operator bool() const noexcept { return !empty(); }

  bool operator==(const VectorRecord &other) const noexcept {
    return data_.load(std::memory_order_relaxed) ==
           other.data_.load(std::memory_order_relaxed);
  }

  bool operator!=(const VectorRecord &other) const noexcept {
    return !(*this == other);
  }

  bool operator==(std::nullptr_t) const noexcept { return empty(); }
  bool operator!=(std::nullptr_t) const noexcept { return !empty(); }

  friend std::ostream &operator<<(std::ostream &os, const VectorRecord &r) {
    return os << "VectorRecord("
              << static_cast<const void *>(
                     r.data_.load(std::memory_order_relaxed))
              << ")";
  }

 protected:
  template <typename MagnitudeT>
  static VectorRecordHeader<MagnitudeT> *GetHeader(
      const char *data_ptr) noexcept {
    return const_cast<VectorRecordHeader<MagnitudeT> *>(
        reinterpret_cast<const VectorRecordHeader<MagnitudeT> *>(data_ptr) - 1);
  }

 private:
  // Set in the reference count of a record with a VectorRecordHeader<double>.
  static constexpr uint32_t kDoubleHeaderBit = 1u << 31;
  static constexpr uint32_t kRefCountMask = ~kDoubleHeaderBit;

  static std::atomic<uint32_t> *GetRefCount(const char *data_ptr) noexcept {
    return reinterpret_cast<std::atomic<uint32_t> *>(
        const_cast<char *>(data_ptr) - 8);
  }

  static bool HasDoubleHeader(const char *data_ptr) noexcept {
    return GetRefCount(data_ptr)->load(std::memory_order_relaxed) &
           kDoubleHeaderBit;
  }

  static void Release(const char *data_ptr) noexcept {
    uint32_t prev =
        GetRefCount(data_ptr)->fetch_sub(1, std::memory_order_acq_rel);
    if ((prev & kRefCountMask) == 1) {
      size_t header_size = (prev & kDoubleHeaderBit)
                               ? sizeof(VectorRecordHeader<double>)
                               : sizeof(VectorRecordHeader<float>);
      Allocator::Free(const_cast<char *>(data_ptr) - header_size);
    }
  }

  explicit VectorRecord(const char *data_ptr) noexcept : data_(data_ptr) {}

  mutable std::atomic<const char *> data_{nullptr};
};

static_assert(sizeof(VectorRecord) == sizeof(void *),
              "VectorRecord must be single pointer sized");

// A VectorRecord whose header type is known statically, so hot loops read the
// reciprocal magnitude without inspecting the header. It adds no state: index
// slots hold these and hand them out as VectorRecord.
template <typename MagnitudeT>
class TypedVectorRecord : public VectorRecord {
 public:
  static constexpr size_t kStorageHeaderSize =
      sizeof(VectorRecordHeader<MagnitudeT>);

  TypedVectorRecord() noexcept = default;
  TypedVectorRecord(std::nullptr_t) noexcept {}
  // The caller guarantees `record` was built with Construct<MagnitudeT>.
  explicit TypedVectorRecord(const VectorRecord &record) noexcept
      : VectorRecord(record) {}
  explicit TypedVectorRecord(VectorRecord &&record) noexcept
      : VectorRecord(std::move(record)) {}

  MagnitudeT GetReciprocalMagnitude() const noexcept {
    const char *ptr = GetRawVector();
    return ptr ? ReciprocalMagnitudeOf(ptr) : MagnitudeT{1};
  }

  // Reciprocal magnitude of the record whose (non-null) vector data starts at
  // data_ptr. Lets hot loops that hold only the raw vector pointer read it.
  static MagnitudeT ReciprocalMagnitudeOf(const char *data_ptr) noexcept {
    return GetHeader<MagnitudeT>(data_ptr)->reciprocal_magnitude_;
  }
};

static_assert(sizeof(TypedVectorRecord<float>) == sizeof(VectorRecord));
static_assert(sizeof(TypedVectorRecord<double>) == sizeof(VectorRecord));

struct VectorRecordWithSize {
  VectorRecord vector_record;
  size_t size{0};

  bool operator==(const VectorRecordWithSize &other) const = default;
  bool operator==(std::nullptr_t) const { return vector_record == nullptr; }
  bool operator!=(std::nullptr_t) const { return vector_record != nullptr; }

  bool operator==(absl::string_view bytes) const {
    return vector_record != nullptr && size == bytes.size() &&
           std::memcmp(vector_record.GetRawVector(), bytes.data(), size) == 0;
  }
  bool operator!=(absl::string_view bytes) const { return !(*this == bytes); }
};

// Type a T vector's reciprocal magnitude and distances are computed and stored
// in: double for FLOAT64, float for every other storage type.
template <typename T>
using MagnitudeOf =
    std::conditional_t<std::is_same_v<T, double>, double, float>;

// Computes 1/||v|| over `size` elements of storage type T. Templated because
// FLOAT16/BFLOAT16 vectors are 2 bytes per element: reading them as float
// would both walk off the end of the buffer and produce garbage magnitudes.
// The accumulation is done in MagnitudeOf<T>, so float for the 2-byte types.
template <typename T>
MagnitudeOf<T> CalcReciprocalMagnitude(const T *src, size_t size);

extern template float CalcReciprocalMagnitude<float>(const float *, size_t);
extern template float CalcReciprocalMagnitude<float16>(const float16 *, size_t);
extern template float CalcReciprocalMagnitude<bfloat16>(const bfloat16 *,
                                                        size_t);
extern template double CalcReciprocalMagnitude<double>(const double *, size_t);

// Verify the running CPU exposes a SIMD-targeted BF16 path required by the
// current simsimd build. Returns OkStatus when:
//   - This build of simsimd does not enable native BF16 (the serial path is
//     then bit-correct), or
//   - The CPU advertises Haswell/Genoa/Sapphire on x86, or NEON-BF16/SVE-BF16
//     on ARM.
// Otherwise returns FailedPreconditionError; callers should refuse to create
// or load a BFLOAT16 index on this host. Called at index-create / RDB-load
// time rather than module load so that nodes lacking a SIMD BF16 path can
// still serve FLOAT32 and FLOAT16 indexes.
absl::Status CheckSimsimdBf16Capability();

// Scales `record` by `reciprocal_magnitude`, interpreting it as elements of
// storage type T. The scale is applied in MagnitudeOf<T> and rounded once back
// into T.
template <typename T>
std::vector<char> NormalizeVector(absl::string_view record,
                                  MagnitudeOf<T> reciprocal_magnitude);

template <typename T>
std::vector<char> NormalizeVector(absl::string_view record,
                                  MagnitudeOf<T> *magnitude = nullptr);

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
  double distance;
  double score;
  uint64_t sequence_number;
  // False only for a compound VECTOR_RANGE match that carries no VR distance
  // (e.g. a doc matched via the non-VR branch of an OR and lies outside the
  // radius). The distance field then holds a sentinel; readers gate the
  // yielded-distance field, sorting, and aggregate LOAD on this flag rather
  // than inspecting the float, which is unreliable under -ffast-math.
  bool has_vr_distance;
  std::optional<RecordsMap> attribute_contents;
  Neighbor()
      : distance(0.0f),
        score(kDefaultScore),
        sequence_number(0),
        has_vr_distance(true) {}
  Neighbor(const InternedStringPtr &external_id, double distance)
      : external_id(external_id),
        distance(distance),
        score(distance),
        sequence_number(0),
        has_vr_distance(true) {}
  Neighbor(const InternedStringPtr &external_id, double distance, double score)
      : external_id(external_id),
        distance(distance),
        score(score),
        sequence_number(0),
        has_vr_distance(true) {}
  Neighbor(const InternedStringPtr &external_id, double distance,
           std::optional<RecordsMap> &&attribute_contents)
      : external_id(external_id),
        distance(distance),
        score(distance),
        sequence_number(0),
        has_vr_distance(true),
        attribute_contents(std::move(attribute_contents)) {}
  Neighbor(Neighbor &&other) noexcept
      : external_id(std::move(other.external_id)),
        distance(other.distance),
        score(other.score),
        sequence_number(other.sequence_number),
        has_vr_distance(other.has_vr_distance),
        attribute_contents(std::move(other.attribute_contents)) {}
  Neighbor &operator=(Neighbor &&other) noexcept {
    if (this != &other) {
      external_id = std::move(other.external_id);
      distance = other.distance;
      score = other.score;
      sequence_number = other.sequence_number;
      has_vr_distance = other.has_vr_distance;
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
                          {"BFLOAT16", data_model::VECTOR_DATA_TYPE_BFLOAT16},
                          {"FLOAT64", data_model::VECTOR_DATA_TYPE_FLOAT64}});

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
  absl::StatusOr<indexes::RecordResult> AddRecord(const InternedStringPtr &key,
                                                  AttributeData &&data) override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<bool> RemoveRecord(const InternedStringPtr &key,
                                    indexes::DeletionType deletion_type =
                                        indexes::DeletionType::kNone) override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<indexes::RecordResult> ModifyRecord(
      const InternedStringPtr &key, AttributeData &&data) override
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  virtual size_t GetCapacity() const = 0;
  bool GetNormalize() const { return normalize_; }
  data_model::DistanceMetric GetDistanceMetric() const {
    return distance_metric_;
  }
  int GetDBNum() const { return db_num_; }
  void OnSwapDB(int new_db_num) override { db_num_ = new_db_num; }
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
  // Non-owning form of GetKeyDuringSearch: nullptr if the id is not tracked.
  const InternedStringPtr *FindKeyDuringSearch(uint64_t internal_id) const
      ABSL_NO_THREAD_SAFETY_ANALYSIS {
    auto it = key_by_internal_id_.find(internal_id);
    return it == key_by_internal_id_.end() ? nullptr : &it->second;
  }
  // The `count` keys nearest to a query, among keys offered one at a time by
  // pre-filtering. Distances stay in the type the index computes them in.
  class NearestKeys {
   public:
    virtual ~NearestKeys() = default;
    // Keeps `key` if it is tracked and fewer than `count` keys are kept, or it
    // is nearer than the farthest kept key, which it then replaces; the
    // replaced key is also erased from `top_keys`. Returns whether `key` was
    // kept.
    virtual bool Add(const InternedStringPtr &key,
                     absl::flat_hash_set<const char *> &top_keys) = 0;
    virtual absl::StatusOr<std::vector<Neighbor>> CreateReply() = 0;
  };
  virtual std::unique_ptr<NearestKeys> CreateNearestKeys(
      absl::string_view query, uint64_t count) = 0;
  template <typename T>
  absl::StatusOr<std::vector<Neighbor>> CreateReply(
      std::priority_queue<std::pair<T, hnswlib::labeltype>> &knn_res);
  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  absl::StatusOr<std::vector<char>> GetVectorDuringSearch(
      const InternedStringPtr &key) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  size_t GetVectorDataSize() const { return GetDataTypeSize() * dimensions_; }

  // Returns the neighbors within `radius` of `query`, unordered. FLAT scans
  // every vector. HNSW runs an approximate KNN search for up to
  // search.max-nonvector-search-results-fetched candidates, so it can miss
  // keys; it falls back to SearchRangeExhaustive when the fetch fills the cap
  // with the farthest candidate still in range.
  virtual absl::StatusOr<std::vector<Neighbor>> SearchRange(
      absl::string_view query, double radius, cancel::Token &cancellation_token,
      std::unique_ptr<hnswlib::BaseFilterFunctor> filter = nullptr) = 0;

  // Returns the distance from the stored vector for `key` to `query` if the
  // distance is <= `radius`; returns std::nullopt if outside the radius;
  // returns an error if the key is not tracked in this index. Search-phase
  // only: lock-free, like GetVectorDuringSearch.
  virtual absl::StatusOr<std::optional<double>> IsWithinVectorRange(
      const InternedStringPtr &key, absl::string_view query,
      double radius) const = 0;
  // Range test for `record`, the raw vector bytes just read back from the
  // database. Like RecomputeDistance it touches no index structure, so it is
  // safe on the main thread outside the search phase, where
  // IsWithinVectorRange is not.
  virtual bool IsRecordWithinVectorRange(absl::string_view record,
                                         absl::string_view query,
                                         double radius) const = 0;
  bool IsVectorIndex() const override { return true; }
  virtual uint64_t GetMaxLoadedLabel() const { return 0; }
  virtual size_t GetLabelCount() const { return 0; }
  FixedSizeAllocator *GetVectorAllocator() const {
    return vector_allocator_.get();
  }
  int GetDimensions() const { return dimensions_; }
  // Provided by VectorType<T> so the per-element byte width and format
  // conversion come from sizeof(T) + if-constexpr on T instead of a
  // runtime switch on GetVectorDataType().
  vmsdk::UniqueValkeyString NormalizeStringAttribute(
      vmsdk::UniqueValkeyString attribute) const override = 0;
  // Returns the vector data type enum. Used to disambiguate FLOAT16 from
  // BFLOAT16 wherever a byte width alone is ambiguous -- both are 2 bytes, so
  // a size-based check would silently route the wrong format.
  virtual data_model::VectorDataType GetVectorDataType() const = 0;
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
  bool IsValidSizeVector(size_t size) const {
    return size == GetVectorDataSize();
  }
  bool IsValidSizeVector(absl::string_view record) const {
    return IsValidSizeVector(record.size());
  }
  // Distance between `query` and the vector in `record`, where `record` is the
  // raw bytes just read back from the database rather than anything the index
  // holds.
  //
  // Used when a document was mutated after the search scored it: the distance
  // the neighbor carries describes the vector the search saw, and the reply is
  // about a different one. Taking the bytes as an argument is what keeps this
  // callable from the main thread, where the index structures are not held
  // under a reader lock.
  virtual absl::StatusOr<double> RecomputeDistance(
      absl::string_view record, absl::string_view query) const = 0;
  const InternedStringPtr &GetInternedAttributeIdentifier() const {
    return interned_attribute_identifier_;
  }
  // Builds a record of `record` and its reciprocal magnitude, interpreting
  // `record` as elements of the concrete storage type and using the header
  // type the index stores. Implemented by VectorType<T>; VectorBase cannot know
  // the element width.
  virtual VectorRecord ConstructRecord(absl::string_view record,
                                       Allocator *allocator) const = 0;
  ~VectorBase() override ABSL_NO_THREAD_SAFETY_ANALYSIS;
  data_model::AttributeDataType GetAttributeDataType() const {
    return attribute_data_type_;
  }

 protected:
  VectorBase(IndexerType indexer_type, int dimensions, size_t element_size,
             size_t record_header_size,
             data_model::AttributeDataType attribute_data_type,
             absl::string_view attribute_identifier, int db_num)
      : IndexBase(indexer_type),
        db_num_(db_num),
        dimensions_(dimensions),
        attribute_identifier_(attribute_identifier),
        interned_attribute_identifier_(
            StringInternStore::Intern(attribute_identifier)),
        attribute_data_type_(attribute_data_type)
#ifndef SAN_BUILD
        ,
        vector_allocator_(CREATE_UNIQUE_PTR(
            FixedSizeAllocator, record_header_size + dimensions * element_size,
            true))
#endif  // !SAN_BUILD
  {
  }

  VectorBase(IndexerType indexer_type, int dimensions,
             data_model::AttributeDataType attribute_data_type,
             absl::string_view attribute_identifier, int db_num)
      : VectorBase(indexer_type, dimensions, sizeof(float),
                   TypedVectorRecord<float>::kStorageHeaderSize,
                   attribute_data_type, attribute_identifier, db_num) {}
  void RemoveRecordDueToError(const InternedStringPtr &key,
                              std::optional<uint64_t> internal_id)
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);

  int RespondWithInfo(ValkeyModuleCtx *ctx) const override;

  // Lock-free search optimization: Phase-based locking guarantees that queries
  // and resizes/mutations are strictly mutually exclusive. Therefore, no data
  // races can occur during the search phase.
  absl::StatusOr<uint64_t> GetInternalIdDuringSearch(
      const InternedStringPtr &key) const ABSL_NO_THREAD_SAFETY_ANALYSIS;
  const InternedStringHashMap<TrackedKeyMetadata> &
  GetTrackedMetadataDuringSearch() const ABSL_NO_THREAD_SAFETY_ANALYSIS {
    return tracked_metadata_by_key_;
  }

  virtual absl::Status AddRecordImpl(uint64_t internal_id,
                                     VectorRecord &&vector_record) = 0;
  virtual absl::Status RemoveRecordImpl(uint64_t internal_id) = 0;
  virtual absl::Status ModifyRecordImpl(uint64_t internal_id,
                                        VectorRecord &&vector_record) = 0;
  virtual int RespondWithInfoImpl(ValkeyModuleCtx *ctx) const = 0;

  virtual size_t GetDataTypeSize() const = 0;
  virtual void ToProtoImpl(
      data_model::VectorIndex *vector_index_proto) const = 0;
  virtual absl::Status SaveIndexImpl(
      RDBChunkOutputStream chunked_out) const = 0;

  virtual VectorRecord &GetVectorLockFree(uint64_t internal_id) const = 0;
  virtual VectorRecord &GetVector(uint64_t internal_id) const = 0;

  int db_num_;
  int dimensions_;
  std::string attribute_identifier_;
  InternedStringPtr interned_attribute_identifier_;
  bool normalize_{false};
  data_model::AttributeDataType attribute_data_type_;
  data_model::DistanceMetric distance_metric_;
  virtual std::optional<hnswlib::tableint> GetAlgoIdLockFree(
      uint64_t internal_id) const = 0;
  mutable absl::Mutex resize_mutex_;

 private:
  absl::StatusOr<uint64_t> TrackKey(const InternedStringPtr &key,
                                    float magnitude)
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<std::optional<uint64_t>> UnTrackKey(
      const InternedStringPtr &key) ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::StatusOr<bool> IsVectorUnchanged(const InternedStringPtr &key,
                                         float magnitude,
                                         const VectorRecord &vector_record)
      ABSL_LOCKS_EXCLUDED(resize_mutex_, key_to_metadata_mutex_);
  absl::StatusOr<uint64_t> GetInternalId(const InternedStringPtr &key) const
      ABSL_LOCKS_EXCLUDED(key_to_metadata_mutex_);
  absl::flat_hash_map<uint64_t, InternedStringPtr> key_by_internal_id_
      ABSL_GUARDED_BY(key_to_metadata_mutex_);

  InternedStringHashMap<TrackedKeyMetadata> tracked_metadata_by_key_
      ABSL_GUARDED_BY(key_to_metadata_mutex_);
  uint64_t inc_id_ ABSL_GUARDED_BY(key_to_metadata_mutex_){0};
  mutable absl::Mutex key_to_metadata_mutex_;
  UniqueFixedSizeAllocatorPtr vector_allocator_{nullptr, nullptr};
};

class PrefilterEvaluator : public query::Evaluator {
 public:
  explicit PrefilterEvaluator(
      const valkey_search::indexes::text::TextIndex *text_index,
      QueryOperations query_operations,
      const valkey_search::IndexSchema *index_schema)
      : query::Evaluator(query_operations),
        text_index_(text_index),
        index_schema_(index_schema) {}
  bool Evaluate(const query::Predicate &predicate,
                const InternedStringPtr &key);
  // Like Evaluate(), but returns the full EvaluationResult so that callers
  // handling VectorRange queries can read the matched vr_distance without a
  // side-channel.
  query::EvaluationResult EvaluateFull(const query::Predicate &predicate,
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
  query::EvaluationResult EvaluateVectorRange(
      const query::VectorRangePredicate &predicate) override;
  const valkey_search::indexes::text::TextIndex *text_index_;
  const InternedStringPtr *key_{nullptr};
  const valkey_search::IndexSchema *index_schema_{nullptr};
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_VECTOR_BASE_H_
