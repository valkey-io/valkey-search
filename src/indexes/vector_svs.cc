/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_svs.h"

#include <algorithm>
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

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "src/attribute_data_type.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/valkey_search_options.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/debug.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#if defined(__linux__) && defined(__x86_64__)
#include <svs/c/svs_c.h>
#include <sys/mman.h>
#include <unistd.h>

#include "absl/strings/str_cat.h"
#include "src/index_schema.pb.h"
#include "src/indexes/svs_index.pb.h"
#include "src/indexes/svs_stream_adapter.h"
#include "src/metrics.h"
#include "vmsdk/src/utils.h"
#endif

namespace valkey_search::indexes {

namespace {
constexpr absl::string_view kUnavailableMsg =
    "SVS_VAMANA is not available on this build (requires x86_64 Linux).";

// The alpha SVS builds with: ALPHA when given, otherwise SVS's per-metric
// default (svs::VAMANA_ALPHA_MINIMIZE_DEFAULT for L2,
// VAMANA_ALPHA_MAXIMIZE_DEFAULT for IP and COSINE), which SVS applies inside
// its build and the C API doesn't report back.
float EffectiveAlpha(const SVSBuildConfig& config,
                     data_model::DistanceMetric metric) {
  if (config.alpha > 0.0f) return config.alpha;
  return metric == data_model::DISTANCE_METRIC_L2 ? 1.2f : 0.95f;
}

// Shared by ToProtoImpl and the RDB header's build_config echo, so the two
// never drift apart. No SVS dependency: safe to call on any platform.
void FillSvsVamanaProto(const SVSBuildConfig& config,
                        data_model::SVSVamanaAlgorithm* proto) {
  proto->set_graph_max_degree(config.graph_max_degree);
  proto->set_construction_window_size(config.construction_window_size);
  proto->set_search_window_size(config.search_window_size);
  proto->set_alpha(config.alpha);
  proto->set_compression(config.compression);
  proto->set_training_threshold(config.training_threshold);
}
}  // namespace

#if defined(__linux__) && defined(__x86_64__)

namespace {

// RDB header format, bumped whenever SVSIndexHeader's shape changes in a
// way that breaks backward compatibility with older readers.
constexpr uint32_t kSvsHeaderFormatVersion = 1;

// Sequential threadpool vtable: SVS invokes parallel_for on the calling
// reader thread. Matches HNSW's concurrency model (one search occupies
// one reader thread; throughput scales via concurrent searches on
// Valkey's reader pool). Static-storage so it survives every index
// handle's lifetime.
size_t SvsThreadpoolSize(void* /*self*/) { return 1; }

bool SvsThreadpoolParallelFor(void* /*self*/,
                              void (*func)(void* svs_param, size_t i),
                              void* svs_param, size_t n,
                              svs_error_h /*out_err*/) {
  for (size_t i = 0; i < n; ++i) {
    func(svs_param, i);
  }
  return true;
}

}  // namespace

// Declared in vector_svs.h for the unit tests.
svs_threadpool_ops_t kSvsThreadpoolOps =
    SVS_INIT_THREADPOOL_OPS(SvsThreadpoolSize, SvsThreadpoolParallelFor);
svs_threadpool_t kSvsThreadpoolIface{&kSvsThreadpoolOps, nullptr};

namespace {

// Huge-page-aware custom allocator vtable. Requests at least one huge
// page in size try three tiers in order: 2 MiB huge pages via
// MAP_HUGETLB, opportunistic THP promotion via madvise(MADV_HUGEPAGE),
// then bare mmap. Smaller requests skip the huge-page routes and use
// small-page mmap so we do not round every metadata block up to 2 MiB.
// Each successful allocation reports its actual size to
// ValkeyModule_IncrExternalMemory so it counts against used_memory /
// maxmemory just like the module's own malloc arena.
constexpr size_t kSvsAllocatorHugePageSize = 2 * 1024 * 1024;

// SVS allocates data and graph in blocks of this size, reserved on the first
// add. SVS's own default (0) is 2^30 bytes. The block is sized to hold
// INITIAL_CAP vectors, within these bounds; an index with no INITIAL_CAP
// (saved before it was honored) uses the maximum. Measured 16–64 MiB behave
// identically for large indexes.
constexpr size_t kSvsMinBlockSizeBytes = 64 * 1024;
constexpr size_t kSvsBlockSizeBytes = 16 * 1024 * 1024;

// Consolidate once deleted slots reach a quarter of the live entries, and at
// least kConsolidateMinDeleted, so the full-graph pass is amortized.
constexpr size_t kConsolidateMinDeleted = 1024;
constexpr size_t kConsolidateLiveDivisor = 4;
// svs_index_build_dynamic_ex rejects a block size that is not a power of two,
// while the stream load rounds one down silently; keep the two paths agreeing.
static_assert(std::has_single_bit(kSvsBlockSizeBytes) &&
                  std::has_single_bit(kSvsMinBlockSizeBytes),
              "SVS block sizes must be powers of two");

// In the pinned SVS revision FINE_GRAIN is an alias of GLOBAL; once upstream
// implements it, writers on one index run in parallel with no module change.
constexpr svs_sync_kind_t kSvsSyncKind = SVS_SYNC_KIND_FINE_GRAIN;

// Reader or writer side of resize_mutex_ for a mutation, chosen at run time
// (see VectorSVS::mutations_exclusive_). Annotated as a shared lock: the
// exclusive case is a superset, so callers that require the shared side are
// satisfied either way.
class ABSL_SCOPED_LOCKABLE SvsMutationLock {
 public:
  SvsMutationLock(absl::Mutex* mu, bool exclusive) ABSL_SHARED_LOCK_FUNCTION(mu)
      : mu_(mu), exclusive_(exclusive) {
    if (exclusive_) {
      mu_->Lock();
    } else {
      mu_->ReaderLock();
    }
  }
  ~SvsMutationLock() ABSL_UNLOCK_FUNCTION() {
    if (exclusive_) {
      mu_->Unlock();
    } else {
      mu_->ReaderUnlock();
    }
  }
  SvsMutationLock(const SvsMutationLock&) = delete;
  SvsMutationLock& operator=(const SvsMutationLock&) = delete;

 private:
  absl::Mutex* mu_;
  bool exclusive_;
};

size_t RoundUpToPageSize(size_t size, size_t page_size) {
  return ((size + page_size - 1) / page_size) * page_size;
}

size_t SvsAllocatorSmallPageSize() {
  static const size_t cached = []() {
    const long v = sysconf(_SC_PAGESIZE);
    return v > 0 ? static_cast<size_t>(v) : size_t{4096};
  }();
  return cached;
}

// Both alloc and dealloc round the caller's size using this helper so
// they always agree on the mapping length passed to munmap.
size_t SvsAllocatorAlignedSize(size_t size, size_t alignment) {
  const size_t page_size = size >= kSvsAllocatorHugePageSize
                               ? kSvsAllocatorHugePageSize
                               : SvsAllocatorSmallPageSize();
  return RoundUpToPageSize(size, std::max<size_t>(alignment, page_size));
}

void* SvsAllocatorAllocate(void* /*self*/, size_t size, size_t alignment,
                           svs_error_h /*out_err*/) {
  if (size == 0) {
    return nullptr;
  }
  const bool huge_eligible = size >= kSvsAllocatorHugePageSize;
  const size_t aligned_size = SvsAllocatorAlignedSize(size, alignment);

  void* ptr = MAP_FAILED;
  if (huge_eligible) {
    ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE,
               MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  }
  if (ptr == MAP_FAILED) {
    ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE,
               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
      return nullptr;
    }
    if (huge_eligible) {
      // Opportunistically ask the kernel to back this region with
      // huge pages if transparent-hugepage is enabled. Best effort;
      // ignore failures. Skipped for small requests where THP
      // promotion is unlikely to fire.
      madvise(ptr, aligned_size, MADV_HUGEPAGE);
    }
  }
  ValkeyModule_IncrExternalMemory(aligned_size);
  return ptr;
}

void SvsAllocatorDeallocate(void* /*self*/, void* ptr, size_t size,
                            size_t alignment) {
  if (ptr == nullptr || size == 0) {
    return;
  }
  const size_t aligned_size = SvsAllocatorAlignedSize(size, alignment);
  munmap(ptr, aligned_size);
  ValkeyModule_DecrExternalMemory(aligned_size);
}

svs_allocator_ops_t kSvsAllocatorOps =
    SVS_INIT_ALLOCATOR_OPS(SvsAllocatorAllocate, SvsAllocatorDeallocate);
svs_allocator_t kSvsAllocatorIface{&kSvsAllocatorOps, nullptr};

// Maps valkey-search's DistanceMetric proto value to SVS's enum. Kept
// tight; distances not supported by SVS (currently: none in v1) return
// nullopt. COSINE vectors are normalized before they reach SVS (see
// PrepareSvsInput), so COSINE uses SVS's dot product: equal to cosine on
// unit vectors, and free of SVS cosine's 0/0 NaN on a zero vector.
std::optional<svs_distance_metric_t> ToSvsDistanceMetric(
    data_model::DistanceMetric metric) {
  switch (metric) {
    case data_model::DISTANCE_METRIC_L2:
      return SVS_DISTANCE_METRIC_EUCLIDEAN;
    case data_model::DISTANCE_METRIC_COSINE:
    case data_model::DISTANCE_METRIC_IP:
      return SVS_DISTANCE_METRIC_DOT_PRODUCT;
    default:
      return std::nullopt;
  }
}

// Selects the SVS internal storage data type from the compression proto
// value. FLOAT32 storage is the default; FP16 and SQ8 are the two open
// v1 compression kinds the SVS C API registers.
std::optional<svs_data_type_t> CompressionToSvsDataType(
    data_model::SVSCompressionType compression,
    data_model::VectorDataType input_type =
        data_model::VECTOR_DATA_TYPE_FLOAT32) {
  switch (compression) {
    case data_model::SVS_COMPRESSION_NONE:
      // No compression: store the input precision, without upcasting.
      return input_type == data_model::VECTOR_DATA_TYPE_FLOAT16
                 ? SVS_DATA_TYPE_FLOAT16
                 : SVS_DATA_TYPE_FLOAT32;
    case data_model::SVS_COMPRESSION_FP16:
      return SVS_DATA_TYPE_FLOAT16;
    case data_model::SVS_COMPRESSION_SQ8:
      // SVS SQ storage accepts INT8 or UINT8. We use INT8 for the v1
      // SQ8 option; symmetric quantization around zero.
      return SVS_DATA_TYPE_INT8;
    default:
      // LVQ / LEANVEC variants are v2 proprietary; rejected here.
      return std::nullopt;
  }
}

// Owns an svs_error_h so it is freed on scope exit. SVS errors are
// heap-allocated by the C API even on the success path (they hold the
// last recorded message); leaking them would grow used_memory.
class ScopedSvsError {
 public:
  ScopedSvsError() : err_(svs_error_create()) {}
  ~ScopedSvsError() {
    if (err_ != nullptr) svs_error_free(err_);
  }
  ScopedSvsError(const ScopedSvsError&) = delete;
  ScopedSvsError& operator=(const ScopedSvsError&) = delete;
  svs_error_h get() const { return err_; }

 private:
  svs_error_h err_;
};

absl::Status SvsErrorToStatus(svs_error_h err, absl::string_view phase) {
  const char* msg = svs_error_get_message(err);
  const std::string text =
      absl::StrCat("SVS ", phase, ": ", msg ? msg : "<no message>");
  switch (svs_error_get_code(err)) {
    case SVS_ERROR_INVALID_ARGUMENT:
      return absl::InvalidArgumentError(text);
    case SVS_ERROR_OUT_OF_MEMORY:
      return absl::ResourceExhaustedError(text);
    case SVS_ERROR_NOT_IMPLEMENTED:
    case SVS_ERROR_UNSUPPORTED_HW:
      return absl::UnimplementedError(text);
    default:
      return absl::InternalError(text);
  }
}

// Storage handle owned via a small RAII wrapper so early-return paths
// during Create() do not leak the SVS-owned resources.
struct StorageDeleter {
  void operator()(svs_storage_h h) const {
    if (h != nullptr) svs_storage_free(h);
  }
};
using StoragePtr =
    std::unique_ptr<std::remove_pointer_t<svs_storage_h>, StorageDeleter>;

struct AlgorithmDeleter {
  void operator()(svs_algorithm_h h) const {
    if (h != nullptr) svs_algorithm_free(h);
  }
};
using AlgorithmPtr =
    std::unique_ptr<std::remove_pointer_t<svs_algorithm_h>, AlgorithmDeleter>;

struct BuilderDeleter {
  void operator()(svs_index_builder_h h) const {
    if (h != nullptr) svs_index_builder_free(h);
  }
};
using BuilderPtr =
    std::unique_ptr<std::remove_pointer_t<svs_index_builder_h>, BuilderDeleter>;

struct SearchParamsDeleter {
  void operator()(svs_search_params_h h) const {
    if (h != nullptr) svs_search_params_free(h);
  }
};
using SearchParamsPtr =
    std::unique_ptr<std::remove_pointer_t<svs_search_params_h>,
                    SearchParamsDeleter>;

// Bridges the base-class filter predicate through the SVS C ABI.
// `self` carries the predicate object; is_member forwards each SVS
// candidate id through operator(); filter_rate hints selectivity to
// SVS's adaptive batch iterator.
extern "C" bool SvsFilterIsMember(void* self, size_t id) {
  auto* functor = static_cast<hnswlib::BaseFilterFunctor*>(self);
  return functor->operator()(static_cast<hnswlib::labeltype>(id));
}
extern "C" float SvsFilterRate(void* /*self*/) {
  // Return 0.0 to signal "no estimate". SVS's adaptive batch iterator
  // takes an empty-return early-exit when the observed hit rate is
  // less than the provided value; hinting 1.0 makes that condition
  // trip on every filter that rejects any candidate. Selectivity is
  // not plumbed into VectorBase::Search today, so 0.0 is the correct
  // conservative signal until the search planner threads its estimate
  // through.
  return 0.0f;
}
svs_id_filter_ops_t kSvsFilterOps =
    SVS_INIT_ID_FILTER_OPS(SvsFilterIsMember, SvsFilterRate);

// Materializes a const float* view of an n-element vector originally
// stored as T. For T=float, returns the source pointer directly and
// leaves scratch untouched. For other T, resizes scratch and fills it
// via the compiler's native narrowing cast. The SVS C API requires
// const float* regardless of the internal storage kind; any internal
// quantization happens inside SVS after this call.
template <typename T>
const float* MakeFp32(const T* src, size_t n_elements,
                      std::vector<float>& scratch) {
  if constexpr (std::is_same_v<T, float>) {
    (void)scratch;
    return src;
  } else {
    scratch.resize(n_elements);
    for (size_t i = 0; i < n_elements; ++i) {
      scratch[i] = static_cast<float>(src[i]);
    }
    return scratch.data();
  }
}

// Rejects a vector whose fp32 representation contains any non-finite
// component (NaN or +/-Inf). SVS's graph build and search spin on NaN
// distances; one bad vector pins the reader or writer thread for the
// life of the process. Checked at ingest and query entry points.
// Uses the IEEE-754 bit pattern (exponent == 0xFF) rather than
// std::isfinite because the indexes TU is built with -ffast-math,
// which lets the compiler fold isnan/isfinite to false.
template <typename T>
absl::Status CheckFiniteVector(const T* src, size_t n_elements) {
  for (size_t i = 0; i < n_elements; ++i) {
    const float v = static_cast<float>(src[i]);
    uint32_t bits;
    std::memcpy(&bits, &v, sizeof(bits));
    if ((bits & 0x7F800000u) == 0x7F800000u) {
      return absl::InvalidArgumentError(
          "SVS_VAMANA: vector contains non-finite component");
    }
  }
  return absl::OkStatus();
}

// Rejects non-finite vectors, then returns the fp32 view SVS ingests or
// searches with. When normalize is set (COSINE), the view is scaled to
// unit length. A zero vector has no direction and is left as zero, so its
// dot product with any vector is 0 and its score is 1.0.
template <typename T>
absl::StatusOr<const float*> PrepareSvsInput(const T* src, size_t n_elements,
                                             bool normalize,
                                             std::vector<float>& scratch) {
  VMSDK_RETURN_IF_ERROR(CheckFiniteVector<T>(src, n_elements));
  if (!normalize) {
    return MakeFp32<T>(src, n_elements, scratch);
  }
  const float reciprocal_magnitude =
      CalcReciprocalMagnitude<T>(src, n_elements);
  scratch.resize(n_elements);
  for (size_t i = 0; i < n_elements; ++i) {
    scratch[i] = static_cast<float>(src[i]) * reciprocal_magnitude;
  }
  return scratch.data();
}

// Converts an SVS search result to __v_score, where smaller is closer.
// SVS returns dot product as a similarity (larger is closer), so IP and
// COSINE report 1 - dot: 0 for an exact match on unit vectors, 1 for
// orthogonal or zero vectors. L2 is already a distance and is passed
// through.
float SvsToDistance(data_model::DistanceMetric metric, float value) {
  switch (metric) {
    case data_model::DISTANCE_METRIC_IP:
    case data_model::DISTANCE_METRIC_COSINE:
      return 1.0f - value;
    default:
      return value;
  }
}

// Block size that holds INITIAL_CAP vectors of the stored type, rounded up to
// a power of two (SVS requires one) and kept within the bounds above.
size_t SvsBlockSizeBytes(const SVSBuildConfig& config, int dimensions) {
  if (config.initial_cap == 0) return kSvsBlockSizeBytes;
  size_t element_bytes = sizeof(float);
  switch (CompressionToSvsDataType(config.compression, config.input_type)
              .value_or(SVS_DATA_TYPE_FLOAT32)) {
    case SVS_DATA_TYPE_FLOAT16:
      element_bytes = 2;
      break;
    case SVS_DATA_TYPE_INT8:
      element_bytes = 1;
      break;
    default:
      break;
  }
  const size_t wanted = size_t{config.initial_cap} *
                        static_cast<size_t>(dimensions) * element_bytes;
  return std::clamp(std::bit_ceil(wanted), kSvsMinBlockSizeBytes,
                    kSvsBlockSizeBytes);
}

// Algorithm/storage/builder trio shared by first-insert bootstrap and RDB
// load, so a loaded index is structurally identical to a freshly built one.
absl::StatusOr<BuilderPtr> AssembleIndexBuilder(
    const SVSBuildConfig& config, data_model::DistanceMetric distance_metric,
    int dimensions, svs_error_h err) {
  auto metric = ToSvsDistanceMetric(distance_metric);
  if (!metric.has_value()) {
    return absl::InvalidArgumentError(
        "SVS_VAMANA: unsupported DISTANCE_METRIC");
  }
  auto storage_type =
      CompressionToSvsDataType(config.compression, config.input_type);
  if (!storage_type.has_value()) {
    return absl::InvalidArgumentError(
        "SVS_VAMANA: COMPRESSION is proprietary / v2 and not available "
        "in this build");
  }

  AlgorithmPtr algo(svs_algorithm_create_vamana(
      config.graph_max_degree, config.construction_window_size,
      config.search_window_size, err));
  if (!algo) return SvsErrorToStatus(err, "algorithm_create_vamana");

  if (config.alpha > 0.0f) {
    if (!svs_algorithm_vamana_set_alpha(algo.get(), config.alpha, err)) {
      return SvsErrorToStatus(err, "algorithm_vamana_set_alpha");
    }
  }

  StoragePtr storage;
  if (config.compression == data_model::SVS_COMPRESSION_SQ8) {
    storage.reset(svs_storage_create_sq(*storage_type, err));
  } else {
    storage.reset(svs_storage_create_simple(*storage_type, err));
  }
  if (!storage) return SvsErrorToStatus(err, "storage_create");

  BuilderPtr builder(svs_index_builder_create(
      *metric, static_cast<size_t>(dimensions), algo.get(), err));
  if (!builder) return SvsErrorToStatus(err, "index_builder_create");

  if (!svs_index_builder_set_storage(builder.get(), storage.get(), err)) {
    return SvsErrorToStatus(err, "index_builder_set_storage");
  }
  if (!svs_index_builder_set_threadpool_custom(builder.get(),
                                               &kSvsThreadpoolIface, err)) {
    return SvsErrorToStatus(err, "index_builder_set_threadpool_custom");
  }
  if (ValkeyModule_IncrExternalMemory != nullptr &&
      ValkeyModule_DecrExternalMemory != nullptr) {
    if (!svs_index_builder_set_allocator_custom(builder.get(),
                                                &kSvsAllocatorIface, err)) {
      return SvsErrorToStatus(err, "index_builder_set_allocator_custom");
    }
  } else {
    if (!svs_index_builder_set_allocator(builder.get(),
                                         SVS_ALLOCATOR_KIND_SIMPLE, err)) {
      return SvsErrorToStatus(err, "index_builder_set_allocator");
    }
  }
  return builder;
}

// Builds the first svs_index on the first HSET after Create() left
// svs_index_ null. svs_index_build_dynamic requires num_vectors > 0.
absl::StatusOr<svs_index_h> BootstrapIndexImpl(
    const SVSBuildConfig& config, data_model::DistanceMetric distance_metric,
    int dimensions, uint64_t label, const float* fp32_vector) {
  ScopedSvsError err;
  auto builder =
      AssembleIndexBuilder(config, distance_metric, dimensions, err.get());
  if (!builder.ok()) return builder.status();

  svs_dynamic_index_params_t params = SVS_INIT_DYNAMIC_INDEX_PARAMS();
  params.blocksize_bytes = config.blocksize_bytes;
  params.sync_kind = kSvsSyncKind;
  svs_index_h index =
      svs_index_build_dynamic_ex(builder->get(), fp32_vector, &label,
                                 /*num_vectors=*/1, &params, err.get());
  if (index == nullptr) {
    return SvsErrorToStatus(err.get(), "index_build_dynamic_ex");
  }
  return index;
}

// Counts every failure once in svs_create_exceptions_cnt (INFO).
absl::StatusOr<svs_index_h> BootstrapIndex(
    const SVSBuildConfig& config, data_model::DistanceMetric distance_metric,
    int dimensions, uint64_t label, const float* fp32_vector) {
  auto result = BootstrapIndexImpl(config, distance_metric, dimensions, label,
                                   fp32_vector);
  if (!result.ok()) {
    ++Metrics::GetStats().svs_create_exceptions_cnt;
  }
  return result;
}

// Compares the v1 fields that determine SVS's on-disk layout
bool BuildConfigMatches(const SVSBuildConfig& expected,
                        const data_model::SVSVamanaAlgorithm& header_config) {
  return expected.graph_max_degree == header_config.graph_max_degree() &&
         expected.construction_window_size ==
             header_config.construction_window_size() &&
         expected.alpha == header_config.alpha() &&
         expected.compression == header_config.compression();
}

}  // namespace

#endif  // __linux__ && __x86_64__

template <typename T>
VectorSVS<T>::VectorSVS(int dimensions, absl::string_view attribute_identifier,
                        data_model::AttributeDataType attribute_data_type,
                        int db_num)
    : VectorType<T>(IndexerType::kSVS, dimensions, attribute_data_type,
                    attribute_identifier, db_num) {}

template <typename T>
VectorSVS<T>::~VectorSVS() {
#if defined(__linux__) && defined(__x86_64__)
  if (svs_index_ != nullptr) {
    svs_index_free(svs_index_);
    svs_index_ = nullptr;
  }
#endif
}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorSVS<T>>> VectorSVS<T>::Create(
    const data_model::VectorIndex& vector_index_proto,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type, int db_num) {
#if defined(__linux__) && defined(__x86_64__)
  if (!ToSvsDistanceMetric(vector_index_proto.distance_metric()).has_value()) {
    return absl::InvalidArgumentError(
        "SVS_VAMANA: unsupported DISTANCE_METRIC");
  }
  const auto& svs_proto = vector_index_proto.svs_vamana_algorithm();
  if (!CompressionToSvsDataType(svs_proto.compression()).has_value()) {
    return absl::InvalidArgumentError(
        "SVS_VAMANA: COMPRESSION is proprietary / v2 and not available in "
        "this build");
  }

  auto instance = std::shared_ptr<VectorSVS<T>>(
      new VectorSVS<T>(vector_index_proto.dimension_count(),
                       attribute_identifier, attribute_data_type, db_num),
      vmsdk::DestructByMainThread<VectorSVS<T>>{});
  instance->Init(vector_index_proto.distance_metric());
  instance->build_config_ = SVSBuildConfig{
      svs_proto.graph_max_degree(),   svs_proto.construction_window_size(),
      svs_proto.search_window_size(), svs_proto.alpha(),
      svs_proto.compression(),
  };
  instance->build_config_.input_type = instance->GetVectorDataType();
  instance->build_config_.initial_cap = vector_index_proto.initial_cap();
  instance->build_config_.blocksize_bytes =
      SvsBlockSizeBytes(instance->build_config_, instance->dimensions_);
  // SQ8 calibrates its range once, from the vectors it compresses, so the
  // index starts uncompressed and converts at the training threshold.
  if (svs_proto.compression() == data_model::SVS_COMPRESSION_SQ8) {
    instance->build_config_.training_threshold = svs_proto.training_threshold();
    instance->compression_pending_.store(
        instance->build_config_.training_threshold > 0,
        std::memory_order_relaxed);
  }
  // svs_index_ stays null until the first HSET bootstraps it via
  // svs_index_build_dynamic. The SVS C API requires num_vectors > 0 at
  // build time and offers no create-empty entry point.
  return instance;
#else
  (void)vector_index_proto;
  (void)attribute_identifier;
  (void)attribute_data_type;
  (void)db_num;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorSVS<T>>> VectorSVS<T>::LoadFromRDB(
    ValkeyModuleCtx* /*ctx*/, const AttributeDataType* attribute_data_type,
    const data_model::VectorIndex& vector_index_proto,
    absl::string_view attribute_identifier, SupplementalContentChunkIter&& iter,
    int db_num) {
#if defined(__linux__) && defined(__x86_64__)
  const absl::Time load_start = absl::Now();
  RDBChunkInputStream input(std::move(iter));
  // On success the terminator invariant already leaves input drained; on
  // any early return below it is not, which aborts the destructor's DCHECK
  // in debug and corrupts later RDB sections in release (design.md "The
  // terminator invariant").
  auto load = [&]() -> absl::StatusOr<std::shared_ptr<VectorSVS<T>>> {
    VMSDK_ASSIGN_OR_RETURN(auto serialized_header, input.LoadChunk());
    data_model::SVSIndexHeader header;
    if (!header.ParseFromString(*serialized_header)) {
      return absl::InternalError("Could not deserialize SVS index header");
    }

    if (header.format_version() != kSvsHeaderFormatVersion) {
      return absl::InvalidArgumentError(absl::StrCat(
          "SVS_VAMANA RDB header format_version mismatch: expected ",
          kSvsHeaderFormatVersion, ", got ", header.format_version()));
    }
    // A payload written by a different SVS version isn't guaranteed to load,
    // so it's skipped and the index is rebuilt from the keyspace once the
    // tracked keys are loaded (OnTrackedKeysLoaded).
    const uint32_t current_svs_version = svs_get_version();
    const bool svs_version_mismatch =
        header.svs_version() != current_svs_version;

    if (header.dimensionality() != vector_index_proto.dimension_count()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "SVS_VAMANA RDB header dimensionality mismatch: expected ",
          vector_index_proto.dimension_count(), ", got ",
          header.dimensionality()));
    }

    VMSDK_ASSIGN_OR_RETURN(auto instance,
                           Create(vector_index_proto, attribute_identifier,
                                  attribute_data_type->ToProto(), db_num));
    if (!BuildConfigMatches(instance->build_config_, header.build_config())) {
      return absl::InvalidArgumentError(
          "SVS_VAMANA RDB header build_config does not match the index "
          "definition");
    }
    if (header.element_type() != instance->GetVectorDataType()) {
      return absl::InvalidArgumentError(absl::StrCat(
          "SVS_VAMANA RDB header element_type mismatch: expected ",
          instance->GetVectorDataType(), ", got ", header.element_type()));
    }
    if (svs_version_mismatch) {
      VMSDK_LOG(WARNING, nullptr)
          << "SVS_VAMANA index " << attribute_identifier
          << " was saved with SVS version " << header.svs_version()
          << "; this build is SVS version " << current_svs_version
          << ". Rebuilding it from the keyspace.";
      instance->rebuild_from_keyspace_ = header.has_index();
      return instance;
    }
    if (!header.has_index()) {
      // Handle-less empty index (design.md "Empty index"): svs_index_ stays
      // null and bootstraps on the first HSET, same as a fresh Create().
      // A correct empty save writes only the header, so the chunk lookahead
      // already leaves the stream at end; anything left is format drift.
      if (!input.AtEnd()) {
        return absl::InvalidArgumentError(
            "SVS_VAMANA RDB load: payload chunks remain after a header with "
            "has_index=false");
      }
      return instance;
    }

    if (header.compression_pending() &&
        instance->build_config_.training_threshold == 0) {
      return absl::InvalidArgumentError(
          "SVS_VAMANA RDB header has compression_pending for an index "
          "without a training threshold");
    }
    instance->compression_pending_.store(header.compression_pending(),
                                         std::memory_order_relaxed);
    const SVSBuildConfig storage_config = instance->StorageConfig();
    SvsRdbInputStream adapter(input);
    svs_stream_interface iface = adapter.AsInterface();
    ScopedSvsError err;
    auto builder = AssembleIndexBuilder(storage_config,
                                        vector_index_proto.distance_metric(),
                                        instance->dimensions_, err.get());
    if (!builder.ok()) return builder.status();

    svs_index_h index = svs_index_load_stream_dynamic(
        builder->get(), &iface, storage_config.blocksize_bytes, err.get());
    if (index == nullptr) {
      if (!adapter.status().ok()) return adapter.status();
      return SvsErrorToStatus(err.get(), "index_load_stream_dynamic");
    }
    // Hand the handle to instance before the check below, so ~VectorSVS owns
    // it on every path out of here and no path needs its own free.
    {
      absl::WriterMutexLock lock(&instance->resize_mutex_);
      instance->svs_index_ = index;
    }
    // The stream load has no parameter form at this pin, so the restored
    // index reports SVS_SYNC_KIND_NONE and cannot guard concurrent writers;
    // keep its mutations exclusive (serialized by resize_mutex_, as before).
    svs_sync_kind_t sync_kind = SVS_SYNC_KIND_NONE;
    if (!svs_index_dynamic_get_sync_kind(index, &sync_kind, err.get())) {
      return SvsErrorToStatus(err.get(), "index_dynamic_get_sync_kind");
    }
    instance->mutations_exclusive_.store(sync_kind == SVS_SYNC_KIND_NONE,
                                         std::memory_order_release);

    // Terminator invariant (design.md): consumed_sentinel() implies AtEnd();
    // AtEnd() == false either way means SVS stopped before the payload end.
    DCHECK(!adapter.consumed_sentinel() || input.AtEnd());
    if (!input.AtEnd()) {
      return absl::InvalidArgumentError(
          "SVS_VAMANA RDB load: payload chunks remain after SVS stopped "
          "reading the stream");
    }
    return instance;
  };
  auto result = load();
  while (!input.AtEnd() && input.LoadChunk().ok()) {
  }
  if (!result.ok()) {
    ++Metrics::GetStats().svs_create_exceptions_cnt;
    return result;
  }
  // A skipped payload is reported when the index is rebuilt instead. The C
  // API reports no load-validation warnings or index-growth events (SVS grows
  // by allocating blocks), so neither is logged.
  if (!(*result)->rebuild_from_keyspace_) {
    size_t num_elements = 0;
    {
      absl::ReaderMutexLock lock(&(*result)->resize_mutex_);
      if ((*result)->svs_index_ != nullptr) {
        svs_index_get_size((*result)->svs_index_, &num_elements, nullptr);
      }
    }
    VMSDK_LOG(NOTICE, nullptr)
        << "SVS index loaded: attribute=" << attribute_identifier
        << ", num_elements=" << num_elements << ", compression="
        << data_model::SVSCompressionType_Name(
               (*result)->build_config_.compression)
        << ", load_ms=" << absl::ToInt64Milliseconds(absl::Now() - load_start);
  }
  return result;
#else
  (void)attribute_data_type;
  (void)vector_index_proto;
  (void)attribute_identifier;
  (void)iter;
  (void)db_num;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
bool VectorSVS<T>::IsValidVectorData(absl::string_view record) const {
#if defined(__linux__) && defined(__x86_64__)
  return CheckFiniteVector<T>(reinterpret_cast<const T*>(record.data()),
                              record.size() / sizeof(T))
      .ok();
#else
  (void)record;
  return true;
#endif
}

template <typename T>
size_t VectorSVS<T>::GetCapacity() const {
  return build_config_.initial_cap;
}

#if defined(__linux__) && defined(__x86_64__)
template <typename T>
absl::StatusOr<std::priority_queue<std::pair<float, hnswlib::labeltype>>>
VectorSVS<T>::SearchKnn(absl::string_view query, uint64_t count,
                        hnswlib::BaseFilterFunctor* filter,
                        std::optional<size_t> ef_runtime) {
  std::priority_queue<std::pair<float, hnswlib::labeltype>> knn;
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(
        absl::StrCat("SVS_VAMANA search: query vector blob size (",
                     query.size(), ") does not match index's expected size (",
                     dimensions_ * GetDataTypeSize(), ")."));
  }

  // Validated before the early returns, so a non-finite query is rejected
  // whether or not the index holds vectors.
  const T* raw = reinterpret_cast<const T*>(query.data());
  std::vector<float> scratch;
  VMSDK_ASSIGN_OR_RETURN(
      const float* fp32_query,
      PrepareSvsInput<T>(raw, static_cast<size_t>(dimensions_), normalize_,
                         scratch));

  absl::ReaderMutexLock lock(&resize_mutex_);
  if (svs_index_ == nullptr) {
    return knn;
  }
  size_t live = 0;
  {
    absl::ReaderMutexLock map_lock(&label_mutex_);
    live = label_to_record_.size();
  }
  const size_t k = std::min<size_t>(count, live);
  if (k == 0) {
    return knn;
  }

  ScopedSvsError err;
  const size_t effective_window = std::max<size_t>(
      k, ef_runtime.value_or(build_config_.search_window_size));
  SearchParamsPtr search_params(
      svs_search_params_create_vamana(effective_window, err.get()));
  if (!search_params) {
    ++Metrics::GetStats().svs_search_exceptions_cnt;
    return SvsErrorToStatus(err.get(), "search_params_create_vamana");
  }

  svs_id_filter_t filter_iface{&kSvsFilterOps, filter};
  svs_id_filter_i filter_arg = filter ? &filter_iface : nullptr;

  svs_search_results_t results = SVS_INIT_SEARCH_RESULTS();
  auto results_cleanup =
      absl::MakeCleanup([&results]() { svs_search_results_free(&results); });

  if (!svs_index_search_topk(svs_index_, fp32_query, /*num_queries=*/1, k,
                             &results, search_params.get(), filter_arg,
                             err.get())) {
    ++Metrics::GetStats().svs_search_exceptions_cnt;
    return SvsErrorToStatus(err.get(), "index_search_topk");
  }

  // Callers rank and compare against a radius by score, so every result must
  // be a distance.
  for (size_t i = 0; i < results.total_results; ++i) {
    knn.push({SvsToDistance(this->distance_metric_, results.distances[i]),
              static_cast<hnswlib::labeltype>(results.indices[i])});
  }
  return knn;
}
#endif

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(
    absl::string_view query, uint64_t count, cancel::Token& cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter,
    std::optional<size_t> ef_runtime, bool enable_partial_results) {
#if defined(__linux__) && defined(__x86_64__)
  VMSDK_ASSIGN_OR_RETURN(auto knn,
                         SearchKnn(query, count, filter.get(), ef_runtime));
  // SVS can't be cancelled mid-search and polls the token too rarely for its
  // periodic deadline check, so check the deadline now that it has returned.
  // Without partial results, a timed-out query reports the timeout.
  if (!enable_partial_results && cancellation_token->IsCancelledNow()) {
    return absl::CancelledError(query::kTimeoutMsg);
  }
  return this->CreateReply(knn);
#else
  (void)query;
  (void)count;
  (void)cancellation_token;
  (void)filter;
  (void)ef_runtime;
  (void)enable_partial_results;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

// Fetches up to search.max-nonvector-search-results-fetched nearest
// neighbors and keeps those within the radius. When the fetch reaches the cap
// with its farthest candidate still in range, keys may be missing, so every
// vector is scanned exactly instead.
template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::SearchRange(
    absl::string_view query, float radius, cancel::Token& cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter) {
#if defined(__linux__) && defined(__x86_64__)
  const size_t max_candidates = static_cast<size_t>(
      options::GetMaxNonVectorSearchResultsFetched().GetValue());
  VMSDK_ASSIGN_OR_RETURN(
      auto knn, SearchKnn(query, max_candidates, filter.get(), std::nullopt));

  const bool fetch_full = knn.size() >= max_candidates;
  if (fetch_full &&
      (knn.empty() || this->ClampCosineDistance(knn.top().first) <= radius)) {
    query::RecordNonVectorResultsFetchedLimited();
    return this->SearchRangeExhaustive(query, radius, cancellation_token,
                                       filter.get());
  }

  std::vector<Neighbor> neighbors;
  neighbors.reserve(knn.size());
  for (; !knn.empty(); knn.pop()) {
    if (cancellation_token->IsCancelled()) {
      break;
    }
    const auto [distance, label] = knn.top();
    const float clamped = this->ClampCosineDistance(distance);
    if (clamped > radius) {
      continue;
    }
    auto key = this->GetKeyDuringSearch(label);
    if (key.ok()) {
      neighbors.emplace_back(*key, clamped);
    }
  }
  return neighbors;
#else
  (void)query;
  (void)radius;
  (void)cancellation_token;
  (void)filter;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::Status VectorSVS<T>::AddRecordImpl(uint64_t internal_id,
                                         VectorRecord&& vector_record) {
#if defined(__linux__) && defined(__x86_64__)
  const T* raw = reinterpret_cast<const T*>(vector_record.GetRawVector());
  std::vector<float> scratch;
  VMSDK_ASSIGN_OR_RETURN(
      const float* fp32,
      PrepareSvsInput<T>(raw, static_cast<size_t>(dimensions_), normalize_,
                         scratch));

  // Reserve the map slot first so a failed SVS mutation can erase it and
  // the two stay in step; no other mutation touches this id meanwhile.
  {
    absl::MutexLock map_lock(&label_mutex_);
    auto [it, inserted] = label_to_record_.try_emplace(internal_id, nullptr);
    if (!inserted) {
      return absl::AlreadyExistsError(
          absl::StrCat("SVS internal_id already present: ", internal_id));
    }
  }
  // Test hook: the map holds the id, SVS does not yet. No lock is held here.
  BACKGROUND_PAUSEPOINT("svs_add_after_map_reserve");
  absl::Status status = InsertVector(internal_id, fp32);
  {
    absl::MutexLock map_lock(&label_mutex_);
    if (!status.ok()) {
      label_to_record_.erase(internal_id);
      return status;
    }
    label_to_record_[internal_id] = std::move(vector_record);
  }
  MaybeConvert();
  return absl::OkStatus();
#else
  (void)internal_id;
  (void)vector_record;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::Status VectorSVS<T>::RemoveRecordImpl(uint64_t internal_id) {
#if defined(__linux__) && defined(__x86_64__)
  {
    absl::ReaderMutexLock map_lock(&label_mutex_);
    if (!label_to_record_.contains(internal_id)) {
      return absl::NotFoundError(
          absl::StrCat("SVS internal_id not found: ", internal_id));
    }
  }
  absl::Status svs_status = absl::OkStatus();
  {
    SvsMutationLock lock(&resize_mutex_,
                         mutations_exclusive_.load(std::memory_order_acquire));
    if (svs_index_ != nullptr) {
      ScopedSvsError err;
      size_t deleted = 0;
      if (!svs_index_dynamic_delete_points(svs_index_, &internal_id,
                                           /*num_vectors=*/1, &deleted,
                                           err.get())) {
        ++Metrics::GetStats().svs_remove_exceptions_cnt;
        svs_status = SvsErrorToStatus(err.get(), "index_dynamic_delete_points");
      }
      deleted_since_consolidate_.fetch_add(deleted, std::memory_order_relaxed);
    }
  }
  // Test hook: SVS no longer holds the id, the map still does. No lock is
  // held here.
  BACKGROUND_PAUSEPOINT("svs_remove_after_svs_delete");
  // Erase the authoritative label mapping regardless of the SVS
  // delete outcome. If SVS reports failure, its internal state for
  // this id is unknown; dropping the map entry ensures search paths
  // that resolve labels through the map first cannot surface a stale
  // id, and a subsequent re-Add on the same id will not collide with
  // the reservation in AddRecordImpl.
  {
    absl::MutexLock map_lock(&label_mutex_);
    label_to_record_.erase(internal_id);
  }
  MaybeConsolidate();
  return svs_status;
#else
  (void)internal_id;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::Status VectorSVS<T>::ModifyRecordImpl(uint64_t internal_id,
                                            VectorRecord&& vector_record) {
#if defined(__linux__) && defined(__x86_64__)
  // No replace primitive in the C API and add_points rejects a duplicate id,
  // so modify is delete then add; the gap is invisible to search (phase gate).
  const T* raw = reinterpret_cast<const T*>(vector_record.GetRawVector());
  std::vector<float> scratch;
  VMSDK_ASSIGN_OR_RETURN(
      const float* fp32,
      PrepareSvsInput<T>(raw, static_cast<size_t>(dimensions_), normalize_,
                         scratch));

  bool replaced = false;
  {
    SvsMutationLock lock(&resize_mutex_,
                         mutations_exclusive_.load(std::memory_order_acquire));
    if (svs_index_ != nullptr) {
      ScopedSvsError err;
      size_t deleted = 0;
      if (!svs_index_dynamic_delete_points(svs_index_, &internal_id,
                                           /*num_vectors=*/1, &deleted,
                                           err.get())) {
        ++Metrics::GetStats().svs_modify_exceptions_cnt;
        return SvsErrorToStatus(err.get(),
                                "modify: index_dynamic_delete_points");
      }
      deleted_since_consolidate_.fetch_add(deleted, std::memory_order_relaxed);
      size_t added = 0;
      if (!svs_index_dynamic_add_points(svs_index_, fp32, &internal_id,
                                        /*num_vectors=*/1, &added, err.get())) {
        // Delete already committed inside SVS; the id is gone from the
        // index and cannot be resurrected here (add just failed). Drop
        // the stale map entry so the two views agree — the base class's
        // RemoveRecordDueToError path will then NotFoundError out
        // cleanly instead of hitting a phantom.
        ++Metrics::GetStats().svs_modify_exceptions_cnt;
        absl::MutexLock map_lock(&label_mutex_);
        label_to_record_.erase(internal_id);
        return SvsErrorToStatus(err.get(), "modify: index_dynamic_add_points");
      }
      {
        absl::MutexLock map_lock(&label_mutex_);
        label_to_record_[internal_id] = std::move(vector_record);
      }
      replaced = true;
    }
  }
  if (replaced) {
    MaybeConsolidate();
    return absl::OkStatus();
  }
  // No handle yet: the modify is the first add.
  VMSDK_RETURN_IF_ERROR(InsertVector(internal_id, fp32));
  absl::MutexLock map_lock(&label_mutex_);
  label_to_record_[internal_id] = std::move(vector_record);
  return absl::OkStatus();
#else
  (void)internal_id;
  (void)vector_record;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

#if defined(__linux__) && defined(__x86_64__)
template <typename T>
absl::Status VectorSVS<T>::InsertVector(uint64_t internal_id,
                                        const float* fp32) {
  {
    SvsMutationLock lock(&resize_mutex_,
                         mutations_exclusive_.load(std::memory_order_acquire));
    if (svs_index_ != nullptr) {
      return AddPoint(internal_id, fp32);
    }
  }
  // No handle yet: re-check under the writer lock so racing first adds
  // build exactly once.
  absl::WriterMutexLock lock(&resize_mutex_);
  if (svs_index_ != nullptr) {
    return AddPoint(internal_id, fp32);
  }
  auto result = BootstrapIndex(StorageConfig(), this->distance_metric_,
                               dimensions_, internal_id, fp32);
  if (!result.ok()) return result.status();
  svs_index_ = *result;
  return absl::OkStatus();
}

template <typename T>
void VectorSVS<T>::MaybeConsolidate() {
  const size_t live = GetLabelCount();
  auto due = [this, live]() {
    return deleted_since_consolidate_.load(std::memory_order_relaxed) >=
           std::max(kConsolidateMinDeleted, live / kConsolidateLiveDivisor);
  };
  if (!due()) return;
  // Consolidation rewrites graph edges, so it excludes the other writers;
  // searches are already excluded by the mutation phase.
  absl::WriterMutexLock lock(&resize_mutex_);
  if (svs_index_ == nullptr || !due()) return;
  ScopedSvsError err;
  if (!svs_index_dynamic_consolidate(svs_index_, err.get())) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 10)
        << "SVS_VAMANA consolidate failed: "
        << SvsErrorToStatus(err.get(), "index_dynamic_consolidate");
    return;
  }
  // The C API's add_points only fills slots past the last compaction, so the
  // slots consolidate freed are reused only after compacting. SVS can't
  // compact an index with no live entries.
  size_t live_in_svs = 0;
  if (svs_index_get_size(svs_index_, &live_in_svs, err.get()) &&
      live_in_svs > 0 &&
      !svs_index_dynamic_compact(svs_index_, /*batchsize=*/0, err.get())) {
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 10)
        << "SVS_VAMANA compact failed: "
        << SvsErrorToStatus(err.get(), "index_dynamic_compact");
    return;
  }
  deleted_since_consolidate_.store(0, std::memory_order_relaxed);
}

template <typename T>
SVSBuildConfig VectorSVS<T>::StorageConfig() const {
  if (!compression_pending_.load(std::memory_order_acquire)) {
    return build_config_;
  }
  SVSBuildConfig config = build_config_;
  config.compression = data_model::SVS_COMPRESSION_NONE;
  config.blocksize_bytes = SvsBlockSizeBytes(config, dimensions_);
  return config;
}

template <typename T>
void VectorSVS<T>::MaybeConvert() {
  // The label map also holds adds still in flight, so it only says a
  // conversion may be due; SVS's own count decides, under the lock.
  if (!compression_pending_.load(std::memory_order_acquire) ||
      GetLabelCount() < build_config_.training_threshold) {
    return;
  }
  // Conversion copies the index, so it excludes writers; searches are already
  // excluded by the mutation phase.
  absl::WriterMutexLock lock(&resize_mutex_);
  if (svs_index_ == nullptr ||
      !compression_pending_.load(std::memory_order_relaxed)) {
    return;
  }
  ScopedSvsError err;
  size_t in_svs = 0;
  if (!svs_index_get_size(svs_index_, &in_svs, err.get()) ||
      in_svs < build_config_.training_threshold) {
    return;
  }
  auto builder = AssembleIndexBuilder(build_config_, this->distance_metric_,
                                      dimensions_, err.get());
  svs_index_h converted = nullptr;
  if (builder.ok()) {
    svs_dynamic_index_params_t params = SVS_INIT_DYNAMIC_INDEX_PARAMS();
    params.blocksize_bytes = build_config_.blocksize_bytes;
    params.sync_kind = kSvsSyncKind;
    converted = svs_index_convert_dynamic_ex(builder->get(), svs_index_,
                                             &params, err.get());
  }
  if (converted == nullptr) {
    ++Metrics::GetStats().svs_conversion_failures_cnt;
    VMSDK_LOG_EVERY_N_SEC(WARNING, nullptr, 10)
        << "SVS_VAMANA compression failed; the index stays uncompressed: "
        << (builder.ok() ? SvsErrorToStatus(err.get(), "index_convert_dynamic")
                         : builder.status());
    return;
  }
  svs_index_free(svs_index_);
  svs_index_ = converted;
  ++Metrics::GetStats().svs_conversions_cnt;
  compression_pending_.store(false, std::memory_order_release);
  // The converted index is built with kSvsSyncKind, even after an RDB load.
  mutations_exclusive_.store(false, std::memory_order_release);
}

template <typename T>
absl::Status VectorSVS<T>::AddPoint(uint64_t internal_id, const float* fp32) {
  ScopedSvsError err;
  size_t added = 0;
  if (!svs_index_dynamic_add_points(svs_index_, fp32, &internal_id,
                                    /*num_vectors=*/1, &added, err.get())) {
    ++Metrics::GetStats().svs_add_exceptions_cnt;
    return SvsErrorToStatus(err.get(), "index_dynamic_add_points");
  }
  return absl::OkStatus();
}
#endif  // __linux__ && __x86_64__

template <typename T>
void VectorSVS<T>::ToProtoImpl(
    data_model::VectorIndex* vector_index_proto) const {
  this->SetProtoDataType(vector_index_proto);
  auto svs_proto = std::make_unique<data_model::SVSVamanaAlgorithm>();
  FillSvsVamanaProto(build_config_, svs_proto.get());
  vector_index_proto->set_allocated_svs_vamana_algorithm(svs_proto.release());
}

template <typename T>
int VectorSVS<T>::RespondWithInfoImpl(ValkeyModuleCtx* ctx) const {
  EmitDataTypeInfo(ctx);
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  const bool trained = build_config_.training_threshold > 0;
  ValkeyModule_ReplyWithArray(ctx, trained ? 18 : 14);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx, std::string(
               LookupKeyByValue(
                   *kVectorAlgoByStr,
                   data_model::VectorIndex::AlgorithmCase::kSvsVamanaAlgorithm))
               .c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "graph_max_degree");
  ValkeyModule_ReplyWithLongLong(ctx, build_config_.graph_max_degree);
  ValkeyModule_ReplyWithSimpleString(ctx, "construction_window_size");
  ValkeyModule_ReplyWithLongLong(ctx, build_config_.construction_window_size);
  ValkeyModule_ReplyWithSimpleString(ctx, "search_window_size");
  ValkeyModule_ReplyWithLongLong(ctx, build_config_.search_window_size);
  ValkeyModule_ReplyWithSimpleString(ctx, "alpha");
  ValkeyModule_ReplyWithDouble(
      ctx, EffectiveAlpha(build_config_, this->distance_metric_));
  ValkeyModule_ReplyWithSimpleString(ctx, "compression");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      data_model::SVSCompressionType_Name(build_config_.compression).c_str());
  if (trained) {
    ValkeyModule_ReplyWithSimpleString(ctx, "training_threshold");
    ValkeyModule_ReplyWithLongLong(ctx, build_config_.training_threshold);
    ValkeyModule_ReplyWithSimpleString(ctx, "compression_pending");
    ValkeyModule_ReplyWithLongLong(
        ctx, compression_pending_.load(std::memory_order_acquire) ? 1 : 0);
  }
  // Bytes SVS holds for the index's graph, data and metadata; 0 before the
  // first add.
  size_t memory_bytes = 0;
  {
    absl::ReaderMutexLock lock(&resize_mutex_);
    if (svs_index_ != nullptr) {
      svs_index_get_memory_usage(svs_index_, &memory_bytes, nullptr);
    }
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "memory_bytes");
  ValkeyModule_ReplyWithLongLong(ctx, static_cast<long long>(memory_bytes));
  return 4;
}

template <typename T>
absl::Status VectorSVS<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
#if defined(__linux__) && defined(__x86_64__)
  // Writer, not reader: svs_index_save_stream consolidates and compacts the
  // live index before serializing, so a foreground save must exclude
  // searches. Safe in the fork child, where suspended pools hold no locks.
  absl::WriterMutexLock lock(&resize_mutex_);

  // Deletes only mark SVS slots, so a handle can outlive its last live
  // entry; SVS's save cannot compact an index with none, so save it as the
  // handle-less empty index and bootstrap afresh on the next write. Ask SVS
  // for its live count: writers update label_to_record_ outside
  // resize_mutex_, so the map can disagree with SVS here.
  size_t live_entries = 0;
  if (svs_index_ != nullptr) {
    ScopedSvsError size_err;
    if (!svs_index_get_size(svs_index_, &live_entries, size_err.get())) {
      return SvsErrorToStatus(size_err.get(), "index_get_size");
    }
  }
  const bool has_index = live_entries > 0;

  data_model::SVSIndexHeader header;
  header.set_format_version(kSvsHeaderFormatVersion);
  header.set_svs_version(svs_get_version());
  header.set_has_index(has_index);
  header.set_compression_pending(
      has_index && compression_pending_.load(std::memory_order_relaxed));
  header.set_element_type(GetVectorDataType());
  header.set_dimensionality(static_cast<uint32_t>(dimensions_));
  FillSvsVamanaProto(build_config_, header.mutable_build_config());

  std::string serialized;
  if (!header.SerializeToString(&serialized)) {
    return absl::InternalError("Could not serialize SVS index header");
  }
  // Own RDB chunk, never bytes prepended to the SVS stream: SVS pads
  // from tellp() at its stream's start, so a prefix would shift alignment.
  VMSDK_RETURN_IF_ERROR(chunked_out.SaveString(serialized));

  if (!has_index) {
    // Handle-less empty index: nothing to hand to SVS. chunked_out's
    // destructor still emits the EOF sentinel below.
    return absl::OkStatus();
  }

  SvsRdbOutputStream adapter(chunked_out);
  svs_stream_interface iface = adapter.AsInterface();
  ScopedSvsError err;
  if (!svs_index_save_stream(svs_index_, &iface, err.get())) {
    if (!adapter.status().ok()) return adapter.status();
    return SvsErrorToStatus(err.get(), "index_save_stream");
  }
  // The save consolidated and compacted the index.
  deleted_since_consolidate_.store(0, std::memory_order_relaxed);
  return absl::OkStatus();
#else
  (void)chunked_out;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
float VectorSVS<T>::ComputeDistance(absl::string_view query,
                                    const VectorRecord& vector_record,
                                    float query_magnitude) const {
  // Distance evaluation uses the space populated by VectorType::Init
  // from the configured distance metric. This mirrors the raw-bytes
  // distance path the base class uses for pre-filter scoring and does
  // not require the SVS index handle.
  return space_->get_dist_func()(query.data(), vector_record.GetRawVector(),
                                 space_->get_dist_func_param(),
                                 query_magnitude);
}

template <typename T>
VectorRecord& VectorSVS<T>::GetVectorLockFree(uint64_t internal_id) const {
  auto it = label_to_record_.find(internal_id);
  CHECK(it != label_to_record_.end())
      << "SVS internal_id not found: " << internal_id;
  return it->second;
}

template <typename T>
VectorRecord& VectorSVS<T>::GetOrCreateVectorLockFree(uint64_t internal_id) {
  // Load-time only (main thread, index not yet published); label_mutex_ is
  // uncontended here and keeps the header's lock contract true.
  absl::MutexLock map_lock(&label_mutex_);
  auto [it, inserted] = label_to_record_.try_emplace(internal_id, nullptr);
  if (!inserted) {
    ++Metrics::GetStats().svs_duplicate_label_on_load_cnt;
  }
  return it->second;
}

template <typename T>
VectorRecord& VectorSVS<T>::GetVector(uint64_t internal_id) const {
  absl::ReaderMutexLock lock(&label_mutex_);
  return GetVectorLockFree(internal_id);
}

template <typename T>
std::optional<hnswlib::tableint> VectorSVS<T>::GetAlgoIdLockFree(
    uint64_t /*internal_id*/) const {
  // SVS addresses vectors by the external id the caller supplies and
  // exposes no distinct algo-side integer identifier. The uint64_t id
  // would also narrow through this return type at large scale. Return
  // nullopt so no caller acts on a synthesized value; existence checks
  // against a VectorSVS use GetVector / label_to_record_ directly.
  return std::nullopt;
}

template <typename T>
absl::Status VectorSVS<T>::OnTrackedKeysLoaded() {
#if defined(__linux__) && defined(__x86_64__)
  absl::Cleanup convert = [this]() { MaybeConvert(); };
  if (!rebuild_from_keyspace_) {
    return absl::OkStatus();
  }
  rebuild_from_keyspace_ = false;
  std::vector<std::pair<uint64_t, VectorRecord>> records;
  {
    absl::ReaderMutexLock map_lock(&label_mutex_);
    records.reserve(label_to_record_.size());
    for (const auto& [id, record] : label_to_record_) {
      if (record) records.emplace_back(id, record);
    }
  }
  std::vector<float> scratch;
  for (const auto& [id, record] : records) {
    VMSDK_ASSIGN_OR_RETURN(
        const float* fp32,
        PrepareSvsInput<T>(reinterpret_cast<const T*>(record.GetRawVector()),
                           static_cast<size_t>(dimensions_), normalize_,
                           scratch));
    VMSDK_RETURN_IF_ERROR(InsertVector(id, fp32));
  }
  VMSDK_LOG(NOTICE, nullptr) << "SVS_VAMANA index rebuilt from "
                             << records.size() << " vectors in the keyspace.";
#endif
  return absl::OkStatus();
}

template <typename T>
uint64_t VectorSVS<T>::GetMaxLoadedLabel() const {
  absl::ReaderMutexLock lock(&label_mutex_);
  uint64_t max_label = 0;
  for (const auto& [id, _] : label_to_record_) {
    if (id > max_label) max_label = id;
  }
  return max_label;
}

template <typename T>
size_t VectorSVS<T>::GetLabelCount() const {
  absl::ReaderMutexLock lock(&label_mutex_);
  return label_to_record_.size();
}

template class VectorSVS<float>;
template class VectorSVS<float16>;
template class VectorSVS<bfloat16>;

}  // namespace valkey_search::indexes
