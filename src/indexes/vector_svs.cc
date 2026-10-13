/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_svs.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <queue>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswlib.h"
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
#include "vmsdk/src/utils.h"
#endif

namespace valkey_search::indexes {

namespace {
constexpr absl::string_view kUnavailableMsg =
    "SVS_VAMANA is not available on this build (requires x86_64 Linux).";

// Shared by ToProtoImpl and the RDB header's build_config echo, so the two
// never drift apart. No SVS dependency: safe to call on any platform.
void FillSvsVamanaProto(const SVSBuildConfig& config,
                        data_model::SVSVamanaAlgorithm* proto) {
  proto->set_graph_max_degree(config.graph_max_degree);
  proto->set_construction_window_size(config.construction_window_size);
  proto->set_search_window_size(config.search_window_size);
  proto->set_alpha(config.alpha);
  proto->set_compression(config.compression);
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

svs_threadpool_ops_t kSvsThreadpoolOps =
    SVS_INIT_THREADPOOL_OPS(SvsThreadpoolSize, SvsThreadpoolParallelFor);
svs_threadpool_t kSvsThreadpoolIface{&kSvsThreadpoolOps, nullptr};

// Huge-page-aware custom allocator vtable. Requests at least one huge
// page in size try three tiers in order: 2 MiB huge pages via
// MAP_HUGETLB, opportunistic THP promotion via madvise(MADV_HUGEPAGE),
// then bare mmap. Smaller requests skip the huge-page routes and use
// small-page mmap so we do not round every metadata block up to 2 MiB.
// Each successful allocation reports its actual size to
// ValkeyModule_IncrExternalMemory so it counts against used_memory /
// maxmemory just like the module's own malloc arena.
constexpr size_t kSvsAllocatorHugePageSize = 2 * 1024 * 1024;

// Block size for both svs_index_build_dynamic and the RDB stream load; a
// mismatch makes a loaded index reserve differently. SVS's "default" (0)
// resolves to 2^30 bytes, materialized up-front on the first HSET;
// 16 MiB keeps the first-add reservation bounded. Measured 16–64 MiB
// behave identically; smaller wastes per-block overhead at large DIM.
constexpr size_t kSvsBlockSizeBytes = 16 * 1024 * 1024;

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
// nullopt.
std::optional<svs_distance_metric_t> ToSvsDistanceMetric(
    data_model::DistanceMetric metric) {
  switch (metric) {
    case data_model::DISTANCE_METRIC_L2:
      return SVS_DISTANCE_METRIC_EUCLIDEAN;
    case data_model::DISTANCE_METRIC_COSINE:
      return SVS_DISTANCE_METRIC_COSINE;
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
    data_model::SVSCompressionType compression) {
  switch (compression) {
    case data_model::SVS_COMPRESSION_NONE:
      return SVS_DATA_TYPE_FLOAT32;
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
  return absl::InternalError(
      absl::StrCat("SVS ", phase, ": ", msg ? msg : "<no message>"));
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
  auto storage_type = CompressionToSvsDataType(config.compression);
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
absl::StatusOr<svs_index_h> BootstrapIndex(
    const SVSBuildConfig& config, data_model::DistanceMetric distance_metric,
    int dimensions, uint64_t label, const float* fp32_vector) {
  ScopedSvsError err;
  auto builder =
      AssembleIndexBuilder(config, distance_metric, dimensions, err.get());
  if (!builder.ok()) return builder.status();

  svs_index_h index = svs_index_build_dynamic(
      builder->get(), fp32_vector, &label, /*num_vectors=*/1,
      /*blocksize_bytes=*/kSvsBlockSizeBytes, err.get());
  if (index == nullptr) {
    return SvsErrorToStatus(err.get(), "index_build_dynamic");
  }
  return index;
}

// Compares the v1 fields that determine SVS's on-disk layout
bool BuildConfigMatches(const SVSBuildConfig& expected,
                        const data_model::SVSVamanaAlgorithm& header_config) {
  return expected.graph_max_degree == header_config.graph_max_degree() &&
         expected.construction_window_size ==
             header_config.construction_window_size() &&
         expected.search_window_size == header_config.search_window_size() &&
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
    uint32_t current_svs_version = svs_get_version();
    if (header.svs_version() != current_svs_version) {
      return absl::InvalidArgumentError(absl::StrCat(
          "SVS_VAMANA RDB svs_version mismatch: index was saved with SVS "
          "version ",
          header.svs_version(), ", this build is SVS version ",
          current_svs_version));
    }

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

    SvsRdbInputStream adapter(input);
    svs_stream_interface iface = adapter.AsInterface();
    ScopedSvsError err;
    auto builder = AssembleIndexBuilder(instance->build_config_,
                                        vector_index_proto.distance_metric(),
                                        instance->dimensions_, err.get());
    if (!builder.ok()) return builder.status();

    svs_index_h index = svs_index_load_stream_dynamic(
        builder->get(), &iface, /*blocksize_bytes=*/kSvsBlockSizeBytes,
        err.get());
    if (index == nullptr) {
      if (!adapter.status().ok()) return adapter.status();
      return SvsErrorToStatus(err.get(), "index_load_stream_dynamic");
    }
    // Hand the handle to instance before the check below, so ~VectorSVS owns
    // it on every path out of here and no path needs its own free.
    instance->svs_index_ = index;

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
size_t VectorSVS<T>::GetCapacity() const {
  return 0;
}

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(
    absl::string_view query, uint64_t count,
    cancel::Token& /*cancellation_token*/,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter,
    std::optional<size_t> ef_runtime, bool /*enable_partial_results*/) {
#if defined(__linux__) && defined(__x86_64__)
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(
        absl::StrCat("SVS_VAMANA search: query vector blob size (",
                     query.size(), ") does not match index's expected size (",
                     dimensions_ * GetDataTypeSize(), ")."));
  }

  absl::ReaderMutexLock lock(&resize_mutex_);
  if (svs_index_ == nullptr || label_to_record_.empty()) {
    return std::vector<Neighbor>{};
  }

  const size_t k = std::min<size_t>(count, label_to_record_.size());
  if (k == 0) {
    return std::vector<Neighbor>{};
  }

  const T* raw = reinterpret_cast<const T*>(query.data());
  VMSDK_RETURN_IF_ERROR(
      CheckFiniteVector<T>(raw, static_cast<size_t>(dimensions_)));
  std::vector<float> scratch;
  const float* fp32_query =
      MakeFp32<T>(raw, static_cast<size_t>(dimensions_), scratch);

  ScopedSvsError err;
  const size_t effective_window = std::max<size_t>(
      k, ef_runtime.value_or(build_config_.search_window_size));
  SearchParamsPtr search_params(
      svs_search_params_create_vamana(effective_window, err.get()));
  if (!search_params) {
    return SvsErrorToStatus(err.get(), "search_params_create_vamana");
  }

  svs_id_filter_t filter_iface{&kSvsFilterOps, filter.get()};
  svs_id_filter_i filter_arg = filter ? &filter_iface : nullptr;

  svs_search_results_t results = SVS_INIT_SEARCH_RESULTS();
  auto results_cleanup =
      absl::MakeCleanup([&results]() { svs_search_results_free(&results); });

  if (!svs_index_search_topk(svs_index_, fp32_query, /*num_queries=*/1, k,
                             &results, search_params.get(), filter_arg,
                             err.get())) {
    return SvsErrorToStatus(err.get(), "index_search_topk");
  }

  std::priority_queue<std::pair<float, hnswlib::labeltype>> knn;
  for (size_t i = 0; i < results.total_results; ++i) {
    knn.push({results.distances[i],
              static_cast<hnswlib::labeltype>(results.indices[i])});
  }
  return this->CreateReply(knn);
#else
  (void)query;
  (void)count;
  (void)filter;
  (void)ef_runtime;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::Status VectorSVS<T>::AddRecordImpl(
    uint64_t internal_id, std::shared_ptr<const VectorRecord>&& vector_record) {
#if defined(__linux__) && defined(__x86_64__)
  const T* raw = reinterpret_cast<const T*>(vector_record->GetRawVector());
  VMSDK_RETURN_IF_ERROR(
      CheckFiniteVector<T>(raw, static_cast<size_t>(dimensions_)));
  std::vector<float> scratch;
  const float* fp32 =
      MakeFp32<T>(raw, static_cast<size_t>(dimensions_), scratch);

  absl::WriterMutexLock lock(&resize_mutex_);
  // Reserve the map slot before touching SVS state. If the hash-map
  // bucket allocation throws bad_alloc, SVS state is still pristine.
  // If the SVS mutation later fails, the cleanup below erases the
  // reservation so the map and SVS stay in step.
  auto [it, inserted] = label_to_record_.try_emplace(internal_id, nullptr);
  if (!inserted) {
    return absl::AlreadyExistsError(
        absl::StrCat("SVS internal_id already present: ", internal_id));
  }
  absl::Cleanup rollback = [this, internal_id] {
    label_to_record_.erase(internal_id);
  };

  if (svs_index_ == nullptr) {
    auto result = BootstrapIndex(build_config_, this->distance_metric_,
                                 dimensions_, internal_id, fp32);
    if (!result.ok()) return result.status();
    svs_index_ = *result;
  } else {
    ScopedSvsError err;
    size_t added = 0;
    if (!svs_index_dynamic_add_points(svs_index_, fp32, &internal_id,
                                      /*num_vectors=*/1, &added, err.get())) {
      return SvsErrorToStatus(err.get(), "index_dynamic_add_points");
    }
  }
  it->second = std::move(vector_record);
  std::move(rollback).Cancel();
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
  absl::WriterMutexLock lock(&resize_mutex_);
  auto it = label_to_record_.find(internal_id);
  if (it == label_to_record_.end()) {
    return absl::NotFoundError(
        absl::StrCat("SVS internal_id not found: ", internal_id));
  }
  // Erase the authoritative label mapping regardless of the SVS
  // delete outcome. If SVS reports failure, its internal state for
  // this id is unknown; dropping the map entry ensures search paths
  // that resolve labels through the map first cannot surface a stale
  // id, and a subsequent re-Add on the same id will not collide with
  // the reservation in AddRecordImpl.
  absl::Status svs_status = absl::OkStatus();
  if (svs_index_ != nullptr) {
    ScopedSvsError err;
    size_t deleted = 0;
    if (!svs_index_dynamic_delete_points(svs_index_, &internal_id,
                                         /*num_vectors=*/1, &deleted,
                                         err.get())) {
      svs_status = SvsErrorToStatus(err.get(), "index_dynamic_delete_points");
    }
  }
  label_to_record_.erase(it);
  return svs_status;
#else
  (void)internal_id;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::Status VectorSVS<T>::ModifyRecordImpl(
    uint64_t internal_id, std::shared_ptr<const VectorRecord>&& vector_record) {
#if defined(__linux__) && defined(__x86_64__)
  // SVS's add_points throws on a duplicate external ID (the translator
  // insert is checked before the data mutation), so replacement is a
  // delete followed by an add under the same lock. No native replace
  // primitive on the C API surface today.
  const T* raw = reinterpret_cast<const T*>(vector_record->GetRawVector());
  VMSDK_RETURN_IF_ERROR(
      CheckFiniteVector<T>(raw, static_cast<size_t>(dimensions_)));
  std::vector<float> scratch;
  const float* fp32 =
      MakeFp32<T>(raw, static_cast<size_t>(dimensions_), scratch);

  absl::WriterMutexLock lock(&resize_mutex_);
  ScopedSvsError err;
  if (svs_index_ != nullptr) {
    size_t deleted = 0;
    if (!svs_index_dynamic_delete_points(svs_index_, &internal_id,
                                         /*num_vectors=*/1, &deleted,
                                         err.get())) {
      return SvsErrorToStatus(err.get(), "modify: index_dynamic_delete_points");
    }
    size_t added = 0;
    if (!svs_index_dynamic_add_points(svs_index_, fp32, &internal_id,
                                      /*num_vectors=*/1, &added, err.get())) {
      // Delete already committed inside SVS; the id is gone from the
      // index and cannot be resurrected here (add just failed). Drop
      // the stale map entry so the two views agree — the base class's
      // RemoveRecordDueToError path will then NotFoundError out
      // cleanly instead of hitting a phantom.
      label_to_record_.erase(internal_id);
      return SvsErrorToStatus(err.get(), "modify: index_dynamic_add_points");
    }
  } else {
    auto result = BootstrapIndex(build_config_, this->distance_metric_,
                                 dimensions_, internal_id, fp32);
    if (!result.ok()) return result.status();
    svs_index_ = *result;
  }
  label_to_record_[internal_id] = std::move(vector_record);
  return absl::OkStatus();
#else
  (void)internal_id;
  (void)vector_record;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

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
  ValkeyModule_ReplyWithArray(ctx, 12);
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
  ValkeyModule_ReplyWithDouble(ctx, build_config_.alpha);
  ValkeyModule_ReplyWithSimpleString(ctx, "compression");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      data_model::SVSCompressionType_Name(build_config_.compression).c_str());
  return 4;
}

template <typename T>
absl::Status VectorSVS<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
#if defined(__linux__) && defined(__x86_64__)
  absl::ReaderMutexLock lock(&resize_mutex_);

  data_model::SVSIndexHeader header;
  header.set_format_version(kSvsHeaderFormatVersion);
  header.set_svs_version(svs_get_version());
  header.set_has_index(svs_index_ != nullptr);
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

  if (svs_index_ == nullptr) {
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
  return absl::OkStatus();
#else
  (void)chunked_out;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
float VectorSVS<T>::ComputeDistance(absl::string_view query,
                                    const VectorRecord* vector_record,
                                    float query_magnitude) const {
  // Distance evaluation uses the space populated by VectorType::Init
  // from the configured distance metric. This mirrors the raw-bytes
  // distance path the base class uses for pre-filter scoring and does
  // not require the SVS index handle.
  return space_->get_dist_func()(query.data(), vector_record->GetRawVector(),
                                 space_->get_dist_func_param(),
                                 query_magnitude);
}

template <typename T>
std::shared_ptr<const VectorRecord>& VectorSVS<T>::GetVectorLockFree(
    uint64_t internal_id) const {
  auto it = label_to_record_.find(internal_id);
  CHECK(it != label_to_record_.end())
      << "SVS internal_id not found: " << internal_id;
  return it->second;
}

template <typename T>
std::shared_ptr<const VectorRecord>& VectorSVS<T>::GetOrCreateVectorLockFree(
    uint64_t internal_id) {
  // label_to_record_ is filled before any query or mutation can reach this
  // index, which is why resize_mutex_ is not taken; an insert rehashes it.
  auto [it, inserted] = label_to_record_.try_emplace(internal_id, nullptr);
  if (!inserted) {
    ++Metrics::GetStats().svs_duplicate_label_on_load_cnt;
  }
  return it->second;
}

template <typename T>
std::shared_ptr<const VectorRecord>& VectorSVS<T>::GetVector(
    uint64_t internal_id) const {
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
uint64_t VectorSVS<T>::GetMaxLoadedLabel() const {
  uint64_t max_label = 0;
  for (const auto& [id, _] : label_to_record_) {
    if (id > max_label) max_label = id;
  }
  return max_label;
}

template <typename T>
size_t VectorSVS<T>::GetLabelCount() const {
  return label_to_record_.size();
}

template class VectorSVS<float>;
template class VectorSVS<float16>;
template class VectorSVS<bfloat16>;

}  // namespace valkey_search::indexes
