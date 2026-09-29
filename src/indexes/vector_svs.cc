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
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "src/attribute_data_type.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

#if defined(__linux__) && defined(__x86_64__)
#include <sys/mman.h>

#include "absl/strings/str_cat.h"
#include "src/index_schema.pb.h"
#include "vmsdk/src/utils.h"

#include <svs/c/svs_c.h>
#endif

namespace valkey_search::indexes {

namespace {
constexpr absl::string_view kUnavailableMsg =
    "SVS_VAMANA is not available on this build (requires x86_64 Linux).";
}  // namespace

#if defined(__linux__) && defined(__x86_64__)

namespace {

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

// Huge-page-aware custom allocator vtable. Every allocation tries three
// tiers in order: 2 MiB huge pages via MAP_HUGETLB, opportunistic THP
// promotion via madvise(MADV_HUGEPAGE), then bare mmap. Each successful
// allocation reports its actual size to ValkeyModule_IncrExternalMemory
// so it counts against used_memory / maxmemory just like the module's
// own malloc arena.
constexpr size_t kSvsAllocatorHugePageSize = 2 * 1024 * 1024;

// Block size passed to svs_index_build_dynamic. SVS's "default" (0)
// resolves to 2^30 bytes, materialized up-front on the first HSET;
// 16 MiB keeps the first-add reservation bounded. Measured 16–64 MiB
// behave identically; smaller wastes per-block overhead at large DIM.
constexpr size_t kSvsBlockSizeBytes = 16 * 1024 * 1024;

size_t RoundUpToPageSize(size_t size, size_t page_size) {
  return ((size + page_size - 1) / page_size) * page_size;
}

void* SvsAllocatorAllocate(void* /*self*/, size_t size, size_t alignment,
                           svs_error_h /*out_err*/) {
  if (size == 0) {
    return nullptr;
  }
  size_t aligned_size = RoundUpToPageSize(
      size, std::max<size_t>(alignment, kSvsAllocatorHugePageSize));

  void* ptr =
      mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
  if (ptr == MAP_FAILED) {
    ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE,
               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
      return nullptr;
    }
    // Opportunistically ask the kernel to back this region with huge
    // pages if transparent-hugepage is enabled. Best effort; ignore
    // failures.
    madvise(ptr, aligned_size, MADV_HUGEPAGE);
  }
  ValkeyModule_IncrExternalMemory(aligned_size);
  return ptr;
}

void SvsAllocatorDeallocate(void* /*self*/, void* ptr, size_t size,
                            size_t alignment) {
  if (ptr == nullptr || size == 0) {
    return;
  }
  size_t aligned_size = RoundUpToPageSize(
      size, std::max<size_t>(alignment, kSvsAllocatorHugePageSize));
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
using StoragePtr = std::unique_ptr<
    std::remove_pointer_t<svs_storage_h>, StorageDeleter>;

struct AlgorithmDeleter {
  void operator()(svs_algorithm_h h) const {
    if (h != nullptr) svs_algorithm_free(h);
  }
};
using AlgorithmPtr = std::unique_ptr<
    std::remove_pointer_t<svs_algorithm_h>, AlgorithmDeleter>;

struct BuilderDeleter {
  void operator()(svs_index_builder_h h) const {
    if (h != nullptr) svs_index_builder_free(h);
  }
};
using BuilderPtr = std::unique_ptr<
    std::remove_pointer_t<svs_index_builder_h>, BuilderDeleter>;

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

// Constructs the first svs_index for a VectorSVS instance on the first
// HSET arriving at an index whose Create() left svs_index_ null.
// Assembles algorithm/storage/builder, wires the sequential threadpool
// and huge-page-aware allocator, and returns the built handle. The
// SVS C API requires num_vectors > 0 for svs_index_build_dynamic; the
// first-add path passes the vector supplied by the ingest call.
absl::StatusOr<svs_index_h> BootstrapIndex(
    const SVSBuildConfig& config,
    data_model::DistanceMetric distance_metric, int dimensions,
    uint64_t label, const float* fp32_vector) {
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

  ScopedSvsError err;

  AlgorithmPtr algo(svs_algorithm_create_vamana(
      config.graph_max_degree, config.construction_window_size,
      config.search_window_size, err.get()));
  if (!algo) return SvsErrorToStatus(err.get(), "algorithm_create_vamana");

  if (config.alpha > 0.0f) {
    if (!svs_algorithm_vamana_set_alpha(algo.get(), config.alpha,
                                        err.get())) {
      return SvsErrorToStatus(err.get(), "algorithm_vamana_set_alpha");
    }
  }

  StoragePtr storage;
  if (config.compression == data_model::SVS_COMPRESSION_SQ8) {
    storage.reset(svs_storage_create_sq(*storage_type, err.get()));
  } else {
    storage.reset(svs_storage_create_simple(*storage_type, err.get()));
  }
  if (!storage) return SvsErrorToStatus(err.get(), "storage_create");

  BuilderPtr builder(svs_index_builder_create(
      *metric, static_cast<size_t>(dimensions), algo.get(), err.get()));
  if (!builder) return SvsErrorToStatus(err.get(), "index_builder_create");

  if (!svs_index_builder_set_storage(builder.get(), storage.get(),
                                     err.get())) {
    return SvsErrorToStatus(err.get(), "index_builder_set_storage");
  }
  if (!svs_index_builder_set_threadpool_custom(builder.get(),
                                               &kSvsThreadpoolIface,
                                               err.get())) {
    return SvsErrorToStatus(err.get(),
                            "index_builder_set_threadpool_custom");
  }
  if (ValkeyModule_IncrExternalMemory != nullptr &&
      ValkeyModule_DecrExternalMemory != nullptr) {
    if (!svs_index_builder_set_allocator_custom(
            builder.get(), &kSvsAllocatorIface, err.get())) {
      return SvsErrorToStatus(err.get(),
                              "index_builder_set_allocator_custom");
    }
  } else {
    if (!svs_index_builder_set_allocator(
            builder.get(), SVS_ALLOCATOR_KIND_SIMPLE, err.get())) {
      return SvsErrorToStatus(err.get(), "index_builder_set_allocator");
    }
  }

  svs_index_h index = svs_index_build_dynamic(
      builder.get(), fp32_vector, &label, /*num_vectors=*/1,
      /*blocksize_bytes=*/kSvsBlockSizeBytes, err.get());
  if (index == nullptr) {
    return SvsErrorToStatus(err.get(), "index_build_dynamic");
  }
  return index;
}

}  // namespace

#endif  // __linux__ && __x86_64__

template <typename T>
VectorSVS<T>::VectorSVS(int dimensions,
                        absl::string_view attribute_identifier,
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
    ValkeyModuleCtx* /*ctx*/,
    const AttributeDataType* /*attribute_data_type*/,
    const data_model::VectorIndex& /*vector_index_proto*/,
    absl::string_view /*attribute_identifier*/,
    SupplementalContentChunkIter&& /*iter*/, int /*db_num*/) {
  return absl::UnimplementedError(kUnavailableMsg);
}

template <typename T>
size_t VectorSVS<T>::GetCapacity() const {
  return 0;
}

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(
    absl::string_view /*query*/, uint64_t /*count*/,
    cancel::Token& /*cancellation_token*/,
    std::unique_ptr<hnswlib::BaseFilterFunctor> /*filter*/,
    std::optional<size_t> /*ef_runtime*/, bool /*enable_partial_results*/) {
  return absl::UnimplementedError(kUnavailableMsg);
}

template <typename T>
absl::Status VectorSVS<T>::AddRecordImpl(
    uint64_t internal_id,
    std::shared_ptr<const VectorRecord>&& vector_record) {
#if defined(__linux__) && defined(__x86_64__)
  const T* raw = reinterpret_cast<const T*>(vector_record->GetRawVector());
  VMSDK_RETURN_IF_ERROR(
      CheckFiniteVector<T>(raw, static_cast<size_t>(dimensions_)));
  std::vector<float> scratch;
  const float* fp32 =
      MakeFp32<T>(raw, static_cast<size_t>(dimensions_), scratch);

  absl::WriterMutexLock lock(&resize_mutex_);
  if (svs_index_ == nullptr) {
    auto result =
        BootstrapIndex(build_config_, this->distance_metric_, dimensions_,
                       internal_id, fp32);
    if (!result.ok()) return result.status();
    svs_index_ = *result;
  } else {
    ScopedSvsError err;
    size_t added = 0;
    if (!svs_index_dynamic_add_points(svs_index_, fp32, &internal_id,
                                      /*num_vectors=*/1, &added,
                                      err.get())) {
      return SvsErrorToStatus(err.get(), "index_dynamic_add_points");
    }
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
absl::Status VectorSVS<T>::RemoveRecordImpl(uint64_t internal_id) {
#if defined(__linux__) && defined(__x86_64__)
  absl::WriterMutexLock lock(&resize_mutex_);
  auto it = label_to_record_.find(internal_id);
  if (it == label_to_record_.end()) {
    return absl::NotFoundError(
        absl::StrCat("SVS internal_id not found: ", internal_id));
  }
  if (svs_index_ != nullptr) {
    ScopedSvsError err;
    size_t deleted = 0;
    if (!svs_index_dynamic_delete_points(svs_index_, &internal_id,
                                         /*num_vectors=*/1, &deleted,
                                         err.get())) {
      return SvsErrorToStatus(err.get(), "index_dynamic_delete_points");
    }
  }
  label_to_record_.erase(it);
  return absl::OkStatus();
#else
  (void)internal_id;
  return absl::UnimplementedError(kUnavailableMsg);
#endif
}

template <typename T>
absl::Status VectorSVS<T>::ModifyRecordImpl(
    uint64_t internal_id,
    std::shared_ptr<const VectorRecord>&& vector_record) {
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
      return SvsErrorToStatus(err.get(),
                              "modify: index_dynamic_delete_points");
    }
    size_t added = 0;
    if (!svs_index_dynamic_add_points(svs_index_, fp32, &internal_id,
                                      /*num_vectors=*/1, &added,
                                      err.get())) {
      return SvsErrorToStatus(err.get(),
                              "modify: index_dynamic_add_points");
    }
  } else {
    auto result =
        BootstrapIndex(build_config_, this->distance_metric_, dimensions_,
                       internal_id, fp32);
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
    data_model::VectorIndex* /*vector_index_proto*/) const {
  LOG(FATAL) << kUnavailableMsg;
}

template <typename T>
int VectorSVS<T>::RespondWithInfoImpl(ValkeyModuleCtx* ctx) const {
  EmitDataTypeInfo(ctx);
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  ValkeyModule_ReplyWithArray(ctx, 12);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      std::string(LookupKeyByValue(
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
    RDBChunkOutputStream /*chunked_out*/) const {
  return absl::UnimplementedError(kUnavailableMsg);
}

template <typename T>
float VectorSVS<T>::ComputeDistance(absl::string_view /*query*/,
                                    const VectorRecord* /*vector_record*/,
                                    float /*query_magnitude*/) const {
  LOG(FATAL) << kUnavailableMsg;
  __builtin_unreachable();
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
