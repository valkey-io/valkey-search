/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_svs.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

#include <exception>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/attribute_data_type.h"
#include "src/indexes/index_base.h"
#include "src/indexes/vector_base.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

namespace valkey_search::indexes {

namespace {

// Convert valkey-search distance metric to SVS MetricType.
svs::runtime::v0::MetricType ToSVSMetric(
    data_model::DistanceMetric metric) {
  switch (metric) {
    case data_model::DISTANCE_METRIC_L2:
      return svs::runtime::v0::MetricType::L2;
    case data_model::DISTANCE_METRIC_IP:
    case data_model::DISTANCE_METRIC_COSINE:
      // COSINE is handled by valkey-search's normalization layer,
      // which converts it to IP with normalized vectors.
      return svs::runtime::v0::MetricType::INNER_PRODUCT;
    default:
      return svs::runtime::v0::MetricType::L2;
  }
}

// Convert protobuf SVSCompressionType to svs::runtime StorageKind.
svs::runtime::v0::StorageKind ToSVSStorageKind(
    data_model::SVSCompressionType compression) {
  switch (compression) {
    case data_model::SVS_COMPRESSION_FP16:
      return svs::runtime::v0::StorageKind::FP16;
    case data_model::SVS_COMPRESSION_LVQ4:
      return svs::runtime::v0::StorageKind::LVQ4x0;
    case data_model::SVS_COMPRESSION_LVQ8:
      return svs::runtime::v0::StorageKind::LVQ8x0;
    case data_model::SVS_COMPRESSION_LVQ4X4:
      return svs::runtime::v0::StorageKind::LVQ4x4;
    case data_model::SVS_COMPRESSION_LVQ4X8:
      return svs::runtime::v0::StorageKind::LVQ4x8;
    default:
      return svs::runtime::v0::StorageKind::FP32;
  }
}

const char* CompressionTypeName(data_model::SVSCompressionType type) {
  switch (type) {
    case data_model::SVS_COMPRESSION_NONE: return "NONE";
    case data_model::SVS_COMPRESSION_FP16: return "FP16";
    case data_model::SVS_COMPRESSION_LVQ4: return "LVQ4";
    case data_model::SVS_COMPRESSION_LVQ8: return "LVQ8";
    case data_model::SVS_COMPRESSION_LVQ4X4: return "LVQ4X4";
    case data_model::SVS_COMPRESSION_LVQ4X8: return "LVQ4X8";
    default: return "UNKNOWN";
  }
}

}  // namespace

template <typename T>
VectorSVS<T>::VectorSVS(
    int dimensions, data_model::DistanceMetric distance_metric,
    const SVSBuildConfig& build_config,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type)
    : VectorBase(IndexerType::kSVS, dimensions, attribute_data_type,
                 attribute_identifier),
      build_config_(build_config) {}

template <typename T>
VectorSVS<T>::~VectorSVS() {
  // Flush any remaining buffered vectors before cleanup
  if (!pending_buffer_.empty()) {
    VMSDK_LOG(WARNING, nullptr)
        << "Destructor flushing " << pending_buffer_.size()
        << " remaining vectors";
    absl::MutexLock lock(&index_mutex_);
    (void)FlushBuffer();  // Ignore errors at shutdown
  }

  if (svs_index_) {
    auto status = svs::runtime::v0::DynamicVamanaIndex::destroy(svs_index_);
    if (!status.ok()) {
      VMSDK_LOG(WARNING, nullptr)
          << "SVS destroy failed: " << status.message();
    }
    svs_index_ = nullptr;
  }
}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorSVS<T>>> VectorSVS<T>::Create(
    const data_model::VectorIndex& vector_index_proto,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type) {
  SVSBuildConfig config;

  // Extract SVS-specific params from protobuf
  if (vector_index_proto.has_svs_vamana_algorithm()) {
    const auto& svs_params = vector_index_proto.svs_vamana_algorithm();
    if (svs_params.graph_max_degree() > 0) {
      config.graph_max_degree = svs_params.graph_max_degree();
    }
    if (svs_params.construction_window_size() > 0) {
      config.construction_window_size = svs_params.construction_window_size();
    }
    if (svs_params.search_window_size() > 0) {
      config.search_window_size = svs_params.search_window_size();
    }
    if (svs_params.alpha() > 0.0f) {
      config.alpha = svs_params.alpha();
    }
    if (svs_params.compression() != data_model::SVS_COMPRESSION_NONE) {
      config.compression = svs_params.compression();
    }
  }

  // SVS requires alpha <= 1.0 for MIP/Cosine distance metrics.
  if (vector_index_proto.distance_metric() ==
          data_model::DISTANCE_METRIC_IP ||
      vector_index_proto.distance_metric() ==
          data_model::DISTANCE_METRIC_COSINE) {
    if (config.alpha > 1.0f) {
      VMSDK_LOG(NOTICE, nullptr)
          << "Clamping SVS alpha from " << config.alpha
          << " to 1.0 (required for IP/COSINE metrics)";
      config.alpha = 1.0f;
    }
  }

  auto index = std::shared_ptr<VectorSVS<T>>(new VectorSVS<T>(
      vector_index_proto.dimension_count(),
      vector_index_proto.distance_metric(), config, attribute_identifier,
      attribute_data_type));

  // Initialize the VectorBase (sets distance_metric_, normalize_, space_)
  index->Init(vector_index_proto.dimension_count(),
              vector_index_proto.distance_metric(), index->space_);

  // Pin OpenMP threads to 1 to prevent SVS from spawning uncontrolled thread
  // pools that interfere with valkey-search's thread model.
#ifdef _OPENMP
  omp_set_num_threads(1);
#endif

  // Build the SVS index immediately.
  auto svs_metric = ToSVSMetric(vector_index_proto.distance_metric());

  svs::runtime::v0::VamanaIndex::BuildParams build_params;
  build_params.graph_max_degree = config.graph_max_degree;
  build_params.construction_window_size = config.construction_window_size;
  build_params.alpha = config.alpha;

  svs::runtime::v0::VamanaIndex::SearchParams search_params;
  search_params.search_window_size = config.search_window_size;

  auto storage_kind = ToSVSStorageKind(config.compression);

  auto status = svs::runtime::v0::DynamicVamanaIndex::build(
      &index->svs_index_,
      vector_index_proto.dimension_count(),
      svs_metric,
      storage_kind,
      build_params,
      search_params);

  if (!status.ok()) {
    return absl::InternalError(
        absl::StrCat("Error building SVS Vamana index: ", status.message()));
  }

  VMSDK_LOG(NOTICE, nullptr)
      << "Created SVS Vamana index with dim="
      << vector_index_proto.dimension_count()
      << " compression=" << CompressionTypeName(config.compression)
      << " graph_max_degree=" << config.graph_max_degree
      << " construction_window_size=" << config.construction_window_size
      << " alpha=" << config.alpha
      << " search_window_size=" << config.search_window_size;

  return index;
}

// --- Mutation methods ---

template <typename T>
absl::Status VectorSVS<T>::AddRecordImpl(uint64_t internal_id,
                                          absl::string_view record) {
  try {
    absl::MutexLock lock(&index_mutex_);

    // Buffer the vector instead of immediate SVS insert (for benchmarking)
    pending_buffer_.push_back({
        .internal_id = internal_id,
        .data = std::vector<char>(record.begin(), record.end())
    });

    // Store raw vector data for retrieval and distance computation during buffering.
    // This is a temporary measure for Iteration 0: SVS does not yet expose
    // reconstruct() or compute_distance(), so we keep a full FP32 copy.
    raw_vectors_[internal_id] = pending_buffer_.back().data;

    // Check if buffer is full → trigger auto-flush
    if (pending_buffer_.size() >= kBufferSize) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());
    }

    ++num_elements_;
    return absl::OkStatus();
  } catch (const std::exception& e) {
    // Rollback: remove from buffer and raw_vectors if exception occurred
    if (!pending_buffer_.empty() &&
        pending_buffer_.back().internal_id == internal_id) {
      pending_buffer_.pop_back();
    }
    raw_vectors_.erase(internal_id);
    return absl::InternalError(
        absl::StrCat("SVS add exception: ", e.what()));
  }
}

template <typename T>
absl::Status VectorSVS<T>::FlushBuffer() {
  // Called under index_mutex_ (exclusive lock already held by AddRecordImpl)

  if (pending_buffer_.empty()) {
    return absl::OkStatus();  // Nothing to flush
  }

  size_t batch_size = pending_buffer_.size();
  VMSDK_LOG(NOTICE, nullptr) << "Flushing " << batch_size << " vectors to SVS graph...";

  buffer_flushing_ = true;  // Block searches during flush

  try {
    // Prepare batch arrays for SVS API
    std::vector<size_t> labels(batch_size);
    std::vector<T> data_flat(batch_size * dimensions_);

    for (size_t i = 0; i < batch_size; ++i) {
      labels[i] = static_cast<size_t>(pending_buffer_[i].internal_id);

      // Copy vector data (flatten for SVS batch API)
      const T* src = reinterpret_cast<const T*>(pending_buffer_[i].data.data());
      std::copy(src, src + dimensions_, data_flat.begin() + i * dimensions_);
    }

    // Batch insert to SVS (this takes ~seconds for 10K vectors)
    auto status = svs_index_->add(
        batch_size, labels.data(), data_flat.data());

    if (!status.ok()) {
      buffer_flushing_ = false;
      return absl::InternalError(
          absl::StrCat("SVS batch add failed: ", status.message()));
    }

    // Clear buffer
    pending_buffer_.clear();
    buffer_flushing_ = false;

    VMSDK_LOG(NOTICE, nullptr) << "Flush complete. SVS graph now has "
                                << num_elements_ << " vectors.";

    return absl::OkStatus();
  } catch (const std::exception& e) {
    buffer_flushing_ = false;
    return absl::InternalError(
        absl::StrCat("SVS flush exception: ", e.what()));
  }
}

template <typename T>
absl::Status VectorSVS<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::MutexLock lock(&index_mutex_);

    // Flush buffered vectors before removal (simplest approach for benchmarking)
    if (!pending_buffer_.empty()) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());
    }

    size_t label = static_cast<size_t>(internal_id);
    auto status = svs_index_->remove(1, &label);

    if (!status.ok()) {
      return absl::InternalError(
          absl::StrCat("SVS remove failed: ", status.message()));
    }

    raw_vectors_.erase(internal_id);
    if (num_elements_ > 0) {
      --num_elements_;
    }
    return absl::OkStatus();
  } catch (const std::exception& e) {
    return absl::InternalError(
        absl::StrCat("SVS remove exception: ", e.what()));
  }
}

template <typename T>
absl::Status VectorSVS<T>::ModifyRecordImpl(uint64_t internal_id,
                                             absl::string_view record) {
  // Atomic modify: hold a single exclusive lock for remove + add to prevent
  // a gap where the vector is absent from the index. HNSW uses a reader lock
  // because hnswlib handles markDelete + addPoint atomically. SVS requires
  // separate remove() + add() calls, so we need exclusive access.
  try {
    absl::MutexLock lock(&index_mutex_);

    // Flush buffered vectors before modification (simplest approach for benchmarking)
    if (!pending_buffer_.empty()) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());
    }

    // Save old vector for rollback in case add() fails after remove().
    auto old_it = raw_vectors_.find(internal_id);
    std::vector<char> old_raw;
    if (old_it != raw_vectors_.end()) {
      old_raw = std::move(old_it->second);
    }

    size_t label = static_cast<size_t>(internal_id);
    auto remove_status = svs_index_->remove(1, &label);
    if (!remove_status.ok()) {
      // Restore old raw vector (it was moved out).
      if (!old_raw.empty()) {
        raw_vectors_[internal_id] = std::move(old_raw);
      }
      return absl::InternalError(
          absl::StrCat("SVS remove (modify) failed: ",
                       remove_status.message()));
    }

    raw_vectors_[internal_id] =
        std::vector<char>(record.begin(), record.end());

    auto add_status = svs_index_->add(
        1, &label, reinterpret_cast<const float*>(record.data()));
    if (!add_status.ok()) {
      // Rollback: restore old vector and re-add to SVS index.
      if (!old_raw.empty()) {
        raw_vectors_[internal_id] = std::move(old_raw);
        auto restore_status = svs_index_->add(
            1, &label,
            reinterpret_cast<const float*>(
                raw_vectors_[internal_id].data()));
        if (!restore_status.ok()) {
          // Failed to restore — vector is lost from SVS graph.
          // Erase raw_vectors_ entry to stay consistent with graph.
          raw_vectors_.erase(internal_id);
          if (num_elements_ > 0) {
            --num_elements_;
          }
        }
      } else {
        // No old vector to restore — just clean up.
        raw_vectors_.erase(internal_id);
        if (num_elements_ > 0) {
          --num_elements_;
        }
      }
      return absl::InternalError(
          absl::StrCat("SVS add (modify) failed: ",
                       add_status.message()));
    }

    return absl::OkStatus();
  } catch (const std::exception& e) {
    return absl::InternalError(
        absl::StrCat("SVS modify exception: ", e.what()));
  }
}

// --- Search ---

// Adapter: bridge hnswlib's BaseFilterFunctor to SVS's IDFilter interface.
class SVSIDFilterAdapter : public svs::runtime::v0::IDFilter {
 public:
  explicit SVSIDFilterAdapter(hnswlib::BaseFilterFunctor* filter)
      : filter_(filter) {}
  bool is_member(size_t id) const override {
    return (*filter_)(static_cast<hnswlib::labeltype>(id));
  }

 private:
  hnswlib::BaseFilterFunctor* filter_;
};

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorSVS<T>::Search(
    absl::string_view query, uint64_t count,
    cancel::Token& cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter,
    std::optional<unsigned> search_window_size) {
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Error parsing vector similarity query: query vector blob size (",
        query.size(), ") does not match index's expected size (",
        dimensions_ * GetDataTypeSize(), ")."));
  }

  auto perform_search =
      [this, count, &filter, &search_window_size,
       &cancellation_token](absl::string_view q)
          -> absl::StatusOr<
              std::priority_queue<std::pair<T, hnswlib::labeltype>>> {
    try {
      // Auto-flush buffered vectors before searching (for benchmarking)
      bool needs_flush = false;
      {
        absl::ReaderMutexLock check_lock(&index_mutex_);
        needs_flush = !pending_buffer_.empty() && !buffer_flushing_;
      }

      if (needs_flush) {
        absl::MutexLock flush_lock(&index_mutex_);
        // Double-check buffer is still non-empty after acquiring write lock
        if (!pending_buffer_.empty()) {
          VMSDK_RETURN_IF_ERROR(FlushBuffer());
        }
      }

      absl::ReaderMutexLock lock(&index_mutex_);

      if (num_elements_ == 0) {
        return std::priority_queue<std::pair<T, hnswlib::labeltype>>();
      }

      // Clamp k to available elements.
      size_t k = std::min(static_cast<size_t>(count), num_elements_);

      // Allocate flat output arrays for SVS search results.
      std::vector<float> distances(k);
      std::vector<size_t> labels(k);

      // Build optional search params override.
      svs::runtime::v0::VamanaIndex::SearchParams params;
      svs::runtime::v0::VamanaIndex::SearchParams* params_ptr = nullptr;
      if (search_window_size.has_value()) {
        params.search_window_size = search_window_size.value();
        params_ptr = &params;
      }

      // Build optional filter adapter.
      std::unique_ptr<SVSIDFilterAdapter> svs_filter;
      if (filter) {
        svs_filter = std::make_unique<SVSIDFilterAdapter>(filter.get());
      }

      auto status = svs_index_->search(
          1,  // single query
          reinterpret_cast<const float*>(q.data()),
          k,
          distances.data(),
          labels.data(),
          params_ptr,
          svs_filter.get());

      if (!status.ok()) {
        return absl::InternalError(
            absl::StrCat("SVS search failed: ", status.message()));
      }

      // SVS search() is synchronous and non-interruptible. Check the
      // cancellation token after completion to avoid wasted post-processing.
      if (cancellation_token->IsCancelled()) {
        return absl::CancelledError(
            "Search operation cancelled due to timeout");
      }

      // Convert flat arrays to priority queue (max-heap by distance).
      // Skip sentinel entries that SVS may produce for filtered searches
      // where fewer than k results match the filter.
      std::priority_queue<std::pair<T, hnswlib::labeltype>> results;
      for (size_t i = 0; i < k; ++i) {
        if (labels[i] == std::numeric_limits<size_t>::max() ||
            std::isinf(distances[i])) {
          continue;
        }
        results.emplace(distances[i],
                        static_cast<hnswlib::labeltype>(labels[i]));
      }
      return results;
    } catch (const std::exception& e) {
      return absl::InternalError(
          absl::StrCat("SVS search exception: ", e.what()));
    }
  };

  if (normalize_) {
    auto norm_record = NormalizeEmbedding(query, GetDataTypeSize());
    VMSDK_ASSIGN_OR_RETURN(
        auto search_result,
        perform_search(absl::string_view(
            reinterpret_cast<const char*>(norm_record.data()),
            norm_record.size())));
    return CreateReply(search_result);
  }
  VMSDK_ASSIGN_OR_RETURN(auto search_result, perform_search(query));
  return CreateReply(search_result);
}

// --- Vector tracking ---

template <typename T>
void VectorSVS<T>::TrackVector(uint64_t internal_id,
                                const InternedStringPtr& vector) {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_[internal_id] = vector;
}

template <typename T>
bool VectorSVS<T>::IsVectorMatch(uint64_t internal_id,
                                  const InternedStringPtr& vector) {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  auto it = tracked_vectors_.find(internal_id);
  if (it == tracked_vectors_.end()) {
    return false;
  }
  return it->second->Str() == vector->Str();
}

template <typename T>
void VectorSVS<T>::UnTrackVector(uint64_t internal_id) {
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_.erase(internal_id);
}

template <typename T>
absl::StatusOr<std::pair<float, hnswlib::labeltype>>
VectorSVS<T>::ComputeDistanceFromRecordImpl(
    uint64_t internal_id, absl::string_view query) const {
  absl::ReaderMutexLock lock(&index_mutex_);
  auto it = raw_vectors_.find(internal_id);
  if (it == raw_vectors_.end()) {
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }

  // Use the hnswlib space interface for distance computation.
  // This is a temporary measure for Iteration 0: SVS does not yet expose
  // compute_distance(). We compute on the raw FP32 copy instead.
  auto dist = space_->get_dist_func()(
      reinterpret_cast<const T*>(query.data()),
      reinterpret_cast<const T*>(it->second.data()),
      space_->get_dist_func_param());

  return std::pair<float, hnswlib::labeltype>{dist, internal_id};
}

template <typename T>
char* VectorSVS<T>::GetValueImpl(uint64_t internal_id) const {
  absl::ReaderMutexLock lock(&index_mutex_);
  auto it = raw_vectors_.find(internal_id);
  if (it == raw_vectors_.end()) {
    return nullptr;
  }
  // Safety: the returned pointer into raw_vectors_ outlives the reader lock
  // held here. This is safe because the caller (VectorBase::GetValue) executes
  // during the read time-slice of IndexSchema's TimeSlicedMRMWMutex, which
  // blocks all writer threads (mutations). The pointer remains valid until the
  // read time-slice ends. Do NOT use this pointer outside that context.
  return const_cast<char*>(it->second.data());
}

// --- Serialization ---

template <typename T>
void VectorSVS<T>::ToProtoImpl(
    data_model::VectorIndex* vector_index_proto) const {
  data_model::VectorDataType data_type;
  if constexpr (std::is_same_v<T, float>) {
    data_type = data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32;
  } else {
    data_type = data_model::VectorDataType::VECTOR_DATA_TYPE_UNSPECIFIED;
  }
  vector_index_proto->set_vector_data_type(data_type);

  auto svs_algo_proto =
      std::make_unique<data_model::SVSVamanaAlgorithm>();
  svs_algo_proto->set_graph_max_degree(build_config_.graph_max_degree);
  svs_algo_proto->set_construction_window_size(
      build_config_.construction_window_size);
  svs_algo_proto->set_search_window_size(build_config_.search_window_size);
  svs_algo_proto->set_alpha(build_config_.alpha);
  svs_algo_proto->set_compression(build_config_.compression);
  vector_index_proto->set_allocated_svs_vamana_algorithm(
      svs_algo_proto.release());
}

template <typename T>
int VectorSVS<T>::RespondWithInfoImpl(ValkeyModuleCtx* ctx) const {
  ValkeyModule_ReplyWithSimpleString(ctx, "data_type");
  if constexpr (std::is_same_v<T, float>) {
    ValkeyModule_ReplyWithSimpleString(
        ctx,
        LookupKeyByValue(*kVectorDataTypeByStr,
                         data_model::VectorDataType::VECTOR_DATA_TYPE_FLOAT32)
            .data());
  } else {
    ValkeyModule_ReplyWithSimpleString(ctx, "UNKNOWN");
  }
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  // 6 pairs: name, graph_max_degree, cws, sws, alpha, compression
  ValkeyModule_ReplyWithArray(ctx, 12);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      LookupKeyByValue(
          *kVectorAlgoByStr,
          data_model::VectorIndex::AlgorithmCase::kSvsVamanaAlgorithm)
          .data());
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
      ctx, CompressionTypeName(build_config_.compression));

  return 4;  // 4 top-level reply pairs: data_type + algorithm
}

template <typename T>
absl::Status VectorSVS<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
  return absl::UnimplementedError(
      "SVS index RDB persistence is not yet implemented");
}

// Explicit template instantiation
template class VectorSVS<float>;

}  // namespace valkey_search::indexes
