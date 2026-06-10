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
#include "src/metrics.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search_options.h"
#include "third_party/hnswlib/hnswlib.h"
#include "vmsdk/src/latency_sampler.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/utils.h"
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
    case data_model::SVS_COMPRESSION_LEANVEC4X4:
      return svs::runtime::v0::StorageKind::LeanVec4x4;
    case data_model::SVS_COMPRESSION_LEANVEC4X8:
      return svs::runtime::v0::StorageKind::LeanVec4x8;
    case data_model::SVS_COMPRESSION_LEANVEC8X8:
      return svs::runtime::v0::StorageKind::LeanVec8x8;
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
    case data_model::SVS_COMPRESSION_LEANVEC4X4: return "LEANVEC4X4";
    case data_model::SVS_COMPRESSION_LEANVEC4X8: return "LEANVEC4X8";
    case data_model::SVS_COMPRESSION_LEANVEC8X8: return "LEANVEC8X8";
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
  // For non-LeanVec indexes, drain any pending buffer into the SVS graph.
  // For LeanVec indexes still in kStaging (training never reached the
  // threshold), there is no built SVS index to flush into; the buffer is
  // simply discarded.
  if (!pending_buffer_.empty() &&
      index_state_ == SVSIndexState::kReady) {
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

  if (leanvec_training_data_) {
    auto status = svs::runtime::v0::LeanVecTrainingData::destroy(
        leanvec_training_data_);
    if (!status.ok()) {
      VMSDK_LOG(WARNING, nullptr)
          << "LeanVec training data destroy failed: " << status.message();
    }
    leanvec_training_data_ = nullptr;
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
    if (svs_params.leanvec_dims() > 0) {
      config.leanvec_dims = svs_params.leanvec_dims();
    }
    if (svs_params.leanvec_training_threshold() > 0) {
      config.leanvec_training_threshold =
          svs_params.leanvec_training_threshold();
    }
    if (svs_params.raw_vector_storage() ==
        data_model::RAW_VECTOR_STORAGE_DROP) {
      config.drop_intern_store = true;
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

  // Configure OpenMP threads used by the SVS runtime. Controlled via the
  // "--svs-omp-threads" module option. PoC default is 1 to avoid interference
  // with valkey-search's own thread pools; setting it higher may reduce
  // per-query latency at the cost of extra CPU per search. 0 means "don't
  // touch" (use the library default / OMP_NUM_THREADS env var).
#ifdef _OPENMP
  long long omp_threads = options::GetSVSOmpThreads().GetValue();
  if (omp_threads > 0) {
    omp_set_num_threads(static_cast<int>(omp_threads));
  }
#endif

  // For non-LeanVec compression types we can build an empty SVS index now
  // and stream vectors in via add() / FlushBuffer(). LeanVec needs a
  // training set first, so we defer the SVS build until the buffer reaches
  // leanvec_training_threshold (see TrainAndBuildLeanVecIndex). During
  // staging the index is fully usable from valkey's perspective except
  // FT.SEARCH (and the few mutation operations that touch the SVS graph)
  // which return FailedPreconditionError.
  if (IsLeanVecCompression(config.compression)) {
    index->index_state_ = SVSIndexState::kStaging;
    VMSDK_LOG(NOTICE, nullptr)
        << "Created SVS Vamana index in STAGING state (LeanVec): dim="
        << vector_index_proto.dimension_count()
        << " compression=" << CompressionTypeName(config.compression)
        << " leanvec_dims=" << config.leanvec_dims
        << " leanvec_training_threshold=" << config.leanvec_training_threshold;
    return index;
  }

  // Build the SVS index immediately for non-LeanVec compression.
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

  index->index_state_ = SVSIndexState::kReady;

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

    Metrics::GetStats().svs_pending_buffer_vectors.fetch_add(
        1, std::memory_order_relaxed);

    // Check if buffer is full → trigger auto-flush.
    // For LeanVec staging the threshold is leanvec_training_threshold;
    // crossing it builds the index and ingests the buffer in one step.
    // For non-LeanVec the threshold is the static kBufferSize.
    if (index_state_ == SVSIndexState::kStaging) {
      if (pending_buffer_.size() >=
          build_config_.leanvec_training_threshold) {
        VMSDK_RETURN_IF_ERROR(TrainAndBuildLeanVecIndex());
      }
    } else if (pending_buffer_.size() >= kBufferSize) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());
    }

    ++num_elements_;
    return absl::OkStatus();
  } catch (const std::exception& e) {
    if (!pending_buffer_.empty() &&
        pending_buffer_.back().internal_id == internal_id) {
      pending_buffer_.pop_back();
      Metrics::GetStats().svs_pending_buffer_vectors.fetch_sub(
          1, std::memory_order_relaxed);
    }
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
  vmsdk::StopWatch flush_timer;

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

    // Record flush duration and counters. Sampling happens only on success
    // so the histogram reflects healthy flushes.
    auto flush_duration = flush_timer.Duration();
    Metrics::GetStats().svs_flush_latency.SubmitSample(flush_duration);
    Metrics::GetStats().svs_flush_cnt.fetch_add(1, std::memory_order_relaxed);
    Metrics::GetStats().svs_flushed_vectors_cnt.fetch_add(
        batch_size, std::memory_order_relaxed);
    // pending_buffer_vectors is a gauge across all SVS indexes. We decrement
    // by what we just flushed; AddRecordImpl increments on each enqueue.
    Metrics::GetStats().svs_pending_buffer_vectors.fetch_sub(
        batch_size, std::memory_order_relaxed);

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
absl::Status VectorSVS<T>::TrainAndBuildLeanVecIndex() {
  // Caller (AddRecordImpl) holds index_mutex_ exclusively.
  if (svs_index_ != nullptr) {
    return absl::FailedPreconditionError(
        "TrainAndBuildLeanVecIndex called when SVS index already exists");
  }
  if (pending_buffer_.empty()) {
    return absl::FailedPreconditionError(
        "TrainAndBuildLeanVecIndex called with empty pending buffer");
  }

  size_t n = pending_buffer_.size();
  VMSDK_LOG(NOTICE, nullptr)
      << "Training LeanVec on " << n << " buffered vectors (leanvec_dims="
      << build_config_.leanvec_dims << ")";

  vmsdk::StopWatch total_timer;
  buffer_flushing_ = true;  // Block searches during build (search rejects in
                            // staging anyway, but keep the gauge accurate).

  try {
    // 1. Flatten buffered vectors into [n × dim] contiguous FP32 for both
    //    LeanVecTrainingData::build (which reads it) and the subsequent
    //    DynamicVamanaIndex::add (which copies it into SVS storage).
    std::vector<float> flat(n * dimensions_);
    std::vector<size_t> labels(n);
    for (size_t i = 0; i < n; ++i) {
      labels[i] = static_cast<size_t>(pending_buffer_[i].internal_id);
      const float* src =
          reinterpret_cast<const float*>(pending_buffer_[i].data.data());
      std::copy(src, src + dimensions_, flat.begin() + i * dimensions_);
    }

    // 2. Train LeanVec compression matrices.
    auto train_status = svs::runtime::v0::LeanVecTrainingData::build(
        &leanvec_training_data_,
        static_cast<size_t>(dimensions_),
        n,
        flat.data(),
        build_config_.leanvec_dims);
    if (!train_status.ok()) {
      buffer_flushing_ = false;
      return absl::InternalError(absl::StrCat(
          "LeanVec training failed: ", train_status.message()));
    }

    // 3. Build empty LeanVec-backed DynamicVamana index using the matrices.
    auto svs_metric = ToSVSMetric(distance_metric_);
    auto storage_kind = ToSVSStorageKind(build_config_.compression);

    svs::runtime::v0::VamanaIndex::BuildParams build_params;
    build_params.graph_max_degree = build_config_.graph_max_degree;
    build_params.construction_window_size =
        build_config_.construction_window_size;
    build_params.alpha = build_config_.alpha;

    svs::runtime::v0::VamanaIndex::SearchParams search_params;
    search_params.search_window_size = build_config_.search_window_size;

    auto build_status = svs::runtime::v0::DynamicVamanaIndexLeanVec::build(
        &svs_index_,
        static_cast<size_t>(dimensions_),
        svs_metric,
        storage_kind,
        leanvec_training_data_,
        build_params,
        search_params);
    if (!build_status.ok()) {
      buffer_flushing_ = false;
      // Drop the partially-trained matrices; caller can retry.
      (void)svs::runtime::v0::LeanVecTrainingData::destroy(
          leanvec_training_data_);
      leanvec_training_data_ = nullptr;
      return absl::InternalError(absl::StrCat(
          "LeanVec index build failed: ", build_status.message()));
    }

    // 4. Ingest the buffered vectors as the first batch — exactly like
    //    FlushBuffer does for non-LeanVec compression types. The SVS
    //    runtime API requires this two-step train→build→add flow for
    //    LeanVec; verified against bindings/cpp/tests/runtime_test.cpp.
    auto add_status = svs_index_->add(n, labels.data(), flat.data());
    if (!add_status.ok()) {
      buffer_flushing_ = false;
      return absl::InternalError(absl::StrCat(
          "LeanVec initial add failed: ", add_status.message()));
    }

    // 5. Transition to ready. The training matrices are now owned by the
    //    SVS index; we release our copy.
    pending_buffer_.clear();
    buffer_flushing_ = false;
    index_state_ = SVSIndexState::kReady;
    (void)svs::runtime::v0::LeanVecTrainingData::destroy(
        leanvec_training_data_);
    leanvec_training_data_ = nullptr;

    auto duration = total_timer.Duration();
    Metrics::GetStats().svs_flush_latency.SubmitSample(duration);
    Metrics::GetStats().svs_flush_cnt.fetch_add(1,
                                                std::memory_order_relaxed);
    Metrics::GetStats().svs_flushed_vectors_cnt.fetch_add(
        n, std::memory_order_relaxed);
    Metrics::GetStats().svs_pending_buffer_vectors.fetch_sub(
        n, std::memory_order_relaxed);

    VMSDK_LOG(NOTICE, nullptr)
        << "LeanVec index ready. Trained on " << n << " vectors, ingested "
        << n << " vectors. State=ready.";
    return absl::OkStatus();
  } catch (const std::exception& e) {
    buffer_flushing_ = false;
    return absl::InternalError(
        absl::StrCat("LeanVec train-and-build exception: ", e.what()));
  }
}

template <typename T>
absl::Status VectorSVS<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::MutexLock lock(&index_mutex_);

    // During LeanVec staging there is no SVS graph yet; remove is rejected.
    if (index_state_ == SVSIndexState::kStaging) {
      return absl::FailedPreconditionError(
          absl::StrCat("Index is training (",
                       pending_buffer_.size(), "/",
                       build_config_.leanvec_training_threshold,
                       " vectors); remove is unavailable until ready."));
    }

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
  try {
    absl::MutexLock lock(&index_mutex_);

    if (index_state_ == SVSIndexState::kStaging) {
      return absl::FailedPreconditionError(
          absl::StrCat("Index is training (",
                       pending_buffer_.size(), "/",
                       build_config_.leanvec_training_threshold,
                       " vectors); modify is unavailable until ready."));
    }

    if (!pending_buffer_.empty()) {
      VMSDK_RETURN_IF_ERROR(FlushBuffer());
    }

    size_t label = static_cast<size_t>(internal_id);
    auto remove_status = svs_index_->remove(1, &label);
    if (!remove_status.ok()) {
      return absl::InternalError(
          absl::StrCat("SVS remove (modify) failed: ",
                       remove_status.message()));
    }

    auto add_status = svs_index_->add(
        1, &label, reinterpret_cast<const float*>(record.data()));
    if (!add_status.ok()) {
      if (num_elements_ > 0) {
        --num_elements_;
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

  // Start total-search timer. Captures lock-wait + core + post-processing.
  vmsdk::StopWatch total_search_timer;
  Metrics::GetStats().svs_search_cnt.fetch_add(1, std::memory_order_relaxed);

  auto perform_search =
      [this, count, &filter, &search_window_size,
       &cancellation_token](absl::string_view q)
          -> absl::StatusOr<
              std::priority_queue<std::pair<T, hnswlib::labeltype>>> {
    try {
      // Auto-flush buffered vectors before searching (for benchmarking)
      bool needs_flush = false;
      bool was_flushing = false;
      {
        absl::ReaderMutexLock check_lock(&index_mutex_);
        // LeanVec indexes have no SVS graph until training fires; reject.
        if (index_state_ == SVSIndexState::kStaging) {
          return absl::FailedPreconditionError(absl::StrCat(
              "Index is training (", pending_buffer_.size(), "/",
              build_config_.leanvec_training_threshold,
              " vectors); search is unavailable until ready."));
        }
        needs_flush = !pending_buffer_.empty() && !buffer_flushing_;
        was_flushing = buffer_flushing_;
      }
      if (was_flushing) {
        Metrics::GetStats().svs_searches_during_flush_cnt.fetch_add(
            1, std::memory_order_relaxed);
      }

      if (needs_flush) {
        absl::MutexLock flush_lock(&index_mutex_);
        // Double-check buffer is still non-empty after acquiring write lock
        if (!pending_buffer_.empty()) {
          VMSDK_RETURN_IF_ERROR(FlushBuffer());
        }
      }

      // Measure lock wait time. If searches are backed up behind a writer
      // (e.g. during FlushBuffer), this is where the blackout shows up.
      vmsdk::StopWatch lock_wait_timer;
      absl::ReaderMutexLock lock(&index_mutex_);
      auto lock_wait = lock_wait_timer.Duration();
      Metrics::GetStats().svs_search_lock_wait_latency.SubmitSample(lock_wait);
      Metrics::GetStats().svs_search_blackout_us_total.fetch_add(
          absl::ToInt64Microseconds(lock_wait), std::memory_order_relaxed);

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

      // Configure OpenMP thread count on this search thread. omp_set_num_threads
      // sets a per-thread ICV, so setting it once on the main thread during
      // index creation does not propagate to reader threads. We re-apply here
      // so the config reflects on each reader thread. 0 = library default.
#ifdef _OPENMP
      {
        long long omp_threads = options::GetSVSOmpThreads().GetValue();
        if (omp_threads > 0) {
          omp_set_num_threads(static_cast<int>(omp_threads));
        }
      }
#endif

      // Measure core SVS search time (isolated from lock wait and post-proc).
      vmsdk::StopWatch core_search_timer;
      auto status = svs_index_->search(
          1,  // single query
          reinterpret_cast<const float*>(q.data()),
          k,
          distances.data(),
          labels.data(),
          params_ptr,
          svs_filter.get());
      Metrics::GetStats().svs_search_core_latency.SubmitSample(
          core_search_timer.Duration());

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
    auto reply = CreateReply(search_result);
    Metrics::GetStats().svs_vector_index_search_latency.SubmitSample(
        total_search_timer.Duration());
    return reply;
  }
  VMSDK_ASSIGN_OR_RETURN(auto search_result, perform_search(query));
  auto reply = CreateReply(search_result);
  Metrics::GetStats().svs_vector_index_search_latency.SubmitSample(
      total_search_timer.Duration());
  return reply;
}

// --- Vector tracking ---

template <typename T>
void VectorSVS<T>::TrackVector(uint64_t internal_id,
                                const InternedStringPtr& vector) {
  if (build_config_.drop_intern_store) return;
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_[internal_id] = vector;
}

template <typename T>
bool VectorSVS<T>::IsVectorMatch(uint64_t internal_id,
                                  const InternedStringPtr& vector) {
  if (build_config_.drop_intern_store) {
    absl::ReaderMutexLock lock(&index_mutex_);
    if (index_state_ != SVSIndexState::kReady || svs_index_ == nullptr) {
      for (const auto& p : pending_buffer_) {
        if (p.internal_id == internal_id) {
          return absl::string_view(p.data.data(), p.data.size()) ==
                 vector->Str();
        }
      }
      return false;
    }
    float dist = 0.0f;
    auto status = svs_index_->get_distance(
        static_cast<size_t>(internal_id),
        reinterpret_cast<const float*>(vector->Str().data()), &dist);
    return status.ok() && dist == 0.0f;
  }
  absl::MutexLock lock(&tracked_vectors_mutex_);
  auto it = tracked_vectors_.find(internal_id);
  if (it == tracked_vectors_.end()) {
    return false;
  }
  return it->second->Str() == vector->Str();
}

template <typename T>
void VectorSVS<T>::UnTrackVector(uint64_t internal_id) {
  if (build_config_.drop_intern_store) return;
  absl::MutexLock lock(&tracked_vectors_mutex_);
  tracked_vectors_.erase(internal_id);
}

template <typename T>
absl::StatusOr<std::pair<float, hnswlib::labeltype>>
VectorSVS<T>::ComputeDistanceFromRecordImpl(
    uint64_t internal_id, absl::string_view query) const {
  {
    absl::ReaderMutexLock lock(&index_mutex_);
    if (index_state_ == SVSIndexState::kStaging) {
      return absl::FailedPreconditionError(
          absl::StrCat("Index is training (",
                       pending_buffer_.size(), "/",
                       build_config_.leanvec_training_threshold,
                       " vectors); search is unavailable until ready."));
    }
    float dist = 0.0f;
    auto status = svs_index_->get_distance(
        static_cast<size_t>(internal_id),
        reinterpret_cast<const float*>(query.data()),
        &dist);
    if (status.ok()) {
      return std::pair<float, hnswlib::labeltype>{dist, internal_id};
    }
  }

  // Fallback: vector is in pending_buffer_ (not yet flushed to SVS graph).
  if (build_config_.drop_intern_store) {
    absl::ReaderMutexLock lock(&index_mutex_);
    for (const auto& p : pending_buffer_) {
      if (p.internal_id == internal_id) {
        auto dist = space_->get_dist_func()(
            reinterpret_cast<const T*>(query.data()),
            reinterpret_cast<const T*>(p.data.data()),
            space_->get_dist_func_param());
        return std::pair<float, hnswlib::labeltype>{dist, internal_id};
      }
    }
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }

  absl::ReaderMutexLock lock(&tracked_vectors_mutex_);
  auto it = tracked_vectors_.find(internal_id);
  if (it == tracked_vectors_.end()) {
    return absl::InternalError(
        absl::StrCat("Couldn't find internal id: ", internal_id));
  }

  auto dist = space_->get_dist_func()(
      reinterpret_cast<const T*>(query.data()),
      reinterpret_cast<const T*>(it->second->Str().data()),
      space_->get_dist_func_param());

  return std::pair<float, hnswlib::labeltype>{dist, internal_id};
}

template <typename T>
char* VectorSVS<T>::GetValueImpl(uint64_t internal_id) const {
  if (build_config_.drop_intern_store) {
    thread_local std::vector<float> tl_buffer;
    tl_buffer.resize(dimensions_);
    absl::ReaderMutexLock lock(&index_mutex_);
    if (index_state_ != SVSIndexState::kReady || svs_index_ == nullptr) {
      for (const auto& p : pending_buffer_) {
        if (p.internal_id == internal_id) {
          return const_cast<char*>(p.data.data());
        }
      }
      return nullptr;
    }
    size_t id = static_cast<size_t>(internal_id);
    auto status = svs_index_->reconstruct_at(1, &id, tl_buffer.data());
    if (!status.ok()) return nullptr;
    return reinterpret_cast<char*>(tl_buffer.data());
  }
  absl::ReaderMutexLock lock(&tracked_vectors_mutex_);
  auto it = tracked_vectors_.find(internal_id);
  if (it == tracked_vectors_.end()) {
    return nullptr;
  }
  return const_cast<char*>(it->second->Str().data());
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
  svs_algo_proto->set_leanvec_dims(build_config_.leanvec_dims);
  svs_algo_proto->set_leanvec_training_threshold(
      build_config_.leanvec_training_threshold);
  svs_algo_proto->set_raw_vector_storage(
      build_config_.drop_intern_store ? data_model::RAW_VECTOR_STORAGE_DROP
                                      : data_model::RAW_VECTOR_STORAGE_KEEP);
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

  // Snapshot state under a read lock so we don't observe a transition
  // mid-reply.
  SVSIndexState state_snapshot;
  size_t buffered_snapshot;
  {
    absl::ReaderMutexLock lock(&index_mutex_);
    state_snapshot = index_state_;
    buffered_snapshot = pending_buffer_.size();
  }

  bool is_leanvec = IsLeanVecCompression(build_config_.compression);
  // Base pairs: name, graph_max_degree, cws, sws, alpha, compression, state,
  // raw_vector_storage.
  // For LeanVec also: leanvec_dims, leanvec_training_threshold,
  // training_progress.
  int n_pairs = is_leanvec ? 11 : 8;
  ValkeyModule_ReplyWithArray(ctx, n_pairs * 2);

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
  ValkeyModule_ReplyWithSimpleString(ctx, "state");
  ValkeyModule_ReplyWithSimpleString(
      ctx, state_snapshot == SVSIndexState::kStaging ? "training" : "ready");
  ValkeyModule_ReplyWithSimpleString(ctx, "raw_vector_storage");
  ValkeyModule_ReplyWithSimpleString(
      ctx, build_config_.drop_intern_store ? "DROP" : "KEEP");

  if (is_leanvec) {
    ValkeyModule_ReplyWithSimpleString(ctx, "leanvec_dims");
    ValkeyModule_ReplyWithLongLong(ctx, build_config_.leanvec_dims);
    ValkeyModule_ReplyWithSimpleString(ctx, "leanvec_training_threshold");
    ValkeyModule_ReplyWithLongLong(
        ctx, build_config_.leanvec_training_threshold);
    ValkeyModule_ReplyWithSimpleString(ctx, "training_progress");
    // "buffered/threshold" string so it's grep-friendly in tests.
    auto progress = absl::StrCat(buffered_snapshot, "/",
                                  build_config_.leanvec_training_threshold);
    ValkeyModule_ReplyWithStringBuffer(ctx, progress.c_str(),
                                        progress.size());
  }

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
