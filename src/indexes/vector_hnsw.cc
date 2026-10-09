/*
 * Copyright (c) 2026, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/vector_hnsw.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <mutex>  // NOLINT(build/c++11)
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "src/attribute_data_type.h"
#include "src/indexes/bfloat16.h"
#include "src/indexes/fp16.h"
#include "src/indexes/index_base.h"
#include "src/indexes/scoring/scorer.h"
#include "src/indexes/vector_base.h"
#include "src/indexes/vector_type.h"
#include "src/metrics.h"
#include "src/query/search.h"
#include "src/rdb_serialization.h"
#include "src/utils/cancel.h"
#include "src/utils/string_interning.h"
#include "src/valkey_search.h"
#include "src/valkey_search_options.h"
#include "valkey_search_options.h"
#include "vmsdk/src/log.h"
#include "vmsdk/src/status/status_macros.h"
#include "vmsdk/src/utils.h"
#include "vmsdk/src/valkey_module_api/valkey_module.h"

// Note that the ordering matters here - we want to minimize the memory
// overrides to just the hnswlib code.
// clang-format off
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/hnswlib.h"
// clang-format on

namespace valkey_search::indexes {

template <typename T>
absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> VectorHNSW<T>::Create(
    const data_model::VectorIndex &vector_index_proto,
    absl::string_view attribute_identifier,
    data_model::AttributeDataType attribute_data_type, int db_num) {
  try {
    auto index = std::shared_ptr<VectorHNSW<T>>(
        new VectorHNSW<T>(vector_index_proto.dimension_count(),
                          attribute_identifier, attribute_data_type, db_num),
        vmsdk::DestructByMainThread<VectorHNSW<T>>{});
    index->Init(vector_index_proto.distance_metric());
    const auto &hnsw_proto = vector_index_proto.hnsw_algorithm();

    index->algo_ = std::make_unique<HNSWIndex>(
        index->space_.get(), vector_index_proto.initial_cap(),
        index->normalize_, hnsw_proto.m(), hnsw_proto.ef_construction(),
        options::GetHNSWAllowReplaceDeleted().GetValue());
    index->algo_->setEf(hnsw_proto.ef_runtime());
    return index;
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_create_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("HNSWLib error while creating a record: ", e.what()));
  }
}

template <typename T>
std::optional<hnswlib::tableint> VectorHNSW<T>::GetAlgoIdLockFree(
    uint64_t internal_id) const {
  auto search = algo_->label_lookup_.find(internal_id);
  if (search == algo_->label_lookup_.end() ||
      algo_->isMarkedDeleted(search->second)) {
    return std::nullopt;
  }
  return search->second;
}

template <typename T>
absl::StatusOr<std::shared_ptr<VectorHNSW<T>>> VectorHNSW<T>::LoadFromRDB(
    ValkeyModuleCtx *ctx, const AttributeDataType *attribute_data_type,
    const data_model::VectorIndex &vector_index_proto,
    absl::string_view attribute_identifier, SupplementalContentChunkIter &&iter,
    int db_num) {
  try {
    auto index = std::shared_ptr<VectorHNSW<T>>(
        new VectorHNSW<T>(vector_index_proto.dimension_count(),
                          attribute_identifier, attribute_data_type->ToProto(),
                          db_num),
        vmsdk::DestructByMainThread<VectorHNSW<T>>{});
    index->Init(vector_index_proto.distance_metric());

    index->algo_ = std::make_unique<HNSWIndex>();
    // initial_cap needs to be provided to retain the original initial_cap if
    // the index being loaded is empty.

    index->algo_->allow_replace_deleted_ =
        options::GetHNSWAllowReplaceDeleted().GetValue();
    RDBChunkInputStream input(std::move(iter));
    index->algo_->normalized_ = index->normalize_;

    auto generator = [allocator = index->GetVectorAllocator()](
                         absl::string_view vector_data,
                         bool is_marked_deleted) {
      if (!is_marked_deleted) {
        return VectorRecord(nullptr);
      }
      // For normalized indexes, tombstones are saved already normalized (see
      // AlgoDeleteRecord), so restore them with a reciprocal magnitude of
      // exactly 1.0; the HNSW distance path relies on this invariant. For
      // other indexes the magnitude is not used.
      return VectorRecord::Construct(
          vector_data, 1.0f, static_cast<FixedSizeAllocator *>(allocator));
    };
    VMSDK_RETURN_IF_ERROR(index->algo_->LoadIndex(
        input, index->space_.get(), vector_index_proto.initial_cap(),
        vector_index_proto.hnsw_algorithm().m(),
        options::GetHNSWValidationEnable().GetValue(), generator));
    // ef_runtime is not persisted in the index contents
    index->algo_->setEf(vector_index_proto.hnsw_algorithm().ef_runtime());
    return index;
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_create_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("HNSWLib error while loading an index: ", e.what()));
  }
}

template <typename T>
VectorHNSW<T>::VectorHNSW(int dimensions,
                          absl::string_view attribute_identifier,
                          data_model::AttributeDataType attribute_data_type,
                          int db_num)
    : VectorType<T>(IndexerType::kHNSW, dimensions, attribute_data_type,
                    attribute_identifier, db_num) {}

template <typename T>
absl::Status VectorHNSW<T>::AddRecordImpl(uint64_t internal_id,
                                          VectorRecord &&vector_record) {
  do {
    try {
      absl::ReaderMutexLock lock(&resize_mutex_);

      algo_->addPoint(QueryVector(vector_record, GetVectorDataSize(),
                                  normalize_, GetVectorDataType()),
                      internal_id, algo_->allow_replace_deleted_);
      return absl::OkStatus();
    } catch (const std::exception &e) {
      std::string error_msg = e.what();
      if (absl::StrContains(
              error_msg,
              "The number of elements exceeds the specified limit")) {
        VMSDK_RETURN_IF_ERROR(ResizeIfFull());
        continue;
      }
      DCHECK(false) << "Unexpected error while adding a record: " << e.what();
      ++Metrics::GetStats().hnsw_add_exceptions_cnt;
      return absl::InternalError(
          absl::StrCat("Error while adding a record: ", e.what()));
    }
  } while (true);
}

template <typename T>
int VectorHNSW<T>::RespondWithInfoImpl(ValkeyModuleCtx *ctx) const {
  EmitDataTypeInfo(ctx);
  ValkeyModule_ReplyWithSimpleString(ctx, "algorithm");
  ValkeyModule_ReplyWithArray(ctx, 8);
  ValkeyModule_ReplyWithSimpleString(ctx, "name");
  ValkeyModule_ReplyWithSimpleString(
      ctx,
      std::string(LookupKeyByValue(
                      *kVectorAlgoByStr,
                      data_model::VectorIndex::AlgorithmCase::kHnswAlgorithm))
          .c_str());
  ValkeyModule_ReplyWithSimpleString(ctx, "m");
  absl::ReaderMutexLock lock(&resize_mutex_);
  ValkeyModule_ReplyWithLongLong(ctx, GetM());
  ValkeyModule_ReplyWithSimpleString(ctx, "ef_construction");
  ValkeyModule_ReplyWithLongLong(ctx, GetEfConstruction());
  ValkeyModule_ReplyWithSimpleString(ctx, "ef_runtime");
  ValkeyModule_ReplyWithLongLong(ctx, GetEfRuntime());
  return 4;
}

template <typename T>
absl::Status VectorHNSW<T>::SaveIndexImpl(
    RDBChunkOutputStream chunked_out) const {
  absl::ReaderMutexLock lock(&resize_mutex_);
  auto serializer = [normalize = normalize_, vector_size = GetVectorDataSize()](
                        const VectorRecord &record, bool is_marked_deleted) {
    if (normalize && !is_marked_deleted) {
      return NormalizeVector<T>(
          absl::string_view(record.GetRawVector(), vector_size));
    }
    return std::vector<char>(record.GetRawVector(),
                             record.GetRawVector() + vector_size);
  };
  return algo_->SaveIndex(chunked_out, serializer);
}

template <typename T>
absl::Status VectorHNSW<T>::ResizeIfFull() {
  {
    absl::ReaderMutexLock lock(&resize_mutex_);
    if (algo_->getCurrentElementCount() < algo_->getMaxElements() ||
        (algo_->allow_replace_deleted_ && algo_->getDeletedCount() > 0)) {
      return absl::OkStatus();
    }
  }
  try {
    absl::WriterMutexLock lock(&resize_mutex_);
    if (algo_->getCurrentElementCount() == algo_->getMaxElements() &&
        (!algo_->allow_replace_deleted_ || algo_->getDeletedCount() == 0)) {
      vmsdk::StopWatch stop_watch;
      auto max_elements = algo_->getMaxElements();
      // Notes
      // 1. Currently HNSWLib doesn't provide a way to shrink an index after
      // it was expanded.
      // 2. Once multithreaded is supported we'll have to make sure that no
      // thread is reading/writing during resize
      auto block_size = ValkeySearch::Instance().GetHNSWBlockSize();
      algo_->resizeIndex(algo_->getMaxElements() + block_size);
      VMSDK_LOG(WARNING, nullptr)
          << "Resizing HNSW Index, current size: " << max_elements
          << ", expand by: " << block_size << ", resize time took: "
          << absl::FormatDuration(stop_watch.Duration());
    }
  } catch (const std::exception &e) {
    ++Metrics::GetStats().hnsw_add_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while adding a record: ", e.what()));
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status VectorHNSW<T>::AlgoDeleteRecord(uint64_t label) {
  std::unique_lock<std::mutex> lock_label(algo_->getLabelOpMutex(label));
  auto hnsw_internal_id = GetAlgoIdLockFree(label);
  if (!hnsw_internal_id.has_value()) {
    return absl::NotFoundError(
        absl::StrCat("Internal ID not found for label: ", label));
  }
  if (!normalize_) {
    algo_->markDeletedInternal(*hnsw_internal_id);
    return absl::OkStatus();
  }
  const auto &stored_record = algo_->GetDataByInternalId(*hnsw_internal_id);
  absl::string_view unnorm_vector(stored_record.GetRawVector(),
                                  GetVectorDataSize());

  auto norm_record = NormalizeVector<T>(unnorm_vector);

  absl::string_view norm_view(norm_record.data(), norm_record.size());
  auto vector_record =
      VectorRecord::Construct(norm_view, 1.0f, GetVectorAllocator());
  algo_->SetDataByInternalId(*hnsw_internal_id, std::move(vector_record));
  algo_->markDeletedInternal(*hnsw_internal_id);
  return absl::OkStatus();
}

template <typename T>
absl::Status VectorHNSW<T>::ModifyRecordImpl(uint64_t internal_id,
                                             VectorRecord &&vector_record) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);
    // addPoint() routes an existing label to an in-place update.
    algo_->addPoint(QueryVector(vector_record, GetVectorDataSize(), normalize_,
                                GetVectorDataType()),
                    internal_id, /*replace_deleted=*/false);
  } catch (const std::exception &e) {
    DCHECK(false) << "Unexpected error while modifying a record: " << e.what();
    ++Metrics::GetStats().hnsw_modify_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while modifying a record: ", e.what()));
  }
  return absl::OkStatus();
}

template <typename T>
absl::Status VectorHNSW<T>::RemoveRecordImpl(uint64_t internal_id) {
  try {
    absl::ReaderMutexLock lock(&resize_mutex_);
    // Normalize the record before marking it deleted so that distance
    // calculations against it (during search/traversal) use magnitude 1.0f.
    VMSDK_RETURN_IF_ERROR(AlgoDeleteRecord(internal_id));
  } catch (const std::exception &e) {
    DCHECK(false) << "Unexpected error while removing a record: " << e.what();
    ++Metrics::GetStats().hnsw_remove_exceptions_cnt;
    return absl::InternalError(
        absl::StrCat("Error while removing a record: ", e.what()));
  }
  return absl::OkStatus();
}

// Paper over the impedance mismatch between the
// cancel::Token and hnswlib::BaseCancellationFunctor.
class CancelCondition : public hnswlib::BaseCancellationFunctor {
 public:
  explicit CancelCondition(cancel::Token &token) : token_(token) {}
  bool isCancelled() override { return token_->IsCancelled(); }

 private:
  cancel::Token &token_;
};

// Compute the range traversal shell outside float arithmetic: FLT_MAX with
// even the default epsilon overflows in float. Query parsing keeps epsilon
// finite, and the double product of two finite floats cannot overflow double.
float ComputeRangeShell(float radius, float epsilon) {
  CHECK(!scoring::IsNaN(radius));
  CHECK(!scoring::IsInf(radius));
  CHECK(!scoring::IsNaN(epsilon));
  CHECK(!scoring::IsInf(epsilon));
  const double shell =
      static_cast<double>(radius) * (1.0 + static_cast<double>(epsilon));
  constexpr float kMaxShell = std::numeric_limits<float>::max();
  return shell >= static_cast<double>(kMaxShell) ? kMaxShell
                                                 : static_cast<float>(shell);
}

// Stop condition of the HNSW range traversal. A node is expanded while it is
// within `shell` of the query or could still improve the `ef` nearest results,
// so the walk first homes in on the query as an ef-bounded KNN search does,
// whatever the radius, and then covers the ball. Results within the shell are
// kept, at most `max_results` of the nearest. Once that many are held within
// the shell the walk stops (SearchRange then scans); while the farthest held
// result is beyond the shell, it only looks for nearer ones.
class RangeStopCondition : public hnswlib::BaseSearchStopCondition<float> {
 public:
  RangeStopCondition(float shell, size_t ef, size_t max_results)
      : shell_(shell),
        ef_(std::clamp<size_t>(ef, 1, max_results)),
        max_results_(max_results) {
    CHECK(!scoring::IsNaN(shell_));
    CHECK(!scoring::IsInf(shell_));
  }

  void add_point_to_result(hnswlib::labeltype, const void *,
                           float dist) override {
    ++num_results_;
    if (nearest_.size() < ef_) {
      nearest_.push(dist);
    } else if (dist < nearest_.top()) {
      nearest_.pop();
      nearest_.push(dist);
    }
  }
  void remove_point_from_result(hnswlib::labeltype, const void *,
                                float) override {
    --num_results_;
  }
  bool should_stop_search(float candidate_dist, float lower_bound) override {
    // Once the fetch is full within the shell it stays so (later results only
    // displace farther ones), and SearchRange then scans exhaustively.
    if (num_results_ >= max_results_ &&
        (candidate_dist > lower_bound || lower_bound <= shell_)) {
      return true;
    }
    return candidate_dist > shell_ && candidate_dist > EfBound();
  }
  bool should_consider_candidate(float dist, float lower_bound) override {
    if (num_results_ >= max_results_ && dist >= lower_bound) {
      return false;
    }
    return dist <= shell_ || dist < EfBound();
  }
  bool should_remove_extra() override { return num_results_ > max_results_; }
  void filter_results(
      std::vector<std::pair<float, hnswlib::labeltype>> &results) override {
    while (!results.empty() && results.back().first > shell_) {
      results.pop_back();
    }
  }

 private:
  float EfBound() const {
    return nearest_.size() < ef_ ? std::numeric_limits<float>::max()
                                 : nearest_.top();
  }

  const float shell_;
  const size_t ef_;
  const size_t max_results_;
  size_t num_results_{0};
  std::priority_queue<float> nearest_;  // The ef nearest distances, max on top.
};

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorHNSW<T>::Search(
    absl::string_view query, uint64_t count, cancel::Token &cancellation_token,
    std::unique_ptr<hnswlib::BaseFilterFunctor> filter,
    std::optional<size_t> ef_runtime, bool enable_partial_results) {
  if (!IsValidSizeVector(query)) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Error parsing vector similarity query: query vector blob size (",
        query.size(), ") does not match index's expected size (",
        dimensions_ * GetDataTypeSize(), ")."));
  }
  float reciprocal_magnitude =
      normalize_ ? CalcReciprocalMagnitude(
                       reinterpret_cast<const T *>(query.data()), dimensions_)
                 : kDefaultMagnitude;
  try {
    CancelCondition cancel_condition(cancellation_token);
    VectorRecord query_record = VectorRecord::Construct(
        query, reciprocal_magnitude, GetVectorAllocator());
    QueryVector embedding(query_record, query.size(), normalize_,
                          GetVectorDataType());
    auto res = algo_->searchKnn(embedding, count, ef_runtime, filter.get(),
                                &cancel_condition);
    if (!enable_partial_results && cancellation_token->IsCancelled()) {
      return absl::CancelledError(query::kTimeoutMsg);
    }
    return CreateReply(res);
  } catch (const std::exception &e) {
    Metrics::GetStats().hnsw_search_exceptions_cnt.fetch_add(
        1, std::memory_order_relaxed);
    return absl::InternalError(e.what());
  }
}

template <typename T>
absl::StatusOr<std::vector<Neighbor>> VectorHNSW<T>::SearchRange(
    absl::string_view query, float radius, cancel::Token &cancellation_token,
    float epsilon, std::unique_ptr<hnswlib::BaseFilterFunctor> filter) {
  const size_t max_candidates = static_cast<size_t>(
      options::GetMaxNonVectorSearchResultsFetched().GetValue());

  // A cap of 0 fills the fetch at once: scan exhaustively.
  if (max_candidates == 0) {
    query::RecordNonVectorResultsFetchedLimited();
    return this->SearchRangeExhaustive(query, radius, cancellation_token,
                                       filter.get());
  }
  // COSINE distances clamp into [0, 2], so radius 2 holds every key: scan,
  // rather than walk the whole graph and depend on the shell reaching
  // unclamped distances just above 2.
  if (this->normalize_ && radius >= 2.0f) {
    return this->SearchRangeExhaustive(query, radius, cancellation_token,
                                       filter.get());
  }

  auto nq = this->NormalizeQueryIfNeeded(query);
  float reciprocal_magnitude =
      this->normalize_
          ? CalcReciprocalMagnitude(nq.view, this->GetVectorDataType())
          : kDefaultMagnitude;

  // The traversal also expands nodes within radius * (1 + epsilon), so paths
  // that leave the ball briefly near its boundary are still followed; only keys
  // within the radius are emitted. Distances are in the radius's own space for
  // every metric (squared for L2).
  QueryVector embedding(VectorRecord::Construct(nq.view, reciprocal_magnitude,
                                                GetVectorAllocator()),
                        nq.view.size(), normalize_, GetVectorDataType());
  std::vector<std::pair<float, hnswlib::labeltype>> raw_results;
  try {
    CancelCondition cancel_condition(cancellation_token);
    RangeStopCondition stop_condition(ComputeRangeShell(radius, epsilon),
                                      algo_->ef_, max_candidates);
    raw_results = algo_->searchStopConditionClosest(
        embedding, stop_condition, filter.get(), &cancel_condition);
  } catch (const std::exception &e) {
    Metrics::GetStats().hnsw_search_exceptions_cnt.fetch_add(
        1, std::memory_order_relaxed);
    return absl::InternalError(e.what());
  }
  // A cancelled walk returns the keys in range it reached, as Search() and the
  // scan do; the reply is a timeout error unless partial results are enabled.
  const bool cancelled = cancellation_token->IsCancelled();

  // A full fetch holds only results within the shell, and the walk stops once
  // it is full, so keys in range may be unvisited; scan exhaustively, as FLAT
  // does.
  if (!cancelled && raw_results.size() >= max_candidates) {
    query::RecordNonVectorResultsFetchedLimited();
    return this->SearchRangeExhaustive(query, radius, cancellation_token,
                                       filter.get());
  }

  std::vector<Neighbor> neighbors;
  neighbors.reserve(raw_results.size());
  for (const auto &[dist, label] : raw_results) {
    // NaN breaks the traversal's ordering, and +inf can stand for an
    // unnormalized infinite stored vector, so the walk cannot be trusted to
    // have reached every key in range. An IP -inf is an ordinary distance,
    // within every radius. IsNaN/IsInf/signbit read the bits, unaffected by
    // -ffast-math.
    if (scoring::IsNaN(dist) || (scoring::IsInf(dist) && !std::signbit(dist))) {
      if (cancelled) {
        continue;
      }
      query::RecordNonVectorResultsFetchedLimited();
      return this->SearchRangeExhaustive(query, radius, cancellation_token,
                                         filter.get());
    }
    const float clamped_dist = this->ClampCosineDistance(dist);
    if (clamped_dist > radius) {
      continue;
    }
    auto key = this->GetKeyDuringSearch(label);
    if (!key.ok()) {
      continue;
    }
    neighbors.emplace_back(*key, clamped_dist);
  }
  return neighbors;
}

template <typename T>
void VectorHNSW<T>::ToProtoImpl(
    data_model::VectorIndex *vector_index_proto) const {
  SetProtoDataType(vector_index_proto);
  absl::ReaderMutexLock lock(&resize_mutex_);
  auto hnsw_algorithm_proto = std::make_unique<data_model::HNSWAlgorithm>();
  hnsw_algorithm_proto->set_ef_construction(GetEfConstruction());
  hnsw_algorithm_proto->set_ef_runtime(GetEfRuntime());
  hnsw_algorithm_proto->set_m(GetM());
  vector_index_proto->set_allocated_hnsw_algorithm(
      hnsw_algorithm_proto.release());
}

template <typename T>
float VectorHNSW<T>::ComputeDistance(absl::string_view query,
                                     const VectorRecord &vector_record,
                                     float query_magnitude) const {
  return algo_->fstdistfunc_(query.data(), vector_record.GetRawVector(),
                             algo_->dist_func_param_, query_magnitude);
}

// Max label stamped on any slot at load time (includes re-labeled tombstones,
// which are absent from label_lookup_). Used to seed inc_id_ on load.
template <typename T>
uint64_t VectorHNSW<T>::GetMaxLoadedLabel() const {
  return static_cast<uint64_t>(algo_->max_loaded_label_);
}

template <typename T>
size_t VectorHNSW<T>::GetLabelCount() const {
  std::unique_lock<std::mutex> lock_label(algo_->label_lookup_lock);
  return algo_->label_lookup_.size();
}

template class VectorHNSW<float>;
template class VectorHNSW<float16>;
template class VectorHNSW<bfloat16>;

}  // namespace valkey_search::indexes
