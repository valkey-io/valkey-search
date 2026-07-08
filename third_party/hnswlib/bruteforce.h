#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/status/status.h"
#include "hnswlib.h"
#include "iostream.h"
#include "third_party/hnswlib/index.pb.h"
#include "vmsdk/src/status/status_macros.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

namespace hnswlib {
template <typename dist_t, typename VectorRecordT>
class BruteforceSearch
    : public AlgorithmInterface<dist_t, VectorRecordT, VectorRecordT> {
 public:
  std::unique_ptr<ChunkedArray> data_;
  size_t cur_element_count_;
  size_t vector_size_{0};
  const size_t data_ptr_size_ = sizeof(VectorRecordT);
  DISTFUNC<dist_t> fstdistfunc_;
  void *dist_func_param_;
  std::mutex index_lock;
  const size_t k_elements_per_chunk{10 * 1024};
  bool normalized_{false};

  std::unordered_map<labeltype, size_t> dict_external_to_internal;

  // Constructs a flat bruteforce vector index.
  // @param s Pointer to distance space interface (e.g., L2 or InnerProduct
  // space).
  // @param normalized Indicates if the metric requires vector normalization
  // (e.g. Cosine distance).
  // @param maxElements Pre-allocated capacity for initial vector storage
  // chunking.
  BruteforceSearch(SpaceInterface<dist_t> *s, bool normalized,
                   size_t maxElements = 0)
      : normalized_{normalized} {
    cur_element_count_ = 0;
    vector_size_ = s->get_data_size();
    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();
    if (maxElements > 0) {
      data_ = std::make_unique<ChunkedArray>(
          sizeof(VectorRecordT) + sizeof(labeltype), k_elements_per_chunk,
          maxElements);
    }
  }

  ~BruteforceSearch() override { clear(); }
  inline VectorRecordT *GetDataPtrByInternalId(size_t internal_id) const {
    return reinterpret_cast<VectorRecordT *>((*data_)[internal_id]);
  }
  labeltype GetLabel(const VectorRecordT *stored_vector) const {
    return *((labeltype *)(reinterpret_cast<const char *>(stored_vector) +
                           sizeof(VectorRecordT)));
  }

  labeltype GetLabel(size_t internal_id) const {
    return GetLabel(GetDataPtrByInternalId(internal_id));
  }

  // Computes distance between two vector records stored in the index.
  // When normalized_ is true (e.g. Cosine metric), scales inner product space
  // by the product of the reciprocal magnitudes of the vectors.
  inline dist_t EvaluateDistance(const VectorRecordT &a,
                                 const VectorRecordT &b) const {
    float reciprocal_mag_product =
        normalized_ ? a->GetReciprocalMagnitude() * b->GetReciprocalMagnitude()
                    : 1.0f;
    return fstdistfunc_(a->GetRawVector(), b->GetRawVector(), dist_func_param_,
                        reciprocal_mag_product);
  }

  void clear() {
    if (data_ != nullptr) {
      for (size_t i = 0; i < cur_element_count_; i++) {
        std::destroy_at(GetDataPtrByInternalId(i));
      }
      data_->clear();
    } else {
      CHECK(cur_element_count_ == 0);
    }
    dict_external_to_internal.clear();
    cur_element_count_ = 0;
  }

  void addPoint(VectorRecordT &&datapoint, labeltype label,
                bool replace_deleted = false) override {
    size_t idx;
    std::unique_lock<std::mutex> lock(index_lock);
    auto search = dict_external_to_internal.find(label);
    if (search != dict_external_to_internal.end()) {
      idx = search->second;
      *GetDataPtrByInternalId(idx) = std::move(datapoint);
    } else {
      if (cur_element_count_ >= data_->getCapacity()) {
        throw std::runtime_error(
            "The number of elements exceeds the specified limit\n");
      }
      idx = cur_element_count_;
      dict_external_to_internal[label] = idx;
      cur_element_count_++;
      VectorRecordT *stored_vector = GetDataPtrByInternalId(idx);
      new (stored_vector) VectorRecordT(std::move(datapoint));
    }
    memcpy(reinterpret_cast<char *>(GetDataPtrByInternalId(idx)) +
               sizeof(VectorRecordT),
           &label, sizeof(labeltype));
  }

  VectorRecordT *getPoint(labeltype cur_external) {
    std::unique_lock<std::mutex> lock(index_lock);
    auto found = dict_external_to_internal.find(cur_external);
    if (found == dict_external_to_internal.end()) {
      return nullptr;
    }
    return GetDataPtrByInternalId(found->second);
  }

  void removePoint(labeltype cur_external) {
    std::unique_lock<std::mutex> lock(index_lock);

    auto found = dict_external_to_internal.find(cur_external);
    if (found == dict_external_to_internal.end()) {
      return;
    }
    size_t cur_c = found->second;
    dict_external_to_internal.erase(found);
    if (cur_element_count_ - 1 == cur_c) {
      std::destroy_at(GetDataPtrByInternalId(cur_c));
      cur_element_count_--;
      return;
    }

    labeltype label = GetLabel(cur_element_count_ - 1);
    dict_external_to_internal[label] = cur_c;
    *GetDataPtrByInternalId(cur_c) =
        std::move(*GetDataPtrByInternalId(cur_element_count_ - 1));
    memcpy(reinterpret_cast<char *>(GetDataPtrByInternalId(cur_c)) +
               sizeof(VectorRecordT),
           reinterpret_cast<const char *>(
               GetDataPtrByInternalId(cur_element_count_ - 1)) +
               sizeof(VectorRecordT),
           sizeof(labeltype));
    std::destroy_at(GetDataPtrByInternalId(cur_element_count_ - 1));
    cur_element_count_--;
  }

  std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const VectorRecordT &query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr) const override {
    std::priority_queue<std::pair<dist_t, labeltype>> topResults;
    if (cur_element_count_ == 0 || k == 0) {
      return topResults;
    }
    const size_t initial_count = std::min(k, cur_element_count_);
    size_t i = 0;
    for (; i < initial_count && (!isCancelled || !isCancelled->isCancelled());
         i++) {
      const VectorRecordT *stored_vector = GetDataPtrByInternalId(i);
      dist_t dist = EvaluateDistance(query_data, *stored_vector);
      labeltype label = GetLabel(stored_vector);
      if ((!isIdAllowed) || (*isIdAllowed)(label)) {
        topResults.emplace(dist, label);
      }
    }
    if (isCancelled && isCancelled->isCancelled()) {
      return topResults;
    }
    dist_t lastdist = topResults.size() < k ? std::numeric_limits<dist_t>::max()
                                            : topResults.top().first;
    for (; i < cur_element_count_ &&
           (!isCancelled || !isCancelled->isCancelled());
         i++) {
      const VectorRecordT *stored_vector = GetDataPtrByInternalId(i);
      dist_t dist = EvaluateDistance(query_data, *stored_vector);
      if (topResults.size() < k || dist <= lastdist) {
        labeltype label = GetLabel(stored_vector);
        if ((!isIdAllowed) || (*isIdAllowed)(label)) {
          topResults.emplace(dist, label);
        }
        if (topResults.size() > k) {
          topResults.pop();
        }

        if (topResults.size() < k) {
          lastdist = std::numeric_limits<dist_t>::max();
        } else {
          lastdist = topResults.top().first;
        }
      }
    }
    return topResults;
  }

  template <typename SavedVectorSerializer>
  absl::Status SaveIndex(OutputStream &output,
                         const SavedVectorSerializer &serializer) {
    data_model::BruteForceIndexHeader header;
    const size_t size_per_element = vector_size_ + sizeof(labeltype);
    header.set_max_elements(data_->getCapacity());
    header.set_size_per_element(size_per_element);
    header.set_curr_element_count(cur_element_count_);
    std::string serialized;
    if (!header.SerializeToString(&serialized)) {
      return absl::InternalError("Could not serialize bruteforce header");
    }
    VMSDK_RETURN_IF_ERROR(
        output.SaveChunk(serialized.data(), serialized.size()));

    // TODO: write in chunks to improve throughput
    std::vector<char> buf(size_per_element);
    for (int i = 0; i < cur_element_count_; i++) {
      const VectorRecordT &stored_vector =
          *reinterpret_cast<const VectorRecordT *>(GetDataPtrByInternalId(i));
      auto serialized_vec = serializer(stored_vector);
      memcpy(buf.data(), serialized_vec.data(), serialized_vec.size());
      memcpy(buf.data() + vector_size_,
             reinterpret_cast<const char *>(GetDataPtrByInternalId(i)) +
                 sizeof(VectorRecordT),
             sizeof(labeltype));
      VMSDK_RETURN_IF_ERROR(output.SaveChunk(buf.data(), size_per_element));
    }
    return absl::OkStatus();
  }
  template <typename SavedVectorGenerator>
  absl::Status LoadIndex(InputStream &input, SpaceInterface<dist_t> *s,
                         const SavedVectorGenerator &generator) {
    clear();
    VMSDK_ASSIGN_OR_RETURN(auto serialized_header, input.LoadChunk());
    auto header = std::make_unique<data_model::BruteForceIndexHeader>();
    if (!header->ParseFromString(*serialized_header)) {
      return absl::InternalError("Could not deserialize bruteforce header");
    }
    const size_t saved_element_count = header->curr_element_count();

    vector_size_ = s->get_data_size();
    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();

    if (header->size_per_element() != s->get_data_size() + sizeof(labeltype)) {
      throw std::runtime_error(
          "Persisted size_per_element does not match expectation.");
    }

    data_ = std::make_unique<ChunkedArray>(
        sizeof(VectorRecordT) + sizeof(labeltype), k_elements_per_chunk,
        header->max_elements());

    for (size_t i = 0; i < saved_element_count; i++) {
      VMSDK_ASSIGN_OR_RETURN(auto chunk, input.LoadChunk());
      labeltype id;
      memcpy((char *)&id, chunk->data() + vector_size_, sizeof(labeltype));
      VectorRecordT *stored_vector = GetDataPtrByInternalId(i);
      new (stored_vector) VectorRecordT(
          generator(absl::string_view(chunk->data(), vector_size_)));
      memcpy(reinterpret_cast<char *>(GetDataPtrByInternalId(i)) +
                 sizeof(VectorRecordT),
             (char *)&id, sizeof(labeltype));
      dict_external_to_internal[id] = i;
      cur_element_count_++;
    }

    return absl::OkStatus();
  }

  void resizeIndex(size_t new_max_elements) {
    if (new_max_elements < cur_element_count_) {
      return;
    }
    data_->resize(new_max_elements);
  }
};
}  // namespace hnswlib
