#pragma once
#include <assert.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "hnswlib.h"
#include "iostream.h"
#include "third_party/hnswlib/index.pb.h"
#include "vmsdk/src/status/status_macros.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

namespace hnswlib {
template <typename dist_t, typename InputVectorT, typename SavedVectorT>
class BruteforceSearch : public AlgorithmInterface<dist_t, InputVectorT> {
 public:
    std::unique_ptr<ChunkedArray> data_;
    size_t cur_element_count_;
    size_t vector_size_{0};
    const size_t data_ptr_size_ = sizeof(SavedVectorT);
    DISTFUNC <dist_t> fstdistfunc_;
    void *dist_func_param_;
    std::mutex index_lock;
    const size_t k_elements_per_chunk{10*1024};

  std::unordered_map<labeltype, size_t> dict_external_to_internal;


    BruteforceSearch(SpaceInterface <dist_t> *s)
        : cur_element_count_(0), dist_func_param_(nullptr) {
    }


    BruteforceSearch(SpaceInterface<dist_t> *s, const std::string &location)
        : cur_element_count_(0), dist_func_param_(nullptr) {
        LoadIndex(location, s);
    }


    BruteforceSearch(SpaceInterface <dist_t> *s, size_t maxElements) {
        cur_element_count_ = 0;
        vector_size_ = s->get_data_size();
        fstdistfunc_ = s->get_dist_func();
        dist_func_param_ = s->get_dist_func_param();
        data_ = std::make_unique<ChunkedArray>(
                sizeof(SavedVectorT) + sizeof(labeltype),
                k_elements_per_chunk,
                maxElements);
    }


    void addPoint(const InputVectorT &datapoint, labeltype label, bool replace_deleted = false) {
        int idx;
        std::unique_lock<std::mutex> lock(index_lock);
        auto search = dict_external_to_internal.find(label);
        if (search != dict_external_to_internal.end()) {
            idx = search->second;
            *reinterpret_cast<SavedVectorT *>((*data_)[idx]) = datapoint;
        } else {
            if (cur_element_count_ >= data_->getCapacity()) {
                throw std::runtime_error("The number of elements exceeds the specified limit\n");
            }
            idx = cur_element_count_;
            dict_external_to_internal[label] = idx;
            cur_element_count_++;
            SavedVectorT *stored_vector = reinterpret_cast<SavedVectorT *>((*data_)[idx]);
            new (stored_vector) SavedVectorT(datapoint);
        }
        memcpy((*data_)[idx] + sizeof(SavedVectorT), &label, sizeof(labeltype));
    }

    SavedVectorT *getPoint(labeltype cur_external) {
      std::unique_lock<std::mutex> lock(index_lock);
      auto found = dict_external_to_internal.find(cur_external);
      if (found == dict_external_to_internal.end()) {
        return nullptr;
      }
      return reinterpret_cast<SavedVectorT *>((*data_)[found->second]);
    }

    void removePoint(labeltype cur_external) {
        std::unique_lock<std::mutex> lock(index_lock);

        auto found = dict_external_to_internal.find(cur_external);
        if (found == dict_external_to_internal.end()) {
            return;
        }
        // Fixing a bug - found->second value must be fetched before it's erased
        size_t cur_c = found->second;
        dict_external_to_internal.erase(found);
        if (cur_element_count_ - 1 == cur_c) {
          std::destroy_at(reinterpret_cast<SavedVectorT *>((*data_)[cur_c]));
          cur_element_count_--;
          return;
        }

        labeltype label = *((labeltype*)((*data_)[cur_element_count_-1] + sizeof(SavedVectorT)));
        dict_external_to_internal[label] = cur_c;
        *reinterpret_cast<SavedVectorT *>((*data_)[cur_c]) = std::move(
                *reinterpret_cast<SavedVectorT *>((*data_)[cur_element_count_-1]));
        memcpy((*data_)[cur_c] + sizeof(SavedVectorT),
                (*data_)[cur_element_count_-1] + sizeof(SavedVectorT),
                sizeof(labeltype));
        std::destroy_at(reinterpret_cast<SavedVectorT *>((*data_)[cur_element_count_-1]));
        cur_element_count_--;
    }


    std::priority_queue<std::pair<dist_t, labeltype >>
    searchKnn(const InputVectorT &query_data, size_t k, BaseFilterFunctor* isIdAllowed = nullptr, BaseCancellationFunctor *isCancelled = nullptr) const override {
        assert(k <= cur_element_count_);
        std::priority_queue<std::pair<dist_t, labeltype >> topResults;
        if (cur_element_count_ == 0) return topResults;
        for (int i = 0; i < k; i++) {
            const SavedVectorT &stored_vector = *reinterpret_cast<const SavedVectorT *>((*data_)[i]);
            dist_t dist = fstdistfunc_(query_data, stored_vector, dist_func_param_);
            labeltype label = *((labeltype*) ((*data_)[i] + sizeof(SavedVectorT)));
            if ((!isIdAllowed) || (*isIdAllowed)(label)) {
                topResults.emplace(dist, label);
            }
        }
        dist_t lastdist = topResults.empty() ? std::numeric_limits<dist_t>::max() : topResults.top().first;
        for (int i = k; i < cur_element_count_ && (!isCancelled || !isCancelled->isCancelled()); i++) {
            const SavedVectorT &stored_vector = *reinterpret_cast<const SavedVectorT *>((*data_)[i]);
            dist_t dist = fstdistfunc_(query_data, stored_vector, dist_func_param_);
            if (dist <= lastdist) {
                labeltype label = *((labeltype *) ((*data_)[i] + sizeof(SavedVectorT)));
                if ((!isIdAllowed) || (*isIdAllowed)(label)) {
                    topResults.emplace(dist, label);
                }
                if (topResults.size() > k)
                    topResults.pop();

                if (!topResults.empty()) {
                    lastdist = topResults.top().first;
                }
            }
        }
        return topResults;
    }

    absl::Status SaveIndex(OutputStream &output) override {
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
        const SavedVectorT &stored_vector = *reinterpret_cast<const SavedVectorT *>((*data_)[i]);
        memcpy(buf.data(), stored_vector.GetRawVector(), vector_size_);
        memcpy(buf.data() + vector_size_, (*data_)[i] + sizeof(SavedVectorT),
              sizeof(labeltype));
        VMSDK_RETURN_IF_ERROR(output.SaveChunk(buf.data(), size_per_element));
      }
      return absl::OkStatus();
    }

    template <typename FunctorT>
    absl::Status LoadIndex(InputStream &input, SpaceInterface<dist_t> *s,
                          FunctorT vector_constructor) {
      if (data_ != nullptr) {
        for (size_t i = 0; i < cur_element_count_; i++) {
          std::destroy_at(reinterpret_cast<SavedVectorT *>((*data_)[i]));
        }
        data_->clear();
      }
      VMSDK_ASSIGN_OR_RETURN(auto serialized_header, input.LoadChunk());
      auto header = std::make_unique<data_model::BruteForceIndexHeader>();
      if (!header->ParseFromString(*serialized_header)) {
        return absl::InternalError("Could not deserialize bruteforce header");
      }
      cur_element_count_ = header->curr_element_count();

      vector_size_ = s->get_data_size();
      fstdistfunc_ = s->get_dist_func();
      dist_func_param_ = s->get_dist_func_param();

      if (header->size_per_element() != s->get_data_size() + sizeof(labeltype)) {
        throw std::runtime_error(
            "Persisted size_per_element does not match expectation.");
      }

      data_ = std::make_unique<ChunkedArray>(sizeof(SavedVectorT) + sizeof(labeltype),
                                            k_elements_per_chunk,
                                            header->max_elements());

      for (int i = 0; i < cur_element_count_; i++) {
        VMSDK_ASSIGN_OR_RETURN(auto chunk, input.LoadChunk());
        labeltype id;
        memcpy((char *)&id, chunk->data() + vector_size_, sizeof(labeltype));
        SavedVectorT *stored_vector = reinterpret_cast<SavedVectorT *>((*data_)[i]);
        new (stored_vector) SavedVectorT(vector_constructor(absl::string_view(chunk->data(), vector_size_)));
        memcpy((*data_)[i] + sizeof(SavedVectorT), (char *)&id, sizeof(labeltype));
        dict_external_to_internal[id] = i;
      }

      return absl::OkStatus();
    }

    void resizeIndex(size_t new_max_elements) {
        data_->resize(new_max_elements);
    }
};
}  // namespace hnswlib
