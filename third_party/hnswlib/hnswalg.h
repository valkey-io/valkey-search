#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "absl/status/status.h"
#include "hnswlib.h"
#include "iostream.h"
#include "src/metrics.h"
#include "third_party/hnswlib/index.pb.h"
#include "visited_list_pool.h"
#include "vmsdk/src/status/status_macros.h"

#ifdef VMSDK_ENABLE_MEMORY_ALLOCATION_OVERRIDES
#include "vmsdk/src/memory_allocation_overrides.h"  // IWYU pragma: keep
#endif

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define htole64(x) OSSwapHostToLittleInt64(x)
#define le64toh(x) OSSwapLittleToHostInt64(x)
#endif

namespace hnswlib {
typedef unsigned int tableint;
typedef unsigned int linklistsizeint;

template <typename dist_t, typename InputVectorT, typename SavedVectorT>
class HierarchicalNSW
    : public AlgorithmInterface<dist_t, InputVectorT, SavedVectorT> {
 public:
  static const tableint MAX_LABEL_OPERATION_LOCKS = 65536;
  static const unsigned char DELETE_MARK = 0x01;
  static const unsigned int ENCODING_VERSION = 0;

  size_t max_elements_{0};
  mutable std::atomic<size_t> cur_element_count_{
      0};  // current number of elements
  size_t size_data_per_element_{0};
  size_t serialize_size_data_per_element_{0};
  size_t size_links_per_element_{0};
  mutable std::atomic<size_t> num_deleted_{0};  // number of deleted elements
  size_t M_{0};
  size_t maxM_{0};
  size_t maxM0_{0};
  size_t ef_construction_{0};
  size_t ef_{0};
  const size_t k_elements_per_chunk{10 * 1024};

  double mult_{0.0}, revSize_{0.0};
  int maxlevel_{0};

  std::unique_ptr<VisitedListPool> visited_list_pool_{nullptr};

  // Locks operations with element by label value
  mutable std::vector<std::mutex> label_op_locks_;

  std::mutex global;
  std::vector<std::mutex> link_list_locks_;

  tableint enterpoint_node_{0};

  size_t size_links_level0_{0};
  size_t offsetData_{0}, offsetLevel0_{0}, label_offset_{0};

  std::unique_ptr<ChunkedArray> data_level0_memory_;
  std::unique_ptr<ChunkedArray> linkLists_;
  std::vector<int> element_levels_;  // keeps level of each element

  size_t vector_size_{0};

  DISTFUNC<dist_t> fstdistfunc_;
  void *dist_func_param_{nullptr};
  bool normalized_{false};

  mutable std::mutex label_lookup_lock;  // lock for label_lookup_
  std::unordered_map<labeltype, tableint> label_lookup_;

  std::default_random_engine level_generator_;
  std::default_random_engine update_probability_generator_;

  mutable std::atomic<long> metric_distance_computations{0};
  mutable std::atomic<long> metric_hops{0};

  bool allow_replace_deleted_ = false;  // flag to replace deleted elements
                                        // (marked as deleted) during insertions

  // Gate for load-time corruption validation, wired from the
  // hnsw-validation-enable config via LoadIndex. Consulted only in loadCheck().
  bool load_validation_enabled_ = false;

  std::mutex deleted_elements_lock;  // lock for deleted_elements
  std::unordered_set<tableint>
      deleted_elements;  // contains internal ids of deleted elements

  HierarchicalNSW(SpaceInterface<dist_t> *s) {}

  HierarchicalNSW() = default;

  HierarchicalNSW(SpaceInterface<dist_t> *s, size_t max_elements,
                  bool normalized, size_t m_value, size_t ef_construction,
                  bool allow_replace_deleted, size_t random_seed = 100)
      : label_op_locks_(MAX_LABEL_OPERATION_LOCKS),
        link_list_locks_(max_elements),
        element_levels_(max_elements),
        normalized_(normalized),
        allow_replace_deleted_(allow_replace_deleted) {
    max_elements_ = max_elements;
    num_deleted_ = 0;
    vector_size_ = s->get_data_size();
    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();
    if (m_value <= 10000) {
      M_ = m_value;
    } else {
      HNSWERR << "warning: M parameter exceeds 10000 which may lead to adverse "
                 "effects."
              << std::endl;
      HNSWERR << "         Cap to 10000 will be applied for the rest of the "
                 "processing."
              << std::endl;
      M_ = 10000;
    }
    maxM_ = M_;
    maxM0_ = M_ * 2;
    ef_construction_ = std::max(ef_construction, M_);
    ef_ = 10;

    level_generator_.seed(random_seed);
    update_probability_generator_.seed(random_seed + 1);

    size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
    offsetData_ = (size_links_level0_ + alignof(SavedVectorT) - 1) &
                  ~(alignof(SavedVectorT) - 1);
    size_data_per_element_ =
        offsetData_ + sizeof(SavedVectorT) + sizeof(labeltype);
    serialize_size_data_per_element_ =
        size_links_level0_ + vector_size_ + sizeof(labeltype);
    label_offset_ = offsetData_ + sizeof(SavedVectorT);
    offsetLevel0_ = 0;

    data_level0_memory_ = std::make_unique<ChunkedArray>(
        size_data_per_element_, k_elements_per_chunk, max_elements);

    cur_element_count_ = 0;

    visited_list_pool_ =
        std::unique_ptr<VisitedListPool>(new VisitedListPool(1, max_elements));

    // initializations for special treatment of the first node
    enterpoint_node_ = -1;
    maxlevel_ = -1;

    linkLists_ = std::make_unique<ChunkedArray>(
        sizeof(void *), k_elements_per_chunk, max_elements);
    size_links_per_element_ =
        maxM_ * sizeof(tableint) + sizeof(linklistsizeint);
    mult_ = 1 / log(1.0 * M_);
    revSize_ = 1.0 / mult_;
  }

  ~HierarchicalNSW() { clear(); }

  void clear() {
    if (data_level0_memory_ != nullptr) {
      for (tableint i = 0; i < cur_element_count_; i++) {
        std::destroy_at(GetDataPtrByInternalId(i));
      }
      data_level0_memory_->clear();
    }
    if (linkLists_ != nullptr) {
      for (tableint i = 0; i < element_levels_.size(); i++) {
        if (element_levels_[i] > 0 &&
            *reinterpret_cast<char **>((*linkLists_)[i]) != nullptr) {
          delete[] (*reinterpret_cast<char **>((*linkLists_)[i]));
        }
      }
      linkLists_->clear();
    }
    valkey_search::Metrics::GetStats().reclaimable_memory -=
        num_deleted_ * vector_size_;
    label_lookup_.clear();
    deleted_elements.clear();
    num_deleted_ = 0;
    cur_element_count_ = 0;
    visited_list_pool_.reset(nullptr);
  }

  struct CompareByFirst {
    constexpr bool operator()(
        std::pair<dist_t, tableint> const &a,
        std::pair<dist_t, tableint> const &b) const noexcept {
      return a.first < b.first;
    }
  };

  void setEf(size_t ef) { ef_ = ef; }

  inline std::mutex &getLabelOpMutex(labeltype label) const {
    // calculate hash
    size_t lock_id = label & (MAX_LABEL_OPERATION_LOCKS - 1);
    return label_op_locks_[lock_id];
  }

  inline labeltype GetExternalLabel(tableint internal_id) const {
    labeltype return_label;
    memcpy(&return_label, ((*data_level0_memory_)[internal_id] + label_offset_),
           sizeof(labeltype));
    return return_label;
  }

  inline void SetExternalLabel(tableint internal_id, labeltype label) const {
    memcpy(((*data_level0_memory_)[internal_id] + label_offset_), &label,
           sizeof(labeltype));
  }

  inline labeltype *GetExternalLabeLp(tableint internal_id) const {
    return (labeltype *)((*data_level0_memory_)[internal_id] + label_offset_);
  }

  inline SavedVectorT *GetDataPtrByInternalId(tableint internal_id) const {
    return reinterpret_cast<SavedVectorT *>(
        (*data_level0_memory_)[internal_id] + offsetData_);
  }

  inline const SavedVectorT &GetDataByInternalId(tableint internal_id) const {
    return *(GetDataPtrByInternalId(internal_id));
  }

  inline void SetDataByInternalId(tableint internal_id,
                                  const InputVectorT &datapoint) {
    *(GetDataPtrByInternalId(internal_id)) = datapoint.GetVectorRecord();
  }
  inline void SetDataByInternalId(tableint internal_id,
                                  const SavedVectorT &datapoint) {
    *(GetDataPtrByInternalId(internal_id)) = datapoint;
  }

  inline void InitDataByInternalId(tableint internal_id,
                                   const InputVectorT &datapoint) {
    new (GetDataPtrByInternalId(internal_id))
        SavedVectorT(datapoint.GetVectorRecord());
  }

  inline dist_t EvaluateDistance(const SavedVectorT &a,
                                 const SavedVectorT &b) const {
    float reciprocal_mag_product =
        normalized_ ? a->GetReciprocalMagnitude() * b->GetReciprocalMagnitude()
                    : 1.0f;
    return fstdistfunc_(a->GetRawVector(), b->GetRawVector(), dist_func_param_,
                        reciprocal_mag_product);
  }
  inline dist_t EvaluateDistance(const InputVectorT &a, const SavedVectorT &b,
                                 bool is_rhs_marked_deleted) const {
    if (is_rhs_marked_deleted) {
      const char *query_vec =
          normalized_ ? a.GetNormalizedVector() : a.GetRawVector();
      return fstdistfunc_(query_vec, b->GetRawVector(), dist_func_param_, 1);
    }
    float reciprocal_mag_product =
        normalized_ ? a.GetReciprocalMagnitude() * b->GetReciprocalMagnitude()
                    : 1.0f;
    return fstdistfunc_(a.GetRawVector(), b->GetRawVector(), dist_func_param_,
                        reciprocal_mag_product);
  }

  int getRandomLevel(double reverse_size) {
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    double r = -log(distribution(level_generator_)) * reverse_size;
    return (int)r;
  }

  size_t getMaxElements() { return max_elements_; }

  size_t getCurrentElementCount() { return cur_element_count_; }

  size_t getDeletedCount() { return num_deleted_; }

  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
  searchBaseLayer(tableint ep_id, const InputVectorT &data_point, int layer) {
    VisitedList *vl = visited_list_pool_->getFreeVisitedList();
    vl_type *visited_array = vl->mass;
    vl_type visited_array_tag = vl->curV;

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        candidateSet;

    dist_t lowerBound;
    if (!isMarkedDeleted(ep_id)) {
      dist_t dist = EvaluateDistance(data_point, GetDataByInternalId(ep_id), false);
      top_candidates.emplace(dist, ep_id);
      lowerBound = dist;
      candidateSet.emplace(-dist, ep_id);
    } else {
      lowerBound = std::numeric_limits<dist_t>::max();
      candidateSet.emplace(-lowerBound, ep_id);
    }
    visited_array[ep_id] = visited_array_tag;

    while (!candidateSet.empty()) {
      std::pair<dist_t, tableint> curr_el_pair = candidateSet.top();
      if ((-curr_el_pair.first) > lowerBound &&
          top_candidates.size() == ef_construction_) {
        break;
      }
      candidateSet.pop();

      tableint curNodeNum = curr_el_pair.second;

      std::unique_lock<std::mutex> lock(link_list_locks_[curNodeNum]);

      int *data;  // = (int *)(linkList0_ + curNodeNum *
                  // size_links_per_element0_);
      if (layer == 0) {
        data = (int *)get_linklist0(curNodeNum);
      } else {
        data = (int *)get_linklist(curNodeNum, layer);
        //                    data = (int *) ((*linkLists_)[curNodeNum] + (layer
        //                    - 1) * size_links_per_element_);
      }
      size_t size = getListCount((linklistsizeint *)data);
      tableint *datal = (tableint *)(data + 1);
#ifdef USE_PREFETCH
      __builtin_prefetch((char *)(visited_array + *(data + 1)), 0, 3);
      __builtin_prefetch((char *)(visited_array + *(data + 1) + 64), 0, 3);
      if (size > 0) {
        __builtin_prefetch(GetDataByInternalId(*datal)->GetRawVector(), 0, 3);
      }
      if (size > 1) {
        __builtin_prefetch(GetDataByInternalId(*(datal + 1))->GetRawVector(), 0,
                           3);
      }
#endif

      for (size_t j = 0; j < size; j++) {
        tableint candidate_id = *(datal + j);
//                    if (candidate_id == 0) continue;
#ifdef USE_PREFETCH
        if (j + 1 < size) {
          __builtin_prefetch((char *)(visited_array + *(datal + j + 1)), 0, 3);
          __builtin_prefetch(
              GetDataByInternalId(*(datal + j + 1))->GetRawVector(), 0, 3);
        }
#endif
        if (visited_array[candidate_id] == visited_array_tag) continue;
        visited_array[candidate_id] = visited_array_tag;
        const SavedVectorT &currObj1 = (GetDataByInternalId(candidate_id));

        dist_t dist1 = EvaluateDistance(data_point, currObj1, isMarkedDeleted(candidate_id));
        if (top_candidates.size() < ef_construction_ || lowerBound > dist1) {
          candidateSet.emplace(-dist1, candidate_id);
#ifdef USE_PREFETCH
          __builtin_prefetch(
              GetDataByInternalId(candidateSet.top().second)->GetRawVector(), 0,
              3);
#endif

          if (!isMarkedDeleted(candidate_id))
            top_candidates.emplace(dist1, candidate_id);

          if (top_candidates.size() > ef_construction_) top_candidates.pop();

          if (!top_candidates.empty()) lowerBound = top_candidates.top().first;
        }
      }
    }
    visited_list_pool_->releaseVisitedList(vl);

    return top_candidates;
  }

  // bare_bone_search means there is no check for deletions and stop condition
  // is ignored in return of extra performance
  template <bool bare_bone_search = true, bool collect_metrics = false>
  std::priority_queue<std::pair<dist_t, tableint>,
                      std::vector<std::pair<dist_t, tableint>>, CompareByFirst>
  searchBaseLayerST(
      tableint ep_id, const InputVectorT &data_point, size_t ef,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr,  // VALKEYSEARCH
      BaseSearchStopCondition<dist_t> *stop_condition = nullptr) const {
    VisitedList *vl = visited_list_pool_->getFreeVisitedList();
    vl_type *visited_array = vl->mass;
    vl_type visited_array_tag = vl->curV;

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        candidate_set;

    dist_t lowerBound;
    if (bare_bone_search ||
        (!isMarkedDeleted(ep_id) &&
         ((!isIdAllowed) || (*isIdAllowed)(GetExternalLabel(ep_id))))) {
      const SavedVectorT &ep_data = GetDataByInternalId(ep_id);
      dist_t dist = EvaluateDistance(data_point, ep_data, isMarkedDeleted(ep_id));
      lowerBound = dist;
      top_candidates.emplace(dist, ep_id);
      if (!bare_bone_search && stop_condition) {
        stop_condition->add_point_to_result(GetExternalLabel(ep_id),
                                            ep_data->GetRawVector(), dist);
      }
      candidate_set.emplace(-dist, ep_id);
    } else {
      lowerBound = std::numeric_limits<dist_t>::max();
      candidate_set.emplace(-lowerBound, ep_id);
    }

    visited_array[ep_id] = visited_array_tag;

    while (!candidate_set.empty()) {
      std::pair<dist_t, tableint> current_node_pair = candidate_set.top();
      dist_t candidate_dist = -current_node_pair.first;

      bool flag_stop_search;
      if (bare_bone_search) {
        flag_stop_search = candidate_dist > lowerBound;
      } else {
        if (isCancelled && isCancelled->isCancelled()) {  // VALKEYSEARCH
          flag_stop_search = true;                        // VALKEYSEARCH
        } else                                            // VALKEYSEARCH
          if (stop_condition) {
            flag_stop_search =
                stop_condition->should_stop_search(candidate_dist, lowerBound);
          } else {
            flag_stop_search =
                candidate_dist > lowerBound && top_candidates.size() == ef;
          }
      }
      if (flag_stop_search) {
        break;
      }
      candidate_set.pop();

      tableint current_node_id = current_node_pair.second;
      int *data = (int *)get_linklist0(current_node_id);
      size_t size = getListCount((linklistsizeint *)data);
      //                bool cur_node_deleted =
      //                isMarkedDeleted(current_node_id);
      if (collect_metrics) {
        metric_hops++;
        metric_distance_computations += size;
      }

      // ---- Three-phase prefetch pipeline -----------------------------------
      // A candidate's vector lives behind a pointer indirection at offsetData_,
      // so reaching it is a two-deep pointer chase (slot -> vec). Because the
      // order of visitation to vectors is determined by the graph structure,
      // the vector addresses look random -- defeating any HW prefetching and
      // substantially increasing latency. Fusing all of that into one loop
      // forces the chase to serialize per candidate. Instead we fission the
      // expansion into three passes, each hiding exactly one level of memory
      // latency with its own lookahead:
      //   Phase 1 - probe visited_array (random gather), collect unvisited ids.
      //   Phase 2 - resolve the slot indirection into concrete vector pointers.
      //   Phase 3 - compute distances + maintain the candidate heaps.
      // The first two are latency-bound gathers (deep lookahead helps); the
      // third is bandwidth-bound (lookahead of 1, head only, HW streams tail).
      // Scratch is thread_local so reader threads each keep one reusable
      // buffer.
#ifdef USE_PREFETCH
      constexpr int kVisitedLookahead = 4;  // P1
      constexpr int kSlotLookahead = 4;     // P2
      constexpr int kVectorLookahead = 1;   // P3
#endif
      thread_local std::vector<tableint> unvisited;
      thread_local std::vector<const SavedVectorT *> vptrs;
      unvisited.clear();

      // Phase 1: filter visited. Preserve neighbor-list order so the heap/
      // lowerBound evolution in phase 3 is identical to the fused loop.
      for (size_t j = 1; j <= size; j++) {
#ifdef USE_PREFETCH
        if (j + kVisitedLookahead <= size) {
          __builtin_prefetch(
              (char *)(visited_array + *(data + j + kVisitedLookahead)), 0, 0);
        }
#endif
        tableint candidate_id = (tableint) * (data + j);
        if (visited_array[candidate_id] != visited_array_tag) {
          visited_array[candidate_id] = visited_array_tag;
          unvisited.push_back(candidate_id);
        }
      }
      const size_t n_unvisited = unvisited.size();

      // Phase 2: resolve slot indirection. Prefetch the pointer slot ahead,
      // then dereference the now-resident slot to the actual vector address.
      vptrs.resize(n_unvisited);
      for (size_t k = 0; k < n_unvisited; k++) {
#ifdef USE_PREFETCH
        if (k + kSlotLookahead < n_unvisited) {
          __builtin_prefetch(
              (*GetDataPtrByInternalId(unvisited[k + kSlotLookahead]))
                  ->GetRawVector(),
              0, 0);
        }
#endif
        vptrs[k] = GetDataPtrByInternalId(unvisited[k]);
      }

      // Phase 3: distance compute. Prefetch the head (~3 lines, two 128B
      // sectors) of the next vector to cover the cold head and bridge the HW
      // streamer's training window; the streamer covers the multi-line tail.
      for (size_t k = 0; k < n_unvisited; k++) {
#ifdef USE_PREFETCH
        if (k + kVectorLookahead < n_unvisited) {
          const char *h = (*vptrs[k + kVectorLookahead])->GetRawVector();
          __builtin_prefetch(h, 0, 0);
          __builtin_prefetch(h + 128, 0, 0);
        }
#endif
        tableint candidate_id = unvisited[k];
        const SavedVectorT *currObj1 = vptrs[k];
        dist_t dist = EvaluateDistance(data_point, *currObj1, isMarkedDeleted(candidate_id));

        bool flag_consider_candidate;
        if (!bare_bone_search && stop_condition) {
          flag_consider_candidate =
              stop_condition->should_consider_candidate(dist, lowerBound);
        } else {
          flag_consider_candidate =
              top_candidates.size() < ef || lowerBound > dist;
        }

        if (flag_consider_candidate) {
          candidate_set.emplace(-dist, candidate_id);
#ifdef USE_PREFETCH
          __builtin_prefetch(
              (*data_level0_memory_)[candidate_set.top().second] +
                  offsetLevel0_,  ///////////
              0, 3);              ////////////////////////
#endif

          if (bare_bone_search ||
              (!isMarkedDeleted(candidate_id) &&
               ((!isIdAllowed) ||
                (*isIdAllowed)(GetExternalLabel(candidate_id))))) {
            top_candidates.emplace(dist, candidate_id);
            if (!bare_bone_search && stop_condition) {
              stop_condition->add_point_to_result(
                  GetExternalLabel(candidate_id), (*currObj1)->GetRawVector(), dist);
            }
          }

          bool flag_remove_extra = false;
          if (!bare_bone_search && stop_condition) {
            flag_remove_extra = stop_condition->should_remove_extra();
          } else {
            flag_remove_extra = top_candidates.size() > ef;
          }
          while (flag_remove_extra) {
            tableint id = top_candidates.top().second;
            top_candidates.pop();
            if (!bare_bone_search && stop_condition) {
              stop_condition->remove_point_from_result(
                  GetExternalLabel(id), GetDataByInternalId(id)->GetRawVector(),
                  dist);
              flag_remove_extra = stop_condition->should_remove_extra();
            } else {
              flag_remove_extra = top_candidates.size() > ef;
            }
          }

          if (!top_candidates.empty()) lowerBound = top_candidates.top().first;
        }
      }
    }

    visited_list_pool_->releaseVisitedList(vl);
    return top_candidates;
  }

  void getNeighborsByHeuristic2(
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst> &top_candidates,
      const size_t M) {
    if (top_candidates.size() < M) {
      return;
    }

    std::priority_queue<std::pair<dist_t, tableint>> queue_closest;
    std::vector<std::pair<dist_t, tableint>> return_list;
    while (top_candidates.size() > 0) {
      queue_closest.emplace(-top_candidates.top().first,
                            top_candidates.top().second);
      top_candidates.pop();
    }

    while (queue_closest.size()) {
      if (return_list.size() >= M) break;
      std::pair<dist_t, tableint> current_pair = queue_closest.top();
      dist_t dist_to_query = -current_pair.first;
      queue_closest.pop();
      bool good = true;

      for (std::pair<dist_t, tableint> second_pair : return_list) {
        dist_t curdist =
            EvaluateDistance(GetDataByInternalId(second_pair.second),
                             GetDataByInternalId(current_pair.second));
        if (curdist < dist_to_query) {
          good = false;
          break;
        }
      }
      if (good) {
        return_list.push_back(current_pair);
      }
    }

    for (std::pair<dist_t, tableint> current_pair : return_list) {
      top_candidates.emplace(-current_pair.first, current_pair.second);
    }
  }

  linklistsizeint *get_linklist0(tableint internal_id) const {
    return (linklistsizeint *)((*data_level0_memory_)[internal_id] +
                               offsetLevel0_);
  }

  linklistsizeint *get_linklist(tableint internal_id, int level) const {
    return (linklistsizeint *)(*reinterpret_cast<char **>(
                                   (*linkLists_)[internal_id]) +
                               (level - 1) * size_links_per_element_);
  }

  linklistsizeint *get_linklist_at_level(tableint internal_id,
                                         int level) const {
    return level == 0 ? get_linklist0(internal_id)
                      : get_linklist(internal_id, level);
  }

  tableint mutuallyConnectNewElement(
      const InputVectorT &data_point, tableint cur_c,
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst> &top_candidates,
      int level, bool isUpdate) {
    size_t Mcurmax = level ? maxM_ : maxM0_;
    getNeighborsByHeuristic2(top_candidates, M_);
    if (top_candidates.size() > M_)
      throw std::runtime_error(
          "Should be not be more than M_ candidates returned by the heuristic");

    std::vector<tableint> selectedNeighbors;
    selectedNeighbors.reserve(M_);
    while (top_candidates.size() > 0) {
      selectedNeighbors.push_back(top_candidates.top().second);
      top_candidates.pop();
    }

    if (selectedNeighbors.empty()) {
      throw std::runtime_error(
          "During insertion, no neighbors found to mutually connect to");
    }

    tableint next_closest_entry_point = selectedNeighbors.back();

    {
      // lock only during the update
      // because during the addition the lock for cur_c is already acquired
      std::unique_lock<std::mutex> lock(link_list_locks_[cur_c],
                                        std::defer_lock);
      if (isUpdate) {
        lock.lock();
      }
      linklistsizeint *ll_cur;
      if (level == 0)
        ll_cur = get_linklist0(cur_c);
      else
        ll_cur = get_linklist(cur_c, level);

      if (*ll_cur && !isUpdate) {
        throw std::runtime_error(
            "The newly inserted element should have blank link list");
      }
      setListCount(ll_cur, selectedNeighbors.size());
      tableint *data = (tableint *)(ll_cur + 1);
      for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
        if (data[idx] && !isUpdate)
          throw std::runtime_error("Possible memory corruption");
        if (level > element_levels_[selectedNeighbors[idx]])
          throw std::runtime_error(
              "Trying to make a link on a non-existent level");

        data[idx] = selectedNeighbors[idx];
      }
    }

    for (size_t idx = 0; idx < selectedNeighbors.size(); idx++) {
      std::unique_lock<std::mutex> lock(
          link_list_locks_[selectedNeighbors[idx]]);

      linklistsizeint *ll_other;
      if (level == 0)
        ll_other = get_linklist0(selectedNeighbors[idx]);
      else
        ll_other = get_linklist(selectedNeighbors[idx], level);

      size_t sz_link_list_other = getListCount(ll_other);

      if (sz_link_list_other > Mcurmax)
        throw std::runtime_error("Bad value of sz_link_list_other");
      if (selectedNeighbors[idx] == cur_c)
        throw std::runtime_error("Trying to connect an element to itself");
      if (level > element_levels_[selectedNeighbors[idx]])
        throw std::runtime_error(
            "Trying to make a link on a non-existent level");

      tableint *data = (tableint *)(ll_other + 1);

      bool is_cur_c_present = false;
      if (isUpdate) {
        for (size_t j = 0; j < sz_link_list_other; j++) {
          if (data[j] == cur_c) {
            is_cur_c_present = true;
            break;
          }
        }
      }

      // If cur_c is already present in the neighboring connections of
      // `selectedNeighbors[idx]` then no need to modify any connections or run
      // the heuristics.
      if (!is_cur_c_present) {
        if (sz_link_list_other < Mcurmax) {
          data[sz_link_list_other] = cur_c;
          setListCount(ll_other, sz_link_list_other + 1);
        } else {
          // finding the "weakest" element to replace it with the new one
          dist_t d_max =
              EvaluateDistance(GetDataByInternalId(cur_c),
                               GetDataByInternalId(selectedNeighbors[idx]));
          // Heuristic:
          std::priority_queue<std::pair<dist_t, tableint>,
                              std::vector<std::pair<dist_t, tableint>>,
                              CompareByFirst>
              candidates;
          candidates.emplace(d_max, cur_c);

          for (size_t j = 0; j < sz_link_list_other; j++) {
            candidates.emplace(
                EvaluateDistance(GetDataByInternalId(data[j]),
                                 GetDataByInternalId(selectedNeighbors[idx])),
                data[j]);
          }

          getNeighborsByHeuristic2(candidates, Mcurmax);

          int indx = 0;
          while (candidates.size() > 0) {
            data[indx] = candidates.top().second;
            candidates.pop();
            indx++;
          }

          setListCount(ll_other, indx);
          // Nearest K:
          /*int indx = -1;
           for (int j = 0; j < sz_link_list_other; j++) {
               dist_t d = fstdistfunc_(GetDataByInternalId(data[j]),
           GetDataByInternalId(rez[idx]), dist_func_param_); if (d > d_max) {
                   indx = j;
                   d_max = d;
               }
           }
           if (indx >= 0) {
               data[indx] = cur_c;
           } */
        }
      }
    }

    return next_closest_entry_point;
  }

  void resizeIndex(size_t new_max_elements) {
    if (new_max_elements < cur_element_count_)
      throw std::runtime_error(
          "Cannot resize, max element is less than the current number of "
          "elements");

    visited_list_pool_.reset(new VisitedListPool(1, new_max_elements));

    element_levels_.resize(new_max_elements);

    std::vector<std::mutex>(new_max_elements).swap(link_list_locks_);

    // Reallocate base layer
    data_level0_memory_->resize(new_max_elements);

    // Reallocate all other layers
    linkLists_->resize(new_max_elements);

    max_elements_ = new_max_elements;
  }

  size_t indexFileSize() const {
    size_t size = 0;
    size += sizeof(offsetLevel0_);
    size += sizeof(max_elements_);
    size += sizeof(cur_element_count_);
    size += sizeof(serialize_size_data_per_element_);
    size += sizeof(label_offset_);
    size += sizeof(offsetData_);
    size += sizeof(maxlevel_);
    size += sizeof(enterpoint_node_);
    size += sizeof(maxM_);

    size += sizeof(maxM0_);
    size += sizeof(M_);
    size += sizeof(mult_);
    size += sizeof(ef_construction_);

    size += cur_element_count_ * serialize_size_data_per_element_;

    for (size_t i = 0; i < cur_element_count_; i++) {
      unsigned int linkListSize =
          element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i]
                                 : 0;
      size += sizeof(linkListSize);
      size += linkListSize;
    }
    return size;
  }
  template <typename SavedVectorSerializer>
  absl::Status SaveIndex(OutputStream &output,
                         const SavedVectorSerializer &serializer) {
    data_model::HNSWIndexHeader header;
    header.set_offset_level_0(offsetLevel0_);
    header.set_max_elements(max_elements_);
    header.set_curr_element_count(cur_element_count_);
    header.set_serialize_size_data_per_element(
        serialize_size_data_per_element_);
    header.set_label_offset(label_offset_);
    header.set_offset_data(size_links_level0_);
    header.set_max_level(maxlevel_);
    header.set_enterpoint_node(enterpoint_node_);
    header.set_max_m(maxM_);
    header.set_max_m_0(maxM0_);
    header.set_m(M_);
    header.set_mult(mult_);
    header.set_ef_construction(ef_construction_);
    std::string serialized;
    if (!header.SerializeToString(&serialized)) {
      return absl::InternalError("Could not serialize HNSW header");
    }
    VMSDK_RETURN_IF_ERROR(
        output.SaveChunk(serialized.data(), serialized.size()));

    if (cur_element_count_ == 0) {
      return absl::OkStatus();
    }

    // Resize internal data structures to match the true max elements so
    // that the saved index is self-consistent.
    data_level0_memory_->resize(max_elements_);
    linkLists_->resize(max_elements_);
    std::vector<char> buf(serialize_size_data_per_element_);
    for (int i = 0; i < cur_element_count_; i++) {
      memcpy(buf.data(), (*data_level0_memory_)[i], size_links_level0_);
      const SavedVectorT &record = GetDataByInternalId(i);
      std::vector<char> serialized_vector = serializer(record, isMarkedDeleted(i));
      memcpy(buf.data() + size_links_level0_, serialized_vector.data(),
             vector_size_);
      memcpy(buf.data() + size_links_level0_ + vector_size_,
             (*data_level0_memory_)[i] + label_offset_, sizeof(labeltype));
      VMSDK_RETURN_IF_ERROR(
          output.SaveChunk(buf.data(), serialize_size_data_per_element_));
    };

    for (size_t i = 0; i < cur_element_count_; i++) {
      unsigned int linkListSize =
          element_levels_[i] > 0 ? size_links_per_element_ * element_levels_[i]
                                 : 0;
      size_t size_to_serialize = htole64(static_cast<size_t>(linkListSize));
      VMSDK_RETURN_IF_ERROR(output.SaveChunk(
          reinterpret_cast<const char *>(&size_to_serialize), sizeof(size_t)));
      if (linkListSize) {
        VMSDK_RETURN_IF_ERROR(output.SaveChunk(
            *reinterpret_cast<char **>((*linkLists_)[i]), linkListSize));
      }
    }
    return absl::OkStatus();
  }

  inline void LoadCheck(bool ok, absl::string_view msg) const {
    if (ok) {
      return;
    }
    if (load_validation_enabled_) {
      throw std::runtime_error(
          absl::StrCat("HNSW index load validation failed: ", msg));
    }
  }

  template <typename SavedVectorGenerator>
  absl::Status LoadIndex(InputStream &input, SpaceInterface<dist_t> *s,
                         size_t max_elements_i, size_t expected_m,
                         bool validate,
                         const SavedVectorGenerator &generator) {
    load_validation_enabled_ = validate;
    VMSDK_ASSIGN_OR_RETURN(auto serialized_header, input.LoadChunk());
    auto header = std::make_unique<data_model::HNSWIndexHeader>();
    if (!header->ParseFromString(*serialized_header)) {
      return absl::InternalError("Could not deserialize HNSW header");
    }

    offsetLevel0_ = header->offset_level_0();
    max_elements_ = header->max_elements();
    cur_element_count_ = header->curr_element_count();
    serialize_size_data_per_element_ =
        header->serialize_size_data_per_element();
    label_offset_ = header->label_offset();
    maxlevel_ = header->max_level();
    enterpoint_node_ = header->enterpoint_node();
    maxM_ = header->max_m();
    maxM0_ = header->max_m_0();
    M_ = header->m();
    mult_ = header->mult();
    ef_construction_ = header->ef_construction();

    // Authoritative parameters come from the index definition (the space and
    // expected_m), never from the untrusted header.
    vector_size_ = s->get_data_size();
    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();

    // Recompute the geometry that governs memory layout. When validation is
    // enabled these are cross-checked against the header below; the header's
    // own copies are only ever used as values to validate against.
    size_links_level0_ = maxM0_ * sizeof(tableint) + sizeof(linklistsizeint);
    offsetData_ = (size_links_level0_ + alignof(SavedVectorT) - 1) &
                  ~(alignof(SavedVectorT) - 1);
    size_data_per_element_ =
        offsetData_ + sizeof(SavedVectorT) + sizeof(labeltype);
    label_offset_ = offsetData_ + sizeof(SavedVectorT);
    size_links_per_element_ =
        maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

    // Resolve capacity: at least the live element count, honoring the larger of
    // the caller-requested cap and the file's recorded capacity.
    size_t cur_count = cur_element_count_;
    size_t max_elements =
        std::max(cur_count, std::max(max_elements_i, max_elements_));
    max_elements_ = max_elements;

    // --- Header validation (the kill switch lives inside LoadCheck) ---
    {
      const size_t exp_m = expected_m > 10000 ? 10000 : expected_m;
      LoadCheck(exp_m >= 1, "M must be >= 1");
      LoadCheck(M_ == exp_m, "header M does not match index definition");
      LoadCheck(maxM_ == M_, "header maxM does not equal M");
      LoadCheck(maxM0_ == 2 * M_, "header maxM0 does not equal 2*M");
      LoadCheck(maxM0_ <= 0xFFFF,
                "maxM0 exceeds the 16-bit neighbor-count field");
      LoadCheck(vector_size_ > 0, "vector size must be > 0");
      LoadCheck(serialize_size_data_per_element_ ==
                    size_links_level0_ + vector_size_ + sizeof(labeltype),
                "serialized element size is inconsistent with the geometry");
      LoadCheck(offsetLevel0_ == 0, "offset_level_0 must be 0");
      if (M_ >= 2) {
        const double expected_mult = 1.0 / std::log(static_cast<double>(M_));
        LoadCheck(mult_ > 0.0 &&
                      std::fabs(mult_ - expected_mult) <= 1e-6 * expected_mult,
                  "mult is inconsistent with M");
      }
      LoadCheck(cur_element_count_ <= max_elements_,
                "curr_element_count exceeds max_elements");
      if (cur_element_count_ == 0) {
        LoadCheck(maxlevel_ == -1 || maxlevel_ == 0,
                  "empty index has a non-trivial max_level");
      } else {
        LoadCheck(maxlevel_ >= 0, "non-empty index has a negative max_level");
        LoadCheck(maxlevel_ <= static_cast<int>(cur_element_count_),
                  "max_level exceeds the element count");
        LoadCheck(enterpoint_node_ < cur_element_count_,
                  "enterpoint_node is out of range");
      }
      LoadCheck(size_data_per_element_ > 0, "size_data_per_element is 0");
      LoadCheck(max_elements_ <= SIZE_MAX / size_data_per_element_,
                "level-0 allocation size overflows");
      LoadCheck(max_elements_ <= SIZE_MAX / sizeof(void *),
                "linkLists allocation size overflows");
      LoadCheck(max_elements_ <= SIZE_MAX / sizeof(int),
                "element_levels allocation size overflows");
    }

    fstdistfunc_ = s->get_dist_func();
    dist_func_param_ = s->get_dist_func_param();

    data_level0_memory_ = std::make_unique<ChunkedArray>(
        size_data_per_element_, k_elements_per_chunk, max_elements);

    for (size_t i = 0; i < cur_element_count_; i++) {
      VMSDK_ASSIGN_OR_RETURN(auto chunk, input.LoadChunk());
      LoadCheck(chunk->size() ==
                    size_links_level0_ + vector_size_ + sizeof(labeltype),
                "level-0 element chunk has the wrong size");
      memcpy((*data_level0_memory_)[i], chunk->data(), size_links_level0_);
      labeltype id;
      memcpy((char *)&id, chunk->data() + size_links_level0_ + vector_size_,
             sizeof(labeltype));
      new (GetDataPtrByInternalId(i)) SavedVectorT(generator(
          absl::string_view(chunk->data() + size_links_level0_, vector_size_),
          isMarkedDeleted(i)));
      memcpy((*data_level0_memory_)[i] + label_offset_, (char *)&id,
             sizeof(labeltype));

      linklistsizeint *ll0 = get_linklist0(i);
      size_t l0_count = getListCount(ll0);
      LoadCheck(l0_count <= maxM0_, "level-0 neighbor count exceeds 2*M");
      tableint *l0_neighbors = (tableint *)(ll0 + 1);
      size_t l0_scan = std::min(l0_count, maxM0_);
      for (size_t j = 0; j < l0_scan; j++) {
        LoadCheck(l0_neighbors[j] < cur_element_count_,
                  "level-0 neighbor id out of range");
        LoadCheck(l0_neighbors[j] != i, "level-0 self-loop");
      }
    }

    size_links_per_element_ =
        maxM_ * sizeof(tableint) + sizeof(linklistsizeint);

    std::vector<std::mutex>(max_elements).swap(link_list_locks_);
    std::vector<std::mutex>(MAX_LABEL_OPERATION_LOCKS).swap(label_op_locks_);

    visited_list_pool_ = std::make_unique<VisitedListPool>(1, max_elements);

    linkLists_ = std::make_unique<ChunkedArray>(
        sizeof(void *), k_elements_per_chunk, max_elements);

    element_levels_ = std::vector<int>(max_elements);
    revSize_ = 1.0 / mult_;
    ef_ = 10;
    for (size_t i = 0; i < cur_element_count_; i++) {
      element_levels_[i] = 0;
      *reinterpret_cast<char **>((*linkLists_)[i]) = nullptr;

      labeltype ext_label = GetExternalLabel(i);
      LoadCheck(label_lookup_.find(ext_label) == label_lookup_.end(),
                "duplicate label in index");
      label_lookup_[ext_label] = i;
      size_t linkListSize;
      VMSDK_ASSIGN_OR_RETURN(auto size_chunk, input.LoadChunk());
      LoadCheck(size_chunk->size() == sizeof(size_t),
                "link-list size chunk has the wrong size");
      memcpy(&linkListSize, size_chunk->data(), sizeof(size_t));
      linkListSize = le64toh(linkListSize);
      if (linkListSize != 0) {
        LoadCheck(linkListSize % size_links_per_element_ == 0,
                  "upper-level link-list size is not a multiple of the stride");
        int level = linkListSize / size_links_per_element_;
        LoadCheck(level <= maxlevel_, "element level exceeds max_level");
        VMSDK_ASSIGN_OR_RETURN(auto link_list_chunk, input.LoadChunk());
        LoadCheck(link_list_chunk->size() == linkListSize,
                  "upper-level link-list chunk has the wrong size");
        size_t alloc_size =
            static_cast<size_t>(level) * size_links_per_element_;
        char *links = new char[alloc_size];
        memset(links, 0, alloc_size);
        memcpy(links, link_list_chunk->data(),
               std::min(alloc_size, link_list_chunk->size()));
        *reinterpret_cast<char **>((*linkLists_)[i]) = links;
        element_levels_[i] = level;
        for (int l = 1; l <= level; l++) {
          LoadCheck(getListCount(get_linklist(i, l)) <= maxM_,
                    "upper-level neighbor count exceeds M");
        }
      }
    }

    if (cur_element_count_ > 0) {
      LoadCheck(enterpoint_node_ < cur_element_count_ &&
                    element_levels_[enterpoint_node_] == maxlevel_,
                "enterpoint node is not at max_level");
    }
    for (size_t i = 0; i < cur_element_count_; i++) {
      for (int level = 1; level <= element_levels_[i]; level++) {
        linklistsizeint *ll = get_linklist(i, level);
        size_t count = getListCount(ll);
        size_t scan = std::min(count, maxM_);
        tableint *neighbors = (tableint *)(ll + 1);
        for (size_t j = 0; j < scan; j++) {
          tableint e = neighbors[j];
          LoadCheck(e < cur_element_count_,
                    "upper-level neighbor id out of range");
          LoadCheck(e != i, "upper-level self-loop");
          LoadCheck(e >= cur_element_count_ || element_levels_[e] >= level,
                    "upper-level neighbor is absent at that level");
        }
      }
    }

    for (size_t i = 0; i < cur_element_count_; i++) {
      if (isMarkedDeleted(i)) {
        num_deleted_ += 1;
        valkey_search::Metrics::GetStats().reclaimable_memory += vector_size_;
        if (allow_replace_deleted_) {
          deleted_elements.insert(i);
        }
      }
    }
    return absl::OkStatus();
  }

  SavedVectorT *getPoint(labeltype label) const {
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
      return nullptr;
    }
    return GetDataPtrByInternalId(search->second);
  }

  template <typename data_t>
  std::vector<data_t> getDataByLabel(labeltype label) const {
    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

    std::unique_lock<std::mutex> lock_table(label_lookup_lock);
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end() || isMarkedDeleted(search->second)) {
      throw std::runtime_error("Label not found");
    }
    tableint internalId = search->second;
    lock_table.unlock();

    const SavedVectorT &data_ptrv = GetDataByInternalId(internalId);
    size_t dim = *((size_t *)dist_func_param_);
    std::vector<data_t> data(dim);
    memcpy(data.data(), data_ptrv.GetRawVector(), dim * sizeof(data_t));
    return data;
  }

  /*
   * Marks an element with the given label deleted, does NOT really change the
   * current graph.
   */
  void markDelete(labeltype label) {
    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

    std::unique_lock<std::mutex> lock_table(label_lookup_lock);
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end()) {
      throw std::runtime_error("Label not found");
    }
    tableint internalId = search->second;
    lock_table.unlock();

    markDeletedInternal(internalId);
  }

  /*
   * Uses the last 16 bits of the memory for the linked list size to store the
   * mark, whereas maxM0_ has to be limited to the lower 16 bits, however, still
   * large enough in almost all cases.
   */
  void markDeletedInternal(tableint internalId) {
    assert(internalId < cur_element_count_);
    if (!isMarkedDeleted(internalId)) {
      unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
      *ll_cur |= DELETE_MARK;
      num_deleted_ += 1;
      valkey_search::Metrics::GetStats().reclaimable_memory += vector_size_;
      if (allow_replace_deleted_) {
        std::unique_lock<std::mutex> lock_deleted_elements(
            deleted_elements_lock);
        deleted_elements.insert(internalId);
      }
    } else {
      throw std::runtime_error(
          "The requested to delete element is already deleted");
    }
  }

  /*
   * Removes the deleted mark of the node, does NOT really change the current
   * graph.
   *
   * Note: the method is not safe to use when replacement of deleted elements is
   * enabled, because elements marked as deleted can be completely removed by
   * addPoint
   */
  void unmarkDelete(labeltype label) {
    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));

    std::unique_lock<std::mutex> lock_table(label_lookup_lock);
    auto search = label_lookup_.find(label);
    if (search == label_lookup_.end()) {
      throw std::runtime_error("Label not found");
    }
    tableint internalId = search->second;
    lock_table.unlock();

    unmarkDeletedInternal(internalId);
  }

  /*
   * Remove the deleted mark of the node.
   */
  void unmarkDeletedInternal(tableint internalId) {
    assert(internalId < cur_element_count_);
    if (isMarkedDeleted(internalId)) {
      unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
      *ll_cur &= ~DELETE_MARK;
      num_deleted_ -= 1;
      valkey_search::Metrics::GetStats().reclaimable_memory -= vector_size_;
      if (allow_replace_deleted_) {
        std::unique_lock<std::mutex> lock_deleted_elements(
            deleted_elements_lock);
        deleted_elements.erase(internalId);
      }
    } else {
      throw std::runtime_error(
          "The requested to undelete element is not deleted");
    }
  }

  /*
   * Checks the first 16 bits of the memory to see if the element is marked
   * deleted.
   */
  bool isMarkedDeleted(tableint internalId) const {
    unsigned char *ll_cur = ((unsigned char *)get_linklist0(internalId)) + 2;
    return *ll_cur & DELETE_MARK;
  }

  unsigned short int getListCount(linklistsizeint *ptr) const {
    return *((unsigned short int *)ptr);
  }

  void setListCount(linklistsizeint *ptr, unsigned short int size) const {
    *((unsigned short int *)(ptr)) = *((unsigned short int *)&size);
  }

  /*
   * Adds point. Updates the point if it is already in the index.
   * If replacement of deleted elements is enabled: replaces previously deleted
   * point if any, updating it with new point
   */
  void addPoint(const InputVectorT &data_point, labeltype label,
                bool replace_deleted = false) override {
    if ((allow_replace_deleted_ == false) && (replace_deleted == true)) {
      throw std::runtime_error(
          "Replacement of deleted elements is disabled in constructor");
    }

    // lock all operations with element by label
    std::unique_lock<std::mutex> lock_label(getLabelOpMutex(label));
    if (!replace_deleted) {
      addPoint(data_point, label, -1);
      return;
    }
    // check if there is vacant place
    tableint internal_id_replaced;
    std::unique_lock<std::mutex> lock_deleted_elements(deleted_elements_lock);
    bool is_vacant_place = !deleted_elements.empty();
    if (is_vacant_place) {
      internal_id_replaced = *deleted_elements.begin();
      deleted_elements.erase(internal_id_replaced);
    }
    lock_deleted_elements.unlock();

    // if there is no vacant place then add or update point
    // else add point to vacant place
    if (!is_vacant_place) {
      addPoint(data_point, label, -1);
    } else {
      // we assume that there are no concurrent operations on deleted element
      labeltype label_replaced = GetExternalLabel(internal_id_replaced);
      SetExternalLabel(internal_id_replaced, label);

      std::unique_lock<std::mutex> lock_table(label_lookup_lock);
      label_lookup_.erase(label_replaced);
      label_lookup_[label] = internal_id_replaced;
      lock_table.unlock();

      unmarkDeletedInternal(internal_id_replaced);
      updatePoint(data_point, internal_id_replaced, 1.0);
    }
  }

  void updatePoint(const InputVectorT &dataPoint, tableint internalId,
                   float updateNeighborProbability) {
    // update the feature vector associated with existing point with new vector
    SetDataByInternalId(internalId, dataPoint);

    int maxLevelCopy = maxlevel_;
    tableint entryPointCopy = enterpoint_node_;
    // If point to be updated is entry point and graph just contains single
    // element then just return.
    if (entryPointCopy == internalId && cur_element_count_ == 1) return;

    int elemLevel = element_levels_[internalId];
    std::uniform_real_distribution<float> distribution(0.0, 1.0);
    for (int layer = 0; layer <= elemLevel; layer++) {
      std::unordered_set<tableint> sCand;
      std::unordered_set<tableint> sNeigh;
      std::vector<tableint> listOneHop =
          getConnectionsWithLock(internalId, layer);
      if (listOneHop.size() == 0) continue;

      sCand.insert(internalId);

      for (auto &&elOneHop : listOneHop) {
        sCand.insert(elOneHop);

        if (distribution(update_probability_generator_) >
            updateNeighborProbability)
          continue;

        sNeigh.insert(elOneHop);

        std::vector<tableint> listTwoHop =
            getConnectionsWithLock(elOneHop, layer);
        for (auto &&elTwoHop : listTwoHop) {
          sCand.insert(elTwoHop);
        }
      }

      for (auto &&neigh : sNeigh) {
        // if (neigh == internalId)
        //     continue;

        std::priority_queue<std::pair<dist_t, tableint>,
                            std::vector<std::pair<dist_t, tableint>>,
                            CompareByFirst>
            candidates;
        size_t size =
            sCand.find(neigh) == sCand.end()
                ? sCand.size()
                : sCand.size() - 1;  // sCand guaranteed to have size >= 1
        size_t elementsToKeep = std::min(ef_construction_, size);
        for (auto &&cand : sCand) {
          if (cand == neigh) continue;

          dist_t distance = EvaluateDistance(GetDataByInternalId(neigh),
                                             GetDataByInternalId(cand));
          if (candidates.size() < elementsToKeep) {
            candidates.emplace(distance, cand);
          } else {
            if (distance < candidates.top().first) {
              candidates.pop();
              candidates.emplace(distance, cand);
            }
          }
        }

        // Retrieve neighbours using heuristic and set connections.
        getNeighborsByHeuristic2(candidates, layer == 0 ? maxM0_ : maxM_);

        {
          std::unique_lock<std::mutex> lock(link_list_locks_[neigh]);
          linklistsizeint *ll_cur;
          ll_cur = get_linklist_at_level(neigh, layer);
          size_t candSize = candidates.size();
          setListCount(ll_cur, candSize);
          tableint *data = (tableint *)(ll_cur + 1);
          for (size_t idx = 0; idx < candSize; idx++) {
            data[idx] = candidates.top().second;
            candidates.pop();
          }
        }
      }
    }

    repairConnectionsForUpdate(dataPoint, entryPointCopy, internalId, elemLevel,
                               maxLevelCopy);
  }

  void repairConnectionsForUpdate(const InputVectorT &dataPoint,
                                  tableint entryPointInternalId,
                                  tableint dataPointInternalId,
                                  int dataPointLevel, int maxLevel) {
    tableint currObj = entryPointInternalId;
    if (dataPointLevel < maxLevel) {
      dist_t curdist =
          EvaluateDistance(dataPoint, GetDataByInternalId(currObj), isMarkedDeleted(currObj));
      for (int level = maxLevel; level > dataPointLevel; level--) {
        bool changed = true;
        while (changed) {
          changed = false;
          unsigned int *data;
          std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
          data = get_linklist_at_level(currObj, level);
          int size = getListCount(data);
          tableint *datal = (tableint *)(data + 1);
#ifdef USE_PREFETCH
          __builtin_prefetch(GetDataByInternalId(*datal)->GetRawVector(), 0, 3);
#endif
          for (int i = 0; i < size; i++) {
#ifdef USE_PREFETCH
            if (i + 1 < size) {
              __builtin_prefetch(
                  GetDataByInternalId(*(datal + i + 1))->GetRawVector(), 1, 3);
            }
#endif
            tableint cand = datal[i];
            dist_t d = EvaluateDistance(dataPoint, GetDataByInternalId(cand), isMarkedDeleted(cand));
            if (d < curdist) {
              curdist = d;
              currObj = cand;
              changed = true;
            }
          }
        }
      }
    }

    if (dataPointLevel > maxLevel)
      throw std::runtime_error(
          "Level of item to be updated cannot be bigger than max level");

    for (int level = dataPointLevel; level >= 0; level--) {
      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst>
          topCandidates = searchBaseLayer(currObj, dataPoint, level);

      std::priority_queue<std::pair<dist_t, tableint>,
                          std::vector<std::pair<dist_t, tableint>>,
                          CompareByFirst>
          filteredTopCandidates;
      while (topCandidates.size() > 0) {
        if (topCandidates.top().second != dataPointInternalId)
          filteredTopCandidates.push(topCandidates.top());

        topCandidates.pop();
      }

      // Since element_levels_ is being used to get `dataPointLevel`, there
      // could be cases where `topCandidates` could just contains entry point
      // itself. To prevent self loops, the `topCandidates` is filtered and thus
      // can be empty.
      if (filteredTopCandidates.size() > 0) {
        bool epDeleted = isMarkedDeleted(entryPointInternalId);
        if (epDeleted) {
          filteredTopCandidates.emplace(
              EvaluateDistance(dataPoint,
                               GetDataByInternalId(entryPointInternalId), true),
              entryPointInternalId);
          if (filteredTopCandidates.size() > ef_construction_)
            filteredTopCandidates.pop();
        }

        currObj = mutuallyConnectNewElement(dataPoint, dataPointInternalId,
                                            filteredTopCandidates, level, true);
      }
    }
  }

  std::vector<tableint> getConnectionsWithLock(tableint internalId, int level) {
    std::unique_lock<std::mutex> lock(link_list_locks_[internalId]);
    unsigned int *data = get_linklist_at_level(internalId, level);
    int size = getListCount(data);
    std::vector<tableint> result(size);
    tableint *ll = (tableint *)(data + 1);
    memcpy(result.data(), ll, size * sizeof(tableint));
    return result;
  }

  tableint addPoint(const InputVectorT &data_point, labeltype label,
                    int level) {
    tableint cur_c = 0;
    {
      // Checking if the element with the same label already exists
      // if so, updating it *instead* of creating a new element.
      std::unique_lock<std::mutex> lock_table(label_lookup_lock);
      auto search = label_lookup_.find(label);
      if (search != label_lookup_.end()) {
        tableint existingInternalId = search->second;
        if (allow_replace_deleted_) {
          if (isMarkedDeleted(existingInternalId)) {
            throw std::runtime_error(
                "Can't use addPoint to update deleted elements if replacement "
                "of deleted elements is enabled.");
          }
        }
        lock_table.unlock();

        if (isMarkedDeleted(existingInternalId)) {
          unmarkDeletedInternal(existingInternalId);
        }
        updatePoint(data_point, existingInternalId, 1.0);

        return existingInternalId;
      }

      if (cur_element_count_ >= max_elements_) {
        throw std::runtime_error(
            "The number of elements exceeds the specified limit");
      }

      cur_c = cur_element_count_;
      cur_element_count_++;
      label_lookup_[label] = cur_c;
    }

    std::unique_lock<std::mutex> templock(global);
    int maxlevelcopy = maxlevel_;
    std::unique_lock<std::mutex> lock_el(link_list_locks_[cur_c]);
    int curlevel = getRandomLevel(mult_);
    if (level > 0) curlevel = level;
    if (curlevel <= maxlevelcopy) {
      templock.unlock();
    }
    element_levels_[cur_c] = curlevel;
    tableint currObj = enterpoint_node_;
    tableint enterpoint_copy = enterpoint_node_;

    memset((*data_level0_memory_)[cur_c] + offsetLevel0_, 0,
           size_data_per_element_);

    // Initialisation of the data and label
    memcpy(GetExternalLabeLp(cur_c), &label, sizeof(labeltype));
    InitDataByInternalId(cur_c, data_point);

    if (curlevel) {
      *reinterpret_cast<char **>((*linkLists_)[cur_c]) =
          new char[size_links_per_element_ * curlevel + 1];
      if (*reinterpret_cast<char **>((*linkLists_)[cur_c]) == nullptr)
        throw std::runtime_error(
            "Not enough memory: addPoint failed to allocate linklist");
      memset(*reinterpret_cast<char **>((*linkLists_)[cur_c]), 0,
             size_links_per_element_ * curlevel + 1);
    }

    if ((signed)currObj != -1) {
      if (curlevel < maxlevelcopy) {
        dist_t curdist =
            EvaluateDistance(data_point, GetDataByInternalId(currObj), isMarkedDeleted(currObj));
        for (int level = maxlevelcopy; level > curlevel; level--) {
          bool changed = true;
          while (changed) {
            changed = false;
            unsigned int *data;
            std::unique_lock<std::mutex> lock(link_list_locks_[currObj]);
            data = get_linklist(currObj, level);
            int size = getListCount(data);

            tableint *datal = (tableint *)(data + 1);
            for (int i = 0; i < size; i++) {
              tableint cand = datal[i];
              if (cand < 0 || cand > max_elements_)
                throw std::runtime_error("cand error");
              dist_t d =
                  EvaluateDistance(data_point, GetDataByInternalId(cand), isMarkedDeleted(cand));
              if (d < curdist) {
                curdist = d;
                currObj = cand;
                changed = true;
              }
            }
          }
        }
      }

      bool epDeleted = isMarkedDeleted(enterpoint_copy);
      for (int level = std::min(curlevel, maxlevelcopy); level >= 0; level--) {
        if (level > maxlevelcopy || level < 0)  // possible?
          throw std::runtime_error("Level error");

        std::priority_queue<std::pair<dist_t, tableint>,
                            std::vector<std::pair<dist_t, tableint>>,
                            CompareByFirst>
            top_candidates = searchBaseLayer(currObj, data_point, level);
        if (epDeleted) {
          top_candidates.emplace(
              EvaluateDistance(data_point, GetDataByInternalId(enterpoint_copy),
                               true),
              enterpoint_copy);
          if (top_candidates.size() > ef_construction_) top_candidates.pop();
        }
        currObj = mutuallyConnectNewElement(data_point, cur_c, top_candidates,
                                            level, false);
      }
    } else {
      // Do nothing for the first element
      enterpoint_node_ = 0;
      maxlevel_ = curlevel;
    }

    // Releasing lock for the maximum level
    if (curlevel > maxlevelcopy) {
      enterpoint_node_ = cur_c;
      maxlevel_ = curlevel;
    }
    return cur_c;
  }
  std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const InputVectorT &query_data, size_t k,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr  // VALKEYSEARCH
  ) const override {
    return searchKnn(query_data, k, std::nullopt, isIdAllowed, isCancelled);
  }

  std::priority_queue<std::pair<dist_t, labeltype>> searchKnn(
      const InputVectorT &query_data, size_t k,
      std::optional<size_t> ef_runtime,
      BaseFilterFunctor *isIdAllowed = nullptr,
      BaseCancellationFunctor *isCancelled = nullptr  // VALKEYSEARCH
  ) const {
    std::priority_queue<std::pair<dist_t, labeltype>> result;
    if (cur_element_count_ == 0) return result;

    tableint currObj = enterpoint_node_;
    dist_t curdist =
        EvaluateDistance(query_data, GetDataByInternalId(enterpoint_node_),
                         isMarkedDeleted(enterpoint_node_));

    for (int level = maxlevel_; level > 0; level--) {
      bool changed = true;
      while (changed) {
        changed = false;
        unsigned int *data;

        data = (unsigned int *)get_linklist(currObj, level);
        int size = getListCount(data);
        metric_hops++;
        metric_distance_computations += size;

        tableint *datal = (tableint *)(data + 1);
        for (int i = 0; i < size; i++) {
          tableint cand = datal[i];
          if (cand < 0 || cand > max_elements_)
            throw std::runtime_error("cand error");
          dist_t d = EvaluateDistance(query_data, GetDataByInternalId(cand),
                                      isMarkedDeleted(cand));
          if (d < curdist) {
            curdist = d;
            currObj = cand;
            changed = true;
          }
        }
      }
    }

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    bool bare_bone_search =
        !num_deleted_ && !isIdAllowed && !isCancelled;  // VALKEYSEARCH
    if (bare_bone_search) {
      top_candidates = searchBaseLayerST<true>(
          currObj, query_data, std::max(ef_runtime.value_or(ef_), k),
          isIdAllowed, isCancelled);
    } else {
      top_candidates = searchBaseLayerST<false>(
          currObj, query_data, std::max(ef_runtime.value_or(ef_), k),
          isIdAllowed, isCancelled);
    }

    while (top_candidates.size() > k) {
      top_candidates.pop();
    }
    while (top_candidates.size() > 0) {
      std::pair<dist_t, tableint> rez = top_candidates.top();
      result.push(std::pair<dist_t, labeltype>(rez.first,
                                               GetExternalLabel(rez.second)));
      top_candidates.pop();
    }
    return result;
  }

  std::vector<std::pair<dist_t, labeltype>> searchStopConditionClosest(
      const InputVectorT &query_data,
      BaseSearchStopCondition<dist_t> &stop_condition,
      BaseFilterFunctor *isIdAllowed = nullptr) const {
    std::vector<std::pair<dist_t, labeltype>> result;
    if (cur_element_count_ == 0) return result;

    tableint currObj = enterpoint_node_;
    dist_t curdist =
        EvaluateDistance(query_data, GetDataByInternalId(enterpoint_node_),
                         isMarkedDeleted(enterpoint_node_));

    for (int level = maxlevel_; level > 0; level--) {
      bool changed = true;
      while (changed) {
        changed = false;
        unsigned int *data;

        data = (unsigned int *)get_linklist(currObj, level);
        int size = getListCount(data);
        metric_hops++;
        metric_distance_computations += size;

        tableint *datal = (tableint *)(data + 1);
        for (int i = 0; i < size; i++) {
          tableint cand = datal[i];
          if (cand < 0 || cand > max_elements_)
            throw std::runtime_error("cand error");
          dist_t d = EvaluateDistance(query_data, GetDataByInternalId(cand),
                                      isMarkedDeleted(cand));

          if (d < curdist) {
            curdist = d;
            currObj = cand;
            changed = true;
          }
        }
      }
    }

    std::priority_queue<std::pair<dist_t, tableint>,
                        std::vector<std::pair<dist_t, tableint>>,
                        CompareByFirst>
        top_candidates;
    top_candidates = searchBaseLayerST<false>(
        currObj, query_data, 0, isIdAllowed, nullptr, &stop_condition);

    size_t sz = top_candidates.size();
    result.resize(sz);
    while (!top_candidates.empty()) {
      result[--sz] = top_candidates.top();
      top_candidates.pop();
    }

    stop_condition.filter_results(result);

    return result;
  }

  void checkIntegrity() {
    int connections_checked = 0;
    std::vector<int> inbound_connections_num(cur_element_count_, 0);
    for (int i = 0; i < cur_element_count_; i++) {
      for (int l = 0; l <= element_levels_[i]; l++) {
        linklistsizeint *ll_cur = get_linklist_at_level(i, l);
        int size = getListCount(ll_cur);
        tableint *data = (tableint *)(ll_cur + 1);
        std::unordered_set<tableint> s;
        for (int j = 0; j < size; j++) {
          assert(data[j] < cur_element_count_);
          assert(data[j] != i);
          inbound_connections_num[data[j]]++;
          s.insert(data[j]);
          connections_checked++;
        }
        assert(s.size() == size);
      }
    }
    if (cur_element_count_ > 1) {
      int min1 = inbound_connections_num[0], max1 = inbound_connections_num[0];
      for (int i = 0; i < cur_element_count_; i++) {
        assert(inbound_connections_num[i] > 0);
        min1 = std::min(inbound_connections_num[i], min1);
        max1 = std::max(inbound_connections_num[i], max1);
      }
      std::cout << "Min inbound: " << min1 << ", Max inbound:" << max1 << "\n";
    }
    std::cout << "integrity ok, checked " << connections_checked
              << " connections\n";
  }
};
}  // namespace hnswlib