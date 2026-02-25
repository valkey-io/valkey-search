#ifndef VALKEYSEARCH_SRC_UTILS_INLINED_MULTISET_H_
#define VALKEYSEARCH_SRC_UTILS_INLINED_MULTISET_H_

#include <algorithm>
#include <functional>  // For std::greater

#include "absl/container/inlined_vector.h"

namespace valkey_search {

template <typename T, size_t N>
class InlinedMultiset {
 public:
  using Storage = absl::InlinedVector<T, N>;
  using iterator = typename Storage::iterator;
  using const_iterator = typename Storage::const_iterator;

  // Maintains the Heap invariant on every insertion
  template <typename... Args>
  void emplace(Args &&...args) {
    storage_.emplace_back(std::forward<Args>(args)...);
    std::push_heap(storage_.begin(), storage_.end(), std::greater<T>());
  }

  // To support batching: push multiple, then call heapify()
  template <typename... Args>
  void push_back_unsorted(Args &&...args) {
    storage_.emplace_back(std::forward<Args>(args)...);
  }

  // O(K) complexity - much faster than Sort for large K
  void heapify() {
    std::make_heap(storage_.begin(), storage_.end(), std::greater<T>());
  }

  // Removes the minimum element (the root of the heap)
  void pop_min() {
    if (storage_.empty()) return;
    // Moves the smallest element to the back, restores heap for the rest
    std::pop_heap(storage_.begin(), storage_.end(), std::greater<T>());
    storage_.pop_back();
  }

  // Access the minimum element in O(1)
  const T &min() const { return storage_.front(); }

  // NOTE: Iterating a heap is NOT sorted.
  // If you need sorted iteration, you must convert it or use sort().
  const_iterator begin() const { return storage_.begin(); }
  const_iterator end() const { return storage_.end(); }

  bool empty() const { return storage_.empty(); }
  void clear() { storage_.clear(); }
  size_t size() const { return storage_.size(); }

 private:
  Storage storage_;
};

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_INLINED_MULTISET_H_