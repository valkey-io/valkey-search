/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
#define VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_

#include <atomic>
#include <cstddef>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"
#include "src/indexes/metric_types.h"

namespace valkey_search {

enum class StringType : uint8_t {
  TAG = 0,
  VECTOR = 1,
  KEY = 2,

  kStringTypeCount
};

class InternedString;

class StringInternStore {
 public:
  friend class InternedString;
  static StringInternStore &Instance() {
    static StringInternStore *instance = new StringInternStore();
    return *instance;
  }
  static std::shared_ptr<InternedString> Intern(absl::string_view str,
                                                StringType string_type,
                                                Allocator *allocator = nullptr);

  bool SetDeleteMark(const char* data, size_t length, bool mark_delete);

  size_t Size() const {
    absl::MutexLock lock(&mutex_);
    size_t total_size = 0;
    for (size_t i = 0; i < static_cast<size_t>(StringType::kStringTypeCount); ++i) {
      total_size += pools_[i].size();
    }
    return total_size;
  }

  struct TypeCounters {
    std::atomic<size_t> memory_bytes{0};
    std::atomic<size_t> object_count{0};
    
    void Update(int64_t memory_delta, int objects_delta) {
      memory_bytes.fetch_add(memory_delta, std::memory_order_relaxed);
      object_count.fetch_add(objects_delta, std::memory_order_relaxed);
    }

    TypeCounters GetCounters() const {
      return {
        {memory_bytes.load(std::memory_order_relaxed)},
        {object_count.load(std::memory_order_relaxed)}
      };
    }
  };

  TypeCounters GetCounters(StringType string_type) const {
    const size_t index = static_cast<size_t>(string_type);
    if (index >= static_cast<size_t>(StringType::kStringTypeCount)) return {{0}, {0}};
    return counters_[index].GetCounters();
  }

  // Get marked deleted counters (only relevant for vectors)
  TypeCounters GetMarkedDeletedCounters() const {
    return marked_deleted_.GetCounters();
  }

  // Update counters for specific string type
  void UpdateCounters(StringType string_type, int64_t memory_delta, int objects_delta);

 private:
  StringInternStore() = default;
  std::shared_ptr<InternedString> InternImpl(absl::string_view str,
                                             StringType string_type,
                                             Allocator *allocator);
  void Release(InternedString *str);
  absl::flat_hash_map<absl::string_view, std::weak_ptr<InternedString>> pools_[static_cast<size_t>(StringType::kStringTypeCount)] ABSL_GUARDED_BY(mutex_);
  TypeCounters counters_[static_cast<size_t>(StringType::kStringTypeCount)];
  TypeCounters marked_deleted_;
  mutable absl::Mutex mutex_;
};

class InternedString {
 public:
  friend class StringInternStore;
  static constexpr uint16_t INIT_USE_COUNT = UINT16_MAX;
  InternedString() = delete;
  InternedString(const InternedString &) = delete;
  InternedString &operator=(const InternedString &) = delete;
  InternedString(InternedString &&) = delete;
  InternedString &operator=(InternedString &&) = delete;
  // Note: The constructor below does not perform actual string interning.
  // It is intended for cases where an API requires a `StringIntern` object
  // but interning is unnecessary or inefficient. For example, this applies
  // when fetching data from remote nodes.
  InternedString(absl::string_view str, StringType string_type) : InternedString(str, false, string_type) {};

  ~InternedString();

  absl::string_view Str() const { return {data_, length_}; }
  operator absl::string_view() const { return Str(); }
  absl::string_view operator*() const { return Str(); }

 private:
  InternedString(absl::string_view str, bool shared, StringType string_type);
  InternedString(char *data, size_t length, StringType string_type);
  void DecrementUseCount();
  void IncrementUseCount();

  char *data_;
  size_t length_;
  std::atomic<uint16_t> use_count_{INIT_USE_COUNT};
  StringType string_type_;
  bool is_shared_;
  bool is_data_owner_;
};

using InternedStringPtr = std::shared_ptr<InternedString>;

struct InternedStringPtrHash {
  template <typename T>
  std::size_t operator()(const T &sp) const {
    return absl::HashOf(sp->Str());
  }
};

struct InternedStringPtrEqual {
  template <typename T, typename U>
  bool operator()(const T &lhs, const U &rhs) const {
    return lhs->Str() == rhs->Str();
  }
};

template <typename T>
using InternedStringMap =
    absl::flat_hash_map<InternedStringPtr, T, InternedStringPtrHash,
                        InternedStringPtrEqual>;
using InternedStringSet =
    absl::flat_hash_set<InternedStringPtr, InternedStringPtrHash,
                        InternedStringPtrEqual>;

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
