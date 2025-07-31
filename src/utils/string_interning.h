/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_
#define VALKEYSEARCH_SRC_UTILS_STRING_INTERNING_H_

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

class InternedString;

class StringInternStore {
 public:
  friend class InternedString;
  friend class InternedString;
  static StringInternStore &Instance() {
    static StringInternStore *instance = new StringInternStore();
    return *instance;
  }
  static std::shared_ptr<InternedString> Intern(absl::string_view str,
                                                Allocator *allocator = nullptr,
                                                indexes::MetricType metric_type = indexes::MetricType::kNone);

  bool MarkDelete(absl::string_view str);
  bool UnmarkDelete(absl::string_view str);

  size_t Size() const {
    absl::MutexLock lock(&mutex_);
    return str_to_interned_.size();
  }

 private:
  StringInternStore() = default;
  std::shared_ptr<InternedString> InternImpl(absl::string_view str,
                                             Allocator *allocator,
                                             indexes::MetricType metric_type);
  void Release(InternedString *str);
  absl::flat_hash_map<absl::string_view, std::weak_ptr<InternedString>>
      str_to_interned_ ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

class InternedString {
 public:
  friend class StringInternStore;
  InternedString() = delete;
  InternedString(const InternedString &) = delete;
  InternedString &operator=(const InternedString &) = delete;
  InternedString(InternedString &&) = delete;
  InternedString &operator=(InternedString &&) = delete;
  // Note: The constructor below does not perform actual string interning.
  // It is intended for cases where an API requires a `StringIntern` object
  // but interning is unnecessary or inefficient. For example, this applies
  // when fetching data from remote nodes.
  InternedString(absl::string_view str, indexes::MetricType metric_type = indexes::MetricType::kNone) : InternedString(str, false, metric_type) {};

  ~InternedString();

  absl::string_view Str() const { return {data_, length_}; }
  operator absl::string_view() const { return Str(); }
  absl::string_view operator*() const { return Str(); }

  void SetMetadataFlags(uint32_t flags) { 
    metadata_flags_ = flags; 
  }

  uint32_t GetMetadataFlags() const { 
    return metadata_flags_; 
  }

  uint32_t* GetMetadataFlagsPtr() { 
    return &metadata_flags_; 
  }

 private:
  InternedString(absl::string_view str, bool shared, indexes::MetricType metric_type = indexes::MetricType::kNone);
  InternedString(char *data, size_t length, indexes::MetricType metric_type = indexes::MetricType::kNone);

  char *data_;
  size_t length_;
  bool is_shared_;
  bool is_data_owner_;
  uint32_t metadata_flags_{0};  // Store MetaData as 32-bit flags
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
