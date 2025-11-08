/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/string_interning.h"

#include <cstring>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"
#include "vmsdk/src/memory_allocation_overrides.h"
#include "vmsdk/src/memory_tracker.h"

namespace valkey_search {

MemoryPool StringInternStore::memory_pool_{0};

struct InlineInternedString : public InternedString {
  // Inline string data follows immediately after this structure.
  // char data_[length_ + 1];  // null-terminated
  InlineInternedString(size_t length) {
    is_inline_ = 1;
    length_ = length;
    ref_count_.store(1, std::memory_order_relaxed);
  }
};
struct OutOfLineInternedString : public InternedString {
  const char* out_of_line_data_;
  OutOfLineInternedString(const char* data, size_t length) {
    is_inline_ = 0;
    out_of_line_data_ = data;
    length_ = length;
    ref_count_.store(1, std::memory_order_relaxed);
  }
};

/*
// This generates warnings.
static_assert(offsetof(OutOfLineInternedString, out_of_line_data_) ==
                  sizeof(InternedString),
              "OutOfLineInternedString layout must match InternedString");
*/

static_assert(sizeof(OutOfLineInternedString) ==
                  sizeof(InternedString) + sizeof(char*),
              "OutOfLineInternedString size must match InternedString size");

InternedString* InternedString::Constructor(absl::string_view str,
                                            Allocator* allocator) {
  // NOTE: isolate memory tracking for allocation.
  IsolatedMemoryScope scope{StringInternStore::memory_pool_};
  CHECK(str.size() <= 0x7fffffff) << "String too large to intern";

  InternedString* ptr;
  char* data;
  if (allocator) {
    //
    // Allocate the InternedString structure and the string data separately.
    //
    data = allocator->Allocate(str.size() + 1);
    ptr = new OutOfLineInternedString(data, str.size());
  } else {
    //
    // Allocate the InternedString structure and the string data in one block.
    //
    size_t total_size = sizeof(InternedString) + str.size() + 1;
    ptr = new (new char[total_size]) InlineInternedString(str.size());
    data = ptr->data_;
  }
  //
  // Now, copy the string data. and finish initializing the InternedString.
  //
  memcpy(data, str.data(), str.size());
  data[str.size()] = '\0';
  return ptr;
}

absl::string_view InternedString::Str() const {
  if (is_inline_) {
    return {data_, length_};
  } else {
    auto ptr = reinterpret_cast<const OutOfLineInternedString*>(this);
    return {ptr->out_of_line_data_, length_};
  }
}

void InternedString::Destructor() {
  // NOTE: isolate memory tracking for deallocation.
  IsolatedMemoryScope scope{StringInternStore::memory_pool_};
  if (StringInternStore::Instance().Release(this)) {
    if (!is_inline_) {
      auto ptr = reinterpret_cast<const OutOfLineInternedString*>(this);
      Allocator::Free(const_cast<char*>(ptr->out_of_line_data_));
    }
    delete[] reinterpret_cast<char*>(this);
  }
}

InternedStringPtr* MakeShadowInternPtrPtr(InternedString* str, void*& storage) {
  storage = str;
  static_assert(sizeof(storage) == sizeof(InternedStringPtr));
  return reinterpret_cast<InternedStringPtr*>(&storage);
}

bool StringInternStore::Release(InternedString* str) {
  absl::MutexLock lock(&mutex_);
  if (str->ref_count_.load(std::memory_order_seq_cst) > 1) {
    //
    // It's possible that between the time we checked the ref count and now,
    // another thread has incremented it. In that case, we don't remove it
    // from the map.
    return false;
  }
  //
  // Need to make an InternStringPtr to look it up in the map.
  // But we don't want to have the refcounts changed, so we create a temporary
  // InternedStringPtr that doesn't modify the refcounts.
  //
  OutOfLineInternedString fake(str->Str().data(), str->Str().size());
  void* storage;
  InternedStringPtr* ptr_ptr = MakeShadowInternPtrPtr(&fake, storage);
  auto it = str_to_interned_.find(*ptr_ptr);
  CHECK(it != str_to_interned_.end());
  str_to_interned_.erase(it);
  return true;
}

InternedStringPtr StringInternStore::Intern(absl::string_view str,
                                            Allocator* allocator) {
  auto result = Instance().InternImpl(str, allocator);
  return result;
}

StringInternStore* StringInternStore::MakeInstance() {
  IsolatedMemoryScope scope{StringInternStore::memory_pool_};
  return new StringInternStore();
}

StringInternStore& StringInternStore::Instance() {
  static StringInternStore* instance = MakeInstance();
  return *instance;
}

std::shared_ptr<InternedString> StringInternStore::InternImpl(
    absl::string_view str, Allocator* allocator) {
  IsolatedMemoryScope scope{memory_pool_};
  absl::MutexLock lock(&mutex_);
  //
  // Construct a fake InternedStringPtr to look up in the map.
  // Doing it carefully to avoid modifying refcounts.
  //
  OutOfLineInternedString fake(str.data(), str.size());
  void* storage;
  InternedStringPtr* ptr_ptr = MakeShadowInternPtrPtr(&fake, storage);
  auto it = str_to_interned_.find(*ptr_ptr);
  if (it != str_to_interned_.end()) {
    return *it;  // Should bump the refcount automatically.
  }
  //
  // Not found, create a new interned string. Without bumping the refcount....
  //
  InternedString* new_ptr = InternedString::Constructor(str, allocator);
  str_to_interned_.insert(std::move(InternedStringPtr(new_ptr)));
  return {new_ptr};
}

int64_t StringInternStore::GetMemoryUsage() { return memory_pool_.GetUsage(); }

StringInternStore::Stats StringInternStore::GetStats() const {
  Stats stats;
  absl::MutexLock lock(&mutex_);
  for (const auto& str : str_to_interned_) {
    auto size = str->Str().size();
    if (str->IsInline()) {
      stats.inline_total_stats_.count_++;
      stats.inline_total_stats_.bytes_ += size;
      stats.by_ref_stats_[str.RefCount()].count_++;
      stats.by_ref_stats_[str.RefCount()].bytes_ += size;
      stats.by_size_stats_[size].count_++;
      stats.by_size_stats_[size].bytes_ += size;
    } else {
      stats.out_of_line_total_stats_.count_++;
      stats.out_of_line_total_stats_.bytes_ += size;
      stats.by_ref_stats_[-str.RefCount()].count_++;
      stats.by_ref_stats_[-str.RefCount()].bytes_ += size;
      stats.by_size_stats_[-size].count_++;
      stats.by_size_stats_[-size].bytes_ += size;
    }
  }
  return stats;
}

}  // namespace valkey_search
