/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/string_interning.h"

#include <cstring>
#include <utility>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/allocator.h"
#include "vmsdk/src/memory_tracker.h"

/*

String interning works by storing a single copy of each distinct string value,
which must be immutable. Interned strings are reference-counted, and when the
last reference to an interned string is released, the string is removed from
the intern pool and its memory is freed.

The property of distinctness allows for fast comparisons and hashing by using
the memory address of the interned string object. Rather than comparing string
contents.

Like std::shared_ptr, each unique instance of the string contains an atomic
reference count that is incremented and decremented as references are created
and destroyed. This allows the references to strings to be moved, duplicated and
destroyed without additional synchronization. Global synchronization is only
needed when obtaining a pointer to a string (which might involve creating a new
interned string if string is unknown) or when the last reference to a string is
released.

In addition to the ref-counted strings themselves is a global map that serves to
ensure that each distinct string is only stored once. This map is protected by a
mutex to ensure thread safety. The map invariant is that an entry exists iff
the associated reference count is greater than zero. Thus incrementing the
reference count of an entry never requires modifying the map and thus doesn't
need to lock the global mutex. However, when decrementing the reference count
for an entry, the 1->0 transition requires removing the entry from the map
atomically in order to maintain the invariant. Here atomically means that the
1->0 transition can only be done reliably when holding the global mutex.
Substantial care in the decrement code is required to ensure this.

*/

namespace valkey_search {

MemoryPool StringInternStore::memory_pool_{0};

struct InlineInternedString : public InternedString {
  // Inline string data follows immediately after this structure.
  // char data_[length_ + 1];  // null-terminated
  InlineInternedString(size_t length) {
    is_inline_ = 1;
    length_ = length;
    ref_count_.store(1, std::memory_order_seq_cst);
  }
};
struct OutOfLineInternedString : public InternedString {
  const char* out_of_line_data_;
  OutOfLineInternedString(const char* data, size_t length) {
    is_inline_ = 0;
    out_of_line_data_ = data;
    length_ = length;
    ref_count_.store(1, std::memory_order_seq_cst);
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

void InternedString::DecrementRefCount() {
  bool completed;
  // This is the hard case, because we need to ensure that the 1->0
  // transition is done while holding the global mutex.
  uint32_t current_value = ref_count_.load(std::memory_order_seq_cst);
  do {
    if (current_value == 0) {
      // This case happens when erasing the entry in the map, which doesn't have
      // an associated reference count, so we just return.
      return;
    }
    if (current_value == 1) {
      //
      // This is the case we care about. Need to remove from map while holding
      // the lock, Destructor will lock the mutex and then attempt the decrement
      // again.
      //
      Destructor();
      return;
    }
    // Do the real decrement, but if somebody else beat us, try again.
    completed =
        ref_count_.compare_exchange_strong(current_value, current_value - 1);
  } while (!completed);
}

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
    // Allocate the InternedString structure and the string data in one
    // block.
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
    if (is_inline_) {
      delete[] reinterpret_cast<char*>(this);
    } else {
      auto ptr = reinterpret_cast<const OutOfLineInternedString*>(this);
      Allocator::Free(const_cast<char*>(ptr->out_of_line_data_));
      delete ptr;
    }
  }
}

InternedStringPtr* MakeShadowInternPtrPtr(InternedString* str, void*& storage) {
  storage = str;
  static_assert(sizeof(storage) == sizeof(InternedStringPtr));
  return reinterpret_cast<InternedStringPtr*>(&storage);
}

bool StringInternStore::Release(InternedString* str) {
  //
  // Need to make an InternStringPtr to look it up in the map.
  // But we don't want to have the refcounts changed, so we create a
  // temporary InternedStringPtr that doesn't modify the refcounts.
  //
  OutOfLineInternedString fake(str->Str().data(), str->Str().size());
  void* storage;
  InternedStringPtr* ptr_ptr = MakeShadowInternPtrPtr(&fake, storage);
  absl::MutexLock lock(&mutex_);
  //
  // Now that we have the lock, try our decrement to see if we really
  // want to destroy this entry.
  //
  auto old_value = str->ref_count_.fetch_sub(1, std::memory_order_seq_cst);
  if (old_value > 1) {
    //
    // Still referenced.
    //
    return false;
  }
  //
  // This is the true 1->0 transition. Remove from map.
  //
  auto it = str_to_interned_.find(*ptr_ptr);
  CHECK(it != str_to_interned_.end()) << "Bad Map State";
  CHECK(str->RefCount() == 0);
  str_to_interned_.erase(
      it);  // Note this will also call the DecrementRefCount, but
            // since refcount is already zero, it will be a no-op.
  return true;
}

InternedStringPtr StringInternStore::Intern(absl::string_view str,
                                            Allocator* allocator) {
  return Instance().InternImpl(str, allocator);
}

StringInternStore* StringInternStore::MakeInstance() {
  IsolatedMemoryScope scope{StringInternStore::memory_pool_};
  return new StringInternStore();
}

StringInternStore& StringInternStore::Instance() {
  static StringInternStore* instance = MakeInstance();
  return *instance;
}

InternedStringPtr StringInternStore::InternImpl(absl::string_view str,
                                                Allocator* allocator) {
  IsolatedMemoryScope scope{memory_pool_};
  //
  // Construct a fake InternedStringPtr to look up in the map.
  // Doing it carefully to avoid modifying refcounts.
  //
  OutOfLineInternedString fake(str.data(), str.size());
  void* storage;
  InternedStringPtr* ptr_ptr = MakeShadowInternPtrPtr(&fake, storage);

  absl::MutexLock lock(&mutex_);
  auto it = str_to_interned_.find(*ptr_ptr);
  if (it != str_to_interned_.end()) {
    return *it;  // will bump the refcount automatically.
  }
  //
  // Create a new interned string. Without bumping the refcount....
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
    auto allocated = str->Allocated();
    auto refcount =
        str.RefCount();  // This is volatile even while holding the lock
    if (str->IsInline()) {
      stats.inline_total_stats_.count_++;
      stats.inline_total_stats_.bytes_ += size;
      stats.inline_total_stats_.allocated_ += allocated;
      stats.by_ref_stats_[refcount].count_++;
      stats.by_ref_stats_[refcount].bytes_ += size;
      stats.by_ref_stats_[refcount].allocated_ += allocated;
      stats.by_size_stats_[size].count_++;
      stats.by_size_stats_[size].bytes_ += size;
      stats.by_size_stats_[size].allocated_ += allocated;
    } else {
      stats.out_of_line_total_stats_.count_++;
      stats.out_of_line_total_stats_.bytes_ += size;
      stats.out_of_line_total_stats_.allocated_ += allocated;
      stats.by_ref_stats_[-refcount].count_++;
      stats.by_ref_stats_[-refcount].bytes_ += size;
      stats.by_ref_stats_[-refcount].allocated_ += allocated;
      stats.by_size_stats_[-size].count_++;
      stats.by_size_stats_[-size].bytes_ += size;
      stats.by_size_stats_[-size].allocated_ += allocated;
    }
  }
  return stats;
}

}  // namespace valkey_search
