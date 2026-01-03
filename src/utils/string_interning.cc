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
mutex to ensure thread safety.

Making this multi-thread safe involves some care to avoid race conditions. The
most difficult case is when one thread is releasing the last reference to a
string (which will delete it from the map), while another thread is trying to
intern the same string (which will try to add it to the map). What makes the
case difficult is the desire to decrement the reference count without holding
the global mutex. This creates the unusual situation where the reference count
of the string is zero, but it's still in the global map. To handle this case,
when a thread tries to intern a string, if it finds the string in the map, it
checks the reference count again after acquiring the mutex. If the reference
count is zero, it means that another thread is in the process of deleting the
string from the map. In this case, the map entry is pointed to the new string,
but the old string's memory is not reclaimed as this still "owned" by the
pointer that's in the process of being destroyed. Conversely, when the
destroying thread acquires the mutex if must check the map to see if the entry
in the map is for the current string before manipulating the global map.

*/

namespace valkey_search {

absl::Mutex cout_mutex_;

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
              "OutOfLineInternedString layout must match
InternedString");
*/

static_assert(sizeof(OutOfLineInternedString) ==
                  sizeof(InternedString) + sizeof(char*),
              "OutOfLineInternedString size must match InternedString size");

void InternedString::IncrementRefCount() {
  auto old_value = ref_count_.fetch_add(1, std::memory_order_seq_cst);
  SYNCOUT("INCR : " << (void*)this << " : " << old_value << "->"
                    << ref_count_.load(std::memory_order_seq_cst));
}

void InternedString::DecrementRefCount() {
  bool completed;
  uint32_t current_value = ref_count_.load(std::memory_order_seq_cst);
  do {
    if (current_value == 0) {
      SYNCOUT("DECR : " << (void*)this << " WAS ZERO (DTOR?)");
      return;
    }
    if (current_value == 1) {
      SYNCOUT("DECR : " << (void*)this << " REFCOUNT ONE");
      Destructor();
      return;
    }
    uint32_t old_value = current_value;
    completed =
        ref_count_.compare_exchange_strong(current_value, current_value - 1);
    SYNCOUT("DECR : " << (void*)this << " : Was: " << old_value << "->"
                      << current_value << " Completed:" << completed);
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
    // Allocate the InternedString structure and the string data
    // separately.
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
  // Now, copy the string data. and finish initializing the
  // InternedString.
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
  SYNCOUT("DTOR: RELEASING : " << (void*)this << " REF:" << RefCount());
  if (StringInternStore::Instance().Release(this)) {
    SYNCOUT("DTOR: DELETING : " << (void*)this);
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
    SYNCOUT("DTOR: STILL REFERENCED AFTER DECR : "
            << (void*)str << " REF: " << " Old: " << old_value
            << " Now: " << str->RefCount());
    return false;
  }
  auto it = str_to_interned_.find(*ptr_ptr);
  CHECK(it != str_to_interned_.end()) << "Bad Map State";
  SYNCOUT("DTOR: REMOVE : " << (void*)str << " REF: " << str->RefCount());
  CHECK(str->RefCount() == 0);
  str_to_interned_.erase(it);
  return true;
}

InternedStringPtr StringInternStore::Intern(absl::string_view str,
                                            Allocator* allocator) {
  auto ptr = Instance().InternImpl(str, allocator);
  SYNCOUT("CTOR: RETURNING : " << (void*)(ptr->Str().data() - 8)
                               << " Refcount: " << ptr.RefCount());
  return ptr;
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
  SYNCOUT("CTOR: Looked up : " << str);
  auto it = str_to_interned_.find(*ptr_ptr);
  if (it != str_to_interned_.end()) {
    auto rc = it->RefCount();
    CHECK(rc > 0);
    SYNCOUT("CTOR: Already Present : " << (void*)((*it)->Str().data() - 8)
                                       << " Ref: " << rc);
    return *it;  // will bump the refcount automatically.
  }
  //
  // Create a new interned string. Without bumping the refcount....
  //
  InternedString* new_ptr = InternedString::Constructor(str, allocator);
  str_to_interned_.insert(std::move(InternedStringPtr(new_ptr)));
  SYNCOUT("CTOR: Create New: " << (void*)(new_ptr->Str().data() - 8)
                               << " Ref: " << new_ptr->RefCount());
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
