/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/indexes/text/rax/rax.h"

#include <cstdlib>
#include <cstring>

#include "src/utils/allocator.h"

extern "C" {
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_malloc(size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
void __wrap_free(void *ptr);
// NOLINTNEXTLINE(readability-identifier-naming)
void *__wrap_realloc(void *ptr, size_t size);
// NOLINTNEXTLINE(readability-identifier-naming)
int __wrap_malloc_usable_size(void *ptr);
}

namespace valkey_search {

namespace {
thread_local RaxTree *t_active_rax = nullptr;

class ActiveRaxGuard {
 public:
  explicit ActiveRaxGuard(RaxTree *rax) : prev_(t_active_rax) {
    t_active_rax = rax;
  }
  ~ActiveRaxGuard() { t_active_rax = prev_; }

 private:
  RaxTree *prev_;
};
}  // namespace

RaxTree::RaxTree(Allocator *allocator)
    : allocator_(allocator ? allocator : &GetThreadLocalSegregatedAllocator()) {
  ActiveRaxGuard guard(this);
  rax_ = RaxNew();
}

RaxTree::~RaxTree() {
  if (rax_) {
    ActiveRaxGuard guard(this);
    RaxFree(rax_);
    rax_ = nullptr;
  }
}

RaxTree::RaxTree(RaxTree &&other) noexcept
    : rax_(other.rax_), allocator_(other.allocator_) {
  other.rax_ = nullptr;
}

RaxTree &RaxTree::operator=(RaxTree &&other) noexcept {
  if (this != &other) {
    if (rax_) {
      ActiveRaxGuard guard(this);
      RaxFree(rax_);
    }
    rax_ = other.rax_;
    allocator_ = other.allocator_;
    other.rax_ = nullptr;
  }
  return *this;
}

void RaxTree::FreeWithCallback(void (*free_callback)(void *)) {
  if (rax_) {
    ActiveRaxGuard guard(this);
    RaxFreeWithCallback(rax_, free_callback);
    rax_ = nullptr;
  }
}

void *RaxTree::AllocateNode(size_t size) {
  if (allocator_) {
    return allocator_->Allocate(size);
  }
  return GetThreadLocalSegregatedAllocator().Allocate(size);
}

void RaxTree::FreeNode(void *ptr) {
  if (!ptr) {
    return;
  }
  SegregatedFixedSizeAllocator::Free(static_cast<char *>(ptr));
}

void *RaxTree::ReallocateNode(void *ptr, size_t new_size) {
  if (!ptr) {
    return AllocateNode(new_size);
  }
  if (new_size == 0) {
    FreeNode(ptr);
    return nullptr;
  }
  auto *seg = dynamic_cast<SegregatedFixedSizeAllocator *>(allocator_);
  if (seg) {
    return seg->Reallocate(static_cast<char *>(ptr), new_size);
  }
  return GetThreadLocalSegregatedAllocator().Reallocate(
      static_cast<char *>(ptr), new_size);
}

int RaxTree::UsableSize(void *ptr) {
  if (!ptr) {
    return 0;
  }
  return static_cast<int>(
      SegregatedFixedSizeAllocator::UsableSize(static_cast<char *>(ptr)));
}

int RaxTree::Insert(std::string_view key, void *data, void **old_data) {
  ActiveRaxGuard guard(this);
  return RaxInsert(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), data, old_data);
}

int RaxTree::Remove(std::string_view key, void **old_data) {
  ActiveRaxGuard guard(this);
  return RaxRemove(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), old_data);
}

void *RaxTree::Find(std::string_view key) const {
  ActiveRaxGuard guard(const_cast<RaxTree *>(this));
  void *val = nullptr;
  RaxFind(rax_,
          reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
          key.size(), &val);
  return val;
}

int RaxTree::Mutate(std::string_view key, RaxMutateCallback mutate,
                    void *caller_context, item_count_op op) {
  ActiveRaxGuard guard(this);
  return RaxMutate(
      rax_, reinterpret_cast<unsigned char *>(const_cast<char *>(key.data())),
      key.size(), mutate, caller_context, op);
}

size_t RaxTree::GetSubtreeItemCount(std::string_view prefix) const {
  ActiveRaxGuard guard(const_cast<RaxTree *>(this));
  return RaxGetSubtreeItemCount(
      rax_,
      reinterpret_cast<unsigned char *>(const_cast<char *>(prefix.data())),
      prefix.size());
}

uint64_t RaxTree::Size() const { return rax_ ? rax_->numele : 0; }

size_t RaxTree::GetAllocSize() const { return rax_ ? rax_->alloc_size : 0; }

extern "C" {
bool IsRaxActive() { return true; }
void *RaxMemMalloc(size_t size) {
  return t_active_rax ? t_active_rax->AllocateNode(size)
                      : GetThreadLocalSegregatedAllocator().Allocate(size);
}
void RaxMemFree(void *ptr) {
  if (!ptr) {
    return;
  }
  if (t_active_rax) {
    t_active_rax->FreeNode(ptr);
    return;
  }
  SegregatedFixedSizeAllocator::Free(static_cast<char *>(ptr));
}
void *RaxMemRealloc(void *ptr, size_t size) {
  return t_active_rax ? t_active_rax->ReallocateNode(ptr, size)
                      : GetThreadLocalSegregatedAllocator().Reallocate(
                            static_cast<char *>(ptr), size);
}
int RaxMemUsableSize(void *ptr) {
  return t_active_rax
             ? t_active_rax->UsableSize(ptr)
             : static_cast<int>(SegregatedFixedSizeAllocator::UsableSize(
                   static_cast<char *>(ptr)));
}
}

}  // namespace valkey_search
