/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/memory_pool.h"

#include <absl/log/check.h>

namespace valkey_search {

void MemoryPool::NewChunk(size_t chunk_size) {
  auto this_chunk_size = std::max(chunk_size, chunk_size_);
  auto chunk =
      reinterpret_cast<Chunk*>(new char[this_chunk_size + sizeof(Chunk)]);
  chunk->size_ = this_chunk_size;
  chunk->leftoff_ = 0;
  allocated_ += this_chunk_size;
  chunks_.emplace_back(chunk);
}

MemoryPool::MemoryPool(size_t chunk_size) : chunk_size_(chunk_size) {
  NewChunk(0);
}

size_t ComputeBytes(size_t bytes, size_t alignment) {
  CHECK(alignment <= 16);  // Only currently supported value
  // Round up the bytes to the alignment,
  return (bytes + 15ul) & ~(15ul);
}

void* MemoryPool::do_allocate(size_t bytes, size_t alignment) {
  CHECK(!chunks_.empty());
  size_t this_bytes = ComputeBytes(bytes, alignment);
  Chunk* chunk = chunks_.back();
  if (chunk->leftoff_ + this_bytes > chunk->size_) {
    NewChunk(this_bytes);
    chunk = chunks_.back();
  }
  CHECK(chunk->leftoff_ + this_bytes <= chunk->size_);
  void* p = chunk->data_ + chunk->leftoff_;
  chunk->leftoff_ += this_bytes;
  inuse_ += this_bytes;
  return p;
}

void MemoryPool::do_deallocate(void* p, size_t bytes, size_t alignment) {
  alignment = std::max(alignment, 16ul);
  bytes = (bytes + alignment - 1) & ~(alignment - 1);
  CHECK(inuse_ >= bytes);
  inuse_ -= bytes;
}

MemoryPool::~MemoryPool() {
  CHECK(inuse_ == 0);
  while (!chunks_.empty()) {
    auto chunk = chunks_.back();
    allocated_ -= chunk->size_;
    chunks_.pop_back();
    delete[] reinterpret_cast<char*>(chunk);
  }
  CHECK(allocated_ == 0);
}

//
// Debugging infrastructure for MemoryPools.
//
bool thread_local MemoryPoolDebuggingEnabled = false;

EnableMemoryPoolDebugging::EnableMemoryPoolDebugging() {
  CHECK(!MemoryPoolDebuggingEnabled);
  MemoryPoolDebuggingEnabled = true;
}

EnableMemoryPoolDebugging::~EnableMemoryPoolDebugging() {
  CHECK(MemoryPoolDebuggingEnabled);
  MemoryPoolDebuggingEnabled = false;
}

class Backtrace {};

static absl::Mutex pool_debug_mutex;
// static absl::flat_hash_map<class K, class V>

//
// This is called out of the malloc chain.
// If pool debugging is enabled, then it captures the current call stack
//
void MemoryPoolDebugCapture() {}

}  // namespace valkey_search