/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#include "src/utils/memory_pool.h"

#include <absl/log/check.h>
#include <execinfo.h>

namespace valkey_search {

void MemoryPool::NewChunk(size_t data_size) {
  auto this_chunk_size = std::max(data_size, chunk_size_);
  auto chunk =
      reinterpret_cast<Chunk*>(new char[this_chunk_size + sizeof(Chunk)]);
  chunk->size_ = this_chunk_size;
  chunk->leftoff_ = 0;
  allocated_ += this_chunk_size;
  chunks_.emplace_back(chunk);
}

MemoryPool::MemoryPool(size_t chunk_size) {
  // Externally chunks are sized to fit efficiently into slabs, so our
  // internal chunk size gets reduced accordingly.
  CHECK(chunk_size > sizeof(Chunk));
  chunk_size_ = chunk_size - sizeof(Chunk);
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
  //
  // Note, because of the way things get destructed, we might get called to
  // deallocate after the pool itself has been destroyed. So be care here....
  //
  alignment = std::max(alignment, 16ul);
  bytes = (bytes + alignment - 1) & ~(alignment - 1);
  CHECK(inuse_ >= bytes);
  inuse_ -= bytes;
}

MemoryPool::~MemoryPool() {
  while (!chunks_.empty()) {
    auto chunk = chunks_.back();
    allocated_ -= chunk->size_;
    chunks_.pop_back();
    delete[] reinterpret_cast<char*>(chunk);
  }
  CHECK(allocated_ == 0);
}

}  // namespace valkey_search
