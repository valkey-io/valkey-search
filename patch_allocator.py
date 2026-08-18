import re

with open('src/utils/allocator.cc', 'r') as f:
    lines = f.readlines()

# Rewrite lines before 176
old_lines = lines[:176]
new_prefix = []
for line in old_lines:
    line = line.replace('bitmap(std::make_unique<std::atomic<uint64_t>[]>(num_bitmap_words))', 'bitmap(std::make_unique<uint64_t[]>(num_bitmap_words))')
    line = line.replace('bitmap[word_index].store(0, std::memory_order_relaxed);', 'bitmap[word_index] = 0;')
    line = line.replace('bitmap[num_bitmap_words - 1].store(unused_mask, std::memory_order_relaxed);', 'bitmap[num_bitmap_words - 1] = unused_mask;')
    new_prefix.append(line)

new_code = """FixedSizeAllocator::FixedSizeAllocator(size_t size, size_t alignment)
    : size_(alignment <= 1 ? size
                           : AlignUp(size, NormalizeAlignment(alignment))),
      alignment_(alignment <= 1 ? 1 : NormalizeAlignment(alignment)),
      owner_thread_id_(std::this_thread::get_id()) {}

FixedSizeAllocator::~FixedSizeAllocator() {
  auto clear_list = [](IntrusiveList<AllocatorChunk> &list) {
    while (!list.Empty()) {
      auto chunk = list.Front();
      list.Remove(chunk);
      chunk_tracker.Untrack(chunk);
      chunk->Release();
    }
  };
  clear_list(fully_used_chunks_);
  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    clear_list(chunk_group);
  }
}

size_t FixedSizeAllocator::ChunkCount() const {
  auto size = fully_used_chunks_.Size();
  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    size += chunk_group.Size();
  }
  return size;
}

void FixedSizeAllocator::SweepRemoteFrees() {
  char* pending = remote_frees_.exchange(nullptr, std::memory_order_acquire);
  while (pending != nullptr) {
    char* next = *reinterpret_cast<char**>(pending);
    AllocatorChunk* chunk = chunk_tracker.FindAndRetainChunk(pending);
    if (chunk) {
      ProcessLocalFree(chunk, pending);
      chunk->Release();
    }
    pending = next;
  }
}

static char *TryAllocateFromChunk(AllocatorChunk *chunk) {
  if (!chunk || chunk->retired) {
    return nullptr;
  }

  size_t start_word = chunk->scan_hint % chunk->num_bitmap_words;
  for (size_t i = 0; i < chunk->num_bitmap_words; ++i) {
    size_t word_index = (start_word + i) % chunk->num_bitmap_words;
    uint64_t old_val = chunk->bitmap[word_index];
    if (old_val != ~0ULL) {
      int bit_index = std::countr_zero(~old_val);
      size_t entry_idx = word_index * kEntriesPerBitmapWord + bit_index;
      chunk->bitmap[word_index] = old_val | (1ULL << bit_index);
      
      chunk->scan_hint = (word_index + 1) % chunk->num_bitmap_words;
      chunk->allocated_count++;
      return chunk->data + entry_idx * chunk->entry_size;
    }
  }
  return nullptr;
}

char *FixedSizeAllocator::Allocate() {
  SweepRemoteFrees();
  
  AllocatorChunk *curr = current_chunk_;
  if (curr) {
    char *ptr = TryAllocateFromChunk(curr);
    if (ptr) {
      if (curr->allocated_count >= curr->entries_in_chunk) {
        UpdateChunkGroup(curr);
        current_chunk_ = nullptr;
      }
      active_allocations_++;
      IncrementRef();
      return ptr;
    }
    UpdateChunkGroup(curr);
    current_chunk_ = nullptr;
  }

  for (auto &chunk_group : chunks_grouped_by_free_entries_) {
    while (!chunk_group.Empty()) {
      auto chunk = chunk_group.Front();
      char *ptr = TryAllocateFromChunk(chunk);
      if (ptr) {
        if (chunk->allocated_count < chunk->entries_in_chunk) {
          current_chunk_ = chunk;
        } else {
          UpdateChunkGroup(chunk);
          current_chunk_ = nullptr;
        }
        active_allocations_++;
        IncrementRef();
        return ptr;
      }
      UpdateChunkGroup(chunk);
    }
  }

  AllocateChunk();
  curr = current_chunk_;
  char *ptr = TryAllocateFromChunk(curr);
  CHECK_NE(ptr, nullptr);
  if (curr->allocated_count >= curr->entries_in_chunk) {
    UpdateChunkGroup(curr);
    current_chunk_ = nullptr;
  }
  active_allocations_++;
  IncrementRef();
  return ptr;
}

char *FixedSizeAllocator::Allocate(size_t size) {
  CHECK_LE(size, ChunkSize());
  return Allocate();
}

char *FixedSizeAllocator::Allocate(size_t size, size_t alignment) {
  CHECK_LE(size, ChunkSize());
  CHECK_LE(alignment, Alignment());
  return Allocate();
}

void FixedSizeAllocator::ProcessLocalFree(AllocatorChunk *chunk, char *ptr) {
  size_t offset = ptr - chunk->data;
  size_t entry_idx = offset / chunk->entry_size;
  size_t word_index = entry_idx / kEntriesPerBitmapWord;
  size_t bit_index = entry_idx % kEntriesPerBitmapWord;

  uint64_t mask = ~(1ULL << bit_index);
  chunk->bitmap[word_index] &= mask;
  
  if (word_index < chunk->scan_hint) {
      chunk->scan_hint = word_index;
  }

  uint32_t prev_allocations = chunk->allocated_count--;
  active_allocations_--;

  uint32_t curr_allocations = prev_allocations - 1;

  int prev_group = CalcChunkFreeGroup(chunk->entries_in_chunk - prev_allocations);
  int curr_group = CalcChunkFreeGroup(chunk->entries_in_chunk - curr_allocations);

  if (prev_group != curr_group || curr_allocations == 0) {
    if (chunk->current_group == -2) {
      DecrementRef();
      return;
    }
    if (curr_allocations == 0) {
      if (chunk->current_group >= 0) {
        chunks_grouped_by_free_entries_[chunk->current_group].Remove(chunk);
      } else if (chunk->current_group == -1) {
        fully_used_chunks_.Remove(chunk);
      }
      if (current_chunk_ == chunk) {
        current_chunk_ = nullptr;
      }
      chunk->current_group = -2;
      chunk->retired = true;
      chunk_tracker.Untrack(chunk);
      chunk->Release();
    } else {
      UpdateChunkGroup(chunk);
    }
  }
  DecrementRef();
}

void FixedSizeAllocator::Free(AllocatorChunk *chunk, char *ptr) {
  if (std::this_thread::get_id() == owner_thread_id_) {
    ProcessLocalFree(chunk, ptr);
  } else {
    char* old_head = remote_frees_.load(std::memory_order_relaxed);
    do {
      *reinterpret_cast<char**>(ptr) = old_head;
    } while (!remote_frees_.compare_exchange_weak(old_head, ptr, std::memory_order_release, std::memory_order_relaxed));
  }
}

void FixedSizeAllocator::UpdateChunkGroup(AllocatorChunk *chunk) {
  if (chunk->current_group == -1) {
    fully_used_chunks_.Remove(chunk);
  } else if (chunk->current_group >= 0) {
    chunks_grouped_by_free_entries_[chunk->current_group].Remove(chunk);
  }

  size_t free_cnt = chunk->entries_in_chunk - chunk->allocated_count;
  if (free_cnt == 0) {
    fully_used_chunks_.PushBack(chunk);
    chunk->current_group = -1;
    if (current_chunk_ == chunk) {
      current_chunk_ = nullptr;
    }
  } else {
    int new_group = CalcChunkFreeGroup(free_cnt);
    chunks_grouped_by_free_entries_[new_group].PushBack(chunk);
    chunk->current_group = new_group;
  }
}

void FixedSizeAllocator::AllocateChunk() {
  auto new_chunk = new AllocatorChunk(this, size_, alignment_);
  size_t free_group = CalcChunkFreeGroup(new_chunk->entries_in_chunk);
  chunks_grouped_by_free_entries_[free_group].PushBack(new_chunk);
  new_chunk->current_group = free_group;
  current_chunk_ = new_chunk;
}

bool Allocator::Free(char *ptr) {
  auto chunk = chunk_tracker.FindAndRetainChunk(ptr);
  if (!chunk) {
    return false;
  }
  chunk->allocator->Free(chunk, ptr);
  chunk->Release();
  return true;
}

size_t Allocator::GetAllocatedSize(char *ptr) {
  auto chunk = chunk_tracker.FindAndRetainChunk(ptr);
  if (!chunk) {
    return 0;
  }
  size_t entry_size = chunk->entry_size;
  chunk->Release();
  return entry_size;
}

SegregatedFixedSizeAllocator::SegregatedFixedSizeAllocator(
    size_t default_alignment) {
  allocators_.reserve(kNumClasses);
  tcaches_.resize(kNumClasses);
  for (unsigned long kSizeClasse : kSizeClasses) {
    size_t alignment = (kSizeClasse >= 128) ? 32 : default_alignment;
    allocators_.push_back(
        CREATE_UNIQUE_PTR(FixedSizeAllocator, kSizeClasse, alignment));
  }
}

namespace {

uint8_t g_size_to_class_index[SegregatedFixedSizeAllocator::kMaxSize + 1];

struct SizeClassLUTInitializer {
  SizeClassLUTInitializer() {
    size_t class_idx = 0;
    for (size_t s = 0; s <= SegregatedFixedSizeAllocator::kMaxSize; ++s) {
      while (class_idx < SegregatedFixedSizeAllocator::kNumClasses &&
             s > SegregatedFixedSizeAllocator::kSizeClasses[class_idx]) {
        ++class_idx;
      }
      g_size_to_class_index[s] = static_cast<uint8_t>(class_idx);
    }
  }
} g_size_class_lut_initializer;

}  // namespace

size_t SegregatedFixedSizeAllocator::GetSizeClassIndex(size_t size) {
  if (size <= kMaxSize) {
    return g_size_to_class_index[size];
  }
  return kNumClasses;
}

char *SegregatedFixedSizeAllocator::Allocate(size_t size) {
  return Allocate(size, 8);
}

char *SegregatedFixedSizeAllocator::Allocate(size_t size, size_t alignment) {
  if (size == 0) {
    return nullptr;
  }
  size_t aligned_size = AlignUp(size, alignment);
  size_t idx = GetSizeClassIndex(aligned_size);
  if (idx < kNumClasses) {
    if (tcaches_[idx].count > 0) {
      return tcaches_[idx].ptrs[--tcaches_[idx].count];
    }
    return allocators_[idx]->Allocate();
  }
  return static_cast<char *>(__wrap_malloc(aligned_size));
}

bool SegregatedFixedSizeAllocator::Free(char *ptr) {
  if (!ptr) {
    return true;
  }
  size_t size = UsableSize(ptr);
  if (size > 0 && size <= kMaxSize) {
    size_t idx = GetSizeClassIndex(size);
    if (idx < kNumClasses) {
      TCache& cache = GetThreadLocalSegregatedAllocator().tcaches_[idx];
      if (cache.count == kMaxCachedPerClass) {
        size_t flush_count = kMaxCachedPerClass / 2;
        for (size_t i = 0; i < flush_count; ++i) {
          Allocator::Free(cache.ptrs[--cache.count]);
        }
      }
      cache.ptrs[cache.count++] = ptr;
      return true;
    }
  }
  
  if (Allocator::Free(ptr)) {
    return true;
  }
  __wrap_free(ptr);
  return true;
}

char *SegregatedFixedSizeAllocator::Reallocate(char *ptr, size_t new_size) {
  if (!ptr) {
    return GetThreadLocalSegregatedAllocator().Allocate(new_size);
  }
  if (new_size == 0) {
    Free(ptr);
    return nullptr;
  }
  size_t old_size = UsableSize(ptr);
  if (old_size >= new_size) {
    return ptr;
  }
  char *new_ptr = GetThreadLocalSegregatedAllocator().Allocate(new_size);
  if (!new_ptr) {
    return nullptr;
  }
  if (old_size > 0) {
    std::memcpy(new_ptr, ptr, std::min(old_size, new_size));
  }
  Free(ptr);
  return new_ptr;
}

size_t SegregatedFixedSizeAllocator::UsableSize(char *ptr) {
  if (!ptr) {
    return 0;
  }
  size_t size = Allocator::GetAllocatedSize(ptr);
  if (size > 0) {
    return size;
  }
  return static_cast<size_t>(__wrap_malloc_usable_size(ptr));
}

void SegregatedFixedSizeAllocator::Free(AllocatorChunk *chunk, char *ptr) {
  chunk->allocator->Free(chunk, ptr);
}

SegregatedFixedSizeAllocator &GetThreadLocalSegregatedAllocator() {
  static thread_local SegregatedFixedSizeAllocator *thread_allocator =
      new SegregatedFixedSizeAllocator();
  return *thread_allocator;
}

SegregatedFixedSizeAllocator &GetDefaultSegregatedAllocator() {
  return GetThreadLocalSegregatedAllocator();
}

}  // namespace valkey_search
"""

with open('src/utils/allocator.cc', 'w') as f:
    f.writelines(new_prefix)
    f.write(new_code)
