import re

with open('src/utils/allocator.h', 'r') as f:
    content = f.read()

content = content.replace('#include <memory>\n#include <vector>', '#include <memory>\n#include <thread>\n#include <vector>')
content = content.replace('std::unique_ptr<std::atomic<uint64_t>[]> bitmap;', 'std::unique_ptr<uint64_t[]> bitmap;')
content = content.replace('std::atomic<size_t> scan_hint{0};', 'size_t scan_hint{0};')
content = content.replace('std::atomic<uint32_t> allocated_count{0};', 'uint32_t allocated_count{0};')
content = content.replace('std::atomic<bool> retired{false};', 'bool retired{false};')

content = content.replace('return active_allocations_.load(std::memory_order_relaxed);', 'return active_allocations_;')
content = content.replace('size_t ChunkCount() const ABSL_LOCKS_EXCLUDED(mutex_);', 'size_t ChunkCount() const;')

# Replace private members in FixedSizeAllocator
private_regex = r'private:.*?IntrusiveList<AllocatorChunk> chunks_grouped_by_free_entries_.*?void AllocateChunk\(\) ABSL_EXCLUSIVE_LOCKS_REQUIRED\(mutex_\);'
replacement = '''private:
  IntrusiveList<AllocatorChunk> chunks_grouped_by_free_entries_[kFreeEntriesPerChunkGroupSize];
  size_t size_;
  size_t alignment_{8};
  IntrusiveList<AllocatorChunk> fully_used_chunks_;
  AllocatorChunk *current_chunk_{nullptr};
  size_t active_allocations_{0};
  std::atomic<char*> remote_frees_{nullptr};
  std::thread::id owner_thread_id_;

  void UpdateChunkGroup(AllocatorChunk *chunk);
  void AllocateChunk();
  void SweepRemoteFrees();
  void ProcessLocalFree(AllocatorChunk *chunk, char *ptr);'''

content = re.sub(private_regex, replacement, content, flags=re.DOTALL)

# Add TCache to SegregatedFixedSizeAllocator
sg_private_regex = r'private:\n  static size_t GetSizeClassIndex\(size_t size\);\n  std::vector<UniqueFixedSizeAllocatorPtr> allocators_;'
sg_replacement = '''private:
  static size_t GetSizeClassIndex(size_t size);
  std::vector<UniqueFixedSizeAllocatorPtr> allocators_;

  static constexpr size_t kMaxCachedPerClass = 256;
  struct TCache {
    size_t count = 0;
    char* ptrs[kMaxCachedPerClass];
  };
  std::vector<TCache> tcaches_;'''

content = re.sub(sg_private_regex, sg_replacement, content)

with open('src/utils/allocator.h', 'w') as f:
    f.write(content)
