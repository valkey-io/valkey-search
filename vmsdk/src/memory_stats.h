#ifndef VMSDK_SRC_MEMORY_STATS_H_
#define VMSDK_SRC_MEMORY_STATS_H_

#include <atomic>
#include <cstddef>

class MemoryStats {
public:
    MemoryStats() : allocated_bytes_(0) {}

    MemoryStats(const MemoryStats&) = delete;
    MemoryStats& operator=(const MemoryStats&) = delete;
    MemoryStats(MemoryStats&&) = delete;
    MemoryStats& operator=(MemoryStats&&) = delete;

    void RecordAllocation(size_t size) {
        allocated_bytes_.fetch_add(size, std::memory_order_relaxed);
    }

    void RecordDeallocation(size_t size) {
        long long current_val = allocated_bytes_.load(std::memory_order_relaxed);
        if (size_t(current_val) <= size) {
            allocated_bytes_.store(0, std::memory_order_relaxed);
        } else {
            allocated_bytes_.fetch_sub(size, std::memory_order_relaxed);
        }
    }

    long long GetAllocatedBytes() const {
        return allocated_bytes_.load(std::memory_order_relaxed);
    }

private:
    std::atomic<long long> allocated_bytes_;
};

#endif // VMSDK_SRC_MEMORY_STATS_H_ 