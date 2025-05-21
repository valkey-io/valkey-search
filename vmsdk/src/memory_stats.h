/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

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
