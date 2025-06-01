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

#include <cstddef>

/**
 * MemoryStats tracks memory allocation and deallocation statistics.
 * 
 * THREAD SAFETY: This class is NOT thread-safe. If concurrent access is required,
 * the caller must provide external synchronization (e.g., using a mutex).
 */
class MemoryStats {
public:
    MemoryStats() = default;

    MemoryStats(const MemoryStats&) = delete;
    MemoryStats& operator=(const MemoryStats&) = delete;
    MemoryStats(MemoryStats&&) = delete;
    MemoryStats& operator=(MemoryStats&&) = delete;

    void RecordAllocation(size_t size) {
        allocated_bytes_ += size;
    }

    void RecordDeallocation(size_t size) {
        if (size_t(allocated_bytes_) <= size) {
            allocated_bytes_ = 0;
        } else {
            allocated_bytes_ -= size;
        }
    }

    long long GetAllocatedBytes() const {
        return allocated_bytes_;
    }

private:
    long long allocated_bytes_{};
};

#endif // VMSDK_SRC_MEMORY_STATS_H_ 
