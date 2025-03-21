/*
 * Copyright (c) 2025, ValkeySearch contributors
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
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "vmsdk/src/memory_allocation.h"

#include <unistd.h>

#include <atomic>
#include <cstdint>

namespace vmsdk {

// Use the standard system allocator by default. Note that this is required
// since any allocation done before Redis module initialization (namely global
// static constructors that do heap allocation, which are run on dl_open) cannot
// invoke Redis modules api since the associated C function pointers are only
// initialized as part of the module initialization process. Refer
// https://redis.com/blog/using-the-redis-allocator-in-rust for more details.
//
// We use a combination of a thread local static variable and a global atomic
// variable to perform the switch to the new allocator. The global is only
// accessed during the initial loading phase, and once we switch allocators the
// thread local variable is exclusively used. This should guarantee that the
// switch is done atomically while not having performance impact during steady
// state.
thread_local static bool thread_using_valkey_module_alloc = false;
static std::atomic<bool> use_valkey_module_alloc_switch = false;

bool IsUsingValkeyAlloc() {
  if (!thread_using_valkey_module_alloc &&
      use_valkey_module_alloc_switch.load(std::memory_order_relaxed)) {
    thread_using_valkey_module_alloc = true;
    return true;
  }
  return thread_using_valkey_module_alloc;
}
void UseValkeyAlloc() {
  use_valkey_module_alloc_switch.store(true, std::memory_order_relaxed);
}
std::atomic<uint64_t> used_memory_bytes{0};

void ResetValkeyAlloc() {
  use_valkey_module_alloc_switch.store(false, std::memory_order_relaxed);
  thread_using_valkey_module_alloc = false;
  used_memory_bytes.store(0, std::memory_order_relaxed);
}

uint64_t GetUsedMemoryCnt() { return used_memory_bytes; }

void ReportAllocMemorySize(uint64_t size) {
  vmsdk::used_memory_bytes.fetch_add(size, std::memory_order_relaxed);
}
void ReportFreeMemorySize(uint64_t size) {
  if (size > used_memory_bytes) {
    vmsdk::used_memory_bytes.store(0, std::memory_order_relaxed);
  } else {
    vmsdk::used_memory_bytes.fetch_sub(size, std::memory_order_relaxed);
  }
}

}  // namespace vmsdk
