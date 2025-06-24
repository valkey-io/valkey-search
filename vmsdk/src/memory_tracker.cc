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

#include "memory_tracker.h"
#include "vmsdk/src/memory_allocation.h"

thread_local MemoryTrackingScope* MemoryTrackingScope::current_scope_tls_ = nullptr;

thread_local MemoryTrackingScope::ScopeEventCallback MemoryTrackingScope::scope_event_callback_ = nullptr;

MemoryTrackingScope::MemoryTrackingScope(std::atomic<int64_t>* pool)
    : target_pool_(pool), memory_delta_(vmsdk::GetMemoryDelta()) {
    prev_scope_ = current_scope_tls_;
    current_scope_tls_ = this;
    NotifyScopeEvent();
}

MemoryTrackingScope::~MemoryTrackingScope() {
    if (target_pool_ != nullptr) {
        int64_t current_delta = vmsdk::GetMemoryDelta();
        int64_t net_change = current_delta - memory_delta_;
        target_pool_->fetch_add(net_change);
        if (prev_scope_ != nullptr) {
            prev_scope_->memory_delta_ += net_change;
        }
    }
    current_scope_tls_ = prev_scope_;
}

MemoryTrackingScope* MemoryTrackingScope::GetCurrentScope() {
    return current_scope_tls_;
}

void MemoryTrackingScope::NotifyScopeEvent() {
    if (scope_event_callback_) {
        scope_event_callback_(this);
    }
}

void MemoryTrackingScope::SetScopeEventCallback(ScopeEventCallback callback) {
    scope_event_callback_ = callback;
}

void MemoryTrackingScope::ClearScopeEventCallback() {
    scope_event_callback_ = nullptr;
}
