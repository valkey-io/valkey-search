#include "memory_tracker.h"
#include "memory_stats.h"

thread_local MemoryTrackingScope* MemoryTrackingScope::current_scope_tls_ = nullptr;

thread_local MemoryTrackingScope::ScopeEventCallback MemoryTrackingScope::scope_event_callback_ = nullptr;

MemoryTrackingScope::MemoryTrackingScope(MemoryStats* index_stats)
    : target_stats_(index_stats), previous_scope_(current_scope_tls_) {
    current_scope_tls_ = this;
    NotifyScopeEvent();
}

MemoryTrackingScope::~MemoryTrackingScope() {
    current_scope_tls_ = previous_scope_;
}

MemoryTrackingScope* MemoryTrackingScope::GetCurrentScope() {
    return current_scope_tls_;
}

MemoryStats* MemoryTrackingScope::GetStats() const {
    return target_stats_;
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
