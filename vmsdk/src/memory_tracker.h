#ifndef VMSDK_SRC_MEMORY_TRACKER_H_
#define VMSDK_SRC_MEMORY_TRACKER_H_

#include <functional>

class MemoryStats;

class MemoryTrackingScope {
public:
    // Test hook function type for scope lifecycle events
    using ScopeEventCallback = std::function<void(const MemoryTrackingScope*)>;
    
    explicit MemoryTrackingScope(MemoryStats* index_stats);
    ~MemoryTrackingScope();

    MemoryTrackingScope(const MemoryTrackingScope&) = delete;
    MemoryTrackingScope& operator=(const MemoryTrackingScope&) = delete;
    MemoryTrackingScope(MemoryTrackingScope&&) = delete;
    MemoryTrackingScope& operator=(MemoryTrackingScope&&) = delete;

    static MemoryTrackingScope* GetCurrentScope();
    MemoryStats* GetStats() const;
    
    // Used for testing
    static void SetScopeEventCallback(ScopeEventCallback callback);
    static void ClearScopeEventCallback();

private:
    MemoryStats* target_stats_;
    MemoryTrackingScope* previous_scope_;

    static thread_local MemoryTrackingScope* current_scope_tls_;
    
    static thread_local ScopeEventCallback scope_event_callback_;
    void NotifyScopeEvent();
};

#endif // VMSDK_SRC_MEMORY_TRACKER_H_ 
