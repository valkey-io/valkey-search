#ifndef VMSDK_SRC_MEMORY_TRACKER_H_
#define VMSDK_SRC_MEMORY_TRACKER_H_

class MemoryStats;

class MemoryTrackingScope {
public:
    explicit MemoryTrackingScope(MemoryStats* index_stats);
    ~MemoryTrackingScope();

    MemoryTrackingScope(const MemoryTrackingScope&) = delete;
    MemoryTrackingScope& operator=(const MemoryTrackingScope&) = delete;
    MemoryTrackingScope(MemoryTrackingScope&&) = delete;
    MemoryTrackingScope& operator=(MemoryTrackingScope&&) = delete;

    static MemoryTrackingScope* GetCurrentScope();
    MemoryStats* GetStats() const;

private:
    MemoryStats* target_stats_;
    MemoryTrackingScope* previous_scope_;

    static thread_local MemoryTrackingScope* current_scope_tls_;
};

#endif // VMSDK_SRC_MEMORY_TRACKER_H_ 