 /*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_GLOBAL_METRICS_H
#define VALKEYSEARCH_SRC_INDEXES_GLOBAL_METRICS_H

#include <atomic>
#include <cassert>
#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "src/utils/string_interning.h"
#include "src/indexes/metric_types.h"
#include "vmsdk/src/info.h"

namespace valkey_search::indexes {

// Packed metadata structure for InternedString (32 bits total)
struct MetaData {
  uint32_t metric_type : 7;          // 7 bits for metric type
  uint32_t use_count : 16;           // 16 bits for use count
  uint32_t ever_used : 1;            // 1 bit to track if ever been used
  uint32_t reserved : 8;             // 8 bits reserved for future use
};

struct MetricData {
  std::atomic<uint64_t> count{0};           
};

const absl::flat_hash_map<MetricType, absl::string_view> kMetricTypeToStr = {
  {MetricType::kVectorsMemory, "vectors_memory"},
  {MetricType::kVectorsMemoryMarkedDeleted, "vectors_memory_marked_deleted"},
  {MetricType::kHnswNodes, "hnsw_nodes"},
  {MetricType::kHnswNodesMarkedDeleted, "hnsw_nodes_marked_deleted"},
  {MetricType::kHnswEdges, "hnsw_edges"},
  {MetricType::kHnswEdgesMarkedDeleted, "hnsw_edges_marked_deleted"},
  {MetricType::kFlatNodes, "flat_nodes"},
  {MetricType::kTags, "tags"},
  {MetricType::kTagsMemory, "tags_memory"},
  {MetricType::kNumericRecords, "numeric_records"},
  {MetricType::kInternedStrings, "interned_strings"},
  {MetricType::kInternedStringsMarkedDeleted, "interned_strings_marked_deleted"},
  {MetricType::kInternedStringsMemory, "interned_strings_memory"},
  {MetricType::kKeysMemory, "keys_memory"}
};

class GlobalIndexStats {
 public:
  static GlobalIndexStats& Instance() {
    static GlobalIndexStats instance;
    return instance;
  }
  
  void Incr(MetricType metric_type, uint64_t value = 1) {
    GetOrCreateMetric(metric_type).count.fetch_add(value, std::memory_order_relaxed);
  }
  
  void Decr(MetricType metric_type, uint64_t value = 1) {
    GetOrCreateMetric(metric_type).count.fetch_sub(value, std::memory_order_relaxed);
  }
  
  uint64_t GetCount(MetricType metric_type) const {
    auto it = metrics_.find(metric_type);
    return it != metrics_.end() ? it->second->count.load(std::memory_order_relaxed) : 0;
  }
  
  absl::flat_hash_map<MetricType, uint64_t> GetAllMetrics() const {
    absl::flat_hash_map<MetricType, uint64_t> result;
    for (const auto& [type, metric] : metrics_) {
      result[type] = metric->count.load(std::memory_order_relaxed);
    }
    return result;
  }
  
 private:
  GlobalIndexStats() = default;
  
  MetricData& GetOrCreateMetric(MetricType metric_type) {
    auto [it, inserted] = metrics_.try_emplace(metric_type, nullptr);
    if (inserted) {
      it->second = std::make_unique<MetricData>();
    }
    return *it->second;
  }

  absl::flat_hash_map<MetricType, std::unique_ptr<MetricData>> metrics_;
};

inline void OnInternedStringAlloc(::valkey_search::InternedString* interned_str, MetricType metric_type) {
  if (!interned_str) return;
  size_t bytes = interned_str->Str().length();
  GlobalIndexStats::Instance().Incr(MetricType::kInternedStringsMemory, bytes);
  GlobalIndexStats::Instance().Incr(MetricType::kInternedStrings, 1);

  if (metric_type == MetricType::kNone) return;

  MetaData metadata{};
  metadata.metric_type = static_cast<uint8_t>(metric_type);
  metadata.use_count = 0;             // Start with 0
  metadata.ever_used = 0;             // Not used yet
  metadata.reserved = 0;
  interned_str->SetMetadataFlags(*reinterpret_cast<uint32_t*>(&metadata));

  GlobalIndexStats::Instance().Incr(metric_type, bytes);
}

inline void OnInternedStringDealloc(::valkey_search::InternedString* interned_str) {
  size_t bytes = interned_str->Str().length();
  GlobalIndexStats::Instance().Decr(MetricType::kInternedStringsMemory, bytes);
  GlobalIndexStats::Instance().Decr(MetricType::kInternedStrings, 1);
  uint32_t metadata_flags = interned_str->GetMetadataFlags();
  if (metadata_flags == 0) return;

  MetaData* metadata = reinterpret_cast<MetaData*>(&metadata_flags);
  assert(metadata->use_count == 0);  // Assert use_count is 0 when deallocating
  MetricType type = static_cast<MetricType>(metadata->metric_type);
  if (type != MetricType::kNone) {
    GlobalIndexStats::Instance().Decr(type, bytes);
  }
}

inline bool OnInternedStringMarkUnused(const std::shared_ptr<::valkey_search::InternedString>& interned_str) {
  if (!interned_str) return false;
  
  uint32_t* flags_ptr = interned_str->GetMetadataFlagsPtr();
  MetaData* metadata = reinterpret_cast<MetaData*>(flags_ptr);
  assert(metadata->use_count > 0);
  metadata->use_count--;

  if (metadata->use_count == 0 && metadata->ever_used) {
    // Only mark as deleted if it was actually used and now back to 0
    MetricType type = static_cast<MetricType>(metadata->metric_type);
    // Only intern strings that may be marked as deleted are kVectorsMemory 
    assert(type == MetricType::kVectorsMemory);
    GlobalIndexStats::Instance().Incr(MetricType::kVectorsMemoryMarkedDeleted, interned_str->Str().length());
    GlobalIndexStats::Instance().Incr(MetricType::kInternedStringsMarkedDeleted, 1);
  }

  return true;
}

inline bool OnInternedStringIncrUsed(const std::shared_ptr<::valkey_search::InternedString>& interned_str) {
  if (!interned_str) return false;
  
  uint32_t* flags_ptr = interned_str->GetMetadataFlagsPtr();
  if (*flags_ptr == 0) return false;
  
  MetaData* metadata = reinterpret_cast<MetaData*>(flags_ptr);
  
  if (metadata->use_count == 0 && metadata->ever_used) {
    // This was marked as deleted, now used again
    MetricType type = static_cast<MetricType>(metadata->metric_type);
    assert(type == MetricType::kVectorsMemory);
    GlobalIndexStats::Instance().Decr(MetricType::kVectorsMemoryMarkedDeleted, interned_str->Str().length());
    GlobalIndexStats::Instance().Decr(MetricType::kInternedStringsMarkedDeleted, 1);
  }

  // Mark as ever used on first increment
  if (!metadata->ever_used) {
    metadata->ever_used = 1;
  }

  metadata->use_count++;

  return true;
}

template<typename InfoFieldInteger>
inline void CreateGlobalMetricsInfoFields() {
  static auto global_metrics_fields = []() {
    std::vector<std::unique_ptr<InfoFieldInteger>> fields;
    
    for (const auto& [metric_type, metric_name] : kMetricTypeToStr) {
      auto count_field = std::make_unique<InfoFieldInteger>(
          "global_metrics", std::string(metric_name),
          vmsdk::info_field::IntegerBuilder()
              .App()
              .Computed([metric_type]() -> long long {
                return static_cast<long long>(GlobalIndexStats::Instance().GetCount(metric_type));
              })
              .CrashSafe());
      
      fields.push_back(std::move(count_field));
    }
    
    return fields;
  }();
}

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_GLOBAL_METRICS_H
