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

#include "absl/strings/string_view.h"
#include "src/utils/string_interning.h"
#include "src/indexes/metric_types.h"
#include "vmsdk/src/info.h"

namespace valkey_search::indexes {

struct MetricData {
  std::atomic<uint64_t> count{0};           
};

constexpr absl::string_view kMetricTypeStrings[] = {
#define METRIC_ENTRY(name, str) str,
  METRIC_TYPES_TABLE
#undef METRIC_ENTRY
};

inline absl::string_view GetMetricTypeString(MetricType metric_type) {
  const auto index = static_cast<size_t>(metric_type);
  if (index >= static_cast<size_t>(MetricType::kMetricTypeCount)) {
    return "";  // Invalid metric type
  }
  return kMetricTypeStrings[index];
}

class GlobalIndexStats {
 public:
  static GlobalIndexStats& Instance() {
    static GlobalIndexStats instance;
    return instance;
  }
  
  void Incr(MetricType metric_type, uint64_t value = 1) {
    GetMetric(metric_type).count.fetch_add(value, std::memory_order_relaxed);
  }
  
  void Decr(MetricType metric_type, uint64_t value = 1) {
    GetMetric(metric_type).count.fetch_sub(value, std::memory_order_relaxed);
  }
  
  uint64_t GetCount(MetricType metric_type) const {
    auto& store = ::valkey_search::StringInternStore::Instance();
    uint64_t total = 0;
    if (metric_type == MetricType::kInternedStrings) {
      for (size_t j = 0; j < static_cast<size_t>(::valkey_search::StringType::kStringTypeCount); ++j) {
        total +=  store.GetCounters(static_cast<::valkey_search::StringType>(j)).object_count;
      }
      return total;
    } else if (metric_type == MetricType::kInternedStringsMemory) {
      for (size_t j = 0; j < static_cast<size_t>(::valkey_search::StringType::kStringTypeCount); ++j) {
        total += store.GetCounters(static_cast<::valkey_search::StringType>(j)).memory_bytes;
      }
      return total;
    } else if (metric_type == MetricType::kVectorsMemory) {
      return store.GetCounters(::valkey_search::StringType::VECTOR).memory_bytes;
    } else if (metric_type == MetricType::kVectorsMemoryMarkedDeleted) {
      return store.GetMarkedDeletedCounters().memory_bytes;
    } else if (metric_type == MetricType::kVectorsMarkedDeleted) {
      return store.GetMarkedDeletedCounters().object_count;
    } else if (metric_type == MetricType::kTagsMemory) {
      return store.GetCounters(::valkey_search::StringType::TAG).memory_bytes;
    } else if (metric_type == MetricType::kKeysMemory) {
      return store.GetCounters(::valkey_search::StringType::KEY).memory_bytes;
    } else {
      const size_t index = static_cast<size_t>(metric_type);
      if (index >= static_cast<size_t>(MetricType::kMetricTypeCount)) {
        return 0;  // Invalid metric type
      }
      return metrics_[index].count.load(std::memory_order_relaxed);
    }
  }

 private:
  GlobalIndexStats() = default;
  
  MetricData& GetMetric(MetricType metric_type) {
    const size_t index = static_cast<size_t>(metric_type);
    assert(index < static_cast<size_t>(MetricType::kMetricTypeCount));
    return metrics_[index];
  }

  MetricData metrics_[static_cast<size_t>(MetricType::kMetricTypeCount)];
};

template<typename InfoFieldInteger>
inline void CreateGlobalMetricsInfoFields() {
  static auto global_metrics_fields = []() {
    std::vector<std::unique_ptr<InfoFieldInteger>> fields;

    for (size_t i = 0; i < static_cast<size_t>(MetricType::kMetricTypeCount); ++i) {
      const auto metric_type = static_cast<MetricType>(i);
      const absl::string_view metric_name = GetMetricTypeString(metric_type);
      if (metric_name.empty()) continue;
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
