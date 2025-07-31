/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_METRIC_TYPES_H
#define VALKEYSEARCH_SRC_INDEXES_METRIC_TYPES_H

namespace valkey_search::indexes {

enum class MetricType {
  kNone,  // Indicates no metric type set
  kVectorsMemory,
  kVectorsMemoryMarkedDeleted,
  
  kHnswNodes,
  kHnswNodesMarkedDeleted,
  kHnswEdges,
  kHnswEdgesMarkedDeleted,
  
  kFlatNodes,
  
  kTags,
  kTagsMemory,
  
  kNumericRecords,
  
  kInternedStrings,
  kInternedStringsMarkedDeleted,
  kInternedStringsMemory,
  
  kKeysMemory,
  
  kMetricTypeCount
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_METRIC_TYPES_H
