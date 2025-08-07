/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 */

#ifndef VALKEYSEARCH_SRC_INDEXES_METRIC_TYPES_H
#define VALKEYSEARCH_SRC_INDEXES_METRIC_TYPES_H

namespace valkey_search::indexes {

#define METRIC_TYPES_TABLE \
  METRIC_ENTRY(kNone, "") \
  METRIC_ENTRY(kVectorsMemory, "vectors_memory") \
  METRIC_ENTRY(kVectorsMemoryMarkedDeleted, "vectors_memory_marked_deleted") \
  METRIC_ENTRY(kVectorsMarkedDeleted, "vectors_marked_deleted") \
  METRIC_ENTRY(kHnswNodes, "hnsw_nodes") \
  METRIC_ENTRY(kHnswNodesMarkedDeleted, "hnsw_nodes_marked_deleted") \
  METRIC_ENTRY(kHnswEdges, "hnsw_edges") \
  METRIC_ENTRY(kHnswEdgesMarkedDeleted, "hnsw_edges_marked_deleted") \
  METRIC_ENTRY(kFlatNodes, "flat_nodes") \
  METRIC_ENTRY(kTags, "tags") \
  METRIC_ENTRY(kTagsMemory, "tags_memory") \
  METRIC_ENTRY(kNumericRecords, "numeric_records") \
  METRIC_ENTRY(kInternedStrings, "interned_strings") \
  METRIC_ENTRY(kInternedStringsMemory, "interned_strings_memory") \
  METRIC_ENTRY(kKeysMemory, "keys_memory")

// Generate the enum from the table above
enum class MetricType {
#define METRIC_ENTRY(name, str) name,
  METRIC_TYPES_TABLE
#undef METRIC_ENTRY
  
  // Sentinel value for array bounds checking
  kMetricTypeCount
};

}  // namespace valkey_search::indexes

#endif  // VALKEYSEARCH_SRC_INDEXES_METRIC_TYPES_H
