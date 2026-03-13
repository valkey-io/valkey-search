---
title: "Valkey Search - Application Metrics"
description: Valkey Search Module Application Metrics
---

# Application Metrics

## App Metrics Reference

The complete list of Application-tier metrics. All fields are prefixed with `search_` in the `INFO SEARCH` output.

| Metric | Unit | Description |
| :--- | :---: | :--- |
| `coordinator_bytes_in` | Bytes | Total bytes received by the coordinator from remote nodes. |
| `coordinator_bytes_out` | Bytes | Total bytes sent by the coordinator to remote nodes. |
| `coordinator_client_get_global_metadata_failure_count` | Count | Failed outgoing GetGlobalMetadata RPCs. |
| `coordinator_client_get_global_metadata_failure_latency_usec` | Microseconds | Latency distribution for failed outgoing GetGlobalMetadata RPCs. |
| `coordinator_client_get_global_metadata_success_count` | Count | Successful outgoing GetGlobalMetadata RPCs. |
| `coordinator_client_get_global_metadata_success_latency_usec` | Microseconds | Latency distribution for successful outgoing GetGlobalMetadata RPCs. |
| `coordinator_client_search_index_partition_failure_count` | Count | Failed outgoing SearchIndexPartition RPCs. |
| `coordinator_client_search_index_partition_failure_latency_usec` | Microseconds | Latency distribution for failed outgoing SearchIndexPartition RPCs. |
| `coordinator_client_search_index_partition_success_count` | Count | Successful outgoing SearchIndexPartition RPCs. |
| `coordinator_client_search_index_partition_success_latency_usec` | Microseconds | Latency distribution for successful outgoing SearchIndexPartition RPCs. |
| `coordinator_server_get_global_metadata_failure_count` | Count | Failed server-side GetGlobalMetadata RPCs. |
| `coordinator_server_get_global_metadata_failure_latency_usec` | Microseconds | Latency distribution for failed server-side GetGlobalMetadata RPCs. |
| `coordinator_server_get_global_metadata_success_count` | Count | Successful server-side GetGlobalMetadata RPCs. |
| `coordinator_server_get_global_metadata_success_latency_usec` | Microseconds | Latency distribution for successful server-side GetGlobalMetadata RPCs. |
| `coordinator_server_listening_port` | Port | gRPC port the coordinator server is listening on. |
| `coordinator_server_search_index_partition_failure_count` | Count | Failed server-side SearchIndexPartition RPCs. |
| `coordinator_server_search_index_partition_failure_latency_usec` | Microseconds | Latency distribution for failed server-side SearchIndexPartition RPCs. |
| `coordinator_server_search_index_partition_success_count` | Count | Successful server-side SearchIndexPartition RPCs. |
| `coordinator_server_search_index_partition_success_latency_usec` | Microseconds | Latency distribution for successful server-side SearchIndexPartition RPCs. |
| `coordinator_threads_cpu_time_sec` | Seconds | Cumulative CPU time of coordinator (gRPC) threads. |
| `hnsw_add_exceptions_count` | Count | Exceptions during HNSW vector add operations. |
| `hnsw_create_exceptions_count` | Count | Exceptions during HNSW index creation. |
| `hnsw_modify_exceptions_count` | Count | Exceptions during HNSW vector modify operations. |
| `hnsw_remove_exceptions_count` | Count | Exceptions during HNSW vector remove operations. |
| `hnsw_search_exceptions_count` | Count | Exceptions during HNSW vector search operations. |
| `number_of_active_indexes` | Count | Number of active (non-dropped) indexes across all databases. |
| `number_of_active_indexes_indexing` | Count | Number of active indexes currently performing background indexing. |
| `number_of_active_indexes_running_queries` | Count | Number of active indexes currently running queries. |
| `number_of_attributes` | Count | Total field attributes across all indexes. |
| `number_of_indexes` | Count | Total number of index schemas created. |
| `total_active_write_threads` | Count | Number of active writer threads (`0` if suspended). |
| `total_indexed_documents` | Count | Total documents indexed across all indexes. |
| `total_indexing_time` | Milliseconds | Cumulative time spent on indexing operations. |
| `background_indexing_status` | String | `IN_PROGRESS` or `NO_ACTIVITY`. |
| `flat_vector_index_search_latency_usec` | Microseconds | Latency distribution for FLAT vector index searches. |
| `hnsw_vector_index_search_latency_usec` | Microseconds | Latency distribution for HNSW vector index searches. |
| `index_reclaimable_memory` | Bytes | Memory freed from indexes but not yet returned to the allocator. |
| `used_memory_bytes` | Bytes | Total memory consumed by all search data structures. |
| `used_memory_human` | SI Bytes | Human-readable form of `used_memory_bytes` (e.g., `1.5MiB`). |
| `failure_requests_count` | Count | Total failed search requests. |
| `hybrid_requests_count` | Count | Hybrid queries combining vector and non-vector filters. |
| `inline_filtering_requests_count` | Count | Queries using inline filtering during vector search. |
| `nonvector_requests_count` | Count | Requests that are exclusively non-vector (text, numeric, tag). |
| `query_prefiltering_requests_cnt` | Count | Queries using pre-filtering before vector search. |
| `result_record_dropped_count` | Count | Result records dropped due to size limits. |
| `successful_requests_count` | Count | Total successful search requests. |
| `vector_requests_count` | Count | Requests that include a vector search component. |
| `rdb_load_failure_cnt` | Count | Failed RDB load operations. |
| `rdb_load_success_cnt` | Count | Successful RDB load operations. |
| `rdb_save_failure_cnt` | Count | Failed RDB save operations. |
| `rdb_save_success_cnt` | Count | Successful RDB save operations. |
| `string_interning_store_size` | Count | Unique strings in the string interning store. |
| `query_queue_size` | Count | Pending operations in the reader thread pool queue. |
| `reader_resumed_cnt` | Count | Times the reader thread pool was resumed after suspension. |
| `used_read_cpu` | Percent | Average CPU utilization of reader threads (`-1` if unavailable). |
| `used_write_cpu` | Percent | Average CPU utilization of writer threads (`-1` if unavailable). |
| `worker_pool_suspend_cnt` | Count | Times worker thread pools were suspended (e.g., fork/RDB). |
| `writer_queue_size` | Count | Pending operations in the writer thread pool queue. |
| `writer_resumed_cnt` | Count | Times the writer thread pool was resumed after suspension. |
| `writer_suspension_expired_cnt` | Count | Times writer suspension exceeded the configured maximum duration. |
| `vector_externing_deferred_entry_cnt` | Count | Deferred entries pending externalization. |
| `vector_externing_entry_count` | Count | Total entries in the vector externalizer. |
| `vector_externing_generated_value_cnt` | Count | Generated values during vector externalization. |
| `vector_externing_hash_extern_errors` | Count | Errors during hash vector externalization. |
| `vector_externing_lru_promote_cnt` | Count | LRU cache promotions in the vector externalizer. |
| `vector_externing_num_lru_entries` | Count | Entries in the vecto
| `rdb_save_success_cnt` | Count | Successful RDB save operations. |
| `rdb_save_failure_cnt` | Count | Failed RDB save operations. |