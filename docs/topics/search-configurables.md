---
title: "Valkey Search - Configuration"
description: Valkey Search Module Configurable Settings
---

# Configurables

The search module uses the Valkey configuration mechanism. Thus each of the named configuration below can be set on the module load command OR via the `CONFIG SET` command.

| Name                                          |  Type   | Default Value | Description                                                                                                                       |
| :-------------------------------------------- | :-----: | :-----------: | :-------------------------------------------------------------------------------------------------------------------------------- |
| search.query-string-bytes                     | Number  |               | Controls the length of the query string for FT.SEARCH command                                                                     |
| search.hnsw-block-size                        | Number  |               | HNSW resize configuration parameter for vector index block allocation                                                             |
| search.reader-threads                         | Number  |               | Controls the reader thread pool size; dynamically resizes pool on modification                                                    |
| search.writer-threads                         | Number  |               | Controls the writer thread pool size; dynamically resizes pool on modification                                                    |
| search.utility-threads                        | Number  |               | Controls the utility thread pool size; dynamically resizes pool on modification                                                   |
| search.max-worker-suspension-secs             | Number  |               | Max time in seconds that worker thread pool is suspended after fork started                                                       |
| search.use-coordinator                        | Boolean |               | Controls whether this instance uses coordinator; can only be set at startup                                                       |
| search.skip-rdb-load                          | Boolean |               | Skip loading vector index data from RDB file                                                                                      |
| search.skip-corrupted-internal-update-entries | Boolean |               | Skip corrupted AOF entries during internal updates                                                                                |
| search.log-level                              |  Enum   |               | Controls module log level verbosity                                                                                               |
| search.prefer-partial-results                 | Boolean |               | Default option for delivering partial results when timeout occurs (uses SOMESHARDS if not explicitly provided)                    |
| search.prefer-consistent-results              | Boolean |               | Default option for delivering consistent results when timeout occurs (uses CONSISTENT if not explicitly provided)                 |
| search.search-result-background-cleanup       | Boolean |               | Enable search result cleanup on background thread                                                                                 |
| search.high-priority-weight                   | Number  |               | Weight for high priority tasks in thread pools; low priority = 100 - this value                                                   |
| search.ft-info-timeout-ms                     | Number  |               | Timeout in milliseconds for FT.INFO fanout command                                                                                |
| search.ft-info-rpc-timeout-ms                 | Number  |               | RPC timeout in milliseconds for FT.INFO fanout command                                                                            |
| search.local-fanout-queue-wait-threshold      | Number  |               | Queue wait threshold in milliseconds for preferring local node in fanout operations                                               |
| search.thread-pool-wait-time-samples          | Number  |               | Sample queue size for thread pool wait time tracking                                                                              |
| search.max-term-expansions                    | Number  |               | Maximum number of words to search in text operations (prefix, suffix, fuzzy) to limit memory usage                                |
| search.search-result-buffer-multiplier        | String  |               | Multiplier for search result buffer size allocation                                                                               |
| search.drain-mutation-queue-on-save           | Boolean |               | Drain the mutation queue before RDB save                                                                                          |
| search.query-string-depth                     | Number  |               | Controls the depth of the query string parsing from the FT.SEARCH cmd                                                             |
| search.query-string-terms-count               | Number  |               | Controls the size of the query string parsing from the FT.SEARCH cmd (number of nodes in predicate tree)                          |
| search.fuzzy-max-distance                     | Number  |               | Controls the maximum allowed edit distance for fuzzy search queries                                                               |
| search.max-vector-knn                         | Number  |               | Controls the max KNN parameter for vector search                                                                                  |
| search.proximity-inorder-compat-mode          | Boolean |               | Controls proximity iterator's inorder/overlap violation check logic (compatibility mode)                                          |
| search.max-prefixes                           | Number  |               | Controls the max number of prefixes per index                                                                                     |
| search.max-tag-field-length                   | Number  |               | Controls the max length of a tag field                                                                                            |
| search.max-numeric-field-length               | Number  |               | Controls the max length of a numeric field                                                                                        |
| search.max-vector-attributes                  | Number  |               | Controls the max number of attributes per index                                                                                   |
| search.max-vector-dimensions                  | Number  |               | Controls the max dimensions for vector indices                                                                                    |
| search.max-vector-m                           | Number  |               | Controls the max M parameter for HNSW algorithm                                                                                   |
| search.max-vector-ef-construction             | Number  |               | Controls the max EF construction parameter for HNSW algorithm                                                                     |
| search.max-vector-ef-runtime                  | Number  |               | Controls the max EF runtime parameter for HNSW algorithm                                                                          |
| search.default-timeout-ms                     | Number  |               | Controls the default timeout in milliseconds for FT.SEARCH                                                                        |
| search.max-search-result-record-size          | Number  |               | Controls the max content size for a record in the search response                                                                 |
| search.max-search-result-fields-count         | Number  |               | Controls the max number of fields in the content of the search response                                                           |
| search.backfill-batch-size                    | Number  |               | Controls the batch size for backfilling indexes                                                                                   |
| search.coordinator-query-timeout-secs         | Number  |               | Controls the gRPC deadline timeout (in seconds) for distributed coordinator query operations.                                     |
| search.max-indexes                            | Number  |               | Controls the maximum number of search indexes that can be created in the system                                                   |
| search.cluster-map-expiration-ms              | Number  |               | Controls how long (in milliseconds) the coordinator caches the cluster topology map before refreshing it from the Valkey cluster. |
