---
title: "Valkey Search - Configuration"
description: Valkey Search Module Configurable Settings
---

# Configurables

The search module uses the Valkey configuration mechanism. Thus each of the named configuration below can be set on the module load command OR via the `CONFIG SET` command.

| Name           |  Type | Default Value | Description                                            |
| :-------------------------------------------- | :-----: | :-----------: | :-------------------------------------------------------------------------------------------------------------------------------- |
| search.query-string-bytes            | Number | 10240 | Maximum allowable length of the query string for FT.SEARCH or FT.AGGREGATE commands |
| search.hnsw-block-size               | Number | 10240 | Number of vectors of space to add to HNSW index size when additional space required. |
| search.reader-threads                | Number |   #cores   | Reader thread pool size; dynamically resizes pool on modification                     |
| search.writer-threads                | Number |   #cores   | Writer thread pool size; dynamically resizes pool on modification                     |
| search.utility-threads               | Number |   1   | Utility thread pool size; dynamically resizes pool on modification                    |
| search.max-worker-suspension-secs    | Number |   60   | Controls how long the worker thread pool quiescing around a fork. Values > 0 are resumption timeouts if a fork runs too long. Values <=0 mean no quiescing.  |
| search.use-coordinator               | Boolean |   false   | Controls whether this instance uses the coordinator. Generally required for CME; can only be set at startup  |
| search.skip-rdb-load                 | Boolean |   false   | Skip loading of saved index data from RDB file. May be useful for recovering from a corrupted RDB or index.     |
| search.skip-corrupted-internal-update-entries | Boolean |  false  | Skip corrupted AOF entries during internal updates. May be useful for recovering from a corrupted AOF file.  |
| search.log-level                     |  Enum |   from core  | Controls module log level verbosity: "debug", "verbose", "notice" or "warning". Default value is to fetch the log level from the core at startup. |
| search.enable-partial-results        | Boolean |   true   | Default option for delivering partial results when timeout occurs (uses SOMESHARDS if not explicitly provided)           |
| search.enable-consistent-results     | Boolean |   false   | Default option for delivering consistent results when timeout occurs (uses CONSISTENT if not explicitly provided)        |
| search.search-result-background-cleanup | Boolean |   true   | Enable search result cleanup on background thread      |
| search.high-priority-weight          | Number |   100   | Fairness for high priority tasks in thread pools [0..100].  |
| search.ft-info-timeout-ms            | Number |   5000   | Timeout in milliseconds for FT.INFO command when CLUSTER or PRIMARY option is selected     |
| search.ft-info-rpc-timeout-ms        | Number |   2500   | Timeout in milliseconds for RPC connections used by FT.INFO when CLUSTER OR PRIMARTY options is selected  |
| search.local-fanout-queue-wait-threshold | Number |   50   | When this value is less than the average read queue wait time (in milliSeconds)  the local node is preferred in a fanout operation. |
| search.thread-pool-wait-time-samples | Number |   100   | Sample queue size for thread pool wait time tracking   |
| search.max-term-expansions           | Number |   200   | Maximum number of words to search in text operations (prefix, suffix, fuzzy) to limit memory usage |
| search.search-result-buffer-multiplier | String |   1.5   | Multiplier for search result buffer size allocation    |
| search.drain-mutation-queue-on-save  | Boolean |   false   | Drain the mutation queue before RDB save.           |
| search.query-string-depth            | Number |   1000   | Controls the depth of the query string parsing from the FT.SEARCH cmd        |
| search.query-string-terms-count      | Number |   1000   | Controls the size of the query string parsing from the FT.SEARCH cmd (number of nodes in predicate tree)                 |
| search.fuzzy-max-distance            | Number |   3   | Controls the maximum allowed edit distance for fuzzy search queries          |
| search.max-vector-knn                | Number |   10000   | Controls the max KNN parameter for vector search       |
| search.proximity-inorder-compat-mode | Boolean |   false   | Controls proximity iterator's inorder/overlap violation check logic (compatibility mode)    |
| search.max-prefixes                  | Number |   8   | Controls the max number of prefixes per index          |
| search.max-tag-field-length          | Number |   256   | Controls the max length of a tag field                 |
| search.max-numeric-field-length      | Number |   128   | Controls the max length of a numeric field             |
| search.max-vector-attributes         | Number |   1000   | Controls the max number of attributes per index        |
| search.max-vector-dimensions         | Number |   32768   | Controls the max dimensions for vector indices         |
| search.max-vector-m                  | Number |   2000000   | Controls the max M parameter for HNSW algorithm        |
| search.max-vector-ef-construction    | Number |   1000000   | Controls the max EF construction parameter for HNSW algorithm                |
| search.max-vector-ef-runtime         | Number |   1000000   | Controls the max EF runtime parameter for HNSW algorithm                     |
| search.default-timeout-ms            | Number |   50000   | Controls the default timeout in milliseconds for FT.SEARCH                   |
| search.max-search-result-record-size | Number |   5242880   | Controls the max content size for a record in the search response  |
| search.max-search-result-fields-count | Number |   500   | Controls the max number of fields in the content of the search response      |
| search.backfill-batch-size           | Number |   10240   | Controls the batch size for backfilling indexes        |
| search.coordinator-query-timeout-secs | Number |   120   | Controls the gRPC deadline timeout (in seconds) for distributed coordinator query operations.      |
| search.max-indexes                   | Number |   1000   | Controls the maximum number of search indexes that can be created in the system                    |
| search.prefiltering-threshold-ratio  | String |   0.001   | Controls when pre-filtering is used vs inline-filtering for hybrid queries (range 0.0 to 1.0) |
| search.async-fanout-threshold        | Number |   30   | Controls the threshold for async fanout operations (minimum number of targets to use async) |
| search.text-rax-target-mutex-pool-size | Number |   256   | Controls the mutex pool size for text rax operations |
| search.max-nonvector-search-results-fetched | Number |   100000   | Controls the maximum number of results to fetch in background threads before content fetching on non-vector (numeric/tag/text) query paths |
| search.max-attributes                | Number |   1000   | Controls the max number of attributes per index |
| search.hide-user-data-from-log       | Boolean |   true   | Controls whether user data is hidden from log output |
| search.cluster-map-expiration-ms     | Number |   250   | Controls how long (in milliseconds) the coordinator caches the cluster topology map before refreshing it from the Valkey cluster. |
