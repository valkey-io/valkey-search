---
title: "Valkey Search - Configuration"
description: Valkey Search Module Configurable Settings
---

# Configurables

The search module uses the Valkey configuration mechanism. Each configuration below can be set as a module load argument or with the `CONFIG SET` command, except developer-only configurations, which can be changed only while `search.debug-mode` is `yes` (default `no`). Developer-only configurations set in the Valkey configuration file are accepted regardless of `search.debug-mode`.

| Name                                          |  Type   | Default Value | Description                                                                                                                       |
| :-------------------------------------------- | :-----: | :-----------: | :-------------------------------------------------------------------------------------------------------------------------------- |
| search.async-fanout-threshold                 | Number | 30 | Minimum number of fanout targets for which remote requests are issued asynchronously. Range: 1 to 10000. |
| search.backfill-batch-size                    | Number | 10240 | Controls the batch size for backfilling indexes. Range: 1 to 2147483647. |
| search.cluster-map-expiration-ms              | Number | 250 | Controls how long (in milliseconds) the coordinator caches the cluster topology map before refreshing it from the Valkey cluster. Range: 0 to 3600000. |
| search.coordinator-query-timeout-secs         | Number | 25 | Controls the gRPC deadline timeout (in seconds) for distributed coordinator query operations. It is independent of the query timeout: with the default `search.default-timeout-ms` of 50000, a remote shard still running after this deadline is treated as a failed shard (see `search.enable-partial-results`). Range: 1 to 3600. |
| search.cursor-max-count                       | Number | 100000 | Maximum `COUNT` accepted by `WITHCURSOR` and `FT.CURSOR READ`. Range: 1 to `INT64_MAX`. |
| search.cursor-max-idle-ms                     | Number | `INT64_MAX` | Maximum `MAXIDLE` (milliseconds) accepted by `WITHCURSOR` |
| search.default-scorer                         | Enum | BM25STD | Scorer used when a query omits `SCORER`. See [Search - Scoring](search-scoring.md). Values: `BM25STD`. |
| search.default-timeout-ms                     | Number | 50000 | Controls the default timeout in milliseconds for FT.SEARCH, FT.AGGREGATE and FT.HYBRID. Range: 1 to 60000. |
| search.drain-mutation-queue-on-save           | Boolean | no | Drain the mutation queue before RDB save |
| search.emulate-release                        | Version | 1.0.0 | Release whose behavior is emulated for gated compatibility fixes (see [COMPATIBILITY.md](../../COMPATIBILITY.md#compatibility-defects)). The default is `<major>.0.0` of the running module, so fixes gated at 1.3.0 are off by default. Range: 1.0.0 to the running module version; the upper bound is lifted while `search.debug-mode` is `yes`. |
| search.enable-consistent-results              | Boolean | no | Default option for delivering consistent results when timeout occurs (uses CONSISTENT if not explicitly provided) |
| search.enable-partial-results                 | Boolean | yes | Default option for delivering partial results when timeout occurs (uses SOMESHARDS if not explicitly provided) |
| search.ft-info-rpc-timeout-ms                 | Number | 2500 | RPC timeout in milliseconds for FT.INFO fanout command. Range: 100 to 300000. Developer only: requires `search.debug-mode yes`. |
| search.ft-info-timeout-ms                     | Number | 5000 | Timeout in milliseconds for FT.INFO fanout command. Range: 100 to 300000. Developer only: requires `search.debug-mode yes`. |
| search.fuzzy-max-distance                     | Number | 3 | Controls the maximum allowed edit distance for fuzzy search queries. Range: 1 to 50. |
| search.high-priority-weight                   | Number | 100 | Weight for high priority tasks in thread pools; low priority = 100 - this value. Range: 0 to 100. |
| search.hnsw-block-size                        | Number | 10240 | HNSW resize configuration parameter for vector index block allocation. Range: 0 to 4294967295. |
| search.info-developer-visible                 | Boolean | no | Show developer metrics in `INFO`. See [INFO SEARCH metrics](search-observables.md). |
| search.local-fanout-queue-wait-threshold      | Number | 50 | Queue wait threshold in milliseconds for preferring local node in fanout operations. Range: 0 to 10000. |
| search.log-level                              | Enum | notice | Controls module log level verbosity. Values: `warning`, `notice`, `verbose`, `debug`. |
| search.max-attributes                         | Number | 1000 | Controls the max number of attributes per index. Range: 1 to 10000. |
| search.max-combine-window                     | Number | 1000000 | Maximum `COMBINE ... WINDOW` accepted by FT.HYBRID; `WINDOW 0` and the `FUNCTION` default window resolve to this value. Range: 1 to 4294967295. Developer only: requires `search.debug-mode yes`. |
| search.max-group-key-expansion                | Number | 65536 | Maximum number of groups one record can expand into in an FT.AGGREGATE GROUPBY over multi-value fields; exceeding it fails the query. Range: 1 to 4294967295. |
| search.max-indexes                            | Number | 1000 | Controls the maximum number of search indexes that can be created in the system. Range: 1 to 10000000. |
| search.max-mutation-queue-size-on-restore     | Number | 10000 | Limits the mutation queue size during RDB restore to prevent memory spikes. Range: 1 to 1000000. |
| search.max-nonvector-search-results-fetched   | Number | 100000 | Maximum number of matches retained per shard by a non-vector query. See [Vector Range Match](search-query.md#vector-range-match) for `VECTOR_RANGE` behavior. Range: 0 to 4294967295. |
| search.max-numeric-field-length               | Number | 128 | Controls the max length of a numeric field. Range: 1 to 256. |
| search.max-prefixes                           | Number | 8 | Controls the max number of prefixes per index. Range: 1 to 16. |
| search.max-query-queue-depth                  | Number | 100000 | When the reader thread pool queue reaches this depth, FT.SEARCH, FT.AGGREGATE and FT.HYBRID are rejected with `Search query queue depth exceeded` before fan-out. Checked on each node that receives the query. 0 means unlimited. Range: 0 to 2147483647. |
| search.max-search-result-fields-count         | Number | 500 | Controls the max number of fields in the content of the search response. Range: 1 to 1000. |
| search.max-search-result-record-size          | Number | 5242880 | Controls the max content size for a record in the search response. Range: 100 to 10485760. |
| search.max-tag-field-length                   | Number | 256 | Controls the max length of a tag field. Range: 1 to 10000. |
| search.max-term-expansions                    | Number | 200 | Maximum number of words to search in text operations (prefix, suffix, fuzzy) to limit memory usage. Range: 1 to 100000. |
| search.max-timeout-ms                         | Number | 60000 | Controls the maximum allowed TIMEOUT value, in milliseconds, for FT.SEARCH, FT.AGGREGATE and FT.HYBRID. Range: 1 to 60000. |
| search.max-vector-attributes                  | Number | 1000 | Deprecated alias of `search.max-attributes`; used instead of it only when explicitly set. Range: 1 to 10000. |
| search.max-vector-dimensions                  | Number | 32768 | Controls the max dimensions for vector indices. Range: 1 to 64000. |
| search.max-vector-ef-construction             | Number | 1000000 | Controls the max EF construction parameter for HNSW algorithm. Range: 1 to 1000000. |
| search.max-vector-ef-runtime                  | Number | 1000000 | Maximum `EF_RUNTIME` accepted for HNSW fields and query overrides. Range: 1 to 1000000. |
| search.max-vector-knn                         | Number | 10000 | Controls the max KNN parameter for vector search. Range: 1 to 100000. |
| search.max-vector-m                           | Number | 2000000 | Controls the max M parameter for HNSW algorithm. Range: 2 to 2000000. |
| search.max-worker-suspension-secs             | Number | 60 | Max time in seconds that worker thread pool is suspended after fork started. Range: 0 to 3600. |
| search.proximity-inorder-compat-mode          | Boolean | no | Controls proximity iterator's inorder/overlap violation check logic (compatibility mode) |
| search.query-string-bytes                     | Number | 10240 | Controls the length of the query string for FT.SEARCH command. Range: 1 to 4294967295. |
| search.query-string-depth                     | Number | 1000 | Controls the depth of the query string parsing from the FT.SEARCH cmd. Range: 1 to 4294967295. |
| search.query-string-terms-count               | Number | 1000 | Controls the size of the query string parsing from the FT.SEARCH cmd (number of nodes in predicate tree). Range: 1 to 10000. |
| search.reader-threads                         | Number | Physical CPU cores | Controls the reader thread pool size; dynamically resizes pool on modification. Range: 1 to 1024. |
| search.search-result-background-cleanup       | Boolean |  | Enable search result cleanup on background thread |
| search.search-result-buffer-multiplier        | Number (decimal) | 1.5 | Multiplier for search result buffer size allocation. Range: 1.0 to 1000.0. |
| search.skip-corrupted-internal-update-entries | Boolean | no | Skip corrupted AOF entries during internal updates |
| search.skip-rdb-load                          | Boolean | no | Skip loading vector index data from RDB file |
| search.tag-min-prefix-length                  | Number | 2 | Minimum number of characters required before trailing `*` in TAG wildcard queries (length excludes `*`). Range: 0 to 4294967295. |
| search.thread-pool-wait-time-samples          | Number | 100 | Sample queue size for thread pool wait time tracking. Range: 10 to 10000. |
| search.use-coordinator                        | Boolean | no | Controls whether this instance uses coordinator; can only be set at startup |
| search.utility-threads                        | Number | 1 | Controls the utility thread pool size; dynamically resizes pool on modification. Range: 1 to 1024. |
| search.vector-chunk-huge-pages                | Boolean | yes | Allocate vector index memory in 2 MB transparent huge pages (needs Valkey 9.2+ and THP not disabled); applies to new allocations |
| search.vector-unshare-batch-size              | Number | 10240 | Number of vector records unshared per server cron tick. Range: 1 to 4294967295. |
| search.writer-threads                         | Number | Physical CPU cores | Controls the writer thread pool size; dynamically resizes pool on modification. Range: 1 to 1024. |

`search.vector-chunk-huge-pages` takes effect when the server provides the external memory APIs (Valkey 9.2+) and transparent huge pages are not disabled for the process or host. Vector memory is then allocated and counted in 2 MB units, so a small index reports a few MB more `used_memory`. A 2 MB chunk is released only when all the vectors in it are deleted, so after most keys of a FLAT index are deleted, `used_memory` stays near its peak until new vectors reuse the space.
