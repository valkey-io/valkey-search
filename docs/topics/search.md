---
title: "Valkey Search - Overview"
description: Valkey Search Module Overview
---

**Valkey-Search** (BSD-3-Clause), provided as a Valkey module, is a high-performance Search engine
optimized for AI-driven / Search / Analytics / Recommendation System related workloads. It delivers single-digit millisecond
latency and high QPS, capable of handling billions of vectors with over 99% recall as part of vector searches. It also provides
support for hybrid / pure non vector workloads including Numeric, Tag, and Full-text searches.

Valkey-Search allows users to create indexes and perform searches, incorporating complex filters.
Users can index data using either **[Valkey Hash](hashes.md)** or **[Valkey-JSON](valkey-json.md)** data types.
The vector queries support Approximate Nearest Neighbor (ANN) search with HNSW and exact matching using K-Nearest Neighbors (KNN).

## Use-Cases Where **Valkey-Search** Shines

Valkey-Search's ability to search billions of vectors with millisecond latencies makes it ideal for real-time applications such as:

- Personalized Recommendations – Deliver instant, highly relevant recommendations based on real-time user interactions.
- Fraud Detection & Security – Identify anomalies and suspicious activity with ultra-fast similarity matching.
- Conversational AI & Chatbots – Enhance response accuracy and relevance by leveraging rapid vector-based retrieval.
- Image & Video Search – Enable multimedia search through real-time similarity detection.
- GenAI & Semantic Search – Power advanced AI applications with efficient vector retrieval for natural language understanding.


## Supported Commands

```plaintext
FT.CREATE
FT.DROPINDEX
FT.INFO
FT._LIST
FT.SEARCH
FT.AGGREGATE
```

For a detailed description of the supported commands, examples and configuration options, see the [Command Reference](../commands/#search).

## Indexes

A Valkey index is similar to a table in a relational database. The rows of the table are Valkey keys and the columns of the table are populated from data extracted from those keys.
Query expressions are constructed like a filter that uses the data in the columns to identify keys. The actual implementation is based on content-driven indexes, not linear filtering. Each index is given a unique name by the application and each Valkey database has a separate namespace of indexes.

Indexes exist separate from the Valkey database. Updates of the database trigger updates of one or more indexes which are done by background threads. Query operations are also performed by background threads optionally switching to the main thread in order to access the Valkey database. The consistency model between these two domains is further described below.

Indexes are created through the [`FT.CREATE`](../commands/ft.create.md) command which defines the rows and columns of the table-like abstraction. The command creates an empty index which can then be populated with data. Applications don't directly put data into an index, rather mutation operations on keys within the declared keyspace of an index automatically update the index with the value of that key. This update operation happens at the key level, i.e., even if only part of a key is mutated, the entire key is updated in the index. In other words, on an index of hash keys, a command like `HSET` which modifies a field of a key causes the entire key to be updated.

## Data Ingestion

Index updates are side effects of data mutation, based on prefix matching. A Valkey keyspace notification is used to capture a copy of the data associated with a key mutation. If this key belongs to multiple indexes, then the captured data goes into the mutation queue for each index independently. The client which executed the mutation command is blocked until all indexes are updated.

Processing of the mutation queue does not respect the order of mutations. If multiple clients are updating different keys the updates may happen in any order not necessarily related to the order in which the mutation commands were executed. If a previous update for the same key is already in the mutation queue then these can be combined. An exception to the reordering rules applies to keys which are updated as part of a multi/exec block or a Lua script. These keys are always ingested as a group which cannot be split or reordered.

## Index Creation and Backfill

The automatic ingestion only applies to keys which are modified _after_ the creation of an index. But what about keys that exist _before_ the creation of an index? These keys cannot be instantly inserted into the index. By default, the creation of an index initiates an internal background process which scans the keyspace to locate and insert preexisting keys into the index. This process, known as backfilling, can be monitored via the `FT.INFO` command. The backfill process runs once after the creation of the index. Once it has completed it will not be initiated again. The backfill process does not directly change the behavior of an index. Applications may continue to modify indexed keys and they will be updated in the same way as if no backfill was executing. Query operations can freely be executed, but will only contain the results from keys which are currently in the index, i.e., all keys modified after creation of the index and _some_ of the keys that existed before creation of the index.

The backfill process can be quite lengthy on a large system, even if no keys are found. Applications that know there are no preexisting keys or that no preexisting keys need to be inserted into the index, can skip the backfill process by specifying `SKIPINITIALSCAN` on the `FT.CREATE` command.

The snapshot process (save or full sync) only partially preserves the state of the backfilling process. The process is able to save the fact that a backfill is in progress, but does not save the backfill cursor (because a `SCAN` cursor isn't valid across reloads).
Thus on reload a backfilling index must restart the backfill at the beginning. However, because the indexes for vector fields are saved and restored, the indexed content of vector fields (the really slow part) is preserved.

## Query Operations

Query commands operate by blocking the client and sending the query to the background threads. The background threads search the indexes to generate a preliminary result set. During this search operation, index mutations are queued, meaning that the preliminary result set is generated from a point-in-time snapshot of the indexes on each shard. In cluster mode, these snapshots are not coordinated across shards. If `NOCONTENT` is specified without `SORTBY`, then the preliminary result set is the final result set.

However, if the query requires content processing, then it is returned to the main thread in order to access the database. If keys within the preliminary result set have been mutated, those keys are revalidated and rescored against the filter, which might remove the key from the result set. For [`FT.SEARCH`](../commands/ft.search.md), the result set is then returned as the command result. For [`FT.HYBRID`](../commands/ft.hybrid.md) and [`FT.AGGREGATE`](../commands/ft.aggregate.md), the result set is input to the aggregation stages specified on the command.

In practice, this means that keys which are mutated during the pendency of a search operation (i.e., mutations that have not completed before the start of the search or are submitted before the results of the search are returned) may or may not be present in the result set. However, if they are present, they will have their most recent values. When content processing runs, in no case will a key be in a result set whose current value doesn't match the filter. Because the revalidation and rescoring process doesn't respect the order of mutation of the keys, the results are not guaranteed to be consistent with the database at any specific point in time.

### Post-search re-filtering and re-scoring

For a key in the preliminary result set that has been mutated, the following processing applies:

- **Re-filtering:** The query conditions are evaluated again. Tag and numeric conditions use the fetched field values, vector range conditions use the fetched vector, and text conditions use the document's current text index. A key that fails the conditions, or that is no longer in the index, is removed.
- **Re-scoring:** For a non-KNN query with query conditions, a surviving mutated key's relevance score is recomputed using the selected scorer. Only mutated keys are re-scored; unchanged keys keep the scores computed during the index search. The recomputed score uses the index's current corpus-level statistics, such as the document count, average document length, and term document frequencies used by `BM25STD`. Because a mutation can change these statistics, which also affect the scores of other keys, a single reply can contain scores computed from different index states. If scores change, the surviving keys are reordered by descending relevance score.
- **KNN distance refresh:** For a KNN query, the distance is recomputed from the key's current vector when that vector is available and valid; otherwise, the distance from the index search is kept. When the query ranks by distance, surviving keys are reordered by ascending distance. For a text query combined with KNN, the distance is refreshed separately from the text relevance score; this `FT.SEARCH` path does not recompute the text relevance score of a KNN query.

Limiting the returned fields with `RETURN` does not disable re-filtering: fields needed to check the query are fetched even if they are not returned.

For example, consider `FT.SEARCH products '@status:{active}'` while another client updates the matching keys:

```
 time   query client                         other client
 ----   ------------------------------       ------------------------------------
  t0                                         HSET product:1 status active
                                             HSET product:2 status active
  t1    FT.SEARCH products '@status:{active}'
  t2    index search (background threads)
          preliminary result set:
          product:1, product:2
  t3                                         HSET product:1 status inactive
  t4    content processing (main thread)
          product:1 mutated -> re-filtered
            status is inactive -> removed
          product:2 not mutated -> kept
  t5    reply: product:2
```

`product:1` matched when the indexes were searched, but its current value no longer matches the filter, so it is removed from the reply. If instead `product:1` had been modified in a way that still matched (for example, a text field change under a text query), it would be kept with its most recent values and a recomputed relevance score, and its position among the surviving keys could change.

### Scope and limitations

Post-search processing operates on the keys already in the preliminary result set. It does not run the search again or discover keys that became matches after the index search. Removing keys can leave fewer results than requested by `LIMIT` or KNN, even if other matching keys exist. `FT.SEARCH` reduces the reported match count by the number of keys removed during content processing; this is not a fresh count of all matches in the database. Reordering the surviving keys also does not guarantee the same top results or page boundaries as a new search against the updated data. Likewise, re-scoring does not guarantee a ranking consistent with the updated index state: unchanged keys are not re-scored, even when an in-flight mutation changes corpus-level statistics in ways that would affect their scores.

[`FT.HYBRID`](../commands/ft.hybrid.md) revalidates each search arm before combining its results. Mutated keys can be removed from an arm, relevance scores or vector distances can be recomputed, and the arm can be reordered before fusion. A key can remain in one arm while being removed from another. As with `FT.SEARCH`, relevance scores are recomputed only for mutated keys in a `SEARCH` arm with query conditions, so such an arm can mix scores computed from different index states. A match-all `SEARCH *` arm keeps the scores from the search.

## Save/Restore

A generated RDB file (either due to an explicit save or full-sync operation) contains index definitions (index metadata), any vector field indexes, and a list of the keys currently in the index. On a load operation, each index is recreated from the definitions, any vector field indexes are reloaded, and any non-vector fields are rebuilt from the loaded Valkey database using the list of keys. If the index had a backfill in progress at the time of the save, then on completion of a load a backfill will be initiated for it.

## Index Replication

Indexes are node-local. Each node, regardless of whether it's a primary or a replica maintains its own index independently. Indexes on replicas are updated by key mutations transmitted on the replication channel and thus are subject to replication lag just like the Valkey database itself.
No additional replication channel traffic is generated for updates. The index backfill process is also node-local, meaning just because one node (a primary) in a replication group has completed its backfill the other nodes may not have.

## Cluster Mode

Search fully supports cluster mode and uses gRPC and protobuf for intra-cluster communication, requiring system configuration to make this additional port available. The gRPC port address is set based on the main Valkey port address. The gRPC port number is usually the Valkey port number plus 20294 (for the default port: 6379 + 20294 = 26673), unless the Valkey port is 6378 in which case the offset is 20295 (6378 + 20295 = 26673 also).

In cluster mode, Valkey distributes keys according to the hash algorithm of the keyname. This placement of data is not affected by the presence of the search module or any search indexes. Since search commands operate at the index level -- not the key level -- search is responsible for dealing with the distribution of data, performing intra-cluster RPC to execute commands as needed. Thus the application interface to search operates the same in cluster and non-cluster mode.

Search uses a simple architecture where index definitions are replicated on every node but the corresponding index only contains the data which is co-resident on that node. Index update operations remain wholly local to a node and will scale horizontally (save/restore operations also wholly node local). Vertical scaling is also effective because of the multi threaded architecture of search.

Query operations are performed by one node of each shard on its local index and the results are transparently merged together to form a full command response. Query operations are subject to increasing overhead as the cluster shard count increases, so they may scale sub-linearly with increasing shard count.

Search recognizes that certain data patterns can be optimized. In particular, it recognizes that if the data for an index is confined to a single slot, then only a single node needs to perform a search operation. Operations on single-slot indexes do not experience increased overhead as cluster size increases and thus will scale horizontally, vertically, and through replicas.

Client-side routing of query operations can have a profound effect on performance. For indexes with data on all shards, performance is improved if the query operations are distributed across the cluster. Any well-known load balancing algorithm should be fine, i.e., round-robin, random, etc. For indexes that are confined to a single slot, routing a query to a node that contains that slot is optimal. To simplify the logic of a client, the system restricts the assignment of index names as follows.

Index names without a hash tag are considered to be cluster-wide indexes (even if the prefix list is confined to a single slot). Index names with a hash tag are considered to be single-slot indexes. The prefix list of a single-slot index must consist solely of prefixes which contain the same hash tag or an error will be generated by the `FT.CREATE` command.

## Index Consistency

The Search architecture relies on having identical index definitions distributed across the cluster. This is implemented with an eventually consistent cross-shard protocol. The protocol relies on a Merkle-tree checksum of all indexes defined on a node being broadcast over the cluster bus periodically. Nodes which discover a mismatch in the checksum contact each other and negotiate a resolution using version numbers and last-writer-wins timestamps, one index at a time. If a node loses the negotiation for an index, it will delete its version of the index and recreate it using the winning definition.

On top of the eventual consistency machinery the individual commands also perform additional consistency checks on the involved index, typically retrying operations until consistency is achieved or a timeout occurs, terminating the command with a consistency error message.

The metadata mutation commands (`FT.CREATE` and `FT.DROPINDEX`) use the consistency machinery described above. The commands operate by mutating the local copy of the metadata and then triggering the convergence protocol. If convergence cannot be achieved within a bounded period of time the command is terminated with an error. No attempt is made to undo any failed metadata mutation. The most likely cause of failure is a shard-down or network partition situation.

The `FT.INFO` command has options that allow aggregation of index statistics and status across the cluster.

## Query Consistency

The query operations: `FT.SEARCH` and `FT.AGGREGATE` can only be executed by nodes that share the same index definition and slot ownership map. Cross-shard query commands contain a checksum of the coordinator's index definition and slot ownership. If a receiving node's index checksum or slot ownership checksum mismatches then the query is rejected and the coordinator will retry the operation. If a timeout occurs then by default an error is returned. The `SOMESHARDS` option of the `FT.SEARCH` command can be used to override this behavior to allow a result to be generated if only a subset of the cross-shard query operations succeed. The `INCONSISTENT` option of `FT.SEARCH` can be used to allow results from nodes with different views of the cluster.

## Configuration Settings

The Search module has a large list of configurable items. See [Search Configurations](../topics/search-configurables.md) for details.

## INFO Fields

See [Search Info Fields](../topics/search-observables.md) for details.
