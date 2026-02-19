---
title: "Valkey Search - Overview"
description: Valkey Search Module Overview
---

# Valkey-Search

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
The search module enhances Valkey by creating searchable indexes for groups of keys. A query composed from data-type specific search operators is used to efficiently locate sets of keys. The `FT.SEARCH` command performs a query and then returns the set of located keys and their content. The `FT.AGGREGATE` command provides server-side computational capabilities by performing a query, and executing a series of user-defined computations on the contents of the located keys.

# Indexes

A Valkey index is similar to a table in a relational database. The rows of the table are Valkey keys and the columns of the table are populated from data extracted from those keys.
Query expressions are constructed like a filter that uses the data in the columns to identify keys. The actual implementation is based on content-driven indexes, not linear filtering. Each index is given a unique name by the application and each Valkey database has a separate namespace of indexes.

Indexes exist separate from the Valkey database. Updates of the database trigger updates of one or more indexes which are done by background threads. Query operations are also performed by background threads optionally switching to the main thread in order to access the Valkey database. The consistency model between these two domains is further described below.

Indexes are created through the [`FT.CREATE`](../commands/ft.create.md) command which defines the rows and columns of the table-like abstraction. The command creates an empty index which can then be populated with data. Applications don't directly put data into an index, rather mutation operations on keys within the declared keyspace of an index automatically update the index with the value of that key. This update operation happens at the key level, i.e., even if only part of a key is mutated, the entire key is updated in the index. In other words, on an index of hash keys, a command like `HSET` which modifies a field of a key causes the entire key to be updated.

# Data Ingestion

Index updates are side effects of data mutation, based on prefix matching. A Valkey keyspace notification is used to capture a copy of the data associated with a key mutation. If this key belongs to multiple indexes, then the captured data goes into the mutation queue for each index independently. The client which executed the mutation command is blocked until all indexes are updated.

Processing of the mutation queue does not respect the order of mutations. If multiple clients are updating different keys the updates may happen in any order not necessarily related to the order in which the mutation commands were executed. If a previous update for the same key is already in the mutation queue then these can be combined. An exception to the reordering rules applies to keys which are updated as part of a multi/exec block or a Lua script. These keys are always ingested as a group which cannot be split or reordered.

# Index Creation and Backfill

The automatic ingestion only applies to keys which are modified _after_ the creation of an index. But what about keys that exist _before_ the creation of an index? These keys cannot be instantly inserted into the index. By default, the creation of an index initiates an internal background process which scans the keyspace to locate and insert preexisting keys into the index. This process, known as backfilling, can be monitored via the `FT.INFO` command. The backfill process runs once after the creation of the index. Once it has completed it will not be initiated again. The backfill process does not directly change the behavior of an index. Applications may continue to modify indexed keys and they will be updated in the same way as if no backfill was executing. Query operations can freely be executed, but will only contain the results from keys which are currently in the index, i.e., all keys modified after creation of the index and _some_ of the keys that existed before creation of the index.

The backfill process can be quite lengthy on a large system even if no keys are found. The backfill process can be suppressed by specifying the `SKIPINITIALSCAN` option on the `FT.CREATE` command. Applications that know _a priori_ that no preexisting keys exist or exist and need not be put into the index can specify this option as an optimization.

The snapshot process (save or full sync) only partially preserves the state of the backfilling process. The process is able to save the fact that a backfill is in progress, but does not save the backfill cursor (because a `SCAN` cursor isn't valid across reloads).
Thus on reload a backfilling index must restart the backfill at the beginning. However, because the indexes for vector fields are saved and restored, the of vector fields (the really slow part) is preserved.

# Query Operations

Query commands operate by blocking the client and sending the query to the background threads to locate a set of keys. Then the client is unblocked and control resumes on the main thread which can access the database to generate the final result. If a key is modified by another client while a query is in progress then that key may or may not be included in the query result. In no case will a key be returned which does not satisfy the query expression.

# Save/Restore

A generated RDB file (either due to an explicit save or full-sync operation) contains index definitions (index metadata), any vector field indexes, and a list of the keys currently in the index. On a load operation, each index is recreated from the definitions, any vector field indexes are reloaded, and any non-vector fields are rebuilt from the loaded Valkey database using the list of keys. If the index had a backfill in progress at the time of the save, then on completion of a load a backfill will be initiated for it.

# Index Replication

Indexes are node-local. Each node, regardless of whether it's a primary or a replica maintains its own index independently. Indexes on replicas are updated by key mutations transmitted on the replication channel and thus are subject to replication lag just like the Valkey database itself.
No additional replication channel traffic is generated for updates. The index backfill process is also node-local, meaning just because one node (a primary) in a replication group has completed its backfill the other nodes may not have.

# Cluster Mode

Search fully supports cluster mode and uses gRPC and protobuf for intra-cluster communication, requiring system configuration to make this additional port available. The gRPC port address is set based on the main Valkey port address. The gRPC port number is usually the Valkey port number plus 20294 (for the default port: 6379 + 20294 = 26673), unless the Valkey port is 6378 in which case the offset is 20295 (6378 + 20295 = 26673 also).

In cluster mode, Valkey distributes keys according to the hash algorithm of the keyname. This placement of data is not affected by the presence of the search module or any search indexes. Since search commands operate at the index level -- not the key level -- search is responsible for dealing with the distribution of data, performing intra-cluster RPC to execute commands as needed. Thus the application interface to search operates the same in cluster and non-cluster mode.

Search uses a simple architecture where index definitions are replicated on every node but the corresponding index only contains the data which is co-resident on that node. Index update operations remain wholly local to a node and will scale horizontally (save/restore operations also wholly node local). Vertical scaling is also effective because of the multithreaded architecture of search.

Query operations are performed by one node of each shard on its local index and the results are transparently merged together to form a full command response. Query operations are subject to increasing overhead as the cluster shard count increases, meaning that query operations may scale sub-linearly with increasing shard count.

Search recognizes that certain data patterns can be optimized. In particular, it recognizes that if the data for an index is confined to a single slot, then only a single node needs to perform a search operation. Operations on single-slot indexes do not experience increased overhead as cluster size increases and thus will scale horizontally, vertically as well as through replicas.

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
