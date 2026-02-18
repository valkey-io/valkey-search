---
title: "Search - Overview"
description: Search Module Overview
---

The search module enhances Valkey by creating searchable indexes for groups of keys. A query composed from data-type specific search operators is used to efficiently locate sets of keys. The `FT.SEARCH` command performs a query and then returns the set of located keys and their content. The `FT.AGGREGATE` command provides server-side computational capabilities by performing a query, and executing a series of user-defined computations on the contents of the located keys.

# Indexes

A Valkey index is similar to a table in a relational database. The rows of the table are Valkey keys and the columns of the table are populated from data extracted from those keys.
Query expressions are constructed like a filter that uses the data in the columns to identify keys. The actual implementation is based on content-driven indexes, not linear filtering. Each index is given a unique name by the application and each Valkey database has a separate namespace of indexes.

Indexes exist separate from the Valkey database. Updates of the database trigger updates of one or more indexes which are done by background threads. Query operations are also performed by background threads optionally switching to the main thread in order to access the Valkey database. The consistency model between these two domains is further described below.

Indexes are created through the [`FT.CREATE`](../commands/ft.create.md) command which defines the rows and columns of the table-like abstraction. The command creates an empty index which can then be populated with data. Applications don't directly put data into an index, rather mutation operations on keys within the declared keyspace of an index automatically update the index with the value of that key. In other words, on an index of hash keys, a command like `HSET` which modifies a key causes any index which includes that key to automatically be updated.

# Data Ingestion

Index updates are side effects of data mutation, based on prefix matching. A Valkey keyspace notification is used to capture a copy of the data associated with a key mutation. If this key belongs to multiple indexes, then the captured data goes into the mutation queue for each index independently. The client which executed the mutation command is blocked until all indexes are updated.

Processing of the mutation queue does not respect the order of mutations. If multiple clients are updating different keys the updates may happen in any order not necessarily related to the order in which the mutation commands were executed. If a previous update for the same key is already in the mutation queue then these can be combined. An exception to the reordering rules applies to keys which are updated as part of a multi/exec block or a Lua script. These keys are always ingested as a group which cannot be split or reordered.

# Query Operations

Query commands operate by blocking the client and sending the query to the background threads to locate a set of keys. Then the client is unblocked and control resumes on the main thread which can access the database to generate the final result. If a key is modified by another client while a query is in progress then that key may or may not be included in the query result. In no case will a key be returned which does not satisfy the query expression.

# Save/Restore

A generated RDB file (either due to an explicit save or full-sync operation) contains index definitions (index metadata), any vector field indexes, and a list of the keys currently in the index. On a load operation, each index is recreated from the definitions, any vector field indexes are reloaded, and any non-vector fields are rebuilt from the loaded Valkey database using the list of keys. If the index had a backfill in progress at the time of the save, then on completion of a load a backfill will be initiated for it.

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
