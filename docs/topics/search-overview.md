---
title: "Search - Overview"
description: Search Module Overview
---

# Indexes

Lots of indexes. Indexes are independent. Commands operate on one index at a time.

# Index Data Model

Keys are rows in a table. Fields are columns. Primary key is Valkey Key.

Any number of fields can be defined.

# Data Ingestion

Index updates are side effects of data mutation, based on prefix matching. Keyspace notification is used to capture a copy of the data associated with the key mutation and the client is blocked. If this key belongs to multiple indexes, then the captured data goes into the mutation queue for each index independently. If a previous update for the same key is already in the mutation queue then these are combined.

Index update is atomic at the key level and/or the multi/exec level.
There is no synchronization of updates across clients.

# Query Operations

Query operations located a set of keys. For FT.SEARCH, the located set of key is used to access the database and fetch some or all of the contents of those keys which are returned as the result of the command. For FT.AGGREGATE, the set of keys is used to access the database and fetch some or all of the contents of those keys, that data is subject to additional processing as specified on the command and the final results are returned.

## Mutations while queries are outstanding

The query string is processed in the background to generate the list of keys. Then the command switches back to the mainthread and validates the keys.
Thus mutations that would exclude a key will be honored, but mutations that would include a key may or may not be honored.

# Save/Restore

Vector Indexes are saved/restored. Non-vector are rebuilt as data is loaded.

# Cluster Mode

Two types of indexes, cross-shard and single-slot.

Cross-cluster communication using gRPC (Port control#).

## Cross-Slot Indexes

Cross-Slot indexes are in every shard. Cross-slot indexes lack a hash-tag. This means that ingestion is local, but query operations must be fanout / merged.

## Single-Slot Indexes

Named with a hash-tag. Scalable reads. Data Plane forwards.

# Cluster Consistency

## Metadata Consistency

Describe how metadata consistency works. It's eventual consistency.

Describe how the FT.CREATE/FT.DROPINDEX command provide a consistent response.

## Query Consistency

What do cross-shard queries

How do query operations: FT.SearCH, FT.AGGREGATE, FT.INFO define consistency?

How does a query ensure that metadata is same on every query? What happens on a mismatch?

SLot migration consistency, how does that work?

ALLSHARDS vs SOMESHARDS ??

CONSISTENT and INCONSISTENT ??

Cluster failure modes

## Configuration Settings

The Search module has a large list of configurable items. See [Search Configurations](../topics/search-configurables.md) for details.

## INFO Fields

See [Search Info Fields](../topics/search-observables.md) for details.
