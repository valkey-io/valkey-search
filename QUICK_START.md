# Quick Start

Follow these steps to set up, build, and run the Valkey server with vector search capabilities. This guide will walk you through creating a vector index, inserting vectors, and issuing queries.

## Step 1: Install Valkey and ValkeySearch

1. Build Valkey from source by following the instructions [here](https://github.com/valkey-io/valkey?tab=readme-ov-file#building-valkey-using-makefile). Make sure to use Valkey version 7.2.6 or later as the previous versions have Valkey module API bugs.
2. Build ValkeySearch module from source by following the instructions [here](https://github.com/valkey-io/valkey-search/blob/main/DEVELOPER.md#manual-setup-of-build-environment).

## Step 2: Run the Valkey Server

Once ValkeySearch is built, run the Valkey server with the ValkeySearch module loaded:

```bash
./valkey-server "--loadmodule libvalkeysearch.so  --reader-threads 64 --writer-threads 64"
```

You should see the Valkey server start, and it will be ready to accept commands.

## Step 3: Create a Vector Index

To enable vector search functionality, you need to create an index for storing vector data.
Start a Valkey CLI session:

```bash
valkey-cli
```

Create a vector field using the FT.CREATE command. For example:

```bash
FT.CREATE myIndex SCHEMA vector VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC COSINE
```

- `vector` is the vector field for storing the vectors.
- `VECTOR HNSW` specifies the use of the HNSW (Hierarchical Navigable Small World) algorithm for vector search.
- `DIM 3` sets the vector dimensionality to 3.
- `DISTANCE_METRIC COSINE` sets the distance metric to cosine similarity.

## Step 4: Insert Some Vectors

To insert vectors, use the `HSET` command:

```bash
HSET my_hash_key_1 vector "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
HSET my_hash_key_2 vector "\x00\xaa\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
```

Replace the `\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?` and `\x00\xaa\x00\x00\x00\x00\x00\x00\x00\x00\x80?` with actual vectors.

## Step 5: Issue Vector Queries

Now that you've created an index and inserted vectors, you can perform a vector search. Use the `FT.SEARCH` command to find similar vectors:

```bash
FT.SEARCH myIndex "*=>[KNN 5 @vector $query_vector]" PARAMS 2 query_vector "\xcd\xccL?\x00\x00\x00\x00\x00\x00\x00\x00"
```

This command performs a K-nearest neighbors search and returns the top 5 closest vectors to the provided query vector.
