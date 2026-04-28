# Quick Start

Follow these steps to set up, build, and run the Valkey server with the valkey-search module. This guide walks you through creating indexes, inserting data, and issuing queries across all supported field types — vector, tag, numeric, and full-text.

## Step 1: Install Valkey and valkey-search

1. Follow the [instructions to build Valkey from source](https://github.com/valkey-io/valkey?tab=readme-ov-file#building-valkey-using-makefile). Make sure to use Valkey version 9.0.1 or later.
2. Follow the [instructions to build the valkey-search module from source](https://github.com/valkey-io/valkey-search/tree/main?tab=readme-ov-file#build-instructions).

## Step 2: Run the Valkey Server

Once valkey-search is built, run the Valkey server with the valkey-search module loaded:

```bash
valkey-server --loadmodule /path/to/libsearch.so
```

For optimal performance, valkey-search matches worker threads to the number of CPU cores on the host. You can override this explicitly:

```bash
valkey-server "--loadmodule /path/to/libsearch.so --reader-threads 64 --writer-threads 64"
```

To enable JSON support, load the [valkey-json](https://github.com/valkey-io/valkey-json) module as well:

```bash
valkey-server --loadmodule /path/to/libsearch.so --loadmodule /path/to/libjson.so
```

## Step 3: Connect via CLI

```bash
valkey-cli
```

---

## Working with Vector Search

### Create a Vector Index

```bash
FT.CREATE myIndex SCHEMA vector VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC COSINE
```

- `vector` is the vector field name for storing the vectors.
- `VECTOR HNSW` uses the Hierarchical Navigable Small World algorithm for approximate nearest neighbor search. The other option is `VECTOR FLAT` for exact brute-force search.
- `DIM 3` sets the vector dimensionality to 3.
- `DISTANCE_METRIC COSINE` sets the distance metric to cosine similarity. Other options are `L2` and `IP`.


### Insert Some Vectors

Vectors must be encoded as 32-bit IEEE 754 floats in little-endian byte order. Each vector must have exactly `DIM` elements.

```bash
HSET my_hash_key_1 vector "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
HSET my_hash_key_2 vector "\x00\xaa\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
```

### Issue a Vector Query

Perform a K-Nearest Neighbors (KNN) search returning the top 5 nearest vectors:

```bash
FT.SEARCH myIndex "*=>[KNN 5 @vector $query_vector]" PARAMS 2 query_vector "\xcd\xccL?\x00\x00\x00\x00\x00\x00\x00\x00"
```

The `*` before `=>` means no pre-filtering — all vectors in the index are searched. Replace `*` with a filter expression (e.g. `@category:{electronics}`) to run a hybrid query that narrows candidates before KNN.

---

## Working with Tag and Numeric Fields

Tag fields store categorical values like status labels or product categories and support exact-match and prefix-match queries. Numeric fields store numbers and support range queries with inclusive or exclusive bounds.

### Create an Index with Tag and Numeric Fields

```bash
FT.CREATE products ON HASH PREFIX 1 product: SCHEMA category TAG price NUMERIC rating NUMERIC
```

- `category` is a TAG field — supports exact-match and prefix-match filtering.
- `price` and `rating` are NUMERIC fields — support range queries.

### Insert Some Data

```bash
HSET product:1 category "electronics" name "Laptop" price 999.99 rating 4.5
HSET product:2 category "electronics" name "Tablet" price 499.00 rating 4.0
HSET product:3 category "electronics" name "Phone" price 299.00 rating 3.8
HSET product:4 category "books" name "Book" price 19.99 rating 4.8
```

### Query by Tag

Return all products in the "electronics" category:

```bash
FT.SEARCH products "@category:{electronics}"
```

Tags support the `|` operator for matching multiple values:

```bash
FT.SEARCH products "@category:{electronics | books}"
```

### Query by Numeric Range

Return products priced between 100 and 1000 with a rating of at least 4.0:

```bash
FT.SEARCH products "@price:[100 1000] @rating:[4.0 +inf]"
```

Parentheses make a bound exclusive. For example, to find products with a price strictly less than 500:

```bash
FT.SEARCH products "@price:[-inf (500]"
```

### Combine Tag and Numeric Filters

```bash
FT.SEARCH products "@category:{books} @price:[10 30] @rating:[4.7 +inf]"
```

---

## Working with Full-Text Search

### Create a Text Index

```bash
FT.CREATE articles ON HASH PREFIX 1 article: SCHEMA title TEXT body TEXT
```

### Insert Some Documents

```bash
HSET article:1 title "Introduction to Valkey" body "Valkey is a high performance key value store with module support"
HSET article:2 title "Search Module Overview" body "The search module provides vector and full text search capabilities"
HSET article:3 title "Getting Started with Vectors" body "Vector search enables similarity matching across high dimensional data"
```

### Term Search

Find articles containing the word "valkey" in any text field:

```bash
FT.SEARCH articles "valkey"
```

Search within a specific field:

```bash
FT.SEARCH articles "@title:valkey"
```

### Prefix Search

Match any word that starts with "search":

```bash
FT.SEARCH articles "search*"
```

### Exact Phrase Search

Match the exact sequence of words:

```bash
FT.SEARCH articles "@body:\"full text search\""
```

### Fuzzy Search

Match words within an edit distance of 1 from "valkee" (catches typos):

```bash
FT.SEARCH articles "%valkee%"
```

### Combining Text with Other Filters

Text matchers can be combined with tag and numeric filters in the same query. Separate predicates with a space for `AND`, or `|` for `OR`:

```bash
FT.CREATE docs ON HASH PREFIX 1 doc: SCHEMA content TEXT category TAG year NUMERIC
HSET doc:1 content "great introduction to databases" category "tech" year 2024
HSET doc:2 content "great recipes for summer" category "cooking" year 2023

FT.SEARCH docs "@category:{tech} database*"
```

---

## Aggregation with FT.AGGREGATE

`FT.AGGREGATE` extends `FT.SEARCH` with server-side data processing. It supports stages like `GROUPBY`, `SORTBY`, `APPLY`, `FILTER`, and `LIMIT` that transform the working set of records in a pipeline.

Using the `products` index from earlier:

```bash
FT.AGGREGATE products "*" LOAD 2 @category @price GROUPBY 1 @category REDUCE AVG 1 @price AS avg_price REDUCE COUNT 0 AS count SORTBY 2 @avg_price DESC
```

This groups all products by category, computes the average price and count per category, and sorts by average price descending.

For a full description of aggregate stages and expression syntax, see the [FT.AGGREGATE documentation](docs/commands/ft.aggregate.md).

---

## Managing Indexes

### List All Indexes

```bash
FT._LIST
```

### Inspect an Index

```bash
FT.INFO products
```

### Drop an Index

```bash
FT.DROPINDEX products
```

### Module-Level Metrics

```bash
INFO SEARCH
```

---

## Further Reading

- [Command Reference](docs/COMMANDS.md) — detailed syntax and options for all commands.
- [Query Language](docs/topics/search-query.md) — full filter expression syntax, logical operators, and text search operators.
- [Data Formats](docs/topics/search-data-formats.md) — ingestion formats for tag, numeric, vector, and text fields.
- [Search Overview](docs/topics/search.md) — architecture, cluster mode, replication, and consistency model.
- [Configuration](docs/topics/search-configurables.md) — tunable parameters for the search module.
- [INFO SEARCH Metrics](docs/topics/search-observables.md) — module-wide memory, latency, query, and thread pool metrics.
