The `FT.CREATE` command creates an empty index and may initiate the backfill process. Each index consists of a number of
field definitions. Each field definition specifies a field name, a field type and a path within each indexed key to
locate a value of the declared type. Some field type definitions have additional sub-type specifiers.

For indexes on `HASH` keys, the default path is the same as the hash member name. The optional `AS` clause can be used to rename
the field if desired

For indexes on `JSON` keys, the path is a `JSON` path to the data of the declared type. The `AS` clause is required in order to provide a field name without the special characters of a `JSON` path.

```
FT.CREATE <index-name>
    [ON HASH | ON JSON]
    [PREFIX <count> <prefix> [<prefix>...]]
    [FILTER <expression>]
    [SCORE default_value]
    [SCORE_FIELD <field_name>]
    [LANGUAGE <language>]
    [SKIPINITIALSCAN]
    [MINSTEMSIZE <min_stem_size>]
    [WITHOFFSETS | NOOFFSETS]
    [NOHL]
    [NOSTOPWORDS | STOPWORDS <count> <word> word ...]
    [PUNCTUATION <punctuation>]
    SCHEMA
        (
            <field-identifier> [AS <field-alias>]
                  NUMERIC
                | TAG [SEPARATOR <sep>] [CASESENSITIVE]
                | TEXT [NOSTEM] [WITHSUFFIXTRIE | NOSUFFIXTRIE] [WEIGHT <weight>]
                | VECTOR [HNSW | FLAT | SVS_VAMANA] <attr_count> [<attribute_name> <attribute_value>]+
            [SORTABLE [UNF]]
        )+
```

- `<index-name>` (required): This is the name you give to your index. If an index with the same name exists already, an error is returned.

- `ON HASH | ON JSON` (optional): Only keys that match the specified type are included into this index. If omitted, HASH is assumed.

- `PREFIX <prefix-count> <prefix>` (optional): If this clause is specified, then only keys that begin with the same bytes as one or more of the specified prefixes will be included into this index. If this clause is omitted, all keys of the correct type will be included. A zero-length prefix would also match all keys of the correct type.

- `FILTER <expression>` (optional): A boolean expression evaluated for each candidate key during ingestion; only keys for which it evaluates to true are included in the index. A comparison involving a missing field is **false**, so a key that has no `status` field is admitted by neither `@status == 'active'` nor `@status != 'active'` — a negation such as `!(@status == 'active')` admits it instead, because the comparison it negates is false. The number of keys excluded by the filter is reported by `FT.INFO` as `filter_rejected_keys`.

  See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.

  A field's value always reaches the expression as its stored bytes, whatever type the `SCHEMA` declares — a `NUMERIC` field is not a number to a `FILTER`. A comparison is numeric only when one side really is a number, meaning a bare numeric literal such as `5` or a number-returning function such as `strlen()`. So `@price > 100` compares numerically, while `@price > '100'` and `@price > @cost` compare the values as strings, byte by byte. In a numeric comparison a value that is not a number is unordered: `@price != 5` is true for it and every other comparison is false.

  Field references use the `@<name>` syntax. For a `HASH` index the expression may reference a field that is **not** declared in the `SCHEMA`; its value is read directly off the key at ingestion time (an absent field makes the comparison false). Because an undeclared field name is read verbatim from the key, a **misspelled** field name does not produce an error — watch `filter_rejected_keys` in `FT.INFO` to detect this. For a `JSON` index every field referenced by the expression must be declared in the `SCHEMA`; referencing an undeclared field is rejected when the index is created.

- `LANGUAGE <language>` (optional): For text fields, the language used to control lexical parsing and stemming. It selects the stemmer and the default punctuation and stop words. Supported values: `ENGLISH` (default), `ARABIC`, `DUTCH`, `FRENCH`, `GERMAN`, `INDONESIAN`, `ITALIAN`, `PORTUGUESE`, `RUSSIAN`, `SPANISH`, `SWEDISH`, `TURKISH`. Languages other than English require 1.3.0 or later.

- `MINSTEMSIZE <min_stem_size>` (optional): For text fields with stemming enabled. This controls the minimum length of a word, in characters (Unicode code points), required for it to be subjected to stemming. The default value is 4. On English indexes with `search.emulate-release` below 1.3.0, the length is counted in bytes (see COMPATIBILITY.md).

- `WITHOFFSETS | NOOFFSETS` (optional): Enables/Disables the retention of per-word offsets within a text field. Offsets are required to perform exact phrase matching and slop-based proximity matching. Thus if offsets are disabled, those query operations will be rejected with an error. The default is `WITHOFFSETS`.

- `NOHL` (optional): This parameter is accepted for compatibility, but has no effect.

- `NOSTOPWORDS | STOPWORDS <count> <word1> <word2>...` (optional): Stop words are words which are not put into the indexes. The default value of `STOPWORDS`is language dependent. For`LANGUAGE ENGLISH` the default is: [a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, their, then, there, these, they, this, to, was, will, with].

- `PUNCTUATION <punctuation>` (optional): A string of characters that define the separation points between words, in addition to whitespace characters (spaces, tabs, newlines, carriage returns, and control characters) which always break words. Each character, including a multi-byte one such as `—`, is matched as a whole Unicode code point; before 1.3 each byte of a multi-byte character was matched separately, which also split unrelated characters sharing those bytes. The default value depends on `LANGUAGE`; for English it is `,.<>{}[]"':;!@#$%^&\*()-+=~/\|?`.

- `SKIPINITIALSCAN` (optional): If specified, this option skips the normal backfill operation for an index. If this option is specified, pre-existing keys which match the `PREFIX` clause will not be loaded into the index during a backfill operation. This clause has no effect on processing of key mutations _after_ an index is created, i.e., keys which are mutated after an index is created and satisfy the data type and `PREFIX` clause will be inserted into that index.

- `SCORE` (optional): Sets the default document score used for text search ranking. The value must be between 0.0 and 1.0. When `SCORE_FIELD` is configured, this value is used as the fallback if a document's score field is missing or cannot be parsed. (default: 1.0)

- `SCORE_FIELD <field_name>` (optional): Specifies the name of a hash field whose numeric value is used as the per-document score. When configured, the value of this field is read during ingestion and stored as the document's relevance score for text search ranking. If the field is missing or cannot be parsed as a valid number, the index-level `SCORE` default is used. The raw value is stored without clamping; the scoring algorithm determines how to handle values at query time.

## Field types

`TAG`: A tag field is a string that contains one or more tag values.

- `SEPARATOR <sep>` (optional): One of these characters `,.<>{}[]"':;!@#$%^&*()-+=~` used to delimit individual tags. If omitted the default value is `,`.
- `CASESENSITIVE` (optional): If present, tag comparisons will be case sensitive. The default is that tag comparisons are NOT case sensitive.

See [Tag Field Format](../topics/search-data-formats.md#tag-fields) for more details and examples.

`TEXT`: A text field is a string that contains words

- `NOSTEM` (optional): If specified, stemming of words on ingestion is disabled.
- `WITHSUFFIXTRIE | NOSUFFIXTRIE` (optional): Enables/Disables the use of a suffix trie to implement suffix-based wildcard queries. If `NOSUFFIXTRIE` is specified, query strings which specify suffix-based wildcard matching will be rejected with an error. The default is `WITHSUFFIXTRIE`.
- `WEIGHT <weight>` (optional): The current implementation only allows the value to be 1.0. This parameter is accepted to make valkey-search more interoperable with RediSearch. (default: 1.0)

See [Text Field Format](../topics/search-data-formats.md#text-fields) for more details and examples.

`NUMERIC`: A numeric field contains a number.

See [Numeric Field Format](../topics/search-data-formats.md#numeric-fields) for details and examples.

`VECTOR`: A vector field contains a vector. Three vector indexing algorithms are currently supported: HNSW (Hierarchical Navigable Small World), FLAT (brute force) and SVS_VAMANA (a Vamana graph index from Intel's Scalable Vector Search library). Each algorithm has a set of additional attributes, some required and other optional.

- `FLAT:` This algorithm provides exact answers, but has runtime proportional to the number of indexed vectors and thus may not be appropriate for large data sets.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector.
  - `TYPE [FLOAT32 | FLOAT16 | BFLOAT16]` (required): Element data type. `FLOAT16` and `BFLOAT16` halve the memory used per vector at some cost in precision; see [Supported Data Types](../topics/search-data-formats.md#supported-data-types).
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm
  - `INITIAL_CAP <size>` (optional): Initial index size.
- `HNSW:` The HNSW algorithm provides approximate answers, but operates substantially faster than `FLAT`.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector.
  - `TYPE [FLOAT32 | FLOAT16 | BFLOAT16]` (required): Element data type. `FLOAT16` and `BFLOAT16` halve the memory used per vector at some cost in precision; see [Supported Data Types](../topics/search-data-formats.md#supported-data-types).
  - `INITIAL_CAP <size>` (optional): Initial index size.
  - `M <number>` (optional): Number of maximum allowed outgoing edges for each node in the graph in each layer. on layer zero the maximal number of outgoing edges will be 2\*M. Default is 16, the maximum is 512\.
  - `EF_CONSTRUCTION <number>` (optional): controls the number of vectors examined during index construction. Higher values for this parameter will improve recall ratio at the expense of longer index creation times. The default value is 200\. Maximum value is 4096\.
  - `EF_RUNTIME <number>` (optional): controls the number of vectors to be examined during a query operation. The default is 10, and the max is 4096\. You can set this parameter value for each query you run. Higher values increase query times, but improve query recall.
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm.
- `SVS_VAMANA:` A graph index from Intel's [Scalable Vector Search](https://github.com/intel/ScalableVectorSearch) (SVS) library. Like `HNSW` it provides approximate answers, and it can also store the vectors compressed. It requires an x86_64 Linux build; on other platforms `FT.CREATE` with `SVS_VAMANA` returns an error.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector.
  - `TYPE [FLOAT32 | FLOAT16]` (required): Element data type. `BFLOAT16` is not supported.
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm.
  - `INITIAL_CAP <size>` (optional): Expected number of vectors. It sizes the blocks SVS allocates its data and graph in, from 64 KiB up to 16 MiB, so a small index doesn't reserve a full block. Without it, blocks are 16 MiB. The index grows past this size as needed.
  - `GRAPH_MAX_DEGREE <number>` (optional): Maximum number of outgoing edges for each node in the graph. Higher values improve recall at the cost of memory and indexing time. The default is 64 and the minimum is 2.
  - `CONSTRUCTION_WINDOW_SIZE <number>` (optional): Number of candidates examined when inserting a vector. Higher values improve recall at the cost of indexing time. The default is 128. It must be at least `GRAPH_MAX_DEGREE`.
  - `SEARCH_WINDOW_SIZE <number>` (optional): Number of candidates examined during a query. The default is 10. A query can override it with `EF_RUNTIME`. Higher values increase query times, but improve query recall.
  - `ALPHA <number>` (optional): Pruning parameter that decides which edges are kept when the graph is built. It must be at least 1.0 for `L2` and in (0, 1.0] for `IP` and `COSINE`; the defaults are 1.2 and 0.95. Moving it away from 1.0 keeps more long-range edges, which can improve recall at the cost of memory.
  - `COMPRESSION [NONE | FP16]` (optional): How SVS stores the vectors. The default is `NONE`, which stores them in the declared `TYPE`. `FP16` stores `FLOAT32` vectors as 16-bit floats, halving their memory; scores then differ slightly from the exact ones, on the order of 1e-3 for normalized vectors.

  The upper bounds of `GRAPH_MAX_DEGREE`, `CONSTRUCTION_WINDOW_SIZE` and `SEARCH_WINDOW_SIZE` are set by the configs `search.max-vector-m`, `search.max-vector-ef-construction` and `search.max-vector-ef-runtime`, which also bound `HNSW`'s `M`, `EF_CONSTRUCTION` and `EF_RUNTIME`.

  Element storage for each `TYPE` and `COMPRESSION`:

  | `TYPE`    | `COMPRESSION NONE` | `COMPRESSION FP16` |
  | :-------- | :----------------- | :----------------- |
  | `FLOAT32` | 4 bytes            | 2 bytes            |
  | `FLOAT16` | 2 bytes            | 2 bytes            |

See [SVS_VAMANA notes](#svs_vamana-notes) for behavior specific to this algorithm.

See [Vector Field Format](../topics/search-data-formats.md#vector-fields) for more details and examples.

The KNN search algorithm operates to locate vectors that are the nearest to the query vector, i.e., looking for the smallest distance value.
The computation of the distance metrics is adjusted from their classical definitions in order to posses this property.
This table shows the actual computation that Search uses when computing the distance between two vectors: $X$ and $Y$.

| Classical Name | Valkey Search Distance Metric Name |   Classical Distance Formula Definition   | Valkey Search Distance Formula                  |
| :------------: | :--------------------------------: | :---------------------------------------: | :---------------------------------------------- |
| Inner Product  |                 IP                 |                 dot(X,Y)                  | 1 - dot(X,Y)                                    |
|   Euclidean    |                 L2                 |         sqrt(sum((x[i]-y[i])^2))          | sum((x[i]-y[i])^2)                              |
|     Cosine     |               COSINE               | dot(x,y) / (magnitude(X) \* magnitude(Y)) | 1 - (dot(X,Y) / (magnitude(X) \* magnitude(Y))) |

### SVS_VAMANA notes

- **Version:** `SVS_VAMANA` requires 1.3.0 or later. A module older than that refuses to load an RDB file with an `SVS_VAMANA` index.
- **Memory:** SVS allocations count toward `used_memory` and `maxmemory`. On Valkey 9.2 or later they are reported as `used_memory_module_external` in `INFO debug`, and allocations of 2 MiB or more try huge pages first. On earlier Valkey versions they are still counted, but huge pages aren't used.
- **Building:** the index is built when the first vector is added, so an empty `SVS_VAMANA` field allocates no SVS memory.
- **Filtered queries:** SVS applies a filter to batches of candidates after each graph traversal, rather than at every node it visits. A query with a filter can therefore return fewer than `K` results when few vectors match. For very selective filters the query planner evaluates the filter first and computes exact distances instead (see `HYBRID_POLICY` in [Search - query language](../topics/search-query.md)).
- **Persistence:** the index is saved in the RDB file. If the file was written by a different version of the SVS library, the index is rebuilt from the keyspace when it loads.

### Field options

`SORTABLE` (optional): This parameter is not required. Any field may be used with `SORTBY` whether or not it is declared `SORTABLE`. It is recorded on the attribute and reported by [`FT.INFO`](ft.info.md), but does not otherwise affect indexing or query behavior.

- `UNF` (optional): Only valid immediately after `SORTABLE`. In RediSearch this keeps the sort value of a `SORTABLE` field in its original form instead of converting it to lowercase. `SORTBY` already compares the stored field value directly, so this has no effect beyond being reported by [`FT.INFO`](ft.info.md).

## Examples

### HNSW example:

```
FT.CREATE my_index_name SCHEMA my_hash_field_key VECTOR HNSW 10 TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE M 4 EF_CONSTRUCTION 100
```

Result:

```
OK
```

### SVS_VAMANA example:

```
FT.CREATE my_index_name SCHEMA my_hash_field_key VECTOR SVS_VAMANA 10 TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE GRAPH_MAX_DEGREE 32 COMPRESSION FP16
```

Result:

```
OK
```

### FLAT example:

```
FT.CREATE my_index_name SCHEMA my_hash_field_key VECTOR Flat TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE INITIAL_CAP 15000
```

Result:

```
OK
```

### HNSW example with a numeric field:

```
FT.CREATE my_index_name SCHEMA my_vector_field_key VECTOR HNSW 10 TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE M 4 EF_CONSTRUCTION 100 my_numeric_field_key NUMERIC
```

Result:

```
OK
```

### HNSW example with multiple tag and numeric fields

```
FT.CREATE my_index_name SCHEMA my_vector_field_key VECTOR HNSW          \
    10 TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE M 4 EF_CONSTRUCTION   \
    100 my_tag_field_key_1 TAG SEPARATOR '@' CASESENSITIVE              \
    my_numeric_field_key_1 NUMERIC my_numeric_field_key_2 NUMERIC my_tag_field_key_2 TAG
```

Result:

```
OK
```
