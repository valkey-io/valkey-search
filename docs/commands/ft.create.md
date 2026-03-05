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
    [SCORE default_value]
    [LANGUAGE <language>]
    [SKIPINITIALSCAN]
    [MINSTEMSIZE <min_stem_size>]
    [WITHOFFSETS | NOOFFSETS]
    [NOSTOPWORDS | STOPWORDS <count> <word> word ...]
    [PUNCTUATION <punctuation>]
    SCHEMA
        (
            <field-identifier> [AS <field-alias>]
                  NUMERIC
                | TAG [SEPARATOR <sep>] [CASESENSITIVE]
                | TEXT [NOSTEM] [WITHSUFFIXTRIE | NOSUFFIXTRIE] [WEIGHT <weight>]
                | VECTOR [HNSW | FLAT] <attr_count> [<attribute_name> <attribute_value>]+
            [SORTABLE]
        )+
```

- `<index-name>` (required): This is the name you give to your index. If an index with the same name exists already, an error is returned.

- `ON HASH | ON JSON` (optional): Only keys that match the specified type are included into this index. If omitted, HASH is assumed.

- `PREFIX <prefix-count> <prefix>` (optional): If this clause is specified, then only keys that begin with the same bytes as one or more of the specified prefixes will be included into this index. If this clause is omitted, all keys of the correct type will be included. A zero-length prefix would also match all keys of the correct type.

- `LANGUAGE <language>` (optional): For text fields, the language used to control lexical parsing and stemming. Currently only the value `ENGLISH` is supported.

- `MINSTEMSIZE <min_stem_size>` (optional): For text fields with stemming enabled. This controls the minimum length of a word required for it to be subjected to stemming. The default value is 4.

- `WITHOFFSETS | NOOFFSETS` (optional): Enables/Disables the retention of per-word offsets within a text field. Offsets are required to perform exact phrase matching and slop-based proximity matching. Thus if offsets are disabled, those query operations will be rejected with an error. The default is `WITHOFFSETS`.

- `NOSTOPWORDS | STOPWORDS <count> <word1> <word2>...` (optional): Stop words are words which are not put into the indexes. The default value of `STOPWORDS`is language dependent. For`LANGUAGE ENGLISH` the default is: [a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, their, then, there, these, they, this, to, was, will, with].

- `PUNCTUATION <punctuation>` (optional): A string of characters that define the separation points between words, in addition to whitespace characters (spaces, tabs, newlines, carriage returns, and control characters) which always break words. The default value is `,.<>{}[]"':;!@#$%^&\*()-+=~/\|?`.

- `SKIPINITIALSCAN` (optional): If specified, this option skips the normal backfill operation for an index. If this option is specified, pre-existing keys which match the `PREFIX` clause will not be loaded into the index during a backfill operation. This clause has no effect on processing of key mutations _after_ an index is created, i.e., keys which are mutated after an index is created and satisfy the data type and `PREFIX` clause will be inserted into that index.

- `SCORE` (optional): The current implementation only allows the value to be 1.0. This parameter is accepted to make valkey-search more interoperable with RediSearch. (default: 1.0)

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

`VECTOR`: A vector field contains a vector. Two vector indexing algorithms are currently supported: HNSW (Hierarchical Navigable Small World) and FLAT (brute force). Each algorithm has a set of additional attributes, some required and other optional.

- `FLAT:` This algorithm provides exact answers, but has runtime proportional to the number of indexed vectors and thus may not be appropriate for large data sets.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector.
  - `TYPE FLOAT32` (required): Data type, currently only FLOAT32 is supported.
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm
  - `INITIAL_CAP <size>` (optional): Initial index size.
- `HNSW:` The HNSW algorithm provides approximate answers, but operates substantially faster than `FLAT`.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector.
  - `TYPE FLOAT32` (required): Data type, currently only FLOAT32 is supported.
  - `INITIAL_CAP <size>` (optional): Initial index size.
  - `M <number>` (optional): Number of maximum allowed outgoing edges for each node in the graph in each layer. on layer zero the maximal number of outgoing edges will be 2\*M. Default is 16, the maximum is 512\.
  - `EF_CONSTRUCTION <number>` (optional): controls the number of vectors examined during index construction. Higher values for this parameter will improve recall ratio at the expense of longer index creation times. The default value is 200\. Maximum value is 4096\.
  - `EF_RUNTIME <number>` (optional): controls the number of vectors to be examined during a query operation. The default is 10, and the max is 4096\. You can set this parameter value for each query you run. Higher values increase query times, but improve query recall.
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm.

See [Vector Field Format](../topics/search-data-formats.md#vector-fields) for more details and examples.

The KNN search algorithm operates to locate vectors that are the nearest to the query vector, i.e., looking for the smallest distance value.
The computation of the distance metrics is adjusted from their classical definitions in order to posses this property.
This table shows the actual computation that Search uses when computing the distance between two vectors: $X$ and $Y$.

| Classical Name | Valkey Search Distance Metric Name |   Classical Distance Formula Definition   | Valkey Search Distance Formula                  |
| :------------: | :--------------------------------: | :---------------------------------------: | :---------------------------------------------- |
| Inner Product  |                 IP                 |                 dot(X,Y)                  | 1 - dot(X,Y)                                    |
|   Euclidean    |                 L2                 |          sqrt(sum(x[i]-y[i])^2)           | sqrt(sum(x[i]-y[i])^2)                          |
|     Cosine     |               COSINE               | dot(x,y) / (magnitude(X) \* magnitude(Y)) | 1 - (dot(X,Y) / (magnitude(X) \* magnitude(Y))) |

### Field options

`SORTABLE` (optional): This parameter is accepted for compatibility, but has no effect and is not required.

## Examples

### HNSW example:

```
FT.CREATE my_index_name SCHEMA my_hash_field_key VECTOR HNSW 10 TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE M 4 EF_CONSTRUCTION 100
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
