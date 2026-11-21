The `FT.CREATE` command creates an empty index and may initiate the backfill process. Each index consists of a number of
field definitions. Each field definition specifies a field name, a field type and a path within each indexed key to
locate a value of the declared type. Some field type definitions have additional sub-type specifiers.

For indexes on `HASH` keys, the default path is the same as the hash member name. The optional `AS` clause can be used to rename
the field if desired.

For indexes on `JSON` keys, the path is a `JSON` path to the data of the declared type. The `AS` clause is required in order to provide a field name without the special characters of a `JSON` path.

The same source field may be indexed more than once, for example as both `TEXT` and `TAG`, as long as each entry has a distinct `AS` alias. Aliases must be unique within an index. An alias, or the field identifier when `AS` is omitted, may not contain any of the characters `,.<>{}[]"':;!@#$%^&*()-+=~/\|?` (`_` is allowed).

```
FT.CREATE <index-name>
    [ON HASH | ON JSON]
    [PREFIX <count> <prefix> [<prefix>...]]
    [FILTER <expression>]
    [SCORE <default_score>]
    [SCORE_FIELD <field_name>]
    [LANGUAGE <language>]
    [SKIPINITIALSCAN]
    [MINSTEMSIZE <min_stem_size>]
    [NOSTEM]
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
                | VECTOR [HNSW | FLAT] <attr_count> [<attribute_name> <attribute_value>]+
            [SORTABLE [UNF]]
        )+
```

`ON`, if given, must immediately follow the index name. The other options before `SCHEMA` may appear in any order. `PREFIX` may be given only once.

- `<index-name>` (required): This is the name you give to your index. If an index with the same name exists already, an error is returned.

- `ON HASH | ON JSON` (optional): Only keys that match the specified type are included into this index. If omitted, HASH is assumed.

- `PREFIX <prefix-count> <prefix>` (optional): If this clause is specified, then only keys that begin with the same bytes as one or more of the specified prefixes will be included into this index. If this clause is omitted, all keys of the correct type will be included. A zero-length prefix would also match all keys of the correct type.

- `FILTER <expression>` (optional): A boolean expression evaluated for each candidate key during ingestion; only keys for which it evaluates to true are included in the index. An empty expression is rejected. A comparison that involves a missing field is **false**. So a key with no `status` field matches neither `@status == 'active'` nor `@status != 'active'`. Use `!(@status == 'active')` to include such keys. The number of keys excluded by the filter is reported by `FT.INFO` as `filter_rejected_keys`.

  FILTER expressions always use 1.3.0 expression semantics, regardless of `search.emulate-release`. An index with a `FILTER` records a minimum module version of 1.3.0, so a module older than 1.3.0 refuses to load it.

  ```
  FT.CREATE fr ON HASH PREFIX 1 product: FILTER "@lang == 'french'" LANGUAGE FRENCH SCHEMA title TEXT
  ```

  See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.

  For a `HASH` index, a field's value reaches the expression as its stored bytes, whatever type the `SCHEMA` declares — a `NUMERIC` field is not a number to a `FILTER`. For a `JSON` index, a `NUMERIC` field is a number, so `@price > @cost` compares numerically; the rest of this paragraph describes `HASH` indexes. A comparison is numeric only when one side really is a number, meaning a bare numeric literal such as `5` or a number-returning function such as `strlen()`. So `@price > 100` compares numerically, while `@price > '100'` and `@price > @cost` compare the values as strings, byte by byte. If one side is a number and the other is a string that is not a number, `==` is false and `!=` is true. `<`, `<=`, `>` and `>=` are an evaluation error, which rejects the key even when the comparison is negated: `!(@price > 100)` does not admit a key whose `price` is not a number.

  Field references use the `@<name>` syntax. For a `HASH` index the expression may reference a field that is **not** declared in the `SCHEMA`; its value is read directly off the key at ingestion time (an absent field makes the comparison false). Because an undeclared field name is read verbatim from the key, a **misspelled** field name does not produce an error — watch `filter_rejected_keys` in `FT.INFO` to detect this. For a `JSON` index every field referenced by the expression must be declared in the `SCHEMA`; referencing an undeclared field is rejected when the index is created.

- `LANGUAGE <language>` (optional): For `TEXT` fields, the language used to control lexical parsing and stemming. It selects the stemmer and the default punctuation and stop words. Supported values: `ENGLISH` (default), `ARABIC`, `DUTCH`, `FRENCH`, `GERMAN`, `INDONESIAN`, `ITALIAN`, `PORTUGUESE`, `RUSSIAN`, `SPANISH`, `SWEDISH`, `TURKISH`. Languages other than English require 1.3.0 or later: an index with a non-English `LANGUAGE` cannot be loaded by, or replicated to, modules before 1.3.0. `LANGUAGE_FIELD` is not supported and is rejected.

- `MINSTEMSIZE <min_stem_size>` (optional): For text fields with stemming enabled. This controls the minimum length of a word, in characters (Unicode code points), required for it to be subjected to stemming. The value must be positive; the default value is 4. On English indexes with `search.emulate-release` below 1.3.0, the length is counted in bytes (see COMPATIBILITY.md).

- `NOSTEM` (optional): Before `SCHEMA`, disables stemming for every `TEXT` field of the index.

- `WITHOFFSETS | NOOFFSETS` (optional): Enables/Disables the retention of per-word offsets within a text field. Offsets are required to perform exact phrase matching and slop-based proximity matching. Thus if offsets are disabled, those query operations will be rejected with an error. The default is `WITHOFFSETS`.

- `NOHL` (optional): This parameter is accepted for compatibility, but has no effect.

- `NOSTOPWORDS | STOPWORDS <count> <word1> <word2>...` (optional): Stop words are words which are not put into the indexes. The default value of `STOPWORDS` is language dependent. For `LANGUAGE ENGLISH` the default is: [a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, the, their, then, there, these, they, this, to, was, will, with]. Run `FT.INFO` to see the effective `stop_words` and `punctuation` of an index.

- `PUNCTUATION <punctuation>` (optional): A string of characters that define the separation points between words, in addition to whitespace characters (spaces, tabs, newlines, carriage returns, and control characters) which always break words. Each character, including a multi-byte one such as `—`, is matched as a whole Unicode code point; before 1.3 each byte of a multi-byte character was matched separately, which also split unrelated characters sharing those bytes. The default value depends on `LANGUAGE`; for English it is `,.<>{}[]"':;!@#$%^&*()-+=~/\|?`.

- `SKIPINITIALSCAN` (optional): If specified, this option skips the normal backfill operation for an index. If this option is specified, pre-existing keys which match the `PREFIX` clause will not be loaded into the index during a backfill operation. This clause has no effect on processing of key mutations _after_ an index is created, i.e., keys which are mutated after an index is created and satisfy the data type and `PREFIX` clause will be inserted into that index.

- `SCORE <default_score>` (optional): Sets the default document score used for text search ranking. The value must be between 0.0 and 1.0 inclusive (default 1.0). When `SCORE_FIELD` is configured, this value is used as the fallback if a document's score field is missing or cannot be parsed.

- `SCORE_FIELD <field_name>` (optional): Specifies the field whose numeric value is used as the per-document score: a hash field name for a `HASH` index, or a JSON path for a `JSON` index. The field need not be declared in `SCHEMA`. When configured, the value of this field is read during ingestion and stored as the document's relevance score for text search ranking. If the field is missing or cannot be parsed as a valid number, the index-level `SCORE` default is used. The raw value is stored without clamping. See [Search - scoring](../topics/search-scoring.md) for how it is used at query time.

## Field types

`TAG`: A tag field is a string that contains one or more tag values.

- `SEPARATOR <sep>` (optional): A single byte (one ASCII character) used to delimit individual tags. A multi-byte character is rejected. If omitted the default value is `,`.
- `CASESENSITIVE` (optional): If present, tag comparisons will be case sensitive. The default is that tag comparisons are NOT case sensitive.

See [Tag Field Format](../topics/search-data-formats.md#tag-fields) for more details and examples.

`TEXT`: A text field is a string that contains words

- `NOSTEM` (optional): If specified, stemming of words on ingestion is disabled.
- `WITHSUFFIXTRIE | NOSUFFIXTRIE` (optional): Enables/Disables the use of a suffix trie to implement suffix-based wildcard queries. If `NOSUFFIXTRIE` is specified, query strings which specify suffix-based wildcard matching will be rejected with an error. The default is `NOSUFFIXTRIE`; specify `WITHSUFFIXTRIE` to enable suffix (`*word`) queries.
- `WEIGHT <weight>` (optional): The current implementation only allows the value to be 1.0. This parameter is accepted to make valkey-search more interoperable with RediSearch. (default: 1.0)

See [Text Field Format](../topics/search-data-formats.md#text-fields) for more details and examples.

`NUMERIC`: A numeric field contains a number.

See [Numeric Field Format](../topics/search-data-formats.md#numeric-fields) for details and examples.

`VECTOR`: A vector field contains a vector. Two vector indexing algorithms are currently supported: HNSW (Hierarchical Navigable Small World) and FLAT (brute force). Each algorithm has a set of additional attributes, some required and other optional.

- `FLAT:` This algorithm provides exact answers, but has runtime proportional to the number of indexed vectors and thus may not be appropriate for large data sets.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector, from 1 to `search.max-vector-dimensions` (default 32768).
  - `TYPE [FLOAT32 | FLOAT16 | BFLOAT16]` (required): Element data type. `FLOAT16` and `BFLOAT16` halve the memory used per vector at some cost in precision; see [Supported Data Types](../topics/search-data-formats.md#supported-data-types).
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm
  - `INITIAL_CAP <size>` (optional): Initial index size. The default is 10240; the range is 1 to 10000000.
  - `BLOCK_SIZE <size>` (optional): Number of vectors by which the index capacity grows when it is full. The default is 1024; the range is 1 to 10000000.
- `HNSW:` The HNSW algorithm provides approximate answers, but operates substantially faster than `FLAT`.
  - `DIM <number>` (required): Specifies the number of dimensions in a vector, from 1 to `search.max-vector-dimensions` (default 32768).
  - `TYPE [FLOAT32 | FLOAT16 | BFLOAT16]` (required): Element data type. `FLOAT16` and `BFLOAT16` halve the memory used per vector at some cost in precision; see [Supported Data Types](../topics/search-data-formats.md#supported-data-types).
  - `INITIAL_CAP <size>` (optional): Initial index size. The default is 10240; the range is 1 to 10000000.
  - `M <number>` (optional): Number of maximum allowed outgoing edges for each node in the graph in each layer. on layer zero the maximal number of outgoing edges will be 2\*M. Default is 16, the minimum is 2. The maximum is set by `search.max-vector-m`.
  - `EF_CONSTRUCTION <number>` (optional): controls the number of vectors examined during index construction. Higher values for this parameter will improve recall ratio at the expense of longer index creation times. The default value is 200. The maximum is set by `search.max-vector-ef-construction`.
  - `EF_RUNTIME <number>` (optional): Controls HNSW search effort. Higher values can improve recall at the expense of longer query times. The default is 10. The maximum is set by `search.max-vector-ef-runtime`. A query can override the field value.
  - `DISTANCE_METRIC [L2 | IP | COSINE]` (required): Specifies the distance algorithm.

See [Vector Field Format](../topics/search-data-formats.md#vector-fields) for more details and examples.

The KNN search algorithm finds the vectors nearest to the query vector, which are the vectors with the smallest distance.
For the distance formula of each metric, see [Vector Distance](../topics/search-scoring.md#vector-distance).

### Field options

`SORTABLE` (optional): This parameter is not required. Any field may be used with `SORTBY` whether or not it is declared `SORTABLE`. It is recorded on the attribute and reported by [`FT.INFO`](ft.info.md), but does not otherwise affect indexing or query behavior. `SORTABLE` on a `VECTOR` field is accepted but has no effect and is not reported.

- `UNF` (optional): Only valid immediately after `SORTABLE`. In RediSearch this keeps the sort value of a `SORTABLE` field in its original form instead of converting it to lowercase. `SORTBY` never converts the sort value to lowercase, so this has no effect beyond being reported by [`FT.INFO`](ft.info.md). It is accepted on any field type, but reported by `FT.INFO` only for `TAG` and `TEXT`.

## Reply

`OK`.

If an error occurs during the index creation, an error is returned.

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
FT.CREATE my_index_name SCHEMA my_hash_field_key VECTOR FLAT 8 TYPE FLOAT32 DIM 20 DISTANCE_METRIC COSINE INITIAL_CAP 15000
```

Result:

```
OK
```

### Same field indexed as TEXT and TAG:

```
FT.CREATE my_index_name SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG
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
