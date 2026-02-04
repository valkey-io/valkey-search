# Command List

- [`FT.AGGREGATE`](#ftaggregate)
- [`FT.CREATE`](#ftcreate)
- [`FT.DROPINDEX`](#ftdropindex)
- [`FT.INFO`](#ftinfo)
- [`FT._LIST`](#ft_list)
- [`FT.SEARCH`](#ftsearch)

#

## FT.AGGREGATE

The `FT.AGGREGATE` command extends the query capabilities of the `FT.SEARCH` command with substantial server-side data processing capabilities. Just as with the `FT.SEARCH` command, the query string is used to locate a set of Valkey keys. Fields from those keys are loaded and processed according to a list of stages specified on the command. Each stage is processed sequentially in the order found on the command with the output of each stage becoming the input to the next stage. Once all of the stages have been processed the resulting data set is returned as the result of the command.

```
FT.AGGREGATE <index-name> <query>
      TIMEOUT <timeout>
    | PARAMS <count> <name> <value> [ <name1> <value1> ...]
    | DIALECT <dialect>
    | LOAD [* | <count> <field1> <field2> ...]
    (
      | APPLY <expression> AS <field>
      | FILTER <expression>
      | LIMIT <offset> <count>
      | SORTBY <count> <field1> [ASC | DESC] <field2 [ASC | DESC] ... [MAX <num>]
      | GROUPBY <count> <field1> <field2> ... [REDUCE <reducer> <count> <arg1> <arg2> ...]*
    )+
```

- **\<index\>** (required): This index name you want to query.
- **\<query\>** (required): The query string, see below for details.
- **TIMEOUT \<timeout\>** (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- **PARAMS \<count\> \<name1\> \<value1\> \<name2\> \<value2\> ...** (optional): `count` is of the number of arguments, i.e., twice the number of value name pairs. See the query string for usage details.
- **DIALECT \<dialect\>** (optional): Specifies your dialect. The only supported dialect is 2.
- **LOAD [* | \<count\> \<field1\> \<field2\> ...]** (optional): The fields of the located keys to be loaded into the aggregation pipeline. The star (\*) indicates that all of the fields of the keys are loaded.
- **APPLY \<expression\> as \<field\>** (optional): For each key of input, the expression is computed and stored into the named field. See <?> for details on the expression syntax.
- **FILTER \<expression\>** (optional): The expression is evaluated for each key of input. Only those keys for which the expression evaluates to true are included in the output. See <?> for details on the expression syntax.
- **LIMIT \<offset\> \<count\>** (optional): The first \<offset\> number of records are discarded. Up to \<count\> records are saved and then the remainder of the records are discarded.
- **SORTBY \<count\> \<field1\> [ASC | DESC] \<field2 [ASC | DESC] ... [MAX \<num\>]** (optional): The input records are sorted according to the specified fields and directions. If the MAX option is specified, only the first \<num\> records after sorting are included in the output.
- **GROUPBY \<count\> \<field1\> \<field2\> ... [REDUCE \<reducer\> \<count\> \<arg1\> \<arg2\> ...]\*** (optional): The input records are grouped according to the specified fields. For each group, the specified reducers are computed and included in the output records. Multiple reducers can be specified and are efficiently computed. See <?> for a list of reducer functions.

## FT.CREATE

The `FT.CREATE` command creates an empty index and may initiate a backfill process. Each index consists of a number of field definitions. Each field definition specifies a field name, a field type and a path within each indexed key to locate a value of the declared type. Some field type definitions have additional sub-type specifiers.

For indexes on HASH keys, the path is the hash member name. The optional `AS` clause provides an explicit field name which will be the same as the path if omitted. Renaming of fields is required when the member name contains special characters.

For indexes on `JSON` keys, the path is a `JSON` path to the data of the declared type. The `AS` clause is required in order to provide a field name without the special characters of a `JSON` path.

An index may contain any number of fields. Each field has a type and set of associated query operations that are specific to that type.

### Field Types

- **TAG**: A tag field is a string that contains one or more tag values. Query operations can be match against one or more tags. Tag matching may be prefix-based, i.e., searching for tags that have a fixed starting sequence of characters with

- **TEXT**: A text field is a string that contains a sequence of words. Text fields offer a rich set of matching operators that operate on one or more words. See <text fields> for more detail on the processing of text fields.

- **NUMERIC**: A numeric field contains a number. For HASH indexes, the number is stored as a string in "C" language format for a floating point number without a trailing size label like "f" or "d". For JSON indexes, this field can be either a string or a number.

- **VECTOR**: A vector field contains a vector. Two vector indexing algorithms are currently supported: HNSW (Hierarchical Navigable Small World) and FLAT (brute force). Each algorithm has a set of additional attributes, some required and others optional.

### Index Creation

Valkey Search requires that an index definition be distributed to all nodes. Within a shard, this is handled by the normal Valkey replication machinery, i.e., index definitions are subject to replication delay.

```bash
FT.CREATE <index-name>
    ON HASH
    [PREFIX <count> <prefix> <prefix>...]
    [LANGUAGE <language>]
    [SKIPINITIALSCAN]
    [MINSTEMSIZE <min_stem_size>]
    SCHEMA
        (
            <field-path> [AS <field-name>]
                  NUMERIC
                | TAG [SEPARATOR <sep>] [CASESENSITIVE]
                | TEXT [PUNCTUATION <punctuation>] [WITHOFFSETS | NOOFFSETS] [NOSTEM] [NOSTOPWORDS | STOPWORDS <word_count> [<word>]+ ] [WITHSUFFIXTRIE | NOSUFFIXTRIE]
                | VECTOR [HNSW | FLAT] <attr_count> [<attribute_name> <attribute_value>]+
            [SORTABLE]
        )+
```

- **\<index-name\>** (required): This is the name you give to your index. If an index with the same name exists already, an error is returned.

- **ON HASH | JSON** (optional): Only keys that match the specified type are included into this index. If omitted, HASH is assumed.

- **PREFIX \<prefix-count\> \<prefix\>** (optional): If this clause is specified, then only keys that begin with the same bytes as one or more of the specified prefixes will be included into this index. If this clause is omitted, all keys of the correct type will be included. A zero-length prefix would also match all keys of the correct type and is the default value.

- **LANGUAGE <language>** (optional): For text fields, the language used to control lexical parsing and stemming. Currently only the value `ENGLISH` is supported.

- **MINSTEMSIZE \<min_stem_size\>** (optional): For text fields with stemming enabled. This controls the minimum length of a word before it is subjected to stemming. The default value is <?>.

- **SKIPINITIALSCAN** (optional): If specific, this option skips the normal backfill operation for an index. If this option is specified, pre-existing keys which match the `PREFIX` clause will not be loaded into the index during a backfill operation. This clause has no effect on processing of key mutations _after_ an index is created, i.e., keys which are mutated after an index is created and satisfy the data type and `PREFIX` clause will be inserted into that index.

## Fields

The `SCHEMA` keyword identifies the start of the field declarations. Each field has a path, a name and a data type. Some data types allow additional information to be supplied to further refine the definition of the index for that field.

- **\<field-path\>** (required): This specifies the location of the data for the field with the key. For `HASH` indexes, the location is just the member name. For `JSON` indexes, it is the full JSON Path to the data. See <??? TBD> for a description of the allowed subset of JSON path.

- **\<field-name\>** (optional for `HASH` indexes, required for `JSON` indexes): This specifies the name used to refer to this field within the search module, e.g., it is used in the query and aggregation expression context to refer to this field of a key. For `JSON` indexes, this must be supplied. Field names are syntactically restricted <??? TBD>.

- **\<field-type\>** (Required): This contains one of `NUMERIC`, `TAG`, `TEXT`, or `VECTOR`. Type-specific declarations may follow this field. Some types require additional declarations, others do not, see below for type-specific declarators.

- **SORTABLE** (optional): This parameter is currently ignored as all field types are considered to be sortable

### Tag Field Specific Declarators

- **SEPARATOR \<sep\>** (optional): One of these characters `,.<>{}[]"':;!@#$%^&*()-+=~` used to delimit individual tags. If omitted the default value is `,`.
- **CASESENSITIVE** (optional): If present, tag comparisons will be case-sensitive. The default is that tag comparisons are NOT case-sensitive

### Text Field Specific Declarators

- **PUNCTUATION \<punctuation\>** (optional): A string of characters that are used to define words in the text field.Punctuation characters are restricted to the characters: <?>. The default value is "<????>".
- **WITHOFFSETS | NOOFFSETS** (optional): Enables/Disables the retention of per-word offsets within a text field. Offsets are required to perform exact phrase matching and slop-based proximity matching. Thus if offsets are disabled, those query operations will be rejected with an error. The default is `WITHOFFSETS`.
- **NOSTOPWORDS | STOPWORDS \<count\> \<word1\> \<word2\>...** (optional): Stop words are not words which are not put into the indexes. The default value of `STOPWORDS` is language dependent. For `LANGUAGE ENGLISH` the default is: <?>.
- **NOSTEM** (optional): If specified, stemming of words on ingestion is disabled.
- **WITHSUFFIXTRIE | NOSUFFIXTRIE** (optional): Enables/Disables the use of a suffix trie to implement suffix-based wildcard queries. If `NOSUFFIXTRIE` is specified, query strings which specify suffix-based wildcard matching will be rejected with an error. The default is `WITHSUFFIXTRIE`.

### Vector Field Specific Declarators

- **FLAT:** The Flat algorithm provides exact answers, but has runtime proportional to the number of indexed vectors and thus may not be appropriate for large data sets.
  - **DIM \<number\>** (required): Specifies the number of dimensions in a vector.
  - **TYPE FLOAT32** (required): Data type, currently only FLOAT32 is supported.
  - **DISTANCE_METRIC \[L2 | IP | COSINE\]** (required): Specifies the distance algorithm
  - **INITIAL_CAP \<size\>** (optional): Initial index size.
- **HNSW:** The HNSW algorithm provides approximate answers, but operates substantially faster than FLAT.
  - **DIM \<number\>** (required): Specifies the number of dimensions in a vector.
  - **TYPE FLOAT32** (required): Data type, currently only FLOAT32 is supported.
  - **DISTANCE_METRIC \[L2 | IP | COSINE\]** (required): Specifies the distance algorithm
  - **INITIAL_CAP \<size\>** (optional): Initial index size.
  - **M \<number\>** (optional): Number of maximum allowed outgoing edges for each node in the graph in each layer. on layer zero the maximal number of outgoing edges will be 2\*M. Default is 16, the maximum is 512\.
  - **EF_CONSTRUCTION \<number\>** (optional): controls the number of vectors examined during index construction. Higher values for this parameter will improve recall ratio at the expense of longer index creation times. The default value is 200\. Maximum value is 4096\.
  - **EF_RUNTIME \<number\>** (optional): controls the number of vectors to be examined during a query operation. The default is 10, and the max is 4096\. You can set this parameter value for each query you run. Higher values increase query times, but improve query recall.

**RESPONSE** OK or Error.

An OK response indicates that the index has been successfully created in an empty state and is immediately available for operations. If SKIPINITIALSCAN was not specified, then a backfill operation is started.

A first class of errors is directly associated with the command itself, for example a syntax error or an attempt to create an index with a name which already exists.

In CME a second class of errors is related to a failure to properly distribute the command across all of the nodes of the cluster.

## FT.DROPINDEX

```
FT.DROPINDEX <index-name>
```

The specified index is deleted. It is an error if that index doesn't exist.

- **\<index-name\>** (required): The name of the index to delete.

**RESPONSE** OK or error.

## FT.INFO

### CMD Mode

When cluster mode is disable, information about the specified index is returned from the executing node:

```
FT.INFO <index-name>
```

- **\<index-name\>** (required): The name of the index to return information about.

### CME Mode

When cluster mode is enabled, additional options are available to direct the executing node to contact other nodes in the cluster to generate aggregated index information. Operations which require responses from other nodes create additional complexity. Two situations additional situations are handled.

First, if a response from a designed node is not received within a fixed time window the `FT.INFO` operation still completes. Typically this is due to either a network partition or a node failure. Options are provided to control whether this is considered an error or whether a best-effort result should be returned (no error).

Second, because index metadata is eventually consistent (see [? metadata consistency protocol] for more information) it's possible that a contacted node generates a response but from a different version of the index. This situation is detected and the query is repeated until either a consistent response is obtained or a time window has expired. Options are provided to control whether this command is complete with an error or a result generated only from the consistent responses should be returned (no error).

```
FT.INFO <index-name>
  (
      [LOCAL | PRIMARY | CLUSTER]
    | [ALLSHARDS | SOMESHARDS]
    | [CONSISTENT INCONSISTENT]
  )+
```

- **LOCAL** (optional): Only the executing (local) node contributes index information. No other nodes in the cluster are contacted to generate index information. This is the default.
- **PRIMARY** (optional): The primary nodes of every shard in the cluster are queried to generate index information.
- **CLUSTER** (optional): All nodes, primary or replica, are queried to generate index information.

- **ALLSHARDS** (optional): If specified, a response is required from every shard in the system. If all responses are not received within a time window the command is terminated with an error. This is the default.
- **SOMESHARDS** (optional): If specified, a response is NOT required from every shard in the system. If all responses are not received within the time window, only the received responses are returned and no error is generated.

- **CONSISTENT** (optional): If specified, the command is terminated with an error if any received response isn't from a consistent version of the index. This is the default.
- **INCONSISTENT** (optional): If specified, a command result is generated using only the responses from nodes with a consistent version of the index.

**RESPONSE**

LOCAL: An array of key value pairs.

- **index_name** (string) The index name
- **num_docs** (integer) Total keys in the index
- **num_records** (integer) Total records in the index
- **hash_indexing_failures** (integer) Count of unsuccessful indexing attempts
- **indexing** (integer) Shows if background indexing is running(1) or not(0).
- **percent_indexed** (integer) Progress of background indexing. Percentage is expressed as a value from 0 to 1
- [?] LANGUAGE, MINSTEMSIZE, SKIPINITIALSCAN ??? [?]
- **index_definition** (array) An array of key/value pairs defining the index
  - **key_type** (string) `HASH` or `JSON`.
  - **prefixes** (array of strings) Prefixes for keys. If no prefixes were specified this will be a 0-length array.
  - **default_score** (integer) This is the default scoring value.
  - **attributes** (array of key/value pairs) Each declared field occupies one element of the array. The key/value pairs of the array define that field.
    - **identifier** (string). The location with the key for this field. For HASH indexes this is the member name. For JSON indexes this is the JSON path.
    - **attribute** (string) The name used to refer to this index in query and aggregation expressions. When the `AS` keyword is provided this is the
    - **type** (string) VECTOR. This is the only available type. [???????]
    - **index** (array of key/value pairs) Extended information about this

    - **capacity** (integer) The current capacity for the total number of vectors that the index can store.
    - **dimensions** (integer) Dimension count
    - **distance_metric** (string) Possible values are L2, IP or Cosine
    - **data_type** (string) FLOAT32. This is the only available data type
    - **algorithm** (array) Information about the algorithm for this field.
      - **name** (string) `HNSW` or `FLAT`
      - **m** (integer) The count of maximum permitted outgoing edges for each node in the graph in each layer. The maximum number of outgoing edges is 2\*M for layer 0\. The Default is 16\. The maximum is 512\.
      - **ef_construction** (integer) The count of vectors in the index. The default is 200, and the max is 4096\. Higher values increase the time needed to create indexes, but improve the recall ratio.
      - **ef_runtime** (integer) The count of vectors to be examined during a query operation. The default is 10, and the max is 4096\.

PRIMARY: An array of key value pairs

- **mode** (string) The FT.INFO mode, should be PRIMARY
- **index_name** (string) The index name
- **num_docs** (string) INTEGER. Total keys in the index
- **num_records** (string) INTEGER. Total records in the index
- **hash_indexing_failures** (string) INTEGER. Count of unsuccessful indexing attempts

CLUSTER: An array of key value pairs

- **mode** (string) The FT.INFO mode, should be CLUSTER
- **index_name** (string) The index name
- **backfill_in_progress** (string) 0 or 1. Is backfill in progress
- **backfill_complete_percent_max** (string) FLOAT32. Maximum backfill complete percent in all nodes
- **backfill_complete_percent_min** (string) FLOAT32. Minimum backfill complete percent in all nodes
- **state** (string) The current state of the index. ready, backfill_in_progress or backfill_paused_by_oom

## FT.\_LIST

```
FT._LIST
```

Lists the currently defined indexes.

**RESPONSE**

An array of strings which are the currently defined index names.

## FT.SEARCH

```
FT.SEARCH <index> <query>
  [NOCONTENT]
  [TIMEOUT <timeout>]
  [PARAMS <count> <name> <value> [ <name> <value> ...]]
  [LIMIT <offset> <num>]
  [DIALECT <dialect>]
```

Performs a search of the specified index. The keys which match the query expression are returned.

- **\<index\>** (required): This index name you want to query.
- **\<query\>** (required): The query string, see below for details.
- **NOCONTENT** (optional): When present, only the resulting key names are returned, no key values are included.
- **TIMEOUT \<timeout\>** (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- **PARAMS \<count\> \<name1\> \<value1\> \<name2\> \<value2\> ...** (optional): `count` is of the number of arguments, i.e., twice the number of value name pairs. See the query string for usage details.
- **RETURN \<count\> \<field1\> \<field2\> ...** (options): `count` is the number of fields to return. Specifies the fields you want to retrieve from your documents, along with any aliases for the returned values. By default, all fields are returned unless the NOCONTENT option is set, in which case no fields are returned. If num is set to 0, it behaves the same as NOCONTENT.
- **LIMIT \<offset\> \<count\>** (optional): Lets you choose a portion of the result. The first `<offset>` keys are skipped and only a maximum of `<count>` keys are included. The default is LIMIT 0 10, which returns at most 10 keys.
- **DIALECT \<dialect\>** (optional): Specifies your dialect. The only supported dialect is 2\.

**RESPONSE**

The command returns either an array if successful or an error.

On success, the first entry in the response array represents the count of matching keys, followed by one array entry for each matching key.
Note that if the `LIMIT` option is specified it will only control the number of returned keys and will not affect the value of the first entry.

When `NOCONTENT` is specified, each entry in the response contains only the matching keyname. Otherwise, each entry includes the matching keyname, followed by an array of the returned fields.

The result fields for a key consists of a set of name/value pairs. The first name/value pair is for the distance computed. The name of this pair is constructed from the vector field name prepended with "\_\_" and appended with "\_score" and the value is the computed distance. The remaining name/value pairs are the members and values of the key as controlled by the `RETURN` clause.

The query string conforms to this syntax:

```
<filtering>=>[ KNN <K> @<vector_field_name> $<vector_parameter_name> <query-modifiers> ]
```

Where:

- **\<filtering\>** Is either a `*` or a filter expression. A `*` indicates no filtering and thus all vectors within the index are searched. A filter expression can be provided to designate a subset of the vectors to be searched.
- **\<vector_field_name\>** The name of a vector field within the specified index.
- **\<K\>** The number of nearest neighbor vectors to return.
- **\<vector_parameter_name\>** A PARAM name whose corresponding value provides the query vector for the KNN algorithm. Note that this parameter must be encoded as a 32-bit IEEE 754 binary floating point in little-endian format.
- **\<query-modifiers\>** (Optional) A list of keyword/value pairs that modify this particular KNN search. Currently two keywords are supported:
  - **EF_RUNTIME** This keyword is accompanied by an integer value which overrides the default value of **EF_RUNTIME** specified when the index was created.
  - **AS** This keyword is accompanied by a string value which becomes the name of the score field in the result, overriding the default score field name generation algorithm.

**Filter Expression**

A filter expression is constructed as a logical combination of Tag and Numeric search operators contained within parenthesis.

**Tag**

The tag search operator is specified with one or more strings separated by the `|` character. A key will satisfy the Tag search operator if the indicated field contains any one of the specified strings.

```
@<field_name>:{<tag>}
or
@<field_name>:{<tag1> | <tag2>}
or
@<field_name>:{<tag1> | <tag2> | ...}
```

For example, the following query will return documents with blue OR black OR green color.

`@color:{blue | black | green}`

As another example, the following query will return documents containing "hello world" or "hello universe"

`@color:{hello world | hello universe}`

**Numeric Range**

Numeric range operator allows for filtering queries to only return values that are in between a given start and end value. Both inclusive and exclusive range queries are supported. For simple relational comparisons, \+inf, \-inf can be used with a range query.

The syntax for a range search operator is:

```
@<field_name>:[ [(] <bound> [(] <bound>]
```

where \<bound\> is either a number or \+inf or \-inf

Bounds without a leading open paren are inclusive, whereas bounds with the leading open paren are exclusive.

Use the following table as a guide for mapping mathematical expressions to filtering queries:

```
min <= field <= max         @field:[min max]
min < field <= max          @field:[(min max]
min <= field < max	        @field:[min (max]
min < field < max	        @field:[(min (max]
field >= min	            @field:[min +inf]
field > min	                @field:[(min +inf]
field <= max	            @field:[-inf max]
field < max	                @field:[-inf (max]
field == val	            @field:[val val]
```

**Logical Operators**

Multiple tags and numeric search operators can be used to construct complex queries using logical operators.

**Logical AND**

To set a logical AND, use a space between the predicates. For example:

```
query1 query2 query3
```

**Logical OR**

To set a logical OR, use the `|` character between the predicates. For example:

```
query1 | query2 | query3
```

**Logical Negation**

Any query can be negated by prepending the `-` character before each query. Negative queries return all entries that don't match the query. This also includes keys that don't have the field.

For example, a negative query on @genre:{comedy} will return all books that are not comedy AND all books that don't have a genre field.

The following query will return all books with "comedy" genre that are not published between 2015 and 2024, or that have no year field:

@genre:\[comedy\] \-@year:\[2015 2024\]

**Operator Precedence**

Typical operator precedence rules apply, i.e., Logical negate is the highest priority, followed by Logical and then Logical Or with the lowest priority. Parenthesis can be used to override the default precedence rules.

**Examples of Combining Logical Operators**

Logical operators can be combined to form complex filter expressions.

The following query will return all books with "comedy" or "horror" genre (AND) published between 2015 and 2024:

`@genre:[comedy|horror] @year:[2015 2024]`

The following query will return all books with "comedy" or "horror" genre (OR) published between 2015 and 2024:

`@genre:[comedy|horror] | @year:[2015 2024]`

The following query will return all books that either don't have a genre field, or have a genre field not equal to "comedy", that are published between 2015 and 2024:

`-@genre:[comedy] @year:[2015 2024]`
