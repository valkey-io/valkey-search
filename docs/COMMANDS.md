# Command List

- [`FT.AGGREGATE`](#ftaggregate)
- [`FT.CREATE`](#ftcreate)
- [`FT.DROPINDEX`](#ftdropindex)
- [`FT.INFO`](#ftinfo)
- [`FT._LIST`](#ft_list)
- [`FT.SEARCH`](#ftsearch)

#

## FT.AGGREGATE

The `FT.AGGREGATE` command extends the query capabilities of the `FT.SEARCH` command with substantial server-side data processing capabilities. Just as with the `FT.SEARCH` command, the query string is used to locate a set of Valkey keys. Attributes from those keys are loaded and processed according to a list of stages specified on the command. Each stage is processed sequentially in the order found on the command with the output of each stage becoming the input to the next stage. The output of the last stage is returned as the result of the command.

```
FT.AGGREGATE <index-name> <query>
      TIMEOUT <timeout>
    | PARAMS <count> <name> <value> [ <name1> <value1> ...]
    | DIALECT <dialect>
    | LOAD [* | <count> <attribute1> <attribute2> ...]
    (
      | APPLY <expression> AS <attribute>
      | FILTER <expression>
      | LIMIT <offset> <count>
      | SORTBY <count> <attribute1> [ASC | DESC] <attribute2 [ASC | DESC] ... [MAX <num>]
      | GROUPBY <count> <attribute1> <attribute2> ... [REDUCE <reducer> <count> <arg1> <arg2> ...]*
    )+
```

- **\<index\>** (required): This index name you want to query.
- **\<query\>** (required): The query string, see [query string](QUERY-STRING.md) for details.
- **TIMEOUT \<timeout\>** (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- **PARAMS \<count\> \<name1\> \<value1\> \<name2\> \<value2\> ...** (optional): `count` is of the number of arguments, i.e., twice the number of name/value pairs. See [query string](QUERY-STRING.md) for usage details.
- **DIALECT \<dialect\>** (optional): Specifies your dialect. The only supported dialect is 2.
- **LOAD [* | \<count\> \<attribute1\> \<attribute2\> ...]** (optional): After the query has located a set of keys. This controls which attributes of those keys are loaded into data passed into the first stage for processing. A star (\*) indicates that all of the attributes of the keys are loaded.

- **APPLY \<expression\> as \<attribute\>** (optional): For each key of input, the expression is computed and stored into the named attribute. See [aggregation syntax](AGGREGATION-SYNTAX.md) for details on the expression syntax.
- **FILTER \<expression\>** (optional): The expression is evaluated for each key of input. Only those keys for which the expression evaluates to true (non-zero) are included in the output. See [aggregation syntax](AGGREGATION-SYNTAX.md) for details on the expression syntax.
- **LIMIT \<offset\> \<count\>** (optional): The first `offset` number of records are discarded. The next `count` records generated as the output of this stage. Any remaining input records are discarded.
- **SORTBY \<count\> \<attribute1\> [ASC | DESC] \<attribute2 [ASC | DESC] ... [MAX \<num\>]** (optional): The input records are sorted according to the specified attributes and directions. If the MAX option is specified, only the first `num` records after sorting are included in the output and the remaining input records are discarded.
- **GROUPBY \<count\> \<attribute1\> \<attribute2\> ... [REDUCE \<reducer\> \<count\> \<arg1\> \<arg2\> ...]\*** (optional): The input records are grouped according to the unique value combinations of the specified attributes. For each group, reducers are updated with that keys from that group and becomes the output records. The grouped input records are discarded. Multiple reducers can be specified and are efficiently computed. See [aggregation syntax](AGGREGATION-SYNTAX.md) for a list of reducer functions and arguments.

## FT.CREATE

The `FT.CREATE` command creates an empty index and may initiate a backfill process. Each index consists of a number of attribute definitions. Each attribute definition specifies a attribute name, a attribute type and a path within each indexed key to locate a value of the declared type. Some attribute type definitions have additional sub-type specifiers.

For indexes on HASH keys, the path is the hash member name. The optional `AS` clause provides an explicit attribute name which will be the same as the path if omitted. Renaming of attributes is required when the member name contains special characters. See
[query string](QUERY-STRING.md) for a list of prohibited characters in attribute names.

For indexes on `JSON` keys, the path is a `JSON` path to the data of the declared type. The `AS` clause is required in order to provide a attribute name without the special characters of a `JSON` path.

An index may contain any number of attributes. Each attribute has a type and a set of associated query operations that are specific to that type. See [data type input formats](DATA-TYPE-FORMATS.md) for details of the format of data in each attribute type. See [query string syntax](QUERY-STRING.md) for details of query operators for each of the data types.

### Index Creation

Valkey Search requires that an index definition be distributed to all nodes. Within a shard, this is handled by the normal Valkey replication machinery, i.e., index definitions are subject to replication delay. However, when cluster mode is enabled the Coordinator distributes index definitions across the shards see [cross shard metadata](CROSS-SHARD-METADATA.md) for details.

```bash
FT.CREATE <index-name>
    [ON HASH | ON JSON]
    [PREFIX <count> <prefix> <prefix>...]
    [LANGUAGE <language>]
    [SKIPINITIALSCAN]
    [MINSTEMSIZE <min_stem_size>]
    [WITHOFFSETS | NOOFFSETS]
    [NOSTOPWORDS | STOPWORDS <count> <word> word ...]
    [PUNCTUATION <punctuation>]
    SCHEMA
        (
            <identifier> [AS <attribute>]
                  NUMERIC
                | TAG [SEPARATOR <sep>] [CASESENSITIVE]
                | TEXT [NOSTEM] [WITHSUFFIXTRIE | NOSUFFIXTRIE]
                | VECTOR [HNSW | FLAT] <count> [<alg_parameter_name> <alg_parameter_value> ...]
            [SORTABLE]
        )+
```

- **\<index-name\>** (required): This is the name you give to your index. If an index with the same name exists already, an error is returned.

- **ON HASH** (optional): Only HASH keys are included into this index. This is the default.
- **ON JSON** (optional): Only JSON keys are included into this index.

- **PREFIX \<prefix-count\> \<prefix\>** (optional): If this clause is specified, then only keys that begin with the same bytes as one or more of the specified prefixes will be included into this index. If this clause is omitted, all keys of the correct type will be included. A zero-length prefix would also match all keys of the correct type and is the default value.

- **LANGUAGE <language>** (optional): For text attributes, the language used to control lexical parsing and stemming. Currently only the value `ENGLISH` is supported.

- **MINSTEMSIZE \<min_stem_size\>** (optional): For text attributes with stemming enabled. This controls the minimum length of a word before it is subjected to stemming. The default value is <?>.

- **WITHOFFSETS | NOOFFSETS** (optional): Enables/Disables the retention of per-word offsets within a text attribute. Offsets are required to perform exact phrase matching and slop-based proximity matching. Thus if offsets are disabled, those query operations will be rejected with an error. The default is `WITHOFFSETS`.

- **NOSTOPWORDS | STOPWORDS \<count\> \<word1\> \<word2\>...** (optional): Stop words are not words which are not put into the indexes. The default value of `STOPWORDS` is language dependent. For `LANGUAGE ENGLISH` the default is: <?>.

- **PUNCTUATION \<punctuation\>** (optional): A string of characters that are used to define words in the text attribute. The default value is `,.<>{}[]"':;!@#$%^&\*()-+=~/\|`.

- **SKIPINITIALSCAN** (optional): If specific, this option skips the normal backfill operation for an index. If this option is specified, pre-existing keys which match the `PREFIX` clause will not be loaded into the index during a backfill operation. This clause has no effect on processing of key mutations _after_ an index is created, i.e., keys which are mutated after an index is created and satisfy the data type and `PREFIX` clause will be inserted into that index.

## Attributes

The `SCHEMA` keyword identifies the start of the attribute declarations. Each attribute has a path, a name and a data type. Some data types allow additional information to be supplied to further refine the definition of the index for that attribute.

- **\<identifier\>** (required): This specifies the location of the data for the attribute with the key. For `HASH` indexes, the location is just the member name. For `JSON` indexes, it is the full JSON Path to the data. See <??? TBD> for a description of the allowed subset of JSON path.

- **\<attribute\>** (optional for `HASH` indexes, required for `JSON` indexes): This specifies the name used to refer to this attribute within the search module, e.g., it is used in the query and aggregation expression context to refer to this attribute of a key. For `JSON` indexes, this must be supplied. Attribute names are syntactically restricted <??? TBD>.

- **\<type\>** (Required): This contains one of `NUMERIC`, `TAG`, `TEXT`, or `VECTOR`. Type specific declarations may follow this keyword. Some types require additional declarations, others do not, see below for type specific declarators.

- **SORTABLE** (optional): This parameter is currently ignored as all attribute types are considered to be sortable.

### Tag Attribute Specific Declarators

- **SEPARATOR \<sep\>** (optional): One of these characters `,.<>{}[]"':;!@#$%^&*()-+=~` used to delimit individual tags. If omitted the default value is `,`.
- **CASESENSITIVE** (optional): If present, tag comparisons will be case-sensitive. The default is that tag comparisons are NOT case-sensitive

### Text Attribute Specific Declarators

- **NOSTEM** (optional): If specified, stemming of words on ingestion is disabled.
- **WITHSUFFIXTRIE | NOSUFFIXTRIE** (optional): Enables/Disables the use of a suffix trie to implement suffix-based wildcard queries. If `NOSUFFIXTRIE` is specified, query strings which specify suffix-based wildcard matching will be rejected with an error. The default is `WITHSUFFIXTRIE`.

### Vector Attribute Specific Declarators

The `VECTOR` attribute type must be followed by a vector index algorithm name. Two algorithms are currently supported: `FLAT` and `HNSW`. They have different declarators as below.

#### Flat Vector Attribute Declarators

The Flat algorithm provides exact answers, but has runtime proportional to the number of indexed vectors and thus may not be appropriate for large data sets.

- **DIM \<number\>** (required): Specifies the number of dimensions in a vector.
- **TYPE \<type\>** (required): Data type, currently only `FLOAT32` is supported.
- **DISTANCE_METRIC \[L2 | IP | COSINE\]** (required): Specifies the distance algorithm
- **INITIAL_CAP \<size\>** (optional): Initial index size.

#### HNSW Vector attribute Declarators

The HNSW algorithm provides approximate answers, but operates substantially faster than FLAT and is appropriate for large data sets.

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

In CME a second class of errors is related to a failure to properly distribute the command across all of the nodes of the cluster. See [cross shard metadata](CROSS-SHARD-METADATA.md) for more details.

## FT.DROPINDEX

```bash
FT.DROPINDEX <index-name>
```

The specified index is deleted. It is an error if that index doesn't exist.

- **\<index-name\>** (required): The name of the index to delete.

**RESPONSE** OK or Error.

An OK response indicates that the index has been successfully deleted.

A first class of errors is directly associated with the command itself, for example a syntax error or an attempt to delete a non-existant index.

In CME a second class of errors is related to a failure to properly distribute the deletion across all of the nodes of the cluster. See [cross shard metadata](CROSS-SHARD-METADATA.md) for more details.

## FT.INFO

### Cluster Mode Diabled

When cluster mode is disable, information about the specified index is returned from the executing node:

```bash
FT.INFO <index-name>
```

- **\<index-name\>** (required): The name of the index to return information about.

### Cluster Mode Enabled

When cluster mode is enabled, additional options are available to direct the executing node to contact other nodes in the cluster to generate aggregated index information.
Operations which require responses from other nodes create additional complexity. Two specific additional situations are handled.

First, if a response from a designated node is not received within a fixed time window the `FT.INFO` operation still completes.
Typically this is due to either a network partition or a node failure.
Options are provided to control whether this is considered an error or whether a best-effort result should be returned (no error).

Second, because index metadata is eventually consistent See [cross shard metadata](CROSS-SHARD-METADATA.md) for more details. It's possible that a contacted node generates a response but from a different version of the index.
This situation is detected and the query is repeated until either a consistent response is obtained or a time window has expired.
Options are provided to control whether this command is complete with an error or a result generated only from the consistent responses should be returned (no error).

```bash
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

### Cluster Mode Disabled or Cluster Mode Enabled with LOCAL option

An array of key value pairs.

- **index_name** (string) The index name
- **index_definition** (a 6 element array)
  - **key_type** (string) `HASH` or `JSON`
  - **prefixes** (array of strings) The declared prefixes for this index
  - **default_score** (string) currently "1.0"
- **attributes** (array of arrays) One entry per declared attribute of the index.
  - **identifier** (string) identifier for this attribute
  - **attribute** (string) The name used to refer to this index in query and aggregation expressions.
  - **type** (string) One of `NUMERIC`, `TAG`, `TEXT` or `VECTOR`
  - per-type extension (see below)
- **num_docs** (integer) Total keys in the index
- **num_records** (integer) Total number of attribute fields indexed.
- **num_total_terms** (integer) Total number of terms in all text attributes in this index.
- **num_unique_terms** (integer) Total number of unique terms in all text attributes in this index.
- **total_postings** (integer) Total number of postings entries in all text attributes in this index.
- **hash_indexing_failures** (integer) Count of unsuccessful indexing attempts
- **backfill_in_progress** (string). "1" if a backfill is currently running. "0" if not.
- **backfill_complete_percent** (string) Estimated progress of background indexing. Percentage is expressed as a fractional value from 0 to 1.0.
- **mutation_queue_size** (string) Number of keys contained in the mutation queue.
- **recent_mutations_queue_delay** (string) 0 if the mutation queue is empty. Otherwise it is the mutation queue occupancy of the of the last key to be ingested in seconds.
- **state** (string) Backfill state. `ready` indicates not backfill is in progress. `backfill_in_progress` backfill operation proceeding normally. `backfill_paused_by_oom` backfill is paused because the Valkey instance is out of memory.
- **punctuation** (string) list of punctuation characters.
- **stopwords** (array of strings) list of stopwords.
- **with_offsets**

### TAG attribute Type Extension

- **SEPARATOR** (string) The actual separator character.
- **CASESENSITIVE** (number) 0 or 1.
- **SIZE** Number of keys that have this tag attribute present.

### TEXT attribute Type Extension

- **WITH_SUFFIX_TRIE** (number) 0 or 1.
- **NO_STEM** (number) 0 or 1.

### VECTOR attribute Type Extension

- **index** (array)
  - **capacity** (integer) The current capacity for the total number of vectors that the index can store.
  - **dimensions** (integer) Dimension count
  - **distance_metric** (string) Possible values are `L2`, `IP` or `COSINE`
  - **size** (integer) Number of valid vectors for this attribute
  - **data_type** (string) `FLOAT32`. This is the only available data type
  - **algorithm** (array of key/value pairs) Extended information about the vector indexing algorithm for this attribute.

#### FLAT VECTOR attribute Type Extension.

- **name** (string) `FLAT`.
- **block_size** (number) block size for this attribute.

#### HNSW VECTOR attribute Type Extension.

- **name** (string) `HNSW`.
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
- **state** (string) The current state of the index, one of: `ready`, `backfill_in_progress` or `backfill_paused_by_oom`

## FT.\_LIST

```bash
FT._LIST
```

Lists the currently defined indexes.

**RESPONSE**

An array of strings which are the currently defined index names.

## FT.SEARCH

```bash
FT.SEARCH <index> <query>
  [NOCONTENT]
  [TIMEOUT <timeout>]
  [PARAMS <count> <name> <value> [ <name> <value> ...]]
  [LIMIT <offset> <num>]
  [INORDER]
  [SLOP <slop>]
  [DIALECT <dialect>]
  [RETURN <count> <attribute1> [AS <property1>] <attribute2> [AS <property2>]...]
  [SORTBY <attribute-name> [ ASC | DESC]]
  [ALLSHARDS | SOMESHARDS]
  [CONSISTENT | INCONSISTENT]
```

Performs a search of the specified index. The keys which match the query expression are returned.

- **\<index\>** (required): This index name you want to query.
- **\<query\>** (required): The query string, see below for details.
- **NOCONTENT** (optional): When present, only the resulting key names are returned, no key values are included.
- **TIMEOUT \<timeout\>** (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- **PARAMS \<count\> \<name1\> \<value1\> \<name2\> \<value2\> ...** (optional): `count` is of the number of arguments, i.e., twice the number of value name pairs. See the query string for usage details.
- **RETURN \<count\> \<attribute1\> [AS \<name\>] \<attribute2\> [AS <name2>] ...** (options): `count` is the number of attributes to return. Specifies the attributes you want to retrieve from your documents, along with any aliases for the returned values. By default, all attributes are returned unless the NOCONTENT option is set, in which case no attributes are returned. If num is set to 0, it behaves the same as NOCONTENT.
- **LIMIT \<offset\> \<count\>** (optional): Lets you choose a portion of the result. The first `<offset>` keys are skipped and only a maximum of `<count>` keys are included. The default is LIMIT 0 10, which returns at most 10 keys.
- **DIALECT \<dialect\>** (optional): Specifies your dialect. The only supported dialect is 2.
- **INORDER** (optional): Indicates that proximity matching of terms must be inorder.
- **SLOP \<slop\>** (Optional): Specifies a slop value for proximity matching of terms.
- **SORTBY \<attribute-name\> [ASC | DESC]** (Optional): If present, results are sorted according the value of the specified attribute and the optional sort-direction instruction. By default, vector results are sorted in distance order and non-vector results are not sorted in any particular order. Sorting is applied before the `LIMIT` clause is applied.
- **ALLSHARDS** (Optional): If specified, the command is terminated with a timeout error if a valid response from all shards is not received within the timeout interval. This is the default.
- **SOMESHARDS** (Optional): If specified, the command will generate a best-effort reply if all shards have not responded within the timeout interval. -**CONSISTENT** (Optional): If specified, the command is terminated with an error if the cluster is in an inconsistent state. This is the default. -**INCONSISTENT** (Optional): If specified, the command will generate a best-effort reply if the cluster remains inconsistent within the timeout interval.

**RESPONSE**

The command returns either an array if successful or an error.

On success, the first entry in the response array represents the count of matching keys, followed by one array entry for each matching key.
Note that if the `LIMIT` option is specified it will only control the number of returned keys and will not affect the value of the first entry.

When `NOCONTENT` is specified, each entry in the response contains only the matching keyname. Otherwise, each entry includes the matching keyname, followed by an array of the returned attributes.

The result attributes for a key consists of a set of name/value pairs. The first name/value pair is for the distance computed. The name of this pair is constructed from the vector attribute name prepended with "\_\_" and appended with "\_score" and the value is the computed distance. The remaining name/value pairs are the members and values of the key as controlled by the `RETURN` clause.
