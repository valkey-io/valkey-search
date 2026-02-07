The `FT.INFO` command provides information about an index. Variants of the command are available in cluster mode to aggregate index information across the cluster.

```
FT.INFO <index-name>
  (
      [LOCAL | PRIMARY | CLUSTER]
    | [ALLSHARDS | SOMESHARDS]
    | [CONSISTENT INCONSISTENT]
  )+
```

- `<index-name>` (required): The name of the index to return information about.

- `LOCAL` (optional): Only the executing (local) node provides index information. This is the default.
- `PRIMARY` (optional): The primary nodes of every shard in the cluster are queried to generate index information. This is only valid when cluster mode is enabled.
- `CLUSTER` (optional): All nodes, primary or replica, are queried to generate index information. This is only valid when cluster mode is enabled.

- `ALLSHARDS` (optional): If specified, a response is required from every shard in the system. If all responses are not received within a time window the command is terminated with an error. This is the default.
- `SOMESHARDS` (optional): If specified, a response is NOT required from every shard in the system. If all responses are not received within the time window, only the received responses are returned and no error is generated.

- `CONSISTENT` (optional): If specified, the command is terminated with an error if any received response isn't from a consistent version of the index. This is the default.
- `INCONSISTENT` (optional): If specified, a command result is generated using only the responses from nodes with a consistent version of the index.

`RESPONSE`

### Response when LOCAL is specified

The LOCAL response contains both the definition of the index as well as the status of that index on the node that executes the command.

An array of key value pairs.

- `index_name` (string) The index name
- `index_definition` (an array of key/value pairs)
  - `key_type` (string) `HASH` or `JSON`
  - `prefixes` (array of strings) The declared prefixes for this index
  - `default_score` (string) currently "1.0"
- `attributes` (array of arrays) One entry per declared attribute of the index.
  - `identifier` (string) identifier for this attribute
  - `attribute` (string) The name used to refer to this index in query and aggregation expressions.
  - `type` (string) One of `NUMERIC`, `TAG`, `TEXT` or `VECTOR`
  - per-type extension (see below)
- `num_docs` (integer) Total keys in the index
- `num_records` (integer) Total number of fields indexed.
- `num_total_terms` (integer) Total number of terms in all text fields in this index.
- `num_unique_terms` (integer) Total number of unique terms in all text fields in this index.
- `total_postings` (integer) Total number of postings entries in all text fields in this index.
- `hash_indexing_failures` (integer) Count of unsuccessful indexing attempts
- `backfill_in_progress` (string). "1" if a backfill is currently running. "0" if not.
- `backfill_complete_percent` (string) Estimated progress of background indexing. Percentage is expressed as a fractional value from 0 to 1.0.
- `mutation_queue_size` (string) Number of keys contained in the mutation queue.
- `recent_mutations_queue_delay` (string) 0 if the mutation queue is empty. Otherwise it is the mutation queue occupancy of the of the last key to be ingested in seconds.
- `state` (string) Backfill state. `ready` indicates not backfill is in progress. `backfill_in_progress` backfill operation proceeding normally. `backfill_paused_by_oom` backfill is paused because the Valkey instance is out of memory.
- `punctuation` (string) list of punctuation characters.
- `stopwords` (array of strings) list of stopwords.
- `with_offsets` (string) "1" if offsets are included. "0" if offsets are not included
- `min_stem_size` (integer) Minimum stemming size for this field.

### TAG Field Type Extension

- `SEPARATOR` (string) The actual separator character.
- `CASESENSITIVE` (number) 0 or 1.
- `SIZE` Number of keys that have this tag attribute present.

### TEXT Field Type Extension

- `WITH_SUFFIX_TRIE` (number) 0 or 1.
- `NO_STEM` (number) 0 or 1.

### VECTOR Field Type Extension

- `index` (array)
  - `capacity` (integer) The current capacity for the total number of vectors that the index can store.
  - `dimensions` (integer) Dimension count
  - `distance_metric` (string) Possible values are `L2`, `IP` or `COSINE`
  - `size` (integer) Number of valid vectors for this attribute
  - `data_type` (string) `FLOAT32`. This is the only available data type
  - `algorithm` (array of key/value pairs) Extended information about the vector indexing algorithm for this attribute.

#### FLAT VECTOR Field Type Extension.

- `name` (string) `FLAT`.
- `block_size` (number) block size for this attribute.

#### HNSW VECTOR Field Type Extension.

- `name` (string) `HNSW`.
- `m` (integer) The count of maximum permitted outgoing edges for each node in the graph in each layer. The maximum number of outgoing edges is 2\*M for layer 0\. The Default is 16\. The maximum is 512\.
- `ef_construction` (integer) The count of vectors in the index. The default is 200, and the max is 4096\. Higher values increase the time needed to create indexes, but improve the recall ratio.
- `ef_runtime` (integer) The count of vectors to be examined during a query operation. The default is 10, and the max is 4096\.

### Response when the PRIMARY option is specified.

An array of key value pairs

- `mode` (string) Will have the value `PRIMARY`.
- `index_name` (string) The index name
- `num_docs` (string) INTEGER. Total keys in the index
- `num_records` (string) INTEGER. Total records in the index
- `hash_indexing_failures` (string) INTEGER. Count of unsuccessful indexing attempts

### Response when the CLUSTER option is specified 

An array of key value pairs

- `mode` (string) Will have the value `CLUSTER`.
- `index_name` (string) The index name
- `backfill_in_progress` (string) 0 or 1. Is backfill in progress
- `backfill_complete_percent_max` (string) FLOAT32. Maximum backfill complete percent in all nodes
- `backfill_complete_percent_min` (string) FLOAT32. Minimum backfill complete percent in all nodes
- `state` (string) The current state of the index, one of: `ready`, `backfill_in_progress` or `backfill_paused_by_oom`

