Detailed information about the specified index is returned.

- `<index-name>` (required): The name of the index to return information about.

**RESPONSE**

An array of key value pairs.

- `index_name` (string) The index name
- `num_docs` (integer) Total keys in the index
- `num_records` (integer) Total records in the index
- `hash_indexing_failures` (integer) Count of unsuccessful indexing attempts
- `indexing` (integer) Binary value. Shows if background indexing is running or not
- `percent_indexed` (decimal) Progress of background indexing. Percentage is expressed as a value from `0` to `1`
- `index_definition` (array) An array of values defining the index
  - `key_type` (string) `HASH` or `JSON`.
  - `prefixes` (array of strings) Prefixes for keys
  - `default_score` (integer) This is the default scoring value for the vector search scoring function, which is used for sorting.
  - `attributes` (array) One array of entries for each field defined in the index.
    - `identifier` (string) field name
    - `attribute` (string) An index field. This is correlated to a specific index `HASH` field.
    - `type` (string) `VECTOR`. This is the only available type.
    - `index` (array) Extended information about this internal index for this field.
      - `capacity` (integer) The current capacity for the total number of vectors that the index can store.
      - `dimensions` (integer) Dimension count
      - `distance_metric` (string) Possible values are `L2`, `IP` or `Cosine`
      - `data_type` (string) `FLOAT32`. This is the only available data type
      - `algorithm` (array) Information about the algorithm for this field.
        - `name` (string) `HNSW` or `FLAT`
        - `m` (integer) The count of maximum permitted outgoing edges for each node in the graph in each layer. The maximum number of outgoing edges is `2*M` for layer `0`. The Default is `16`. The maximum is `512`.
        - `ef_construction` (integer) The count of vectors in the index. The default is `200`, and the max is `4096`. Higher values increase the time needed to create indexes, but improve the recall ratio.
        - `ef_runtime` (integer) The count of vectors to be examined during a query operation. The default is 10, and the max is 4096.
