Performs a search of the specified index. The keys which match the query expression are returned.

```
FT.SEARCH <index> <query>
  [ALLSHARDS | SOMESHARDS]
  [CONSISTENT | INCONSISTENT]
  [DIALECT 2]
  [INFIELDS <count> <field> [<field> ...]]
  [INKEYS <count> <key> [<key> ...]]
  [INORDER]
  [LIMIT <offset> <num>]
  [NOCONTENT]
  [PARAMS <count> <name> <value> [ <name> <value> ...]]
  [RETURN <count> <field> [AS <name>] <field> [AS <name>]...]
  [SCORER <scorer>]
  [SLOP <slop>]
  [SORTBY <field> [ ASC | DESC]]
  [TIMEOUT <timeout>]
  [VERBATIM]
  [WITHCURSOR [COUNT <count>] [MAXIDLE <maxidle>]]
  [WITHSCORES]
  [WITHSORTKEYS]
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search-query.md) for details, including KNN queries and the [`VECTOR_RANGE` matcher](../topics/search-query.md#vector-range-match).
- `ALLSHARDS` (Optional): If specified, the command is terminated with a timeout error if a valid response from all shards is not received within the timeout interval. This is the default when `search.enable-partial-results` is `no`; otherwise `SOMESHARDS` is the default.
- `CONSISTENT` (Optional): If specified, the command is terminated with an error if the cluster is in an inconsistent state. This is the default when `search.enable-consistent-results` is `yes`; otherwise `INCONSISTENT` is the default.
- `DIALECT 2` (optional): Specifies your dialect. The only supported dialect is 2, which is also the default.
- `INCONSISTENT` (Optional): If specified, the command will generate a best-effort reply if the cluster remains inconsistent within the timeout interval.
- `INFIELDS <count> <field> [<field> ...]` (optional): Restricts full-text term matching to the listed TEXT fields, named by attribute name (the `AS` alias when one is given). `<count>` is the number of fields that follow, at most 64; duplicate names are ignored. If `<count>` is 0, INFIELDS is a no-op. An error is returned if a listed field does not exist or is not a TEXT field, or if the query contains an explicit `@field:` text term whose field is not listed. Non-text predicates and vector KNN matching are not restricted.
- `INKEYS <count> <key> [<key> ...]` (optional): Restricts results to the specified set of document keys. `<count>` is the number of keys that follow and must be a non-negative integer. Duplicate keys are deduplicated. If `<count>` is 0, no results are returned. The response count reflects only keys that match both the query and the INKEYS set. On a KNN query, the K nearest neighbors are chosen from the listed keys only.
- `LIMIT <offset> <count>` (optional): Lets you choose a portion of the result. The first `<offset>` keys are skipped and only a maximum of `<count>` keys are included. The default is LIMIT 0 10, which returns at most 10 keys.
- `NOCONTENT` (optional): When present, only the resulting key names are returned, no key values are included.
- `PARAMS <count> <name> <value> [<name> <value> ...]` (optional): `count` is the number of arguments, i.e., twice the number of name/value pairs. Parameters that the query does not use are ignored. See [Search - query language](../topics/search-query.md) for details.
- `RETURN <count> <field> [AS <name>] <field> [AS <name>] ...` (optional): Specifies the fields you want to retrieve from your documents, along with any renaming for the returned values. `count` is the number of arguments that follow, including each `AS` and its name. For example, `RETURN 4 color AS c city` returns two fields. By default, all fields are returned unless the `NOCONTENT` option is set, in which case no fields are returned. If `count` is 0, it behaves the same as `NOCONTENT`. If `RETURN` is given more than once, only the last one applies when `search.emulate-release` is 1.3.0 or later; with the default setting (1.0.0), the fields of all the `RETURN` clauses are combined and any `RETURN 0` acts as `NOCONTENT`. See [COMPATIBILITY.md](../../COMPATIBILITY.md).
- `INORDER` (optional): Indicates that proximity matching of text terms in the query must be in order.
- `SLOP <slop>` (Optional): Specifies a slop value for proximity matching of text terms in the query.
- `VERBATIM` (Optional): If specified, stemming is not applied to text terms in the query.
- `SOMESHARDS` (Optional): If specified, the command will generate a best-effort reply if all shards have not responded within the timeout interval.
- `SORTBY <field> [ASC | DESC]` (Optional): If present, results are sorted according to the value of the specified field. `ASC` (the default) sorts in ascending order; `DESC` sorts in descending order. `<field>` is an index attribute name (its `AS` alias if it has one), the KNN distance field (`__<field>_score` or the KNN `AS` name), or a `VECTOR_RANGE` distance name. Any other name is an error. Keys that do not have the field, or whose numeric value cannot be parsed, are placed at the end in both directions. On a KNN query, `SORTBY` reorders only the K nearest neighbors. Sorting is applied before the `LIMIT` clause is applied.

  Without `SORTBY`, KNN results are sorted by distance, nearest first, unless the KNN filter contains a text or tag clause. Other results, including KNN results with a text or tag filter, are sorted by relevance score, highest first. See [Search - Scoring](../topics/search-scoring.md).
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds. If omitted, the index's [`QUERY_TIMEOUT`](ft.create.md) is used, or `search.default-timeout-ms` (50000) if the index does not set one. The value must not exceed `search.max-timeout-ms` (default 60000).
- `SCORER <scorer>` (Optional): Selects the relevance scoring function used to rank results. The only supported scorer now is `BM25STD`. The default is the value of `search.default-scorer` (`BM25STD`). See [Search - Scoring](../topics/search-scoring.md).
- `WITHCURSOR [COUNT <count>] [MAXIDLE <maxidle>]` (Optional): Returns at most `<count>` of the keys selected by the `LIMIT` clause and saves the remaining keys in a cursor, which is read with [`FT.CURSOR`](ft.cursor.md). `<count>` must be between 1 and `search.cursor-max-count`, the default is 1000. `<maxidle>` is the number of milliseconds the cursor may go unread before it is destroyed; it must be between 1 and `search.cursor-max-idle-ms`, the default is 300000 (or `search.cursor-max-idle-ms` if that is smaller). If `COUNT` or `MAXIDLE` is given more than once, the last value is used. The cursor holds only the keys selected by `LIMIT`, whose default is `LIMIT 0 10`, so give `LIMIT 0 <n>` to page through more than 10 keys. This option is a Valkey extension.
- `WITHSCORES` (Optional): Augments the output with the relevance score computed for each returned key. For a KNN query without a text or tag filter, the score is 0; use the distance field instead.
- `WITHSORTKEYS` (Optional): Augments the output with the value of the `SORTBY` field for each returned key. It is ignored with `NOCONTENT`. See [WITHSCORES and WITHSORTKEYS](#withscores-or-withsortkeys-was-specified) for the format.

## Reply

The shape and contents of the response depend on the options specified in the command.
In all cases, the response is an array with the first element being a count of the number of keys which match the query.
The value of this count is unaffected by the presence of the `LIMIT` clause. For a KNN query the count is at most K.

The `LIMIT` clause trims the list of matched keys to generate a list of returned keys. The remainder of the response array is for the returned keys.

### `NOCONTENT` or `RETURN 0` was specified.

The remainder of the response array is one entry per returned key, consisting of the key name.

### `RETURN` with arguments was specified.

The remainder of the response array is two entries per returned key. The first entry is the key name and
the second entry is an array of name/value pairs. The array of name/value pairs is driven by the `RETURN` clause.
Each of the named fields in the `RETURN` clause is returned along with the value of that field for this particular key. If the named field isn't present in this key then it won't be included.
For a KNN query, the distance is included only if the `RETURN` clause lists the distance field (`__<field>_score` or the KNN `AS` name) -- see [Search - query language](../topics/search-query.md) for details on how to control the name of that field.
For a vector range query, the distance is included only if the query names it with `AS <name>` or `$YIELD_DISTANCE_AS: <name>`, and the `RETURN` clause lists that name.

### Neither `NOCONTENT` nor `RETURN` was specified.

If the index is on `HASH` keys, then the result is the same as if a `RETURN` clause had been present that listed all of the fields of a key.

If the index is on `JSON` keys, then one name/value pair is inserted with name `$` and the value being the entire JSON key as a string.
In addition, if this is a vector search, then one additional name/value pair will be included which is the computed vector distance for this returned key -- see [Search - query language](../topics/search-query.md) for details on how to control the name of that field.
For a vector range query, the distance is included only if the query names it with `AS <name>` or `$YIELD_DISTANCE_AS: <name>`.

### `WITHSCORES` or `WITHSORTKEYS` was specified.

With `WITHSCORES`, the relevance score of each key, as a string, follows the key name. This also applies with `NOCONTENT`.
With `WITHSORTKEYS` (ignored with `NOCONTENT`), the sort key follows the score. Then comes the array of name/value pairs, unless `NOCONTENT` was given.

The format of the sort key depends on `search.emulate-release` (see [COMPATIBILITY.md](../../COMPATIBILITY.md)):

- 1.3.0 or later: the value is prefixed with `#` for a numeric field or a vector distance, and with `$` for any other field. If the key has no value for the field, or there is no `SORTBY`, the sort key is nil.
- Earlier versions, including the default setting of 1.0.0: the prefix is always `#`. If there is no value, the sort key is the string `#`.

For example, the reply to `FT.SEARCH index @color:{blue} WITHSCORES WITHSORTKEYS SORTBY city RETURN 1 city` has this layout, with `search.emulate-release` set to 1.3.0 and the hash data from the [examples](#non-vector-queries-on-hash-index):

```
1) (integer) 1
2) "key1"
3) "<score>"
4) "$London"
5) 1) "city"
   2) "London"
```

### `WITHCURSOR` was specified.

The response is a three element array:

1. The count of the number of keys which match the query, as above.
2. An array with one element per returned key. Each element is an array holding the entries for that key described above, e.g. the key name and its array of name/value pairs.
3. The cursor id to pass to [`FT.CURSOR READ`](ft.cursor.md), or 0 if this reply contains all the remaining keys, in which case no cursor is created.

### Errors

If the queue of pending queries has reached `search.max-query-queue-depth` (default 100000; 0 disables the check), the command fails with `Search query queue depth exceeded`.

## Examples

### Non-vector Queries On Hash Index

For these queries the following index and data definitions were used:

```
FT.CREATE index on hash SCHEMA color TAG city TAG
hset key1 color blue city London
hset key2 color black city Paris
hset key3 color green city Berlin
hset key4 color white city Tokyo
hset key5 color blend cityextra Unknown
```

A simple query returning all data of matched keys

```
FT.SEARCH index @color:{bl*}
1) (integer) 3
2) "key1"
3) 1) "city"
   2) "London"
   3) "color"
   4) "blue"
4) "key5"
5) 1) "color"
   2) "blend"
   3) "cityextra"
   4) "Unknown"
6) "key2"
7) 1) "city"
   2) "Paris"
   3) "color"
   4) "black"
```

The same query with the `NOCONTENT` clause.

```
FT.SEARCH index @color:{bl*} NOCONTENT
1) (integer) 3
2) "key1"
3) "key5"
4) "key2"
```

The same query with a `RETURN` clause.

```
FT.SEARCH index @color:{bl*} RETURN 2 color city
1) (integer) 3
2) "key1"
3) 1) "color"
   2) "blue"
   3) "city"
   4) "London"
4) "key5"
5) 1) "color"
   2) "blend"
6) "key2"
7) 1) "color"
   2) "black"
   3) "city"
   4) "Paris"
```

### Non-vector Queries On JSON Index

Repeating the queries above, except the data is JSON

```
FT.CREATE index on json SCHEMA $.color as color TAG $.city as city TAG
json.set key1 . {"color":"blue","city":"London"}
json.set key2 . {"color":"black","city":"Paris"}
json.set key3 . {"color":"green","city":"Berlin"}
json.set key4 . {"color":"white","city":"Tokyo"}
json.set key5 . {"color":"blend","cityextra":"Unknown"}
```

A simple query returning all data of matched keys

```
FT.SEARCH index @color:{bl*}
1) (integer) 3
2) "key2"
3) 1) "$"
   2) "{\"color\":\"black\",\"city\":\"Paris\"}"
4) "key5"
5) 1) "$"
   2) "{\"color\":\"blend\",\"cityextra\":\"Unknown\"}"
6) "key1"
7) 1) "$"
   2) "{\"color\":\"blue\",\"city\":\"London\"}"
```

The same query with `NOCONTENT`

```
FT.SEARCH index @color:{bl*} NOCONTENT
1) (integer) 3
2) "key2"
3) "key5"
4) "key1"
```

The same query with a `RETURN` clause.

```
FT.SEARCH index @color:{bl*} RETURN 2 color city
1) (integer) 3
2) "key2"
3) 1) "color"
   2) "black"
   3) "city"
   4) "Paris"
4) "key5"
5) 1) "color"
   2) "blend"
6) "key1"
7) 1) "color"
   2) "blue"
   3) "city"
   4) "London"
```

### Pure vector search query

For this example, assume we're building a property searching index where customers can search properties based on some features.
Assume we have a list of properties with the following attributes:

- Description - vector embedding for given property.
- Other fields - each property can have other metadata as well. However, for simplicity, other fields are ignored in this example.

At first, we create an `HNSW` index with the description as a vector field using the `FT.CREATE` command:

```
FT.CREATE idx SCHEMA description VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC L2
```

Now we can insert a few properties (this can be done prior to index creation as well) using the `HSET` command:

```
HSET p1 description "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
HSET p2 description "\x00\x00\x00\x00\x00\x00\x80?\x00\x00\x00\x00"
HSET p3 description "\x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00"
HSET p4 description "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
HSET p5 description "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?"
```

Now we can perform queries using the `FT.SEARCH` command. The following query returns up to five of the most similar
properties to the provided query vector:

```
FT.SEARCH idx "*=>[KNN 5 @description $query_vector]" PARAMS 2 query_vector "\xcd\xccL?\x00\x00\x00\x00\x00\x00\x00\x00" DIALECT 2
```

Returned result:

```
 1) (integer) 5
 2) p3
 3) 1) __description_score
    2) 0.0399999953806
    3) description
    4) \x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00
 4) p5
 5) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?
 6) p4
 7) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?
 8) p2
 9) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x80?\x00\x00\x00\x00
10) p1
11) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?
```

### Sorting by the KNN distance

The same query, returning the three farthest of the five nearest properties, and only their distance:

```
FT.SEARCH idx "*=>[KNN 5 @description $query_vector AS dist]" PARAMS 2 query_vector "\xcd\xccL?\x00\x00\x00\x00\x00\x00\x00\x00" SORTBY dist DESC LIMIT 0 3 RETURN 1 dist DIALECT 2
```

`SORTBY` reorders only the five nearest neighbors.
