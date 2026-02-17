Performs a search of the specified index. The keys which match the query expression are returned.

```
FT.SEARCH <index> <query>
  [ALLSHARDS | SOMESHARDS]
  [CONSISTENT | INCONSISTENT]
  [DIALECT <dialect>]
  [INORDER]
  [LIMIT <offset> <num>]
  [NOCONTENT]
  [PARAMS <count> <name> <value> [ <name> <value> ...]]
  [RETURN <count> <field> [AS <name>] <field> [AS <name>]...]
  [SLOP <slop>]
  [SORTBY <field> [ ASC | DESC]]
  [TIMEOUT <timeout>]
  [VERBATIM]
  [WITHSORTKEYS]
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search-query.md) for details.
- `ALLSHARDS` (Optional): If specified, the command is terminated with a timeout error if a valid response from all shards is not received within the timeout interval. This is the default.
- `CONSISTENT` (Optional): If specified, the command is terminated with an error if the cluster is in an inconsistent state. This is the default.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `INORDER` (optional): Indicates that proximity matching of terms must be inorder.
- `INCONSISTENT` (Optional): If specified, the command will generate a best-effort reply if the cluster remains inconsistent within the timeout interval.
- `LIMIT <offset> <count>` (optional): Lets you choose a portion of the result. The first `<offset>` keys are skipped and only a maximum of `<count>` keys are included. The default is LIMIT 0 10, which returns at most 10 keys.
- `NOCONTENT` (optional): When present, only the resulting key names are returned, no key values are included.
- `PARAMS <count> <name> <value> [<name> <value> ...]` (optional): `count` is of the number of arguments, i.e., twice the number of value/name pairs. [Search - query language](../topics/search-query.md) for details.
- `RETURN <count> <field> [AS <name>] <field> [AS <name>] ...` (options): `count` is the number of fields to return. Specifies the fields you want to retrieve from your documents, along with any renaming for the returned values. By default, all fields are returned unless the `NOCONTENT` option is set, in which case no fields are returned. If num is set to 0, it behaves the same as `NOCONTENT`.
- `SLOP <slop>` (Optional): Specifies a slop value for proximity matching of terms.
- `SOMESHARDS` (Optional): If specified, the command will generate a best-effort reply if all shards have not responded within the timeout interval.
- `SORTBY <field> [ASC | DESC]` (Optional): If present, results are sorted according the value of the specified field and the optional sort-direction instruction. By default, vector results are sorted in distance order and non-vector results are not sorted in any particular order. Sorting is applied before the `LIMIT` clause is applied.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- `VERBATIM` (Optional): If specified stemming is not applied to term searches.
- `WITHSORTKEYS` (Optional): If `SORTBY` is specified then enabling this option augments the output with the value of the field used for sorting.

Response

The shape and contents of the response depend on the options specified in the command.
In all cases, the response is an array with the first element being a count of the number of keys which match the query.
The value of this count is unaffected by the presence of the `LIMIT` clause.

The `LIMIT` clause trims the list of matched keys to generate a list of returned keys. The remainder of the response array is for the returned keys.

### `NOCONTENT` or `RETURN 0` was specified.

The remainder of the response array is one entry per returned key, consisting of the key name.

### `RETURN` with arguments was specified.

The remainder of the response array is two entries per returned key. The first entry is the key name and
the second entry is an array of name/value pairs. The array of name/value pairs is driven by the `RETURN` clause.
Each of the named fields in the `RETURN` clause is returned along with the value of that field for this particular key. If the named field isn't present in this key then it won't be included.
In addition, if this is a vector search, then one additional name/value pair will be included which is the computed vector distance for this returned key -- see [Search - query language](../topics/search-query.md) for details on how to control the name of that field.

### Neither `NOCONTENT` nor `RETURN` was specified.

If the index is on `HASH` keys, then the result is the same as if a `RETURN` clause had been present that listed all of the fields of a key.

If the index is on `JSON` keys, then one name/value pair is inserted with name `$` and the value being the entire JSON key as a string.
In addition, if this is a vector search, then one additional name/value pair will be included which is the computed vector distance for this returned key -- see [Search - query language](../topics/search-query.md) for details on how to control the name of that field.

# Examples

## Non-vector Queries On Hash Index

For these queries the following index and data definitions were used:

```
FT.CREATE index on hash SCHEMA color TAG city TAG
hset key1 color blue city London
hset key2 color black city Paris
hset key3 color green city Berlin
hset key4 color white city Tokyo
hset key5 color blend
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

## Non-vector Queries On JSON Index

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

## Pure vector search query

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
 2) p5
 3) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?
 4) p4
 5) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?
 6) p2
 7) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x80?\x00\x00\x00\x00
 8) p1
 9) 1) __description_score
    2) 1.6400001049
    3) description
    4) \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80?
10) p3
11) 1) __description_score
    2) 0.0399999953806
    3) description
    4) \x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00
```

## Query On JSON Index without RETURN clause

Simple query showing the JSON output

## Simple Query with RETURN Clause

Simple query showing selecting return of additional fields

## Query showing NOCONTENT

## Query showing SORTBY
