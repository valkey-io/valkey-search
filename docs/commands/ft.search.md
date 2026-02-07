Performs a search of the specified index. The keys which match the query expression are returned.

```
FT.SEARCH <index> <query>
  [NOCONTENT]
  [TIMEOUT <timeout>]
  [PARAMS <count> <name> <value> [ <name> <value> ...]]
  [LIMIT <offset> <num>]
  [INORDER]
  [SLOP <slop>]
  [DIALECT <dialect>]
  [RETURN <count> <field> [AS <name>] <field> [AS <name>]...]
  [SORTBY <field> [ ASC | DESC]]
  [ALLSHARDS | SOMESHARDS]
  [CONSISTENT | INCONSISTENT]
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search.query.md) for details.
- `NOCONTENT` (optional): When present, only the resulting key names are returned, no key values are included.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- `PARAMS <count> <name> <value> [<name> <value> ...]` (optional): `count` is of the number of arguments, i.e., twice the number of value/name pairs. [Search - query language](../topics/search.query.md) for details.
- `RETURN <count> <field> [AS <name>] <field> [AS <name>] ...` (options): `count` is the number of fields to return. Specifies the fields you want to retrieve from your documents, along with any renaming for the returned values. By default, all fields are returned unless the `NOCONTENT` option is set, in which case no fields are returned. If num is set to 0, it behaves the same as `NOCONTENT`.
- `LIMIT <offset> <count>` (optional): Lets you choose a portion of the result. The first `<offset>` keys are skipped and only a maximum of `<count>` keys are included. The default is LIMIT 0 10, which returns at most 10 keys.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `INORDER` (optional): Indicates that proximity matching of terms must be inorder.
- `SLOP <slop>` (Optional): Specifies a slop value for proximity matching of terms.
- `SORTBY <field> [ASC | DESC]` (Optional): If present, results are sorted according the value of the specified field and the optional sort-direction instruction. By default, vector results are sorted in distance order and non-vector results are not sorted in any particular order. Sorting is applied before the `LIMIT` clause is applied.
- `ALLSHARDS` (Optional): If specified, the command is terminated with a timeout error if a valid response from all shards is not received within the timeout interval. This is the default.
- `SOMESHARDS` (Optional): If specified, the command will generate a best-effort reply if all shards have not responded within the timeout interval. 
- `CONSISTENT` (Optional): If specified, the command is terminated with an error if the cluster is in an inconsistent state. This is the default. 
- `INCONSISTENT` (Optional): If specified, the command will generate a best-effort reply if the cluster remains inconsistent within the timeout interval.

Response

On success, the first entry in the response array represents the count of matching keys, followed by one array entry for
each matching key. Note that if the `LIMIT` option is specified it will only control the number of returned keys and will
not affect the value of the first entry.

When `NOCONTENT` is specified, each entry in the response contains only the keyname,
Otherwise, each entry includes the keyname, followed by an array of the returned fields.

The array of returned fields for a key is a set of name/value pairs. The set of name/value pairs for a key is controlled by the `RETURN` clause. For a vector query an additional name/value pair is returned to provide the vector distance that was computed for this key. See [Search - query language](../topics/search.query.md) for details on how to control that name.

## Complete example: Simple vector search query

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

