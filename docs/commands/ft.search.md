Performs a search of the specified index. The keys which match the query expression are returned.

- `index` (required): This index name you want to query.
- `query` (required): The query string, see below for details.
- `NOCONTENT` (optional): When present, only the resulting key names are returned, no key values are included.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliSeconds.
- `PARAMS <count> <name1> <value1> <name2> <value2> ...` (optional): `count` is of the number of arguments,
    i.e., twice the number of value name pairs. See the query string for usage details.
- `RETURN <count> <field1> <field2> ...` (options): `count` is the number of fields to return.
    Specifies the fields you want to retrieve from your documents, along with any aliases for the returned values.
    By default, all fields are returned unless the `NOCONTENT` option is set, in which case no fields are returned.
    If num is set to 0, it behaves the same as `NOCONTENT`.
- `LIMIT <offset> <count` (optional): Lets you choose a portion of the result.
    The first `<offset>` keys are skipped and only a maximum of `<count>` keys are included. The default is LIMIT 0 10,
    which returns at most 10 keys.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.

On success, the first entry in the response array represents the count of matching keys, followed by one array entry for
each matching key. Note that if the `LIMIT` option is specified it will only control the number of returned keys and will
not affect the value of the first entry.

When `NOCONTENT` is specified, each entry in the response contains only the matching keyname,
Otherwise, each entry includes the matching keyname, followed by an array of the returned fields.

The result fields for a key consists of a set of name/value pairs. The first name/value pair is for the distance computed.
The name of this pair is constructed from the vector field name prepended with `__` and appended with `_score` and
the value is the computed distance. The remaining name/value pairs are the members and values of the key
as controlled by the `RETURN` clause.

The query string conforms to this syntax:

```
<filtering>=>[ KNN <K> @<vector_field_name> $<vector_parameter_name> <query-modifiers> ]
```

Where:

- `filtering` Is either a `*` or a filter expression. A `*` indicates no filtering and thus all vectors within the
    index are searched. A filter expression can be provided to designate a subset of the vectors to be searched.
- `vector_field_name` The name of a vector field within the specified index.
- `K` The number of nearest neighbor vectors to return.
- `vector_parameter_name` A `PARAM` name whose corresponding value provides the query vector for the KNN algorithm.
    Note that this parameter must be encoded as a 32-bit IEEE 754 binary floating point in little-endian format.
- `query-modifiers` (Optional) A list of keyword/value pairs that modify this particular KNN search. Currently two
    keywords are supported:
  - `EF_RUNTIME` This keyword is accompanied by an integer value which overrides the default value of `EF_RUNTIME`
    specified when the index was created.
  - `AS` This keyword is accompanied by a string value which becomes the name of the score field in the result,
    overriding the default score field name generation algorithm.

## Filter Expression

A filter expression is constructed as a logical combination of Tag and Numeric search operators contained within parenthesis.

## Tag

The tag search operator is specified with one or more strings separated by the `|` character.
A key will satisfy the Tag search operator if the indicated field contains any one of the specified strings.

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

## Numeric Range

Numeric range operator allows for filtering queries to only return values that are in between a given start and end value.
Both inclusive and exclusive range queries are supported. For simple relational comparisons, \+inf, \-inf can be used
with a range query.

The syntax for a range search operator is:

```
@<field_name>:[ [(] <bound> [(] <bound>]
```

where <bound> is either a number or \+inf or \-inf

Bounds without a leading open paren are inclusive, whereas bounds with the leading open parenthesis are exclusive.

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

## Logical Operators

Multiple tags and numeric search operators can be used to construct complex queries using logical operators.

### Logical `AND`

To set a logical AND, use a space between the predicates. For example:

```
query1 query2 query3
```

### Logical `OR`

To set a logical OR, use the `|` character between the predicates. For example:

```
query1 | query2 | query3
```

**Logical Negation**

Any query can be negated by prepending the `-` character before each query. Negative queries return all entries that don't
match the query. This also includes keys that don't have the field.

For example, a negative query on @genre:{comedy} will return all books that are not comedy AND all books that don't have
a genre field.

The following query will return all books with "comedy" genre that are not published between 2015 and 2024, or that have
no year field:

`@genre: [comedy] \-@year:[2015 2024]`

## Operator Precedence

Typical operator precedence rules apply, i.e., Logical negate is the highest priority, followed by Logical and and then
Logical Or with the lowest priority. Parenthesis can be used to override the default precedence rules.

**Examples of Combining Logical Operators**

Logical operators can be combined to form complex filter expressions.

The following query will return all books with "comedy" or "horror" genre (AND) published between 2015 and 2024:

`@genre:[comedy|horror] @year:[2015 2024]`

The following query will return all books with "comedy" or "horror" genre (OR) published between 2015 and 2024:

`@genre:[comedy|horror] | @year:[2015 2024]`

The following query will return all books that either don't have a genre field, or have a genre field not equal to "comedy",
that are published between 2015 and 2024:

`-@genre:[comedy] @year:[2015 2024]`

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

