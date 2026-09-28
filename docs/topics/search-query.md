---
title: "Valkey Search - Queries"
description: Valkey Search Module Query Language Syntax, Semantics and Examples
---

The query of the `FT.SEARCH` and `FT.AGGREGATE` commands identifies a subset of the keys in the index to be processed by those commands.
The syntax and semantics of the query string is identical for both commands.

A query has three different formats: pure-vector, hybrid-vector and non-vector.
A non-vector query can also select keys by vector distance, with the [vector range matcher](#vector-range-match).

# Pure Vector Queries

A pure-vector query performs a K Nearest Neighbors (KNN) query of a single vector field within the index.

```
*=>[ KNN <K> @<field> $<parameter> [EF_RUNTIME <ef-value>] [AS <name>] ]
```

# Hybrid Vector Queries

A hybrid query adds a filter expression to indicate which keys within the index are candidates for results.

```
<filter>=>[ KNN <K> @<field> $<parameter> [EF_RUNTIME <ef-value>] [AS <name>] ]
```

# Non-vector Query

A non-vector query consists solely of a filter:

```
<filter>
```

Where:

- `<filter>` (optional): Filter expression, see below
- `K` (required): The number of nearest neighbor vectors to return.
- `field` (required): The name of a vector field within the specified index.
- `parameter` (required): A `PARAM` name whose corresponding value provides the query vector for the KNN algorithm.
  Note that this parameter must be encoded in little-endian byte order using the element type declared by the index (`TYPE FLOAT32`, `FLOAT16` or `BFLOAT16`); see [Supported Data Types](search-data-formats.md#supported-data-types). Its length must therefore be `DIM * 4` bytes for `FLOAT32` and `DIM * 2` bytes for the 16-bit types.
- `EF_RUNTIME <ef-value>` (optional): Overrides the default value of `EF_RUNTIME` specified when the index was created.
- `AS <name>` (optional): Overrides the default naming of the output distance field. By default this field is constructed by appending the string "\_\_score" to the name of the vector field.

## Filter Expression

A filter identifies a set of keys. Filters can be constructed using individual query operator as well as by combining operators with `AND`, `OR`, `NEGATE` operators.

It is not the case that the of filtering terminology implies an O(N) scan of keys in an index. Valkey search intelligently combines the usage of secondary indexes and simple filtering to efficiently locate the keys that a filter identifies. Determining the computational complexity of a particular filter is difficult, but generally no worse than O(log N) and sometimes as fast as O(1).

The BNF for a filter is:

```
<filter>        ::= <logical-or>

<logical-or>    ::= <logical-and>
                  | <logical-or> "|" <logical-and>
<logical-and>   ::= <logical-not>
                  | <logical-and> " " <logical-not>
<logical-not>   ::= <matcher>
                  | "-" <logical-not>
<matcher>       ::= <tag-match>
                  | <numeric-match>
                  | <term-match>
                  | <phrase-match>
                  | <fuzzy-match>
                  | <vector-range-match>
                  | "(" <logical-or> ")"

```

### Tag Match

The tag match operator is specified with one or more match strings separated by the `|` character.
Tag match supports both exact match and prefix match.
If a match string end with an `*` then prefix matching is performed, otherwise exact matching is performed.
Case insensitive matching can be configured when the field was declared.

```
@<field-name>:{<tag>}
or
@<field-name>:{<tag1> | <tag2>}
or
@<field-name>:{<tag1> | <tag2> | ...}
```

For example, the following query will return documents with blue OR black OR green color.

```
@color:{blue | black | green}
```

As another example, the following query will return documents containing "hello world" or "hello universe"

```
@color:{hello world | hello universe}
```

This example will match black or any word that starts with fred:

```
@color:{black | fred*}
```

For more examples see [Tag Fields](#example-tag-queries).

### Numeric Range Match

Numeric range matcher selects keys based on the value of a numeric field being between a given start and end value.
Both inclusive and exclusive range queries are supported. For simple relational comparisons, \+inf, \-inf can be used
with a range query.

The syntax for a range search operator is:

```
@<field-name>:[ [(] <bound> [(] <bound>]
```

where <bound> is either a decimal integer, floating-point number or ±inf

Bounds without a leading open paren are inclusive, whereas bounds with the leading open parenthesis are exclusive.

Use the following table as a guide for mapping mathematical expressions to filtering queries:

| Desired comparison | Numeric matcher    |
| :----------------: | :----------------- |
| min ≤ field ≤ max  | @field:[min max]   |
| min < field ≤ max  | @field:[(min max]  |
| min ≤ field < max  | @field:[min (max]  |
| min < field < max  | @field:[(min (max] |
|    field ≥ min     | @field:[min +inf]  |
|    field > min     | @field:[(min +inf] |
|    field ≤ max     | @field:[-inf max]  |
|    field < max     | @field:[-inf (max] |
|    field = val     | @field:[val val]   |

Examples of numeric matchers

```
@price:[10 100]           10 ≤ field ≤ 100
@price:[(10 100.5]        10 < field ≤ 100.5
@price:[-inf (1e2]        price < 100
```

### Vector Range Match

The vector range matcher selects the keys whose vector field is within a given distance, the radius, of a query vector.
Unlike a KNN query it has no result count: every key within the radius matches.
It can be combined with other matchers using AND, OR and negation.

```
@<field-name>:[VECTOR_RANGE <radius> $<parameter>]
@<field-name>:[VECTOR_RANGE <radius> $<parameter>]=>{$YIELD_DISTANCE_AS: <name>}
@<field-name>:[VECTOR_RANGE <radius> $<parameter>]=>{$YIELD_DISTANCE_AS: <name>; $EPSILON: <epsilon>}
```

- `field-name` (required): A `VECTOR` field of the index, `HNSW` or `FLAT`.
- `radius` (required): A non-negative number, or `$<name>` to take it from `PARAMS`. A key matches when its distance to the query vector is less than or equal to the radius, which can be `inf`. The distance depends on the `DISTANCE_METRIC` of the field:
  - `L2`: the **squared** Euclidean distance.
  - `IP`: `1 - dot(a, b)`, which is negative when the dot product is greater than 1.
  - `COSINE`: `1 - cos(a, b)`, between 0 and 2. Because of floating-point rounding, the distance between identical vectors can be slightly greater than 0, so use a small positive radius rather than 0 to match them.
- `parameter` (required): A `PARAMS` name whose value is the query vector, encoded as for a KNN query (see above).
- `$YIELD_DISTANCE_AS: <name>` (optional): Returns the distance of each key within the radius under `<name>`. Without it, no distance is returned. When `RETURN` is used, the distance is returned only if `RETURN` lists `<name>`. `<name>` can be used by `SORTBY`, and as `@<name>` by the stages of `FT.AGGREGATE`. `FT.SEARCH` rejects a name that is an attribute of the index; `FT.AGGREGATE` accepts it, and `@<name>` then refers to the distance, not the attribute.
- `$EPSILON: <epsilon>` (optional): Accepted on `HNSW` fields for compatibility and ignored. It must be greater than 0. It is an error on `FLAT` fields.

The keyword and the attribute names are case-insensitive. As an extension, the distance can also be named with `AS <name>` inside the brackets, as in a KNN query: `@<field-name>:[VECTOR_RANGE <radius> $<parameter> AS <name>]`.

Restrictions:

- A query can contain at most one vector range matcher, and a vector range matcher cannot be used in the filter of a KNN query (Redis accepts both).
- `EF_RUNTIME` is not supported.
- The attributes must directly follow the closing `]`, so `(@v:[VECTOR_RANGE 0.2 $vec])=>{$YIELD_DISTANCE_AS: dist}` is an error. `$weight` is not supported, and attribute values are not substituted from `PARAMS`.

Order and scores: a vector range query is a non-vector query, so its results are not sorted by distance, and `WITHSCORES` reports the relevance score of the other matchers of the query (0 if there are none).
To get the closest keys first, yield the distance and sort on it, for example `SORTBY <name> ASC`.

Non-finite distances: a NaN or `+inf` distance, from a NaN or infinite vector component, is within no radius, not even `inf`; a `-inf` distance, which only `IP` can produce, is within every radius.

`HNSW` fields: the search examines at most `search.max-nonvector-search-results-fetched` (default 100000) candidates, so the result is approximate, as for a KNN query, and can miss keys within the radius; a query that matches at least that many keys is answered by scanning the whole index. `FLAT` fields are always searched exhaustively. In a cluster, each shard applies the setting to its own keys.

Limitation: when a query combines a vector range matcher with other matchers using `|` (OR), and it matches more keys than `search.max-nonvector-search-results-fetched`, some of the matching keys can be missing from the result.

For examples, see [Example Vector Range Queries](#example-vector-range-queries).

## Text Search Operators

Unlike the other search operators. The text search operators do not require that a field be specified. If a field is not specified for a text search operator, then all text fields within the index are searched. Regardless, if multiple text search operators are combined in an expression, then only keys which have all of the text search operators will satisfy the query.

### Term Search

The term search operator matches a single word. If the word is a stop word, the term search operator is removed from the query expression.
Term searches are subject to stemming unless the `VERBATIM` option is specified.

Examples include:

```
hello                   matches the word hello in any text field
@t:hello                matches the word hello but only in the t field (t must be a text field)
```

### Prefix Matching

A term with a trailing `*` matches any word that starts with that term.

```
hello*                  matches words that start with hello such as hello, hello1, hello_world but not ohello
@t:hello*               matches words that start with hello in the t field (t must be a text field)
```

### Suffix Matching

A term with a leading `*` matches any word that ends with that term.

Note that suffix searching will only locate words in fields that have `WITHSUFFIXTRIE` specified, i.e., fields declared with `NOSUFFIXTRIE` will not be searched.
If a field specifier is added to a suffix term search and that particular field was declared with `NOSUFFIXTRIE` then an error will be issued.

```
*hello                  matches words that end with hello such as hello, ohello but not hello1
@t:*hello               matches words that end with hello in the t field (t must be a text field with WITHSUFFIXTRIE)
```

### Exact Phrase Search

The exact phrase search operator matches an exact sequence of words in a text field. The words to be matched are enclosed in double quotes. The words are not subject to stop word removal nor stemming, otherwise this is equivalent to having the same words in a query with `SLOP 0` and `INORDER` options being specified.

```
"hello world"            matches the exact phrase "hello world" in any text field
@t:"hello world"         matches the exact phrase "hello world" in the t field (t must be a text field)
```

### Fuzzy Search

The fuzzy search operator matches words within a fixed damerau-levenshtein distance. See [damerau-levenshtein edit distance](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance) for more information. Fuzzy matching is specified by enclosing the base word in percent symbols `%` one for each allowable edit distance. The maximum allowed edit distance is control by the configuration setting `search.fuzzy-max-distance`.

```
%hello%                 matches words that are one edit away from hello such as hello, hello1
%%hello%%              matches words that are two edits away from hello such as hello, hello1, ohello, hllo
@t:%hello%             matches words that are one edit away from hello but only in the t field
%%hello%               Error: leading and trailing count % count are different
```

## Logical Operators

### Logical Negation

Any query can be negated by prepending the `-` character before each query. Negative queries return all keys that don't match the query. This also includes keys that don't have the field.

For example, the negative query `-@genre:{comedy}` will return all books that are not comedy AND all books that don't have a genre field.

The following query will return all books with "comedy" genre that are not published between 2015 and 2024, or that have no year field:

```
@genre: {comedy} \-@year:[2015 2024]
```

### Logical `OR`

To set a logical OR, use the `|` character between the predicates.

Example:

```
query1 | query2 | query3
```

### Logical `AND`

To specify the `AND` operation use a space between the predicates. If the `INORDER` and `SLOP` options are not provided then the `AND` operation is done at the key level. This means that any key which satisfies each of the predicates will match.

If either of the `INORDER` or `SLOP` options are provided, then the `AND` operation is extended beyond simple key matching to also include positional matching of the text searching operators. Positional matching requires that each predicate not only match the same key but that the text searching operators (term, prefix, suffix, exact phrase and fuzzy) must also match words that satisfy the requirements of `INORDER` and `SLOP` in the same field of the same key.

For example:

```
query1 query2 query3
```

### Proximity `AND`

When two or more predicates of an AND operation contain text matchers, it becomes possible to also perform positional matching. Positional matching extends key-based matching to additionally require that matching words meet specified distance and ordering constraints. Positional matching is only applied within a single Text field. There is no positional relationship between terms in different Text fields.

Position matching is enabled when either the `SLOP` or `INORDER` clauses are used on the command and applies to all multi-predicate AND operations within the current command.

# Examples

## Example Tag Queries

For these examples, the following index declaration and data set will be used.

```
valkey-cli FT.CREATE index SCHEMA color TAG
valkey-cli HSET key1 color blue
valkey-cli HSET key2 color black
valkey-cli HSET key3 color green
valkey-cli HSET key4 color beige
valkey-cli HSET key5 color "beige,green"
valkey-cli HSET key6 color "hello world, green is my heart"
```

### Simple Tag Query

```
valkey-cli FT.SEARCH index @color:{blue} RETURN 1 color
1) (integer) 1
2) "key1"
3) 1) "color"
   2) "blue"
```

### Multiple Tag Query

```
valkey-cli FT.SEARCH index "@color:{blue | black}" RETURN 1 color
1) (integer) 2
2) "key2"
3) 1) "color"
   2) "black"
4) "key1"
5) 1) "color"
   2) "blue"
```

### Prefix Tag Query

```
valkey-cli FT.SEARCH index @color:{b\*} RETURN 1 color
1) (integer) 4
2) "key2"
3) 1) "color"
   2) "black"
4) "key1"
5) 1) "color"
   2) "blue"
6) "key4"
7) 1) "color"
   2) "beige"
8) "key5"
9) 1) "color"
   2) "beige,green"
```

### Complex Tag Query

```
valkey-cli FT.SEARCH index @color:{b*|green} RETURN 1 color
1) (integer) 2
2) "key3"
3) 1) "color"
   2) "green"
4) "key5"
5) 1) "color"
   2) "beige,green"
```

## Example Logical Operators

Logical operators can be combined to form complex filter expressions.

The following query will return all books with "comedy" or "horror" genre (AND) published between 2015 and 2024:

```

@genre:{comedy|horror} @year:[2015 2024]

```

The following query will return all books with "comedy" or "horror" genre (OR) published between 2015 and 2024:

```

@genre:{comedy|horror} | @year:[2015 2024]

```

The following query will return all books that either don't have a genre field, or have a genre field not equal to "comedy",
that are published between 2015 and 2024:

```
-@genre:{comedy} @year:[2015 2024]

```

## Example Vector Range Queries

For these examples, the following index and data are used. The commands are entered at the `valkey-cli` prompt, which decodes the `\x` escapes of the vectors. The vectors of `p1`, `p2` and `p3` are (1, 0), (0, 2) and (3, 4), and the query vector is (0, 0), so the `L2` distances are 1, 4 and 25.

```
FT.CREATE idx SCHEMA category TAG v VECTOR HNSW 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2
HSET p1 category shoes v "\x00\x00\x80?\x00\x00\x00\x00"
HSET p2 category shirts v "\x00\x00\x00\x00\x00\x00\x00@"
HSET p3 category shoes v "\x00\x00@@\x00\x00\x80@"
```

The keys within distance 5, closest first, with their distance:

```
FT.SEARCH idx "@v:[VECTOR_RANGE 5 $vec]=>{$YIELD_DISTANCE_AS: dist}" PARAMS 2 vec "\x00\x00\x00\x00\x00\x00\x00\x00" SORTBY dist RETURN 1 dist DIALECT 2
1) (integer) 2
2) "p1"
3) 1) "dist"
   2) "1"
4) "p2"
5) 1) "dist"
   2) "4"
```

A vector range matcher combined with a tag matcher, with the radius given as a parameter:

```
FT.SEARCH idx "@category:{shoes} @v:[VECTOR_RANGE $r $vec]" PARAMS 4 r 30 vec "\x00\x00\x00\x00\x00\x00\x00\x00" NOCONTENT DIALECT 2
1) (integer) 2
2) "p1"
3) "p3"
```

The closest distance per category:

```
FT.AGGREGATE idx "@v:[VECTOR_RANGE 30 $vec]=>{$YIELD_DISTANCE_AS: dist}" PARAMS 2 vec "\x00\x00\x00\x00\x00\x00\x00\x00" GROUPBY 1 @category REDUCE MIN 1 @dist AS closest SORTBY 2 @closest ASC DIALECT 2
1) (integer) 2
2) 1) category
   2) "shoes"
   3) closest
   4) "1"
3) 1) category
   2) "shirts"
   3) closest
   4) "4"
```
