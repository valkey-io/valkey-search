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
