# Command List

- [`FT.CREATE`](#ftcreate)
- [`FT.DROPINDEX`](#ftdropindex)
- [`FT.INFO`](#ftinfo)
- [`FT._LIST`](#ft_list)
- [`FT.SEARCH`](#ftsearch)

#

## FT.CREATE

The `FT.CREATE` command creates an empty index and initiates the backfill process. Each index consists of a number of field definitions. Each field definition specifies a field name, a field type and a path within each indexed key to locate a value of the declared type. Some field type definitions have additional sub-type specifiers.

For indexes on HASH keys, the path is the same as the hash member name. The optional `AS` clause can be used to rename the field if desired.
Renaming of fields is especially useful when the member name contains special characters.

For indexes on JSON keys, the path is a JSON path to the data of the declared type. Because the JSON path always contains special characters, the `AS` clause is required.

```bash
FT.CREATE <index-name>
    ON HASH
    [PREFIX <count> <prefix> [<prefix>...]]
    [SKIPINITIALSCAN]
    SCHEMA
        (
            <field-identifier> [AS <field-alias>]
                  NUMERIC
                | TAG [SEPARATOR <sep>] [CASESENSITIVE]
                | VECTOR [HNSW | FLAT] <attr_count> [<attribute_name> <attribute_value>]+
            [SORTABLE]
        )+
```

- **\<index-name\>** (required): This is the name you give to your index. If an index with the same name exists already, an error is returned.

- **ON HASH | JSON** (optional): Only keys that match the specified type are included into this index. If omitted, HASH is assumed.

- **PREFIX \<prefix-count\> \<prefix\>** (optional): If this clause is specified, then only keys that begin with the same bytes as one or more of the specified prefixes will be included into this index. If this clause is omitted, all keys of the correct type will be included. A zero-length prefix would also match all keys of the correct type.

- **SKIPINITIALSCAN** (optional): If this clause is specified, then no backfill operation is performed. This means that pre-existing keys which match the prefix clause will not be loaded into the index.

### Field types

**TAG**: A tag field is a string that contains one or more tag values.

- **SEPARATOR \<sep\>** (optional): One of these characters `,.<>{}[]"':;!@#$%^&*()-+=~` used to delimit individual tags. If omitted the default value is `,`.
- **CASESENSITIVE** (optional): If present, tag comparisons will be case-sensitive. The default is that tag comparisons are NOT case-sensitive

**NUMERIC**: A numeric field contains a number.

**VECTOR**: A vector field contains a vector. Two vector indexing algorithms are currently supported: HNSW (Hierarchical Navigable Small World) and FLAT (brute force). Each algorithm has a set of additional attributes, some required and other optional.

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

### Field options

**SORTABLE**: This parameter is currently ignored as all field types are considered to be sortable

**RESPONSE** OK or error.

## FT.DROPINDEX

```
FT.DROPINDEX <index-name>
```

The specified index is deleted. It is an error if that index doesn't exist.

- **\<index-name\>** (required): The name of the index to delete.

**RESPONSE** OK or error.

## FT.INFO

```
FT.INFO <index-name>
```

Detailed information about the specified index is returned.

- **\<index-name\>** (required): The name of the index to return information about.

**RESPONSE**

An array of key value pairs.

- **index_name** (string) The index name
- **num_docs** (integer) Total keys in the index
- **num_records** (integer) Total records in the index
- **hash_indexing_failures** (integer) Count of unsuccessful indexing attempts
- **indexing** (integer) Binary value. Shows if background indexing is running or not
- **percent_indexed** (integer) Progress of background indexing. Percentage is expressed as a value from 0 to 1
- **index_definition** (array) An array of values defining the index
  - **key_type** (string) HASH. This is the only available key type.
  - **prefixes** (array of strings) Prefixes for keys
  - **default_score** (integer) This is the default scoring value for the vector search scoring function, which is used for sorting.
  - **attributes** (array) One array of entries for each field defined in the index.
    - **identifier** (string) field name
    - **attribute** (string) An index field. This is correlated to a specific index HASH field.
    - **type** (string) VECTOR. This is the only available type.
    - **index** (array) Extended information about this internal index for this field.
      - **capacity** (integer) The current capacity for the total number of vectors that the index can store.
      - **dimensions** (integer) Dimension count
      - **distance_metric** (string) Possible values are L2, IP or Cosine
      - **data_type** (string) FLOAT32. This is the only available data type
      - **algorithm** (array) Information about the algorithm for this field.
        - **name** (string) HNSW or FLAT
        - **m** (integer) The count of maximum permitted outgoing edges for each node in the graph in each layer. The maximum number of outgoing edges is 2\*M for layer 0\. The Default is 16\. The maximum is 512\.
        - **ef_construction** (integer) The count of vectors in the index. The default is 200, and the max is 4096\. Higher values increase the time needed to create indexes, but improve the recall ratio.
        - **ef_runtime** (integer) The count of vectors to be examined during a query operation. The default is 10, and the max is 4096\.

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
  [PARAMS nargs <name> <value> [ <name> <value> ...]]
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
