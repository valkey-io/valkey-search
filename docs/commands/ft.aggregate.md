The `FT.AGGREGATE` command extends the query capabilities of the `FT.SEARCH` command with substantial server-side data processing capabilities.

The first step of the command is to use the index name and query string to locate a list of Valkey keys. This process is identical to the `FT.SEARCH` command.

The second step is executed once for each key found in the first step. The contents of the key are extracted as directed by the `LOAD` clause, plus any declared attribute (other than a vector field) that a later stage references as `@<name>`, resulting in a set of name/value pairs which are collected into a record. Thus the output of this step is the working set of records, one record for each key found in the first step. If the query is a KNN query then the initial working set will be sorted by distance, otherwise the working set will be in no particular order.

The next steps are to execute the list of stages provided on the command, sequentially one at a time. Each step takes as input the working set of records and performs some transformation on it, i.e., the output of one stage is fed into the input of the next stage.

Once all stages have been executed, the working set of records output from the last stage is returned as the result of the command.

```
FT.AGGREGATE <index-name> <query>
    [ADDSCORES]
    [DIALECT 2]
    [INORDER]
    [LOAD * | LOAD <count> <field> [AS <alias>] [<field> [AS <alias>] ...]]
    [PARAMS <count> <name> <value> [ <name> <value> ...]]
    [SCORER <scorer>]
    [SLOP <slop>]
    [TIMEOUT <timeout>]
    [VERBATIM]
    [WITHCURSOR [COUNT <count>] [MAXIDLE <maxidle>]]
    (
        APPLY <expression> AS <field>
      | FILTER <expression>
      | GROUPBY <count> [<field> ... ] [REDUCE <reducer> <count> [<expression> ...] [AS <name>] [REDUCE ...]]
      | LIMIT <offset> <count>
      | SORTBY <count> <expression> [ASC | DESC] [<expression> [ASC | DESC] ...] [MAX <num>]
    )+
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search-query.md) for details.
- `ADDSCORES` (optional): Adds the relevance score computed for each key to its record as the field `__score`, which later stages can reference as `@__score`.
- `DIALECT 2` (optional): Specifies your dialect. The only supported dialect is 2, which is also the default.
- `INORDER` (optional): Indicates that proximity matching of terms must be in order.
- `LOAD * | LOAD <count> <field> [AS <alias>] [<field> [AS <alias>] ...]` (optional): This controls which fields of those keys are loaded into the working set. A star (\*) indicates that all of the fields of the keys are loaded. The key itself can be loaded by specifying `@__key`. For vector queries, the distance can also be loaded by using its `AS` name (for KNN the default is `__<field>_score`).

  `<count>` is a count of the arguments that follow, not a count of fields. An `AS` keyword and its alias each count against `<count>`, so `LOAD 4 @a @b AS c` loads two fields using four arguments.

  Each `<field>` may be a declared attribute name (`@price`), or, for a JSON index, a JSON path (`$.price`). A JSON path that resolves to a declared attribute is loaded as that attribute.

  `AS <alias>` renames the loaded field: it is emitted under `<alias>` in the result and is referenced as `@<alias>` by later stages. Without an `AS` clause the field is emitted under the name given in the `LOAD` clause (for a JSON attribute this requires `search.emulate-release` 1.3.0 or later; below that it is emitted under its JSON path). An alias may reuse the name of a declared attribute, in which case it hides that attribute for the remainder of the pipeline. If an `AS` alias collides with another output name in the same `LOAD` clause, the command fails; loading the same field twice without `AS` is allowed.

  Honoring `AS` in the `LOAD` clause is a compatibility fix gated on `search.emulate-release` being `1.3.0` or greater; under an emulated release below that, `AS` is treated as an ordinary field name. Accepting a JSON path as `<field>` is not gated, since before it was supported such a load simply failed. See [COMPATIBILITY.md](../../COMPATIBILITY.md) for details.
- `PARAMS <count> <name> <value> [<name> <value> ...]` (optional): `<count>` is the number of arguments that follow, i.e., twice the number of `name`/`value` pairs. `PARAMS` can be used in both the query string as well as within an expression context. See [Search - query language](../topics/search-query.md) for usage details.
- `SCORER <scorer>` (optional): Selects the relevance scoring function used to rank text results. The only supported scorer is `BM25STD`, which is also the default.
- `SLOP <slop>` (optional): Specifies a slop value for proximity matching of terms.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- `VERBATIM` (optional): If specified stemming is not applied to term searches.
- `WITHCURSOR [COUNT <count>] [MAXIDLE <maxidle>]` (optional): Returns at most `<count>` records and saves the remaining records in a cursor, which is read with [`FT.CURSOR`](ft.cursor.md). `<count>` must be between 1 and `search.cursor-max-count` (default 100000), the default is 1000. `<maxidle>` is the number of milliseconds the cursor may go unread before it is destroyed; it must be between 1 and `search.cursor-max-idle-ms` (default unlimited, see [configurables](../topics/search-configurables.md)), the default is 300000 (or `search.cursor-max-idle-ms` if that is smaller). `WITHCURSOR` may appear anywhere after the query, including between stages. If it is given more than once, the last one is used. See [`FT.CURSOR`](ft.cursor.md) for an example.

- `APPLY <expression> AS <field>` (optional): An expression is computed and inserted into the record. See [APPLY Stage](#apply-stage) below. See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.
- `FILTER <expression>` (optional): The filter expression is applied, see [FILTER Stage](#filter-stage) for more details. See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.
- `GROUPBY <count> [<field> ...] [REDUCE <reducer> <count> [<expression> ...] [AS <name>] ...]` (optional): The working set is grouped into buckets according to the input fields, each written as `@<field>`. `AS <name>` names the reducer's output; see [Reducers](#reducers) for the default name. One summarization record is generated for each bucket including the outputs of each reducer. See [GROUPBY Stage](#groupby-stage) for details.
- `LIMIT <offset> <count>` (optional): The working set is trimmed, see [LIMIT Stage](#limit-stage) for details.
- `SORTBY <count> <expression> [ASC | DESC] [<expression> [ASC | DESC] ...] [MAX <num>]` (optional): The working set is sorted. See [SORTBY Stage](#sortby-stage) for more details.

## Processing Stages

### APPLY Stage

The `APPLY` stage processes each record of the working set in sequence. For each record the expression is computed with output being set into the record with the provided field name. It is allowed to overwrite an existing field. The apply stage normally keeps every record. With `search.emulate-release` 1.3.0 or later, a record whose expression evaluates to _missing_ is dropped. `exists`, `concat` and `&&` don't produce missing, so `APPLY exists(@f) AS present` keeps the record. See [missing values](../topics/search-expressions.md#primary-values). An expression that fails with an error fails the command. See [COMPATIBILITY.md](../../COMPATIBILITY.md).

### FILTER Stage

The `FILTER` stage processes each record of the working set in sequence. For each record the expression is computed. If the expression is false (see [Logical Operators](../topics/search-expressions.md#logical-operators)) the record is discarded, else the record remains. An expression that fails with an error fails the command.

### LIMIT Stage

The `LIMIT` stage discards records from the working set based on the provided offset and count.

### SORTBY Stage

The `SORTBY` stage reorders the records in accordance with a sort key. A sort key can be constructed using a number of expressions optionally combined with a direction. `<count>` counts the arguments that follow, including each `ASC` or `DESC`, so `SORTBY 2 @price DESC` sorts by one key.

`SORTBY` keeps at most `MAX` records; without `MAX` it keeps 10. With `search.emulate-release` 1.3.0 or later, `MAX 0` is the same as omitting it, and a `LIMIT <offset> <count>` stage directly before or after the `SORTBY` raises this bound to at least `<offset>`+`<count>`, so `SORTBY 2 @price ASC LIMIT 100 10` returns records 101-110. Below 1.3.0 only `MAX` sets the bound, so a following `LIMIT` sees at most `MAX` (by default 10) records. See [COMPATIBILITY.md](../../COMPATIBILITY.md).

### GROUPBY Stage

The `GROUPBY` stage organizes the input records into buckets based on the values of the specified fields.
For each unique combination of values a separate bucket is created to hold all records that have that combination of values. A field whose value is an array (such as a multi-value JSON field or the output of `TOLIST`) puts the record into one bucket per element, or one per combination when several fields hold arrays; an empty array groups as nil. A record that would go into more than `search.max-group-key-expansion` buckets fails the command.

Each bucket of records is processed into a single output record, discarding the bucket contents. That output record has two sections. The first section has one value for each of the specified `GROUPBY` fields. This section provides the values that formed (named) this unique bucket.

The second section is the output of the reducers for that bucket. Reducers provide an efficient mechanism for reducing (summarizing) the contents of a bucket. Each reducer function processes each record of the bucket and generates a single output value which is inserted into the second section of `GROUPBY` output record for this bucket.

The output of the `GROUPBY` stage is one record for each unique bucket.

With a `<count>` of 0 (`GROUPBY 0`) no fields are specified and all records go into a single bucket, so the output is one record holding only the reducer outputs, or no record if the working set is empty.

#### Reducers

The following reducer functions are available. SUM, AVG, STDDEV and QUANTILE convert their input to a number. A group with no numeric value gives `0` for SUM and AVG, or `nan` with `search.emulate-release` 1.3.0 or later. MIN and MAX do so only with `search.emulate-release` 1.3.0 or later: values that are not numbers are skipped, and a group with no numeric value gives `inf` for MIN and `-inf` for MAX. Below 1.3.0, MIN and MAX compare values as they are, so a `TAG` or text field gives a string, and a group with no value has no MIN or MAX entry in its output record. The other reducers operate on the values as they are.

Without `AS <name>`, the reducer's output is named from the reducer and its argument. For a reducer whose argument is a single field, such as `REDUCE SUM 1 @Price`, the name is `__generated_aliassumprice` with `search.emulate-release` 1.3.0 or later: `__generated_alias` followed by the reducer name and the field name without `@`, all lowercased. Below 1.3.0 it is `SUM(@Price)`. `FIRST_VALUE` always uses the `FIRST_VALUE(<args>)` form. Use `AS` to give the output a stable name for later stages. See [COMPATIBILITY.md](../../COMPATIBILITY.md).

| Syntax                        | Function                                                                                                                           |
| :---------------------------- | :--------------------------------------------------------------------------------------------------------------------------------- |
| COUNT 0                       | Number of records                                                                                                                  |
| COUNT_DISTINCT 1 <expression> | The exact number of distinct values of the expression. Caution: this consumes memory proportional to the number of distinct values. |
| COUNT_DISTINCTISH 1 <expression> | Approximate number of distinct values using HyperLogLog (P=14), with ~0.81% standard error. Uses at most ~12KB memory per group. Preferred over COUNT_DISTINCT for large groups. Nil values are excluded. |
| SUM 1 <expression>            | The numerical sum of the values of the expression.                                                                                 |
| MIN 1 <expression>            | The smallest value of the expression (see above for values that are not numbers).                                                  |
| MAX 1 <expression>            | The largest value of the expression (see above for values that are not numbers).                                                   |
| AVG 1 <expression>            | The numerical average of the values of the expression.                                                                             |
| STDDEV 1 <expression>         | The standard deviation the values of the expression.                                                                               |
| FIRST_VALUE 1 <expression>    | The first value of the expression encountered in the group. Order depends on record retrieval order. Use only when order does not matter. |
| FIRST_VALUE 3 <expression> BY <expression> | The value of the first expression from the record with the smallest comparison expression (ascending). Ties broken by first-encountered order. |
| FIRST_VALUE 4 <expression> BY <expression> ASC\|DESC | The value of the first expression from the record with the minimum (ASC) or maximum (DESC) comparison expression. Ties broken by first-encountered order. Invalid keyword arguments (e.g., wrong BY token or unrecognized direction) produce a parse-time error. |
| RANDOM_SAMPLE 2 <expression> <sample_size> | A random sample of values from the expression using reservoir sampling. Returns an array of up to sample_size elements (maximum 1000). Nil values are excluded. Output is non-deterministic. |
| TOLIST 1 <expression>         | An array of the distinct values of the expression in the group, in first-encountered order. Nil values are skipped; an array value contributes its elements. |
| QUANTILE 2 <expression> <quantile> | An approximation of the value at the given quantile of the expression. `<quantile>` is a numeric literal from 0 to 1; 0 returns the smallest value and 1 the largest. Nil, NaN and non-numeric strings are ignored. The result is always one of the remaining values: with n values, its position in sorted order (1 for the smallest) differs from `<quantile>` × n, rounded up and at least 1, by at most 1% of n. For groups of more than 500 values the result can depend on the order in which records are processed. If a group has no remaining numeric values, the result is `nan`. |

## Reply

There are two reply formats depending on whether the `WITHCURSOR` option was specified.

If `WITHCURSOR` was not specified, then the output is an array of N+1 elements, where N is the number of records output by the final processing stage:

| Element | Type    | Contents                                 |
| :------ | :------ | :--------------------------------------- |
| `[0]`   | Integer | N, the number of records that follow.    |
| `[1]`   | Array   | The first record: its field/value pairs. |
| ...     |         |                                          |
| `[N]`   | Array   | The last record: its field/value pairs.  |

If `WITHCURSOR` was specified, then the output is an array of two elements:

| Element  | Type    | Contents                                                                                                                                              |
| :------- | :------ | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `[0]`    | Array   | The records returned by this reply, in N+1 elements:                                                                                                  |
| `[0][0]` | Integer | N, the number of records returned.                                                                                                                    |
| `[0][1]` | Array   | The first record: its field/value pairs.                                                                                                              |
| ...      |         |                                                                                                                                                       |
| `[0][N]` | Array   | The last record: its field/value pairs.                                                                                                               |
| `[1]`    | Integer | The cursor id to pass to [`FT.CURSOR READ`](ft.cursor.md), or 0 if this reply contains all the remaining records, in which case no cursor is created. |

## Examples

Count the products in each category and total their prices, most numerous category first:

```
> FT.AGGREGATE idx * GROUPBY 1 @category REDUCE COUNT 0 AS count REDUCE SUM 1 @price AS total SORTBY 2 @count DESC
1) (integer) 2
2) 1) "category"
   2) "books"
   3) "count"
   4) "3"
   5) "total"
   6) "60"
3) 1) "category"
   2) "toys"
   3) "count"
   4) "2"
   5) "total"
   6) "25"
```
