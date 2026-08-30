The `FT.AGGREGATE` command extends the query capabilities of the `FT.SEARCH` command with substantial server-side data processing capabilities.

The first step of the command is to use the index name and query string to locate a list of Valkey keys. This process is identical to the `FT.SEARCH` command.

The second step is executed once for each key found in the first step. The contents of the key are extracted as directed by the `LOAD` clause, resulting in a set of name/value pairs which are collected into a record. Thus the output of this step is the working set of records, one record for each key found in the first step. If the query is a pure vector or hybrid vector then the initial working set will be sorted by distance, otherwise the working set will be in no particular order.

The next steps are to execute the list of stages provided on the command, sequentially one at a time. Each step takes as input the working set of records and performs some transformation on it, i.e., the output of one stage is fed into the input of the next stage.

Once all stages have been executed, the working set of records output from the last stage is returned as the result of the command.

```
FT.AGGREGATE <index-name> <query>
    [DIALECT <dialect>]
    [INORDER]
    [LOAD * | LOAD <count> <field> [AS <alias>] [<field> [AS <alias>] ...]]
    [PARAMS <count> <name> <value> [ <name> <value> ...]]
    [SLOP <slop>]
    [TIMEOUT <timeout>]
    [VERBATIM]
    (
      | APPLY <expression> AS <field>
      | FILTER <expression>
      | GROUPBY <count> <field> [<field> ... ] [[REDUCE <reducer> <count> [<expression> [<expression> ...]]] [ REDUCE ...]]
      | LIMIT <offset> <count>
      | SORTBY <count> <expression> [ASC | DESC] [<expression [ASC | DESC] ...] [MAX <num>]
    )+
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search-query.md) for details.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `INORDER` (optional): Indicates that proximity matching of terms must be in order.
- `LOAD * | LOAD <count> <field> [AS <alias>] [<field> [AS <alias>] ...]` (optional): This controls which fields of those keys are loaded into the working set. A star (\*) indicates that all of the fields of the keys are loaded. The key itself can be loaded by specifying `@__key`. For vector queries, the distance can also be loaded by using the name of that field.

  `<count>` is a count of the arguments that follow, not a count of fields. An `AS` keyword and its alias each count against `<count>`, so `LOAD 4 @a @b AS c` loads two fields using four arguments.

  Each `<field>` may be a declared attribute name (`@price`), or, for a JSON index, a JSON path (`$.price`). A JSON path that resolves to a declared attribute is loaded as that attribute.

  `AS <alias>` renames the loaded field: it is emitted under `<alias>` in the result and is referenced as `@<alias>` by later stages. Without an `AS` clause the field is emitted under the name given in the `LOAD` clause. An alias may reuse the name of a declared attribute, in which case it hides that attribute for the remainder of the pipeline. A single `LOAD` clause must not produce the same output name twice; doing so is an error.

  Honoring `AS` in the `LOAD` clause is a compatibility fix gated on `search.emulate-release` being `1.3.0` or greater; under an emulated release below that, `AS` is treated as an ordinary field name. Accepting a JSON path as `<field>` is not gated, since before it was supported such a load simply failed. See [COMPATIBILITY.md](../../COMPATIBILITY.md) for details.
- `PARAMS <count> <name> <value> [<name> <value> ...]` (optional): `count` is of the number of arguments, i.e., twice the number of `name`/`value` pairs. `PARAMS` can be used in both the query string as well as within an expression context. See [Search - query language](../topics/search-query.md) for usage details.
- `SLOP <slop>` (Optional): Specifies a slop value for proximity matching of terms.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- `VERBATIM` (Optional): If specified stemming is not applied to term searches.

- `APPLY <expression> as <field>` (optional): An expression is computed and insert into the record. See [APPLY Stage](#apply-stage) below. See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.
- `FILTER <expression>` (optional): The filter expression is applied, see [FILTER Stage](#filter-stage) for more details. See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.
- `GROUPBY <count> <field> <field> ... [REDUCE <reducer> <count> [<expression> [<expression> ...]]]` (optional): The working set is grouped into buckets according to the input fields. One summarization record is generated for each bucket including the outputs of each reducer. See [GROUPBY Stage](#groupby-stage) for details.
- `LIMIT <offset> <count>` (optional): The working set is trimmed, see [LIMIT Stage](#limit-stage) for details.
- `SORTBY <count> <expression> [ASC | DESC] [<expression> [ASC | DESC] ...] [MAX <num>]` (optional): The working set is sorted. See [SORTBY Stage](#sortby-stage) for more details.

# Result

The output is an array. The first element of this array is a scalar number with no particular meaning and should be ignored. The remainder of the array is one element for each record output by the final processing stage.

Each record is represented by an array which contains the field/value pairs of each record.

# Processing Stages

## APPLY Stage

The `APPLY` stage processes each record of the working set in sequence. For each record the expression is computed with output being set into the record with the provided field name. It is allowed to overwrite an existing field. The apply stage does not change the number of records in the working set.

## FILTER Stage

The `FILTER` stage processes each record of the working set in sequence. For each record the expression is computed. If the expression evaluates to zero then the record is discarded else the record remains.

## LIMIT Stage

The `LIMIT` stage discards records from the working set based on the provided offset and count.

## SORTBY Stage

The `SORTBY` stage reorders the records in accordance with a sort key. A sort key can be constructed using a number of expressions optionally combined with a direction.

If the MAX clause is present, then the output is trimmed after the first N records. It is more efficient to use the MAX clause than to follow the `SORTBY` stage with a limit stage.

## GROUPBY Stage

The `GROUPBY` stage organizes the input records into buckets based on the values of the specified fields.
For each unique combination of values a separate bucket is created to hold all records that have that combination of values.

Each bucket of records is processed into a single output record, discarding the bucket contents. That output record has two sections. The first section has one value for each of the specified `GROUPBY` fields. This section provides the values that formed (named) this unique bucket.

The second section is the output of the reducers for that bucket. Reducers provide an efficient mechanism for reducing (summarizing) the contents of a bucket. Each reducer function processes each record of the bucket and generates a single output value which is inserted into the second section of `GROUPBY` output record for this bucket.

The output of the `GROUPBY`stage is one record for each unique bucket.

### Reducers

The following reducer functions are available. The reducer functions that take an input expression will convert that expression into a number.

| Syntax                        | Function                                                                                                                           |
| :---------------------------- | :--------------------------------------------------------------------------------------------------------------------------------- |
| COUNT 0                       | Number of records                                                                                                                  |
| COUNT_DISTINCT 1 <expression> | The exact number of distinct values of the expression. Caution this consumes memory proportional to the number of distinct values. |
| SUM 1 <expression>            | The numerical sum of the values of the expression.                                                                                 |
| MIN 1 <expression>            | The smallest numerical values of the expression.                                                                                   |
| MAX 1 <expression>            | The largest numerical values of the expression.                                                                                    |
| AVG 1 <expression>            | The numerical average of the values of the expression.                                                                             |
| STDDEV 1 <expression>         | The standard deviation the values of the expression.                                                                               |
| FIRST_VALUE 1 <expression>    | The first value of the expression encountered in the group. Order depends on record retrieval order. Use only when order does not matter. |
| FIRST_VALUE 3 <expression> BY <expression> | The value of the first expression from the record with the smallest comparison expression (ascending). Ties broken by first-encountered order. |
| FIRST_VALUE 4 <expression> BY <expression> ASC\|DESC | The value of the first expression from the record with the minimum (ASC) or maximum (DESC) comparison expression. Ties broken by first-encountered order. Invalid keyword arguments (e.g., wrong BY token or unrecognised direction) produce a parse-time error. |
