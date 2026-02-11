The `FT.AGGREGATE` command extends the query capabilities of the `FT.SEARCH` command with substantial server-side data processing capabilities.

The first step of the command is to use the index name and query string to locate a list of Valkey keys. This process is identical to the `FT.SEARCH` command.

The second step is executed once for each key found in the first step. The contents of the key are extracted as directed by the `LOAD` clause, resulting in a set of name/value pairs which are collected into a record. Thus the output of this step is the working set of records, one record for each key found in the first step.

The next steps are to execute the list of stages provided on the command, sequentially one at a time. Each step takes as input the working set of records and performs some transformation on it, i.e., the output of one stage is fed into the input of the next stage.

Once all stages have been executed, the working set of records output from the last stage is returned as the result of the command.

```
FT.AGGREGATE <index-name> <query>
    TIMEOUT <timeout>
    PARAMS <count> <name> <value> [ <name> <value> ...]
    DIALECT <dialect>
    LOAD [* | <count> <field> [<field> ...]]
    (
      | APPLY <expression> AS <field>
      | FILTER <expression>
      | LIMIT <offset> <count>
      | SORTBY <count> <expression> [ASC | DESC] [<expression [ASC | DESC] ...] [MAX <num>]
      | GROUPBY <count> <field> [<field> ... ] [[REDUCE <reducer> <count> [<expression> [<expression> ...]]] [ REDUCE ...]]
    )+
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search-query.md) for details.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- `PARAMS <count> <name> <value> [<name> <value> ...]` (optional): `count` is of the number of arguments, i.e., twice the number of `name`/`value` pairs. Params can be used in both the query string as well as within an expression context. See [Search - query language](../topics/search-query.md) for usage details.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `LOAD [* | <count> <field> <field> ...]` (optional): This controls which fields of those keys are loaded into the working set. A star (\*) indicates that all of the fields of the keys are loaded. The key itself can be loaded by specifying `@__key`. For vector queries, the distance can also be loaded by using the name of that field.

- `APPLY <expression> as <field>` (optional): An expression is computed and insert into the record. See [APPLY Stage](#apply-stage) below. See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.
- `FILTER <expression>` (optional): The filter expression is applied, see [FILTER Stage](#filter-stage) for more details. See [Search - expressions](../topics/search-expressions.md) for details on the expression syntax.
- `LIMIT <offset> <count>` (optional): The working set is trimmed, see [LIMIT Stage](#limit-stage) for details.
- `SORTBY <count> <expression> [ASC | DESC] [<expression> [ASC | DESC] ...] [MAX <num>]` (optional): The working set is sorted. See [SORTBY Stage](#sortby-stage) for more details.
- `GROUPBY <count> <field> <field> ... [REDUCE <reducer> <count> [<expression> [<expression> ...]]]` (optional): The working set is grouped into buckets according to the input fields. One summarization record is generated for each bucket including the outputs of each reducer. See [GROUPBY Stage](#groupby-stage) for details.

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

If the MAX clause is present, then the output is trimmed after the first N records. It is more efficient to use the MAX clause than to follow the sortby stage with a limit stage.

## GROUPBY Stage

The `GROUPBY` stage organizes the input records into buckets based on the values of the specified fields.
For each unique combination of values a separate bucket is created to hold all records that have that combination of values.

Each bucket of records is processed into a single output record, discarding the bucket contents. That output record has two sections. The first section has one value for each of the specified groupby fields. This section provides the values that formed (named) this unique bucket.

The second section is the output of the reducers for that bucket. Reducers provide an efficient mechanism for reducing (summarizing) the contents of a bucket. Each reducer function processes each record of the bucket and generates a single output value which is inserted into the second section of groupby output record for this bucket.

The output of the groupby stage is one record for each unique bucket.

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
