The `FT.AGGREGATE` command extends the query capabilities of the `FT.SEARCH` command with substantial server-side data processing capabilities. Just as with the `FT.SEARCH` command, the query string is used to locate a set of Valkey keys. Fields from those keys are loaded and processed according to a list of stages specified on the command. Each stage is processed sequentially in the order found on the command with the output of each stage becoming the input to the next stage. The output of the last stage is returned as the result of the command.

```
FT.AGGREGATE <index-name> <query>
      TIMEOUT <timeout>
    | PARAMS <count> <name> <value> [ <name> <value> ...]
    | DIALECT <dialect>
    | LOAD [* | <count> <field> [<field> ...]]
    (
      | APPLY <expression> AS <field>
      | FILTER <expression>
      | LIMIT <offset> <count>
      | SORTBY <count> <field1> [ASC | DESC] <field2 [ASC | DESC] ... [MAX <num>]
      | GROUPBY <count> <field1> <field2> ... [REDUCE <reducer> <count> <arg1> <arg2> ...]*
    )+
```

- `<index>` (required): This index name you want to query.
- `<query>` (required): The query string, see [Search - query language](../topics/search.query.md) for details.
- `TIMEOUT <timeout>` (optional): Lets you set a timeout value for the search command. This must be an integer in milliseconds.
- `PARAMS <count> <name1> <value1> <name2> <value2> ...` (optional): `count` is of the number of arguments, i.e., twice the number of name/value pairs. See [Search - query language](../topics/search.query.md) for usage details.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `LOAD [* | <count> <field> [<field> ...]]` (optional): After the query has located a set of keys. This controls which attributes of those keys are loaded into data passed into the first stage for processing. A star (\*) indicates that all of the attributes of the keys are loaded.

- `APPLY <expression> as <field>` (optional): For each key of input, the expression is computed and stored into the named field. See [Search - expressions](../topics/search.expressions.md) for details on the expression syntax.
- `FILTER <expression>` (optional): The expression is evaluated for each key of input. Only those keys for which the expression evaluates to true (non-zero) are included in the output. See [Search - expressions](../topics/search.expressions.md) for details on the expression syntax.
- `LIMIT <offset> <count>` (optional): The first `offset` number of records are discarded. Up to the next `count` records generated as the output of this stage. Any remaining input records beyond `offset` + `count` are discarded.
- `SORTBY <count> <field> [ASC | DESC] <field> [ASC | DESC] ... [MAX <num>]` (optional): The input records are sorted according to the specified fields and directions. If the MAX option is specified, only the first `num` records after sorting are included in the output and the remaining input records are discarded.
- `GROUPBY <count> <field> <field> ... [REDUCE <reducer> <count> <arg1> <arg2> ...]`* (optional): The input records are grouped according to the unique value combinations of the specified fields. For each group, reducers are updated with that keys from that group and becomes the output records. The grouped input records are discarded. Multiple reducers can be specified and are efficiently computed. See [Search - expressions](../topics/search.expressions.md) for details on the expression syntax.



