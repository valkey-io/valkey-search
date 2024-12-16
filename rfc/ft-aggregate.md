---
RFC: (PR number)
Status: (Change to Proposed when it's ready for review)
---

# Add FT.AGGREGATE command

## Abstract

The ```FT.AGGREGATE``` first executes an ```FT.SEARCH``` command and then processes those results through a specified series of processing stages. The execution model for the processing stages is sequential, i.e., the output of the encapsulated ```FT.SEARCH``` command becomes the input to the first stage. The output of the first stage becomes the input to the second stage, etc. The output of the last stage is returned as the result of the ```FT.AGGREGATE``` command.

The intra-stage result is an ordered list of records. Each record consists of a set of fields, each field has a unique name and a typed value. Supported types include: numeric and string.

There are five types of processing stages:

| Type | Function |
| --- | --- |
| APPLY | For each input record a new name/value pair is computed, using data only from that record. |
| FILTER | Input records are retained or discarded according to a per-record expression |
| SORTBY | The input records are sorted as specified. |
| LIMIT | A slice of the input records is retained. |
| GROUPBY | The input records are sorted into buckets according to the values of the specified fields. For each bucket, the contained records are reduced according a the specified reduction functions and replaced by a single record which contains the output of the reduction functions. |

## Design considerations

Care must be taken to honor user-supplied ```TIMEOUT``` as well as system imposted memory limitations.

## Specification

After parsing and validating the input  ```FT.AGGREGATE``` logically creates and executes an ```FT.SEARCH``` command.
The query string is taken from the incoming command verbatim.
The generated ```RETURN``` clause is computed from the logical sum of the incoming ```RETURN``` and ```LOAD``` clauses as well as any additional data required by the processing stages.
The results of the encapsulated ```FT.SEARCH``` command are then processed by each aggregation stage

### Command Syntax

```
FT.AGGREGATE <index> <query>
   [LOAD * | [count field [field ...]]]
   [TIMEOUT timeout]
   [PARAMS count name value [name value ...]]
   stage [ stage ...]   
```

Where ```stage``` can be selected from any of the following:

```
FILTER expression
APPLY expression AS name
LIMIT offset num  
GROUPBY count property [property ...] [REDUCE function count arg [arg ...] [AS name] [REDUCE function count arg [arg ...] [AS name] ...]] ...]
SORTBY count [ property ASC | DESC [property ASC | DESC ...]] [MAX num]
```

#### ```FILTER``` Stage

For each record in the result the expression is evaluated. Records where the expression evaluates to true are retained otherwise they are dropped. The ordering of the records is retained.

#### ```APPLY``` Stage

For each record in the result, the expression is evaluated and the output of that expression is applied to that record using the supplied name. If the supplied name already exists in that record, then it is overwritten. The ordering of the records is retained.

Apply expressions follow the normal rules for expressions in most programming languages.

Numeric constants are the usual decimal integer and floating point expression formats, plus _inf_, _+inf_ and _-inf_.

String constants are denoted with single or double quotes. A backslash quote and be used to insert a quote character. All strings are interpreted as UTF-8. 

For numerical computations the operators for addition(+), subtraction(-), multiplication(*), division(/), modulo(%) and exponentiation(^) are supported. All numerical operations are performed in 64-bit floating point.

The standard 6 comparison operators: ==, !=, <, <=, >, >= are provided and can operate on either strings or numbers and yield either a numeric 0 for false or 1 for true.

The boolean operators and (&&), or (||)  and negate(!) are provided and treat numeric zero as false and all other values as true.

Builtin functions are invoked with a name and a parenthesized list of arguments separated by commas. In general, arguments can be arbitraty expressions. 

Supported numeric functions include:

| Syntax | Operation |
| :---: | :--- |
| exists(@property) | 1 if the property is present, else 0.  |
| log(number) | natural log |
| abs(number) | absolute value |
| ceil(number) | Round to smallest not less than number |
| floor(number) | Round to largest not greater than number |
| log2(number) | $log_2(number)$ |
| exp(number) | $e^{number}$ |
| sqrt(number) | $\sqrt{number}$ |

Supported string functions include:

| Syntax | Operation |
| :---: | --- |
| upper(s) | Convert to upper case |
| lower(s) | Convert to lower case |
| startswith(s1, s2) | 1 if s2 matches the start of s1 otherwise 0 |
| contains(s1, s2) | The number of occurences of s2 in s1 |
| strlen(s) | Number of bytes in the string |
| substr(s, offset, length) | The string extraced from s starting at offset for length characters. A length of -1 means the remainder of the string |

#### ```LIMIT``` Stage

The first offset records are dropped, the next num records are retained and remaining records are dropped.

#### ```SORTBY``` Stage

The records in the result are sorted according using the specified properies and ordering. If the ``MAX``` clause is present then only first first num records are retained.

#### ```GROUPBY``` Stage

The input records are grouped into buckets. There is one bucket for each unique value of the specified groupby properties.
The records within a bucket are reduced according to the specific list of reducers and their arguments.
For each bucket a new record is created an populated with the values of the groupby properties and the results of each reducer for that bucket.

|  Syntax | Function |
| :--- | :--- |
| COUNT 0 | Number of records |
| COUNT_DISTINCT 1 *property* | The exact number of distinct values of the property. Caution this consumes memory proportional to the number of distinct values. |
| COUNT_DISTINCTISH 1 *property* | The approximate number of distince values of the property. This uses the hyperloglog facility of Valkey. |
| SUM 1 *property* | The numerical sum of the values of the property. |
| MIN 1 *property* | The smallest numerical values of the property. |
| MAX 1 *property* | The largest numerical values of the property. |
| AVG 1 *property* | The numerical average of the values of the property.
| STDDEV 1 *property* | The standard deviation the values of the property.
| QUANTILE 2 *property* *quantile* | The specified quantile (between 0 and 1) of the values of the property |

### Result
Returns an array or error reply.

If the operation completes successfully, it returns an array.
The first element is an integer with no particular meaning (should be ignored).
The remaining elements are the results output by the last stage.
Each element is an array of field name and value pairs.

### Authentication and Authorization (Optional)

Same security issues as with ```FT.SEARCH```
