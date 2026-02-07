The `FILTER`, `APPLY`, `SORTBY` and `GROUPBY` stages of `FT.AGGREGATE` use expressions to compute values.

The expression engine operates on scalar values. Values in the expression language are dynamically typed and can be either Nil (missing/invalid), Numeric (64-bit floating point) or String (a sequence of bytes).

The syntax of expressions generally follows the "C" language syntax and operator precedence rules with some specialized syntax for accessing fields of records and parameters from the `FT.AGGREGATE` command.

```
<expression>    ::= <logical-or>

<logical-or>    ::= <logical-and>
                  | <logical-or> "||" <logical-and>

<logical-and>   ::=  <relational>
                  | <logical-and> "&&" <relational>

<relational>    ::= <multiplicative>
                  | <relational> <rel-op> <multiplicative>

<rel-op>        ::= "<" | "<=" | "==" | "!=" | ">" | ">="

<multiplicative>::= <additive>
                  | <multiplicative> <mul-op> <additive>

<mul-op>        ::= "*" | "/" | "^"

<additive>      ::= <unary>
                  | <additive> <add-op> <unary>

<add-op>        ::= "+" | "-"

<unary>         ::= <primary>
                  | <unary> <unary-op> <primary>

<unary-op>      ::= "!"

<primary>       ::= <numeric-constant>
                | <string-constant>
                | <field-ref>
                | <parameter-ref>
                | <function-call>
                | "(" <expression> ")"

<field-ref>     ::= "@" <identifier>

<parameter-ref> ::= "$" <identifier>

<function-call> ::= <identifier> "(" <arg-list> ")"

<arg-list>      ::=
                  | <primary>
                  | <arg-list> "," <primary>

```

# Primary Values

Numeric constants are the usual syntax of decimal integer and floating point expression formats, plus _inf_, _+inf_ and _-inf_.

String constants are enclosed with single (`'`) or double quotes(`"`). A backslash (`\`) followed by a quote can be used to insert a quote character.

A parameter reference selects a string value provided by the `PARAMS` clause of the executing `FT.AGGREGATE` command.

A field-reference is used to access values from the record currently being processed. If the field doesn't exist then the reference returns a Nil value.

# Operators

The following numeric operators are available. String values are converted to numeric, but become a Nil if the conversion fails. If either input to a numeric operator is Nil, then the output is Nil.

| Syntax | Operation      |
| :----: | :------------- |
|   +    | Addition       |
|   -    | Subtraction    |
|   \*   | Multiplication |
|   /    | Division       |
|   %    | Modulo         |
|   ^    | Exponentiation |

The following relational operators are available. Relational operators perform conversions according to the following prioritized rules:

1. If both inputs are Nil, they are considered equal.
2. If one of the inputs is Nil then the comparison is unordered.
3. If both inputs are numbers, then a numeric comparison is performed.
4. If both inputs are strings, then a string comparison is performed.
5. If either input is a number and the other input can be converted to a number, then a numeric comparison is performed.
6. The two inputs are compared as strings.

| Syntax | Operation                |
| :----: | :----------------------- |
|   <    | Less than                |
|   <=   | Less than or equal to    |
|   ==   | Equal to                 |
|   !=   | Not equal to             |
|   >    | Greater than             |
|   >=   | Greater than or equal to |

The logical operators `&&`, `||` and `!` generate a numeric 1 or 0 output by treating incoming numeric zeros as false and any other value as true.

# Functions

## Numeric Processing Functions

Supported numeric functions are listed. Numeric functions always convert their inputs to a number. If the conversion fails then a Nil is returned.

|    Syntax     | Operation                                |
| :-----------: | :--------------------------------------- |
|  log(number)  | natural log                              |
|  abs(number)  | absolute value                           |
| ceil(number)  | Round to smallest not less than number   |
| floor(number) | Round to largest not greater than number |
| log2(number)  | $log_2(number)$                          |
|  exp(number)  | $e^{number}$                             |
| sqrt(number)  | $\sqrt{number}$                          |

## String Processing Functions

String processing functions.

|          Syntax           | Operation                                                                                                              |
| :-----------------------: | ---------------------------------------------------------------------------------------------------------------------- |
|         upper(s)          | Convert to upper case                                                                                                  |
|         lower(s)          | Convert to lower case                                                                                                  |
|    startswith(s1, s2)     | 1 if s2 matches the start of s1 otherwise 0                                                                            |
|     contains(s1, s2)      | The number of occurrences of s2 in s1                                                                                  |
|         strlen(s)         | Number of bytes in the string                                                                                          |
| substr(s, offset, length) | The string extracted from s starting at offset for length characters. A length of -1 means the remainder of the string |
|    concat(s1, s2, ...)    | Concatentate up to 50 string expressions into a single string                                                          |

## Timestamp Processing Functions

Time functions extract various time fields from a numeric UTC Unix timestamp using the "gmtime" C library function.

Supported time functions include:

|           Syntax            | Operation                                        |
| :-------------------------: | :----------------------------------------------- |
|    dayofweek(timestamp)     | Extract day of week from timestamp. Range: 0-6   |
|    dayofmonth(timestamp)    | Extract day of month from timestamp. Range: 1-31 |
|    dayofyear(timestamp)     | Extract day of year from timestamp. Range: 0-365 |
|   monthofyear(timestamp)    | Extract day of month from timestamp. Range: 0-11 |
|       year(timestamp)       | Extract year from timestamp.                     |
|      minute(timestamp)      | Extract minute from timestamp. Range: 0-59       |
|       hour(timestamp)       | Extract hour from timestamp. Range: 0-23         |
|       day(timestamp)        | Extract day from timestamp. Range: 1-31          |
|      month(timestamp)       | Extract month from timestamp. Range: 0-11        |
|   timefmt(timestamp, fmt)   | Convert timestamp to string using strftime.      |
| parsetime(time_string, fmt) | Parse time string into timestamp using strptime. |

## Exists Function

The exists function returns 1 if the input argument is Nil else 0. This can be used to test if a field is present in the current record.

|    Syntax     | Operation                      |
| :-----------: | :----------------------------- |
| exists(value) | 1 if the value is Nil, else 0. |
