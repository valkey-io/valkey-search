---
title: "Valkey Search - Aggregation Expressions"
description: Valkey Search Module Aggregation Expression Language
---

The `FILTER`, `APPLY`, `SORTBY` and `GROUPBY` stages of `FT.AGGREGATE` use expressions to compute values.

The expression engine operates on input values, generating output values. Values in the expression language are dynamically typed and can be either Nil (missing/invalid), Numeric (64-bit floating point) or String (a sequence of bytes). A value can also be an array, such as the output of the `TOLIST` and `RANDOM_SAMPLE` reducers or a multi-value JSON field.

The syntax of expressions generally follows the "C" language syntax and operator precedence rules with some specialized syntax for accessing fields of records and parameters from the `FT.AGGREGATE` command.

```
<expression>    ::= <logical-or>

<logical-or>    ::= <logical-and>
                  | <logical-or> "||" <logical-and>

<logical-and>   ::=  <relational>
                  | <logical-and> "&&" <relational>

<relational>    ::= <additive>
                  | <relational> <rel-op> <additive>

<rel-op>        ::= "<" | "<=" | "==" | "!=" | ">" | ">="

<additive>      ::= <multiplicative>
                  | <additive> <add-op> <multiplicative>

<add-op>        ::= "+" | "-"

<multiplicative>::= <power>
                  | <multiplicative> <mul-op> <power>

<mul-op>        ::= "*" | "/"

<power>         ::= <primary>
                  | <primary> "^" <power>

<primary>       ::= <numeric-constant>
                | <string-constant>
                | <field-ref>
                | <parameter-ref>
                | <function-call>
                | "!" <primary>
                | "(" <expression> ")"

<field-ref>     ::= "@" <identifier>

<parameter-ref> ::= "$" <identifier>

<function-call> ::= <identifier> "(" <arg-list> ")"

<arg-list>      ::=
                  | <expression>
                  | <arg-list> "," <expression>

```

This grammar applies with `search.emulate-release` 1.3.0 or later. Below 1.3.0 (the default), `^` has the same precedence as `*` and `/` and is left-associative, and `&&` has the same precedence as `||`, so `@a || @b && @c` is `(@a || @b) && @c`. See [COMPATIBILITY.md](../../COMPATIBILITY.md).

# Primary Values

Numeric constants are the usual syntax of decimal integer and floating point expression formats, plus _inf_, _+inf_ and _-inf_.

String constants are enclosed with single (`'`) or double quotes(`"`). A backslash (`\`) followed by a quote can be used to insert a quote character.

A parameter-ref selects a string value provided by the `PARAMS` clause of the executing `FT.AGGREGATE` command. With `search.emulate-release` 1.3.0 or later, a parameter whose text is a number is a numeric value.

A field-ref is used to access values from the record currently being processed. If the field doesn't exist then the reference returns a Nil value. With `search.emulate-release` 1.3.0 or later, a reference to a field the key does not have is _missing_: an operator or function (other than `exists` and `&&`) given a missing value returns missing, and an `APPLY` whose expression is missing drops the record.

# Operators

## Arithmetic Operators

The following numeric operators are available. String values are converted to numeric, but become a Nil if the conversion fails. If either input to a numeric operator is Nil, then the output is Nil.

| Syntax | Operation      |
| :----: | :------------- |
|   +    | Addition       |
|   -    | Subtraction    |
|   \*   | Multiplication |
|   /    | Division       |
|   ^    | Exponentiation |

## Relational Operators

The following relational operators are available. Relational operators perform conversions according to the following prioritized rules:

1. If both inputs are Nil, they are considered equal.
2. If one of the inputs is Nil then the comparison is unordered.
3. If both inputs are numbers, then a numeric comparison is performed.
4. If both inputs are strings, then a string comparison is performed.
5. If either input is a number and the other input can be converted to a number, then a numeric comparison is performed.
6. The two inputs are compared as strings.

With `search.emulate-release` 1.3.0 or later, the kind of comparison is instead chosen from the operand types when the expression is compiled: numeric if either operand is numeric (such as a numeric constant, a `NUMERIC` field or a numeric function), else string if either operand is a string. In a numeric comparison a string that does not convert to a number is an error for `<`, `<=`, `>` and `>=`, which fails the command, while `==` is false and `!=` is true. So `@f < 5` compares numbers, while `@f < '5'` compares strings when `@f` is a `TAG` field.

| Syntax | Operation                |
| :----: | :----------------------- |
|   <    | Less than                |
|   <=   | Less than or equal to    |
|   ==   | Equal to                 |
|   !=   | Not equal to             |
|   >    | Greater than             |
|   >=   | Greater than or equal to |

## Logical Operators

The logical operators `&&`, `||` and `!` generate 0 for false and 1 for true. A value is false when it is 0, Nil or an empty string (with `search.emulate-release` below 1.2.1, any string), and true otherwise. With `search.emulate-release` 1.3.0 or later, `&&` and `||` evaluate their right operand only when the left one does not decide the result.

# Functions

## Numeric Processing Functions

Supported numeric functions are listed. Numeric functions always convert their inputs to a number. If the conversion fails then a Nil is returned.

|      Syntax       | Operation                                    |
| :---------------: | :------------------------------------------- |
|  log(expression)  | ln(expression)                               |
|  abs(expression)  | \| expression \|                             |
| ceil(expression)  | Round to smallest not less than expression   |
| floor(expression) | Round to largest not greater than expression |
| log2(expression)  | log2(expression)                             |
|  exp(expression)  | exp(expression)                              |
| sqrt(expression)  | sqrt(expression)                             |

## String Processing Functions

String processing functions. Inputs are converted to strings as appropriate.

|          Syntax           | Operation                                                                                                              |
| :-----------------------: | ---------------------------------------------------------------------------------------------------------------------- |
|         upper(s)          | Convert to upper case (ASCII Only)                                                                                     |
|         lower(s)          | Convert to lower case (ASCII Only)                                                                                     |
|    startswith(s1, s2)     | 1 if s2 matches the start of s1 otherwise 0                                                                            |
|     contains(s1, s2)      | The number of occurrences of s2 in s1                                                                                  |
|         strlen(s)         | Number of bytes in the string                                                                                          |
| substr(s, offset, length) | `length` bytes of s starting at `offset`. A negative offset counts from the end of s. A negative length ends that many bytes before the end of s |
|    concat(s1, s2, ...)    | Concatenate up to 50 string expressions into a single string                                                           |

## Timestamp Processing Functions

Time functions extract various time fields from a numeric UTC Unix timestamp using the `gmtime` C library function, or round the timestamp down.

Supported time functions include:

|           Syntax            | Operation                                        |
| :-------------------------: | :----------------------------------------------- |
|    dayofweek(timestamp)     | Extract day of week from timestamp. Range: 0-6   |
|    dayofmonth(timestamp)    | Extract day of month from timestamp. Range: 1-31 |
|    dayofyear(timestamp)     | Extract day of year from timestamp. Range: 0-365 |
|   monthofyear(timestamp)    | Extract month of year from timestamp. Range: 0-11 |
|       year(timestamp)       | Extract year from timestamp.                     |
|      minute(timestamp)      | Timestamp rounded down to the start of its minute |
|       hour(timestamp)       | Timestamp rounded down to the start of its hour  |
|       day(timestamp)        | Timestamp rounded down to the start of its day   |
|      month(timestamp)       | Timestamp rounded down to the start of its month |
| timefmt(timestamp [, fmt])  | Convert timestamp to string using strftime. `fmt` defaults to `%FT%TZ`. |
| parsetime(time_string [, fmt]) | Parse time string into timestamp using strptime. `fmt` defaults to `%FT%TZ`. |

## Exists Function

The exists function returns 0 if the input argument is Nil else 1. This can be used to test if a field is present in the current record.

|    Syntax     | Operation                      |
| :-----------: | :----------------------------- |
| exists(value) | 0 if the value is Nil, else 1. |

## Conditional Function

`case` returns `if_true` when the condition is true, and `if_false` otherwise.
Only the returned expression is evaluated, so the other one may refer to a
field that the record does not have.

|               Syntax               | Operation                                                 |
| :--------------------------------: | :-------------------------------------------------------- |
| case(condition, if_true, if_false) | `if_true` when `condition` is true, otherwise `if_false`. |

The condition is false when it is 0, an empty string, or Nil, and true
otherwise. When it refers to a missing field or fails with an error:

- In `FT.AGGREGATE` with `search.emulate-release` below `1.3.0` (the default),
  a missing field is Nil, so the condition is false.
- Otherwise, the missing field or the error becomes the result of `case`, and
  neither expression is evaluated.

For example, the following gives records without `foo` the value `no foo`:

```
case(exists(@foo), @foo, "no foo")
```
