---
title: "Search - Data Formats"
description: Search Module Formats for Input Field Data
---
title: "Valkey Search - Data Formats"
description: Valkey Search Module Formats for Input Field Data
---

This document describes the data formats accepted by the search module for each field type during ingestion (when keys are created or modified) and how those values are processed before being stored in the index.

# Tag Fields

For both HASH and JSON index types, the incoming data for a tag field is a single UTF-8 string. A tag field can contain one or more tag values within a single string.

## Tag Ingestion

On ingestion, the input string is split using the declared `SEPARATOR` character into individual tags. The default separator is `,` (comma). A different separator can be specified when the field is declared with [`FT.CREATE`](../commands/ft.create.md):

```
FT.CREATE idx SCHEMA color TAG SEPARATOR "|"
```

The valid separator characters are: `,.<>{}[]"':;!@#$%^&*()-+=~`

After splitting, each tag is processed as follows:

1. Leading and trailing ASCII whitespace is stripped from each tag.
2. Empty tags (those that are empty or whitespace only after trimming) are silently discarded.
3. Duplicate tags within a single field value are deduplicated.

If the field value produces no valid tags after processing, the key is tracked but not indexed for that field.

### Case Sensitivity

By default, tag matching is case-insensitive. The `CASESENSITIVE` option on `FT.CREATE` changes this behavior:

```
FT.CREATE idx SCHEMA color TAG CASESENSITIVE
```

When `CASESENSITIVE` is not set, tags like `"Blue"` and `"blue"` are treated as equivalent during search. When `CASESENSITIVE` is set, they are treated as distinct values.

### Ingestion Separator vs. Query Separator

The separator specified in `FT.CREATE` applies only during data ingestion. In query expressions (`FT.SEARCH`), the separator between tags is always `|` (pipe), regardless of the ingestion separator. See the [query documentation](search-query.md#tag-match) for details.

## Examples of Tag Values

Given `FT.CREATE idx SCHEMA color TAG` (default separator `,`):

```
HSET doc:1 color "blue"                       --> tags: {"blue"}
HSET doc:2 color "blue,red"                   --> tags: {"blue", "red"}
HSET doc:3 color " blue , red "               --> tags: {"blue", "red"}
HSET doc:4 color "blue,,red"                  --> tags: {"blue", "red"}
HSET doc:5 color "hello world, green is nice" --> tags: {"hello world", "green is nice"}
HSET doc:6 color "  "                         --> no tags (key is untracked)
```

Note that spaces within a tag are preserved. In the example above, `"hello world"` is a single tag containing a space.

Given `FT.CREATE idx SCHEMA skills TAG SEPARATOR "|"`:

```
HSET doc:1 skills "python|java|go"    --> tags: {"python", "java", "go"}
HSET doc:2 skills "python | java"     --> tags: {"python", "java"}
```

# Numeric Fields

A numeric field contains a single number. On ingestion, the field value is parsed as a 64-bit IEEE 754 double-precision floating-point number.

## Accepted Formats

The following formats are accepted on ingestion:

- **Integers**: `"42"`, `"-100"`, `"0"`
- **Floating-point**: `"3.14"`, `"-0.5"`, `"2.0"`
- **Scientific notation**: `"1e2"`, `"3.14e-5"`
- **Positive infinity**: `"inf"`, `"Inf"`, `"+inf"`
- **Negative infinity**: `"-inf"`, `"-Inf"`

The following are rejected:

- **NaN**: `"nan"`, `"NaN"`, `"NAN"` -- explicitly rejected on ingestion.
- **Non-numeric strings**: `"hello"`, `""` -- the key becomes untracked for this field.

When a value is rejected, the key is not indexed for that field. This is not treated as an error; the key simply does not appear in numeric range queries.

## Precision and Range

All numeric values are stored internally as 64-bit double-precision floating-point numbers:

- **Range**: approximately +/-1.7976931348623157 x 10^308
- **Integer precision**: integers up to 2^53 (9,007,199,254,740,992) are represented exactly; larger integers may lose precision.
- **Decimal precision**: approximately 15-17 significant decimal digits.

## Examples of Numeric Values

```
HSET product:1 price "999.99"     --> stored as 999.99
HSET product:2 price "42"         --> stored as 42.0
HSET product:3 price "-100"       --> stored as -100.0
HSET product:4 price "1e2"        --> stored as 100.0
HSET product:5 price "inf"        --> stored as +infinity
HSET product:6 price "nan"        --> rejected (key untracked for this field)
HSET product:7 price "hello"      --> rejected (key untracked for this field)
```

See the [query documentation](search-query.md#numeric-range-match) for details on querying numeric ranges.

# Vector Fields

A vector field contains a fixed-length array of floating-point numbers. The data format differs between HASH and JSON index types.

## Supported Data Type

Currently, only `FLOAT32` (32-bit IEEE 754 single-precision floating-point) is supported. The data type is specified as a required parameter in the [`FT.CREATE`](../commands/ft.create.md) command:

```
FT.CREATE idx SCHEMA embedding VECTOR HNSW 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC L2
```

## HASH Vector Format

For HASH-type indexes, vectors are stored as raw binary blobs. Each element is a 32-bit IEEE 754 single-precision float in little-endian byte order. The total blob size must be exactly `DIM * 4` bytes.

For example, a 3-dimensional FLOAT32 vector `[0.0, 0.0, 1.0]` is stored as 12 bytes:

```
\x00\x00\x00\x00   (0.0 as little-endian FLOAT32)
\x00\x00\x00\x00   (0.0 as little-endian FLOAT32)
\x00\x00\x80\x3f   (1.0 as little-endian FLOAT32)
```

In Python, a HASH vector can be created with:

```python
import numpy as np
vector = np.array([0.0, 0.0, 1.0], dtype=np.float32).tobytes()
client.hset("doc:1", mapping={"embedding": vector})
```

Or equivalently with `struct`:

```python
import struct
vector = struct.pack("<3f", 0.0, 0.0, 1.0)  # '<' = little-endian, 'f' = float32
client.hset("doc:1", mapping={"embedding": vector})
```

If the blob size does not match the expected `DIM * 4` bytes, the vector is rejected and the key is not indexed for that field.

## JSON Vector Format

For JSON type indexes, vectors are stored as a JSON string containing a bracketed, comma-separated list of floating-point values:

```
JSON.SET doc:1 $ '{"embedding": "[1.0, 0.0, 0.0]"}'
```

Note that the vector is a JSON **string value** (enclosed in quotes), not a native JSON array. The search module parses this string internally, splitting on commas (with whitespace skipped) and converting each element to a 32-bit float.

In Python:

```python
import json
vector = [1.0, 0.0, 0.0]
vector_str = "[" + ",".join(str(v) for v in vector) + "]"
client.json().set("doc:1", "$", {"embedding": vector_str})
```

The parser is flexible about whitespace and extra commas:

```
"[0.1, 0.2, 0.3]"       --> valid
"[ 0.1, ,0.2,0.3,]"     --> valid (extra commas and spaces are tolerated)
"[0.1, 0.2, a]"          --> rejected (non-numeric element)
```

## Query Vectors

Regardless of whether the index is on HASH or JSON keys, query vectors provided via `PARAMS` to `FT.SEARCH` must always use the binary blob format (the same format as HASH vectors).

## Examples of Vector Field Values

A 3-dimensional vector with HASH storage:

```
FT.CREATE idx ON HASH SCHEMA vec VECTOR FLAT 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC L2

HSET doc:1 vec "\x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00"
```

A 3-dimensional vector with JSON storage:

```
FT.CREATE idx ON JSON SCHEMA $.vec AS vec VECTOR FLAT 6 TYPE FLOAT32 DIM 3 DISTANCE_METRIC L2

JSON.SET doc:1 $ '{"vec": "[1.0, 0.0, 0.0]"}'
```

# Text Fields

A text field contains a string of words. On ingestion, the raw string passes through a multi-stage processing pipeline to produce a sequence of searchable tokens.

## Text Ingestion

The pipeline consists of four stages applied in order:

1. **Lexical Analysis** -- split text into words
2. **Case Folding** -- convert to lowercase
3. **Stop Word Removal** -- filter out common words
4. **Stemming** -- reduce words to root forms

### Lexical Analysis

The input string is split into words based on punctuation characters and whitespace. A "word" is any contiguous sequence of characters that is not punctuation and not whitespace.

**Whitespace characters** always break words. This includes spaces, tabs, newlines, carriage returns, and control characters.

**Punctuation characters** are configurable. The default set is:

```
,.<>{}[]"':;!@#$%^&*()-+=~/\|
```

A custom punctuation set can be specified at index creation time:

```
FT.CREATE idx PUNCTUATION ",.!?" SCHEMA content TEXT
```

The punctuation set operates on individual bytes. Multi-byte UTF-8 characters (such as CJK characters or emoji) are not treated as punctuation and become part of the surrounding word.

**Escape sequences**: A backslash (`\`) before a punctuation character causes that character to be treated as part of the word rather than as a word boundary. For example, `hello\,world` produces the single token `hello,world`.

The input must be valid UTF-8. Invalid UTF-8 sequences cause the record to be rejected.

### Case Folding

After tokenization, each word is converted to lowercase. For ASCII text, this uses standard ASCII lowercasing. For text containing non-ASCII characters, Unicode case folding (via ICU) is applied.

### Stop Word Removal

Stop words are common words that are excluded from the index. The default stop word list for English is:

> a, an, and, are, as, at, be, but, by, for, if, in, into, is, it, no, not, of, on, or, such, that, their, then, there, these, they, this, to, was, will, with

Stop words can be customized at index creation time:

```
FT.CREATE idx STOPWORDS 2 the a SCHEMA content TEXT
FT.CREATE idx NOSTOPWORDS SCHEMA content TEXT
```

`NOSTOPWORDS` or `STOPWORDS 0` disables stop word removal entirely.

Removed stop words do not occupy a position in the token sequence. For example, `"the quick brown fox"` (where "the" is a stop word) produces positions: quick=0, brown=1, fox=2. This is relevant for phrase matching and proximity queries.

### Stemming

Stemming reduces words to their root form so that morphological variants match each other. For example, "running", "runs", and "runner" all stem to "run". The stemming algorithm is language-specific; currently only English (Snowball stemmer) is supported.

Stemming is controlled by these options:

- `LANGUAGE ENGLISH` (schema-level): Specifies the stemming language. Currently only `ENGLISH` is supported.
- `NOSTEM` (per-field): Disables stemming for a specific text field.
- `MINSTEMSIZE <size>` (schema-level): Words shorter than this length are not stemmed. The default is 4.

When stemming is enabled, the original word (not the stemmed form) is stored in the index. The stem mapping is recorded separately so that a search for a stemmed root expands to match all words that share that root.

## Text Ingestion Examples

### Lexical Processing Examples

Given the default punctuation set:

| Input             | Tokens                        |
| :---------------- | :---------------------------- |
| `"hello world"`   | `hello`, `world`              |
| `"hello, world!"` | `hello`, `world`              |
| `"it's a test"`   | `it`, `s`, `a`, `test`        |
| `"hello-world"`   | `hello`, `world`              |
| `"price: $100"`   | `price`, `100`                |
| `"hello\,world"`  | `hello,world` (escaped comma) |

### Stop Word Removal Example

Given the default stop word list and input `"the quick brown fox is not fast"`:

| Stage                   | Result                                              |
| :---------------------- | :-------------------------------------------------- |
| After tokenization      | `the`, `quick`, `brown`, `fox`, `is`, `not`, `fast` |
| After case folding      | `the`, `quick`, `brown`, `fox`, `is`, `not`, `fast` |
| After stop word removal | `quick`, `brown`, `fox`, `fast`                     |

The stop words `the`, `is`, and `not` are removed. The remaining words have positions 0 through 3.

### Stemming Example

Given English stemming with the default `MINSTEMSIZE` of 4 and input `"The Running Searches cat"`:

| Stage                   | Result                                               |
| :---------------------- | :--------------------------------------------------- |
| After tokenization      | `The`, `Running`, `Searches`, `cat`                  |
| After case folding      | `the`, `running`, `searches`, `cat`                  |
| After stop word removal | `running`, `searches`, `cat`                         |
| Indexed tokens          | `running` (position 0), `searches` (position 1), `cat` (position 2) |
| Stem mappings           | `run` -> {`running`}, `search` -> {`searches`}       |

The word `the` is removed as a stop word. The word `cat` (3 characters) is below the minimum stem size and is not stemmed. A search for `run` will match `running` through the stem mapping.
