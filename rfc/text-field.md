---
RFC: (PR number)
Status: (Change to Proposed when it's ready for review)
---

# Title
Text Searching

## Abstract

The existing search platform is extended with a new field type, ```TEXT``` is defined. The query language is extended to support locating keys with text fields based on term, wildcard, fuzzy and exact phrase matching.

## Motivation

Text searching is useful in many applications.

## Terminology

In the context of this RFC.

| Term        | Meaning                                                                                                                                                                                                                     |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| key         | A Valkey key. Depending on the context of usage could be the text of the key name or the contents of the HASH or JSON key.                                                                                                  |
| field       | A component of an index. Each field has a type and a path.                                                                                                                                                                  |
| index       | A collection of fields and field-indexes. The object created by the ```FT.CREATE``` command.                                                                                                                                |
| field-index | A data structure associated with a field that is intended to accelerate the operation of the search operators for this field type.                                                                                          |
| character   | An Unicode character. A character may occupy 1-4 bytes of a UTF-8 encoded string.                                                                                                                                           |
| word        | A syntactic unit of text consisting of a vector of characters. A word is delimited by un-escaped punctuation and/or whitespace.                                                                                             |
| token       | same as a word                                                                                                                                                                                                              |
| text        | A UTF-8 encoded string of bytes.                                                                                                                                                                                            |
| stemming    | A process of mapping similar words into a common base word. For example, the words _drives_, _drove_ and _driven_ would be replaced with the  _drive_.                                                                      |
| term        | The output of stemming a word.                                                                                                                                                                                              |
| prefix tree | A mapping from input string to an output object. Insert/delete operations are O(length(input)). Additional operations include the ability to iterate over the entries that share a common prefix in O(length(prefix)) time. |

## Design considerations

The text searching facility provides machinery that decomposes text fields into terms and field-indexes them.
The query facility of the ```FT.SEARCH``` and ```FT.AGGREGATE``` commands is enhanced to select keys based on combinations of term, fuzzy and phrase matching together with the existing selection operators of TAG, Numeric range and Vector searches.

### Tokenization process

A tokenization process is applied to strings of text to produce a vector of terms.
Tokensization is applied in two contexts. 
First as part of the ingestion process for text fields of keys.
Second to process query string words and phrases.

The tokenization process has four steps.

1. The text is tokenized, removing punctuation and redundant whitespace, generating a vector of words.
2. Latin upper-case characters are converted to lower-case.
3. Stop words are removed from the vector.
4. Words with more than a fixed number of characters are replaced with their stemmed equivalent term according to the selected language.
```
┌─────────────────┐     ┌───────────────┐     ┌──────────────┐     ┌───────────────┐
│  Input Text     │     │ Tokenization  │     │ Case         │     │ Stop Word     │
│  "The Running   │ ──> │ ["The",       │ ──> │ ["the",      │ ──> │ ["running",   │
│   Searches cat" │     │  "Running",   │     │  "running",  │     │  "searches",  │
└─────────────────┘     │  "Searches",  │     │  "searches", │     │  "cat"]       │
                        │  "cat"]       │     │  "cat"]      │     └───────┬───────┘
                        └───────────────┘     └──────────────┘             │
                                                                           V
                                                                      ┌───────────────┐
                                                                      │ Stemming      │
                                                                      │ ["run",       │
                                                                      │  "search",    │
                                                                      │  "cat"]       │
                                                                      └───────────────┘
```

Step 1 of the tokenization process is controlled by a specified list of punctuation characters. 
Sequences of one or more punctuations characters delimit word boundaries.
Note that individual punctuation characters can be escaped using the normal backslash prefix syntax, causing the next character to be treated as non-punctuation.

### Text Field-Index Structure

The text field-index is a mapping from a term to a list of locations.
Depending on the configuration, the list of locations may be just a list of keys or a list of key/term-offset pairs. The list of locations is also known as a postings or a postings list.

The mapping structure is built from one or two prefix trees. When present, the second prefix tree is built from each inserted term sequenced in reverse order, effectively providing a suffix tree.

Here is a visual example. Note that in the drawing, the sections noted as "same postings as _xxxx_" mean that the pointer actually points to the same memory area as referenced. In other words, when a suffix tree is present, the postings list is referenced by both (think shared_ptr).

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                  TextIndex                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ ┌──────────────────┐                                ┌──────────────────┐     │
│ │   Prefix Tree    │             Postings           │   Suffix Tree    │     │
│ └──────────────────┘                                └──────────────────┘     │
│ │                  │                                │                  │     │
│ │  ┌────────────┐  │          ┌────────────┐        │  ┌────────────┐  │     │
│ │  │   apple    │──┼─────────>│ key1: [0]  │<───────┼──│   elppa    │  │     │
│ │  └────────────┘  │          │ key2: [3]  │        │  └────────────┘  │     │
│ │                  │          └────────────┘        │                  │     │
│ │                  │                                │                  │     │
│ │  ┌────────────┐  │          ┌────────────┐        │  ┌────────────┐  │     │
│ │  │   banana   │──┼─────────>│ key1: [1]  │<───────┼──│   ananab   │  │     │
│ │  └────────────┘  │          │ key3: [2]  │        │  └────────────┘  │     │
│ │                  │          └────────────┘        │                  │     │
│ │                  │                                │                  │     │
│ │  ┌────────────┐  │          ┌────────────┐        │  ┌────────────┐  │     │
│ │  │   cherry   │──┼─────────>│ key2: [0]  │<───────┼──│   yrrehc   │  │     │
│ │  └────────────┘  │          │ key3: [8]  │        │  └────────────┘  │     │
│ │                  │          └────────────┘        │                  │     │
│ │                  │                                │                  │     │
│ └──────────────────┘                                └──────────────────┘     │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```


### Search query operators

Unlike the Vector, Tag and Numeric search operators the specification of a field is optional. If the field specification is omitted then a match is declared if any text field matches the search operator.

#### Term matching

There are three types of term matching: exact, wildcard and fuzzy. Exact term matching is self-descriptive, i.e., only keys containing exactly the specified text are matched.

Wildcard matching provides a subset of reg-ex style matching of terms.
Initially, only a single wildcard specifier ```*``` is allowed which matches any number of characters in a term.
The wildcard can be at any position within the term, providing prefix, suffix and infix style matching.
Note, in this proposal, the second prefix tree is required to perform infix and suffix matching.

#### Fuzzy matching

Fuzzy word matching is specified by enclosing a word in pairs of percent signs ```%```. This term matches words within one [Levenshtein edit distance](https://en.wikipedia.org/wiki/Levenshtein_distance) edit distance for each pair of percent signs from the specified word. 

#### Phrase matching

Exact phrase matching is specified by enclosing a sequence of term-specifiers in double quotes ```"```. In order to perform phrase matching, the field-index must be configured to contain offsets. In the context of phrase matching a term-specifier is either a term match or a fuzzy match specifier.

The syntax of the phrase matching term allows the specification of two additional modifiers: `Inorder` and `Slop`. The `Slop` modifier controls the maximum distance between the matching terms. A `Slop` value of zero means that the matched terms must be sequentially in order. A `Slop` value of 1 indicates that the matched terms could have up to 1 addition non-matched word between them. The default value for `Slop` is zero.

The `Inorder` modifier is a boolean which controls whether the ordering of the matches terms must match the ordering in the phrase. If `Inorder` is `false` then the terms can be matched in any order. The default is `true`.

### Example Search operations

These examples provide an example query string as a block of text. When used in an FT.SEARCH or FT.AGGREGATE command, this text is the contents of a single field in that command.
Thus if the command is submitted through some application like valkey-cli which will subject its command line to various shell-like metadata operations (quote removal, environment variable substitution, etc.) then the reader must compensate for that, for example by escaping spaces which valkey-cli would tend to separate into separate command fields.

#### Term Matching Examples

| Example | Interpretation | Required Index Options |
|:-:|---|:-:|
| `abcd` | exact term match | none |
| `a*`   | matches any term starting with `a` | none |
| `fred*` | matches any term starting with `fred` | none |
| `*z` | matches any term ending with `z` | SUFFIXTRIE |
| `a*b`  | matches any term starting with `a` and ending with `b` | SUFFIXTRIE |

#### Fuzzy Matching Examples

| Example | Interpretation |
|:-:|---|
| %abc% | Matches any term within one edit-distance of `abc` |
| %%abc%% | Matches any term within two edit-distances of `abc` |

#### Phrase Matching Examples

All phrase matching queries require the `WITHOFFSETS` index creation option.

| Example | Interpretation |
|:-:|---|
| `"a b"` | Matches term `a` immediately followed by term `b` |
| `"a %b%"` | Matches term `a` immediately followed by any term within one edit-distance of `b` |
| `"a*b c"` | Matches any term that starts with `a` and ends with `b` followed by the term `c` |
| `"a b c"` | Matches the terms `a`, `b` and `c` in that order with no intervening words |
| `"a b c" => [$inorder:false]` | Matches the terms `a`, `b`, and `c` in any order with no intervening words |
| `"a b" => [$inorder:false, $slop:1]"` | Matches the terms `a` and `b` in any order with up to one intervening term |

### Search Query Operations Flow

The following diagram illustrates the flow of a text search operation from query to results:

```
┌──────────────────┐
│ Search Query     │
│ e.g., "quick     │
│ brown fox"       │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐     ┌──────────────────────────────────────────┐
│ Query Parser     │────▶│ Parsed Query Tree                        │
└────────┬─────────┘     │                                          │
         │               │    ┌─────────┐                           │
         │               │    │ PHRASE  │                           │
         │               │    └────┬────┘                           │
         │               │         │                                │
         │               │  ┌──────┴──────┼─────────────┐           │
         │               │  │             │             |           │
         │               │  ▼             ▼             ▼           │
         │               │ ┌─────┐     ┌─────┐     ┌─────┐          │
         │               │ │quick│     │brown│     │ fox │          │
         │               │ └─────┘     └─────┘     └─────┘          │
         │               └──────────────────────────────────────────┘
         ▼
┌──────────────────┐
│ Query Execution  │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Term Dictionary Lookup                                               │
│                                                                      │
│ ┌─────────────┐         ┌─────────────┐         ┌─────────────┐      │
│ │Term: "quick"│         │Term: "brown"│         │Term: "fox"  │      │
│ └──────┬──────┘         └──────┬──────┘         └──────┬──────┘      │
│        │                       │                       │             │
│        ▼                       ▼                       ▼             │
│ ┌──────────────┐        ┌──────────────┐        ┌──────────────┐     │
│ │Postings List │        │Postings List │        │Postings List │     │
│ └──────┬───────┘        └──────┬───────┘        └──────┬───────┘     │
│        │                       │                       │             │
└────────┼───────────────────────┼───────────────────────┼─────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
┌───────────────────────────────────────────────────────────────────┐
│ Position Filtering (for phrase matching)                          │
│                                                                   │
│ Check if terms appear in correct sequence with proper spacing     │
│                                                                   │
│ ┌─────────────────────────────────────────────────────────────┐   │
│ │ Document: key1                                              │   │
│ │                                                             │   │
│ │ [quick:pos2]───┐                                            │   │
│ │                │                                            │   │
│ │ [brown:pos3]───┼─── Sequential? YES                         │   │
│ │                │                                            │   │
│ │ [fox:pos4]─────┘                                            │   │
│ └─────────────────────────────────────────────────────────────┘   │
│                                                                   │
└───────────────────────────────────────────────────────┬───────────┘
                                                        │
                                                        ▼
                                             ┌───────────────────┐
                                             │ Results           │
                                             │ [key1, key5, ...] │
                                             └───────────────────┘
```

## System Architecture

The following diagram illustrates the overall architecture and data flow of the text search system:

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        Text Search System Architecture                        │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  ┌───────────────┐        ┌────────────────┐         ┌────────────────────┐   │
│  │ Client        │        │ Command        │         │ Search Query       │   │
│  │ Applications  │───────▶│ Processing     │────────▶│ Execution Engine   │   │
│  └───────────────┘        │ (FT.SEARCH)    │         └──────────┬─────────┘   │
│                           └────────────────┘                     │            │
│                                    ▲                             │            │
│                                    │                             │            │
│                                    │                             ▼            │
│                                    │                  ┌────────────────────┐  │
│                                    │                  │ Text Index         │  │
│                                    │                  │                    │  │
│                                    │                  │ - Prefix Tree      │  │
│                                    │                  │ - Suffix Tree      │  │
│                                    │                  │ - Postings Lists   │  │
│                                    │                  └──────────┬─────────┘  │
│                                    │                             │            │
│                           ┌────────┴───────┐                     │            │
│  ┌───────────────┐        │ Result         │◀────────────────────┘            │
│  │ Client        │◀───────│ Formatting     │                                  │
│  │ Applications  │        │                │                                  │
│  └───────────────┘        └────────────────┘                                  │
│                                                                               │
│                                                                               │
│  ┌───────────────┐        ┌────────────────┐         ┌────────────────────┐   │
│  │ Client        │        │ Command        │         │ Text Index         │   │
│  │ Applications  │───────▶│ Processing     │────────▶│ Builder            │   │
│  └───────────────┘        │ (FT.CREATE)    │         └──────────┬─────────┘   │
│                           └────────────────┘                     │            │
│                                                                  │            │
│                                                                  ▼            │
│                                                       ┌────────────────────┐  │
│                                                       │ Tokenization       │  │
│                                                       │ Pipeline           │  │
│                                                       │                    │  │
│                                                       │ 1. Split on punct. │  │
│                                                       │ 2. Case conversion │  │
│                                                       │ 3. Stop word filter│  │
│                                                       │ 4. Stemming        │  │
│                                                       └──────────┬─────────┘  │
│                                                                  │            │
│                                                                  ▼            │
│                                                       ┌────────────────────┐  │
│                                                       │ Term Storage in    │  │
│                                                       │ Prefix/Suffix Trees│  │
│                                                       └────────────────────┘  │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘

```
## Specification

Each ```TEXT``` field has the a set of configurables some control the process to convert a string into a vector of terms, others control the contents of the generated index.

| Metadata          | Type              | Meaning                                                                    | Default Value |
| ----------------- | ----------------- | -------------------------------------------------------------------------- | ------------- |
| Punctuation       | vector<character> | Characters that separate words during the tokenization process.            |
| Stop Words        | vector<String>    | List of stop words to be removed during decomposition                      |
| Stemming Language | Enumeration       | Snowball stemming library language control                                 |
| Suffix Tree       | Boolean           | Controls the presence/absence of a suffix tree.                            |
| Word Offsets      | Boolean           | Indicates if the postings for a word contain word offsets or just a count. |

### Extension of the ```FT.CREATE``` command

Clauses are provided to control the configuration. 
If supplied before the ```SCHEMA``` keyword then the default value of the clause is changed for any text fields declared in this command.
If supplied as part of an individual field declaration, i.e., after the ```SCHEMA``` keyword, then it sets the configurable for just that field.

```
[PUNCTUATION <string>]
```
The characters of this string are used to split the input string into words. Note, the splitting process allows escaping of input characters using the usual backslash notation. This string cannot be empty. Default value is: _TBD_


```
[WITHSUFFIXTRIE | NOSUFFIXTRIE]
```

Controls the presence/absence of the second prefix tree in the field-index. Default is ```NOSUFFIXTRIE```.

```
[WITHOFFSETS | NOOFFSETS]
```

Controls whether term-offsets are tracked in the field-index. Default is ```WITHOFFSETS```.

```
[NOSTOPWORDS | STOPWORDS <count> [word ...]]
```

Controls the application and list of stop words. Note that ```STOPWORDS 0``` is equivalent to ```NOSTOPWORDS```. The default stop words are:

**a,    is,    the,   an,   and,  are, as,  at,   be,   but,  by,   for,
 if,   in,    into,  it,   no,   not, of,  on,   or,   such, that, their,
 then, there, these, they, this, to,  was, will, with**.

```
[MINSTEMSIZE <size>]
```

This clause controls the minimum number of characters in a word before it is subjected to stemming. Default value is 4.

```
[NOSTEM | LANGUAGE <language>]
```

Controls the stemming algorithm. Supported languages are: 

**none, arabic, armenian, basque, catalan, danish, dutch, english, estonian, finnish, french, german, greek, hindi, hungarian, indonesian, irish, italian, lithuanian, nepali, norwegian, porter, portuguese, romanian, russian, scripts, serbian, spanish, swedish, tamil, turkish, yiddish**

The default language is **english**. Note: ```LANGUAGE none``` is equivalent to ```NOSTEM```.

### Query Language Extensions

While the syntax or Vector, Tag and Numeric query operators requires the presence of a field specifier, it is optional for text query operators. 
If a field specifier is omitted for a text query operator then this is syntactic sugar for specifying all of the text fields.

As with the other query operators, the text query operators fit into the and/or/negate/parenthesized query language structure.

#### Single Term Matching

The syntax for single term matching is simply the text of the term itself. If one character of the term is an asterisk ```*``` it can match any number of characters forming a wildcard search. If the field-index does not have the suffix tree, then the position of the wildcard is restricted to only the end of the term.

#### Phrase Matching

Phrase matching is specified by enclosing a sequence of terms in a pair of double quotation marks ```"```. Phrase matching is only possible when term-offsets are specified in the index.

### Limits

To avoid combinatorial explosion certain operations have configurable limits applied.

| Name                 | Default | Limit                                                             |
| -------------------- | ------- | ----------------------------------------------------------------- |
| max-fuzzy-distance   | 2       | The maximum edit distance for a fuzzy search.                     |
| max-wildcard-matches | 200     | Maximum number of words that a single wildcard match can generate |

### Dependencies

snowball library https://snowballstem.org/ and https://github.com/snowballstem