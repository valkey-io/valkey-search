---
RFC: (PR number)
Status: (Change to Proposed when it's ready for review)
---

# Title (Required)
Text Searching

## Abstract (Required)

The existing search platform is extended with a new field type, ```TEXT``` is defined. The query language is extended to support locating keys with text fields based on term, wildcard, fuzzy and exact phrase matching.

## Motivation (Required)

Text searching is useful in many applications.

## Terminology

In the context of this RFC.

| Term | Meaning |
| --- | --- |
| key | A Valkey key. Depending on the context of usage could be the text of the key name or the contents of the HASH or JSON key. |
| field | A component of an index. Each field has a type and a path.|
| index | A collection of fields and field-indexes. The object created by the ```FT.CREATE``` command. |
| field-index | A data structure associated with a field that is intended to accelerate the operation of the search operators for this field type. |
| character | An Unicode character. A character may occupy 1-4 bytes of a UTF-8 encoded string. |
| word | A syntactic unit of text consisting of a vector of characters. A word is delimited by un-escaped punctuation and/or whitespace. |
| token | same as a word |
| text | A UTF-8 encoded string of bytes. |
| stemming | A process of mapping similar words into a common base word. For example, the words _drives_, _drove_ and _driven_ would be replaced with the  _drive_. |
| term | The output of stemming a word. |
| prefix tree | A mapping from input string to an output object. Insert/delete operations are O(length(input)). Additional operations include the ability to iterate over the entries that share a common prefix in O(length(prefix)) time. |

## Design considerations (Required)

The text searching facility provides machinery that decomposes text fields into terms and field-indexes them.
The query facility of the ```FT.SEARCH``` and ```FT.AGGREGATE``` commands is enhanced to select keys based on combinations of words, wildcard, fuzzy and phrase matching.

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

Step 1 of the tokenization process is controlled by a specified list of punctuation characters. 
Sequences of one or more punctuations characters delimit word boundaries.
Note that individual punctuation characters can be escaped using the normal backlash prefix syntax, causing the next character to be treated as non-punctuation.

### Text Field-Index Structure

The text field-index is a mapping from a term to a list of locations.
Depending on the configuration, the list of locations may be just a list of keys or a list of key/term-offset pairs.

The mapping structure is built from one or two prefix trees. When present, the second prefix tree is built from each inserted term sequenced in reverse order, effectively providing a suffix tree.

### Search query operators

Unlike the Vector, Tag and Numeric search operators the specification of a field is optional. If the field specification is omitted then a match is declared if any field matches the search operator.

#### Term matching

There are three types of term matching: exact, wildcard and fuzzy. Exact term matching is self-descriptive, i.e., only keys containing exactly the specified text are matched.

Wildcard matching provides a subset of reg-ex style matching of terms.
Initially, only a single wildcard specifier ```*``` is allowed which matches any number of characters in a term.
The wildcard can be at any position within the term, providing prefix, suffix and infix style matching.
Note, in this proposal, the second prefix tree is required to perform infix and suffix matching.

#### Phrase matching

Exact phrase matching is specified by enclosing a sequence of terms in double quotes ```"```. In order to perform phrase matching, the field-index must be configured to contain term-offsets.

## Specification (Required)

Each ```TEXT``` field has the a set of configurables some control the process to convert a string into a vector of terms, others control the contents of the generated index.

| Metadata | Type | Meaning | Default Value |
| --- | --- | --- | --- |
| Punctuation | vector<character> | Characters that separate words during the tokenization process. |  
| Stop Words | vector<String> | List of stop words to be removed during decomposition |
| Stemming Language | Enumeration | Snowball stemming library lanugage control |
| Suffix Tree | Boolean | Controls the presence/absence of a suffix tree. |
| Word Offsets | Boolean | Indicates if the postings for a word contain word offsets or just a count. |

### Extension of the ```FT.CREATE``` command

Clauses are provided to control the configuration. 
If supplied before the ```SCHEMA``` keyword then the default value of the clause is changed for any text fields declared in this command.
If supplied as part of an individual field declaration, i.e., after the ```SCHEMA``` keyword, then it sets the configurable for just that field.

```
[PUNCTUATION <string>]
```
The characters of this string are used to split the input string into words. Note, the splitting process allows escaping of input characters using the usual backslash notation. This string cannot be empty. Default value is: 


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

| Name | Default | Limit |
| --- | --- | --- |
| max-fuzzy-distance | 2 | The maximum edit distance for a fuzzy search. |
| max-wildcard-matches | 200 | Maximum number of words that a single wildcard match can generate |




### Dependencies (Optional)

snowball library https://snowballstem.org/ and https://github.com/snowballstem