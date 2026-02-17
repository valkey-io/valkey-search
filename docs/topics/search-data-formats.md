---
title: "Search - Data Formats"
description: Search Module Formats for Input Field Data
---

# Tag Fields

For both HASH and JSON index types, the incoming data is a single UTF-8 string.

On ingestion, each incoming field can contain multiple tags. The input field is split using the declared `SEPARATOR` character into separate tags.
Leading and trailing spaces are removed from each split field to form the final tag.

## Examples of Tag Values

Single/multiple, with different separators, etc.

# Numeric Fields

Formal definition of acceptable numbers.

## Examples of Numeric Values

# Vector Fields

Definition of HASH vector fields (IEEE binary, etc). Fixed length, etc.

Definition of JSON vector fields

## Examples of vector field values

Both JSON and HASH examples of vectors

# Text Fields

## Text Ingestion

Describe each stage of the text ingestion processing. How a string becomes a sequence of words.

### Lexical Analysis

### Stop Word Removal

Does a removed stop word increment the position???

### Stemming

## Text Ingestion Examples

### Lexical Processing Examples

### Stop word removal example

### Stemming example
