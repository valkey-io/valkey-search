---
RFC: 1263
Status: Proposed
---

# Multi-Language Support for Full-Text Search

## Abstract

The existing English-only Full-Text Search (FTS) feature is extended to support
11 additional languages: French, German, Spanish, Italian, Portuguese,
Russian, Swedish, Turkish, Dutch, Indonesian, and Arabic. The current
English Lexer class is replaced with a `LanguageProcessor` — a
composable pipeline of segmenters, token filters, and query tokenizers — so
that per-language behavior is expressed as a
composition of standalone primitives. The pipeline is wired to Snowball's
per-language stemmers, punctuation sourced from the Unicode Common Locale Data Repository,
Apache Lucene-derived stop words, and ICU-backed Unicode normalization and case
folding. The feature is gated on module version 1.3 so that mixed-version
clusters fail closed for non-English index metadata.

## Motivation

FTS today can only tokenize, stem, and filter English text. This forces
non-English deployments either to store text unnormalized (destroying
recall on morphologically rich languages) or to move off Valkey for search
workloads that involve any language other than English. The languages
introduced in this RFC cover the majority of non-English FTS demand
observed on comparable systems and unblock a broad range of concrete
workloads:

- **Global e-commerce and product catalogs.** Product discovery is the
  dominant conversion path for online retail, and shoppers typically
  search in their native language and expect morphological variants of a
  product name to match. Retailers with catalogs primarily in French,
  German, Spanish, Italian, or Portuguese today either accept degraded
  recall on Valkey or would need to route non-English traffic to a 
  separate search stack.
- **Multi-lingual chat, session, and support history.** Applications
  that store per-user conversation, transcript, or activity logs in the
  user's own language issue short queries against small per-user
  corpora. Short queries have no redundancy to fall back on — if a
  single term fails to match a stemmed variant or a differently-cased
  form of the indexed text, the whole query returns no results.
  Language-aware stemming and case folding are what keep these queries usable.
- **RAG (Retrieval-Augmented Generation) for non-English corpora.** LLM
  applications increasingly retrieve from non-English knowledge bases
  (product docs, regulatory text, medical or legal corpora, customer
  reviews). Retrieval quality directly determines answer quality; if 
  fields are not indexed with language-awareness, precision and recall
  suffer.
- **Regional applications with user-generated content.** Platforms in
  languages like Portuguese, Italian, Arabic, and Dutch — reviews, comments, 
  forums, social feeds — contain user-generated text with inconsistent
  input (mixed diacritics, ligatures from copy-paste, combining
  sequences). A shared canonical form for both indexed content and
  query terms is what makes results consistent across users regardless
  of how the same word was entered.

The design deliberately treats the pipeline as an extension point:
adding future languages (including CJK, which require different
segmenters and do not need stemming) is a composition change, not a
change to the ingestion or query hot paths.

## Design considerations

Text handling was previously a single monolithic `Lexer` class with
hard-coded English tokenization, ASCII-only punctuation, and English
Snowball stemming. This is replaced with a `LanguageProcessor` — a
pipeline built from three pluggable primitive types.

- **`Segmenter`** — splits a text span into tokens (1→N). The default
  implementation `PunctuationSegmenter` is used by all 12 Snowball
  languages and parameterized per-language via a `PunctuationSet` sourced
  from the Unicode Common Locale Data Repository (CLDR) v46.
- **`TokenFilter`** — transforms or drops a token (1→1 or 1→0). Default
  implementations `NormalizeCaseFoldFilter` (Unicode NFC + case fold,
  locale-aware for Turkish dotless-I) and `StopWordFilter` (per-language
  default lists sourced from Apache Lucene) are shared across all 12
  Snowball languages and parameterized per-language via their inputs
  (locale, stop-word set).
- **`Stemmer` / `QueryTokenizer`** — accessors stored on the processor
  for callers that need them outside the ingestion loop (stem-map
  building, delete path, query-time word extraction). `SnowballStemFilter`
  is per-language (one `sb_stemmer` per language); `PunctuationQueryTokenizer`
  is shared across Snowball languages and delegates word-boundary
  detection to `PunctuationSegmenter::IsPunctuation()`.

Segmenters run sequentially (each further splits the previous output),
then every surviving token is threaded through the filter chain. As the 
LanguageProcessor is intended to be stateless, stemming is deliberately 
not part of the ingestion pipeline — tokens are indexed in their 
normalized-but-unstemmed form and the stemmer is invoked separately 
when a stem-map or query expansion is needed. This keeps the ingestion 
path oblivious to language while preserving the ability to query-expand 
via stem roots at read time.

### Ingestion pipeline

```
Input text
    │
    ▼
┌────────────────────────────┐
│ Segment()                  │
│   PunctuationSegmenter     │  UTF-8 codepoint aware; per-language
│   text → [tokens]          │  punctuation set (e.g. Arabic, French,
│                            │  German, ...)
└────────────────────────────┘
    │
    ▼
┌────────────────────────────┐
│ ApplyFilters()             │
│   NormalizeCaseFoldFilter  │  token → token (NFC + Unicode
│                            │  casefold; locale-aware for Turkish)
│   StopWordFilter           │  token → token | ∅
└────────────────────────────┘
    │
    ▼
Indexed tokens  ──┐
                  │  (separately) GetStemmer()->BuildStemMap()
                  ▼           stem_root → {surface_forms}
             Stem map
```

### Query pipeline

Query grammar characters (`@`, `(`, `)`, `|`, `"`, `*`, `%`, `-`, `\`) are
handled by the parser, and word-boundary detection is delegated to a
pluggable `QueryTokenizer` bound to the index's language:

```
Query expression
    │
    ▼
┌────────────────────────────────────────────────────┐
│ FilterParser walks query syntax chars              │
│   Word extraction → QueryTokenizer                 │
│     PunctuationQueryTokenizer (Snowball languages) │
│     [future] CJK-specific tokenizers               │
│   Escape handling → \<char>                        │
└────────────────────────────────────────────────────┘
    │
    ├── Regular term ──────────────────────┐
    │   NormalizeCaseFoldFilter            │
    │   StopWordFilter (if stop → drop)    │
    │   Stemmer.GetStemRoot()              │→ TermPredicate
    │                                      │
    ├── Wildcard / Fuzzy ──────────────────┤
    │   NormalizeCaseFoldFilter only       │→ Prefix/Suffix/FuzzyPredicate
    │                                      │
    └── Exact phrase (quoted) ─────────────┘
        NormalizeCaseFoldFilter only        → TermPredicate(exact=true)
```

`Fuzzy` DP is codepoint-aware so `MINSTEMSIZE` and edit distance work
correctly for multi-byte scripts (Arabic, Russian, Turkish). This includes
buffering across `Rax` edge boundaries so a codepoint split by radix-tree
edge compression decodes correctly. `Proximity` and `OrProximity` operate
on word-level positions in an already-tokenized stream and are unaffected
by multi-byte handling.

### Version gating and cluster safety

Multi-language support ships behind a `MetadataVersion` bump to `1.3`.
Non-English languages are gated by a version check so mixed-version clusters 
fail closed: nodes running a module version < 1.3 will reject index metadata carrying a
non-English `LANGUAGE`. English and unspecified language values remain
accepted on all versions for backward compatibility.

## Compatibility divergences

We intentionally diverge from RediSearch with the behaviors below.
Each divergence is intentional: these behaviors align Valkey Search's
tokenization pipeline with the semantics used by **Apache Lucene**, which
produces measurably better retrieval quality than RediSearch's current 
behavior.

**Backward compatibility note for English.** English is the only language
that FTS in Valkey shipped with prior to this RFC, and its default
stop-word list is preserved from the original release (which derived
those stop words from the RediSearch documentation). We keep the current
English list intact so existing English indexes and queries continue to
behave exactly as before. A future release can introduce a divergence
that switches English defaults to Apache Lucene's English stop-word list;
that change would be called out separately and gated appropriately.

**Scope note on multi-language indexes.** An index in this release holds
text in a single language. Because a `LANGUAGE` clause at query time
would only be meaningful if a single index could contain fields in
multiple languages — which is not supported — this RFC deliberately does
not add a query-time `LANGUAGE` argument (RediSearch does). Users who
need to search over multiple languages create one index per language.
The [Future extension for multi-language indexes](#future-extension-for-multi-language-indexes)
section below describes how this restriction can be relaxed in a future
release.

### 1. Snowball 3.0.1 vs Snowball 2.1.0 (Dutch)

Valkey Search uses Snowball 3.0.1, whereas RediSearch uses Snowball [2.1.0](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisearch/redisearch-2.0-release-notes/). Snowball 3.0.1 uses the
Kraaij-Pohlmann algorithm for Dutch stemming, which strips grammatical
prefixes; Snowball 2.1.0's Dutch Porter stemmer does not. The result is
that Valkey Search reduces some Dutch words to the same stem where
RediSearch reduces them to different stems.

```
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE dutch
    SCHEMA content TEXT
HSET doc:1 content "geschilderd"

FT.SEARCH idx "schilder" DIALECT 2
  → RediSearch: 0 results  (Porter: "geschilderd" is not reduced, no match)
  → Valkey:     1 result   (K-P: "geschilderd" stems to "schilder", matches)
```

Apache Lucene's Dutch analyzer uses Kraaij-Pohlmann-style prefix
stripping, matching Valkey Search's behavior.

### 2. ICU `utf8Fold` vs simple Unicode lowercasing

`NormalizeCaseFoldFilter` uses ICU's `CaseMap::utf8Fold` during ingestion
and querying. `utf8Fold` decomposes characters like `ß` to `ss` and
ligatures like `ﬁ` to `fi` so searches match across these equivalent
forms. RediSearch uses simple Unicode lowercasing, which preserves the
original characters.

```
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE german
    SCHEMA content TEXT NOSTEM
HSET doc:1 content "Straße"

FT.SEARCH idx "strasse" DIALECT 2
  → RediSearch: 0 results  (lowercases to "straße", no match for "strasse")
  → Valkey:     1 result   (utf8Fold: "Straße"→"strasse", matches)

FT.CREATE idx2 ON HASH PREFIX 1 doc: LANGUAGE english
    SCHEMA content TEXT NOSTEM
HSET doc:1 content "ﬁnancial"

FT.SEARCH idx2 "financial" DIALECT 2
  → RediSearch: 0 results  (preserves ﬁ ligature, no match for "financial")
  → Valkey:     1 result   (utf8Fold: "ﬁnancial"→"financial", matches)
```

Apache Lucene's analyzers apply case folding equivalent to `utf8Fold` for
these forms, matching Valkey Search's behavior.

### 3. Per-language default stop words

Valkey Search ships default stop words for every language it supports.
The lists are sourced from Apache Lucene's per-language stop-word sets and
are applied automatically at both ingestion and query time when the
`FT.CREATE` command does not specify a `STOPWORDS` override. RediSearch's
[documentation](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/stopwords/) states that only English stop words are filtered during ingestion and query time.

Consequence: queries containing language-specific stop words behave
differently.

```
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE turkish
    SCHEMA body TEXT
HSET doc:1 body "alışveriş"

FT.SEARCH idx "alışveriş yapmak" DIALECT 2
  → RediSearch: 0 results  ("yapmak" not a stop word, requires both terms,
                             no match)
  → Valkey:     1 result   ("yapmak" is a Turkish stop word, filtered from
                             query, searches only "alışveriş", matches)
```

**English defaults are preserved for backward compatibility.** As mentioned above, 
switching English defaults to Lucene's English stop words is intentionally left for a future
release; users who want Lucene-style English stop-word behavior today
can supply it explicitly via `STOPWORDS`.

### 4. Per-language default punctuation

`PunctuationSegmenter` applies per-language default punctuation sets
sourced from Unicode CLDR v46 (`src/indexes/text/punctuation.h`).
RediSearch's tokenization [documentation](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/escaping/) does not indicate that language-specific
default punctuation is applied.

For Spanish, for example, Valkey Search uses the inverted punctuation
characters `¡` (U+00A1) and `¿` (U+00BF) as default token separators:

```
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE spanish
    SCHEMA body TEXT
HSET doc:1 body "vienes mañana"
HSET doc:2 body "¿vienes mañana al parque?"

FT.SEARCH idx "vienes" DIALECT 2
  → RediSearch: 1 result   (¿ is NOT a separator → doc:2 tokenized as
                             ["¿vienes","mañana","al","parque"];
                             "vienes" ≠ "¿vienes", only doc:1 matches)
  → Valkey:     2 results  (¿ IS a Spanish punctuation separator → doc:2
                             tokenized as ["vienes","mañana","al","parque"];
                             both docs contain "vienes")
```

Apache Lucene's language analyzers use CLDR-derived tokenizer rules that
treat these characters as separators, matching Valkey Search's behavior.

### 5. Strict `NOSTEM` enforcement (pre-existing)

This divergence is **pre-existing** — it is a property of the original
English-only FTS implementation and is not introduced by the
multi-language changes. It is documented here to show how it manifests
with non-English ingestion and lookup.

`NOSTEM` in Valkey Search is enforced strictly at both index time and
query time: only the original surface form is stored, and queries never
run the stemmer. In RediSearch, `NOSTEM` appears to apply at index time,
but when querying, only if a field is specified — non-field queries
bypass `NOSTEM` and querying stemmed forms on `NOSTEM` indexes returns
results.

```
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE indonesian
    SCHEMA body TEXT WITHSUFFIXTRIE NOSTEM
HSET doc:1 body "burung lambat kuat lumba kebenaran elang jendela"

FT.SEARCH idx "kekuatan burung" DIALECT 2
  → RediSearch: 1 result   (stems "kekuatan"→"kuat", matches "kuat" in body)
  → Valkey:     0 results  (NOSTEM: no stemming, "kekuatan" ≠ "kuat")

FT.SEARCH idx "@body:kekuatan" DIALECT 2
  → RediSearch: 0 results  (field-targeted query → NOSTEM check works)
```

Valkey Search's behavior aligns with Lucene, where a field configured
without a stemmer is not silently stemmed at query time regardless of
whether the query specifies the field.

### 6. Fuzzy search over original vs stemmed forms (pre-existing)

This divergence is also **pre-existing** — Valkey Search's fuzzy path has
always operated on indexed (unstemmed) forms and stems are not stored as
independent postings entries. Multi-language support did not change this
behavior; it is included here to illustrate how the divergence manifests
in a non-English example.

For fuzzy search, RediSearch returns documents whose stems are within
one edit distance of the query term. Valkey Search fuzzy-matches against
the indexed (unstemmed) form only. Illustrated in Arabic:

```
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE arabic
    SCHEMA body TEXT WITHSUFFIXTRIE
HSET doc:1 body "حرية"    (freedom, 4 code points: ح-ر-ي-ة)

# With stemming enabled
FT.SEARCH idx "%حر%" DIALECT 2
 → RediSearch: 1 result  (fuzzy matches at distance 0 from stem حر)
 → Valkey:     0 results (trie has only حرية; distance from حر is 2)

# With NOSTEM: only حرية stored, no stem entry
FT.CREATE idx2 ... NOSTEM
FT.SEARCH idx2 "%حر%" DIALECT 2
 → RediSearch: 0 results
 → Valkey:     0 results
```

### Future extension for multi-language indexes

An index in this release holds text in a single language, and there is no
query-time `LANGUAGE` argument because a query-time language would only
be meaningful if a single index could contain fields in multiple
languages. Two forward-compatible extensions are envisioned, as discussed in
the original [FTS RFC](https://github.com/valkey-io/valkey-rfc/pull/24/changes#diff-48ff4af40094c1df661c73561650315e01d872a043921be3a1a45eb2ffa39638R431)
and related [comments](https://github.com/valkey-io/valkey-rfc/pull/24/changes#r2210835085):

1. **`LANGUAGE_FIELD` in `FT.CREATE`.** A future release can add a
   `LANGUAGE_FIELD` clause to `FT.CREATE` naming a document field whose
   value marks each document's language. Ingestion would then route each
   document through the corresponding `LanguageProcessor`. Queries
   would need a `LANGUAGE` argument on `FT.SEARCH` to select the
   processor used at query time.
2. **`FILTER` in `FT.CREATE`.** Rather than allowing multiple languages
   into one index, a `FILTER` clause on `FT.CREATE` (an arbitrary
   expression evaluated against each document's fields) can restrict
   membership so that each index still holds exactly one language, even
   when the underlying corpus is mixed. For example, an English index
   would include only documents where `@language == "English"`. This
   approach avoids the semantic complexity of a mixed-language index
   while covering the same use case.


## Usage examples

### 1. Create a French index and search across morphological variants

```
FT.CREATE fr_idx ON HASH PREFIX 1 doc:fr: LANGUAGE french
    SCHEMA title TEXT content TEXT
HSET doc:fr:1 title "chaussures d'été"  content "Les chaussures rouges."
HSET doc:fr:2 title "chapeaux"          content "Un chapeau bleu."

# Snowball stems "chaussures" → "chausur"; a search for the singular
# "chaussure" also matches doc:fr:1.
FT.SEARCH fr_idx "chaussure" DIALECT 2
  → 1 result: doc:fr:1

# The apostrophe in "d'été" is a French default punctuation character;
# a search for "été" matches too.
FT.SEARCH fr_idx "été" DIALECT 2
  → 1 result: doc:fr:1
```

### 2. German with `ß`↔`ss` case folding and compound words

```
FT.CREATE de_idx ON HASH PREFIX 1 doc:de: LANGUAGE german
    SCHEMA body TEXT
HSET doc:de:1 body "Die Straße ist lang."
HSET doc:de:2 body "Geschwindigkeitsbegrenzung eingehalten."

# utf8Fold matches "strasse" against indexed "straße".
FT.SEARCH de_idx "strasse" DIALECT 2
  → 1 result: doc:de:1

# Compound word retained as a single token; prefix search works.
FT.SEARCH de_idx "Geschwindigkeits*" DIALECT 2
  → 1 result: doc:de:2
```

### 3. Turkish locale-aware case folding (dotted/dotless I)

```
FT.CREATE tr_idx ON HASH PREFIX 1 doc:tr: LANGUAGE turkish
    SCHEMA body TEXT
HSET doc:tr:1 body "İstanbul çok güzel."
HSET doc:tr:2 body "istanbul haritası"

# NormalizeCaseFoldFilter with locale="tr" folds İ→i (not İ→i̇).
# Both documents are found regardless of the case in the query.
FT.SEARCH tr_idx "istanbul" DIALECT 2
  → 2 results: doc:tr:1, doc:tr:2

FT.SEARCH tr_idx "İSTANBUL" DIALECT 2
  → 2 results: doc:tr:1, doc:tr:2
```

### 4. Arabic normalization (NFKC + Unicode case fold)

Two ways of encoding the same Arabic word. Without NFKC these would not
match — the codepoints differ even though the glyphs are visually
identical on modern fonts. Presentation-form data commonly appears in
text from legacy systems, PDFs, and OCR output.

```
FT.CREATE ar_idx ON HASH PREFIX 1 doc:ar: LANGUAGE arabic
    SCHEMA body TEXT

HSET doc:ar:1 body "المكتبة"
    # basic Arabic letters:
    # U+0627 U+0644 U+0645 U+0643 U+062A U+0628 U+0629

HSET doc:ar:2 body "ﺍﻟﻤﻜﺘﺒﺔ"
    # same word using Arabic Presentation Forms-B:
    # U+FE8D U+FEDF U+FEE1 U+FEDA U+FE97 U+FE91 U+FE93

# NFKC normalizes the presentation-form codepoints to their basic-letter
# equivalents, so both documents tokenize to the same term.
FT.SEARCH ar_idx "المكتبة" DIALECT 2
  → 2 results: doc:ar:1, doc:ar:2
```

### 5. Override default stop words

Every non-English language ships with a default Apache Lucene stop-word
list. Use `NOSTOPWORDS` to disable it, or `STOPWORDS` to provide a
custom set. The following contrasts the default and overridden behavior
for Turkish, where `için` ("for") is in the default list:

```
# Default: Turkish index uses the Lucene default stop-word list
FT.CREATE tr_default ON HASH PREFIX 1 doc:tr_d: LANGUAGE turkish
    SCHEMA body TEXT
HSET doc:tr_d:1 body "kitap için okuma"

# "için" is a default Turkish stop word → filtered from both the indexed
# tokens and the query. The query is effectively empty and returns no
# results.
FT.SEARCH tr_default "için" DIALECT 2
  → 0 results

# Custom: same corpus, but override the stop-word list to exclude "için"
FT.CREATE tr_custom ON HASH PREFIX 1 doc:tr_c: LANGUAGE turkish
    STOPWORDS 2 ve veya SCHEMA body TEXT
HSET doc:tr_c:1 body "kitap için okuma"

# "için" is no longer treated as a stop word → indexed and searchable.
FT.SEARCH tr_custom "için" DIALECT 2
  → 1 result: doc:tr_c:1
```

### 6. `NOSTEM` for exact-form indexes

Without `NOSTEM`, Snowball stems both indexed content and the query, so
a search for one Italian conjugation matches documents containing any
other conjugation that stems to the same root. With `NOSTEM`, only the
exact indexed surface form matches:

```
FT.CREATE it_stem   ON HASH PREFIX 1 doc:it_s: LANGUAGE italian
    SCHEMA body TEXT
FT.CREATE it_nostem ON HASH PREFIX 1 doc:it_n: LANGUAGE italian
    SCHEMA body TEXT NOSTEM

HSET doc:it_s:1 body "correre"
HSET doc:it_n:1 body "correre"

# With stemming: "corriamo" stems to the same root as "correre" → match.
FT.SEARCH it_stem   "corriamo" DIALECT 2
  → 1 result: doc:it_s:1

# With NOSTEM on the same content: no stemming applied anywhere,
# "corriamo" ≠ "correre" → no match.
FT.SEARCH it_nostem "corriamo" DIALECT 2
  → 0 results
```

### 7. Separating languages by key prefix

Since an index holds a single language, deployments with mixed-language
corpora create one index per language and route documents by key prefix
(or, in a future release, by `FILTER` on a language field):

```
FT.CREATE en_idx ON HASH PREFIX 1 doc:en: LANGUAGE english SCHEMA body TEXT
FT.CREATE ru_idx ON HASH PREFIX 1 doc:ru: LANGUAGE russian SCHEMA body TEXT

HSET doc:en:1 body "The quick brown fox."
HSET doc:ru:1 body "Быстрая коричневая лиса."

FT.SEARCH en_idx "quick" DIALECT 2   # matches doc:en:1
FT.SEARCH ru_idx "быстрый" DIALECT 2 # stems to "быстр", matches doc:ru:1
```
