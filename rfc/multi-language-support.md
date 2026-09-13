---
RFC: 1263
Status: Proposed
---

# Multi-Language Support for Full-Text Search

## Abstract

The existing English-only Full-Text Search (FTS) feature is extended to support
11 additional languages: French, German, Spanish, Italian, Portuguese,
Russian, Swedish, Turkish, Dutch, Indonesian, and Arabic. The current
English Lexer class is replaced with a polymorphic `Language` interface —
each language is a first-class, self-contained object implementing
segmentation, normalization, stemming, and stop-word filtering. The
implementation (`SnowballLanguage`) is wired to Snowball's per-language
stemmers, punctuation sourced from the Unicode Common Locale Data Repository,
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

The design deliberately treats the `Language` interface as an extension point:
adding future languages (including CJK, which require different
segmenters and do not need stemming) is a new `Language` subclass, not a
change to the ingestion or query hot paths.

## Design considerations

Text handling was previously a single monolithic `Lexer` class with
hard-coded English tokenization, ASCII-only punctuation, and English
Snowball stemming. This is replaced with a polymorphic **`Language`**
interface. Callers program to `Language*`; the concrete type is selected
once at index-creation time and shared, immutably, across every index
using that language.

- **`Language`** is the strategy interface. It exposes data accessors
  (`GetDefaultPunctuation`, `GetDefaultStopWords`, `GetNormalizationForm`,
  `CaseFoldLocale`, `Name`, `Id`) and behavioral methods (`Tokenize`,
  `TokenizeWithStemMap`, `NormalizeInPlace`,
  `IsStopWord`, `GetStemmer`, `GetPunctuationSet`, `IsSupported`,
  `MinRequiredVersion`).
- **`SnowballLanguage`** is the shared base for all punctuation-segmented,
  Snowball-stemmed languages. It implements the entire segmentation
  algorithm once via a private `SegmentInternal` method (parameterized by
  `handle_escapes` / `filter_stop_words`), driving `Tokenize` and
  `TokenizeWithStemMap`. Concrete subclasses only supply data (punctuation
  string, stop-word list, normalization form, locale, stemmer algorithm
  name). At construction it normalizes each stop word through the *same*
  `NormalizeCaseFoldFilter` used for tokens, so stop-word matching is
  Unicode-consistent with the tokens it filters.
  **Normalization is per-language.** Each `SnowballLanguage` subclass
  declares its own `NormalizationForm` and casefold locale.
  `NormalizeCaseFoldFilter` applies these in a fixed order — Unicode
  normalization first, then case folding — to every token and stop word.
  Arabic uses **NFKC** (compatibility decomposition) so that Arabic
  Presentation Forms-B codepoints normalize to their basic-letter
  equivalents. All other Snowball languages use **NFC** (canonical
  composition), which is sufficient for diacritics and combining sequences
  but does not apply compatibility mappings. Turkish uses locale-aware case
  folding via `CaseMap::utf8ToLower("tr")` to handle the dotted/dotless I
  distinction; all other languages use locale-independent
  `CaseMap::utf8Fold`.
- **`LanguageRegistry`** is a process-wide singleton holding one shared,
  immutable `Language` instance per enum value; callers never construct
  concrete languages directly. `LANGUAGE_UNSPECIFIED` maps to English.
  Unrecognized language enum values are rejected loudly rather than
  falling back to English, preventing older module versions from
  incorrectly indexing data they cannot tokenize.
- **`CustomizedLanguage`** decorates a base language with FT.CREATE
  `PUNCTUATION` / `STOPWORDS` overrides, delegating normalization, locale,
  version gating, and stemming to the base. A user-supplied `PUNCTUATION`
  string fully replaces the language's default punctuation set (it is
  not merged). A user-supplied `STOPWORDS` list likewise replaces the
  default list. Both ingestion (`SegmentInternal`) and query parsing
  (`FilterParser`) obtain the effective punctuation from the same
  `language.GetPunctuationSet()` call, so there is no ingestion/query
  mismatch. English retains its legacy ASCII-only punctuation set for
  backward compatibility; new languages add CLDR-derived non-ASCII
  punctuation on top of the ASCII base.
- **`SnowballStemFilter`** adapts the external `libstemmer` C API to the
  `Stemmer` interface, using a thread-local `sb_stemmer` cache keyed by
  language enum to avoid mutex contention on ingestion threads. Exposes
  `GetStemRoot()` for single-word stemming (delete path, query expansion)
  and `BuildStemMap()` for ingestion-time batch stem→surface-form mapping.
- **`PunctuationSet`** (ASCII `std::bitset<128>` + non-ASCII
  `flat_hash_set<uint32_t>`) is built by `BuildPunctuationSet()` in
  `language.h`. It seeds from the per-language punctuation string and also
  adds all Unicode whitespace codepoints via ICU's `\p{White_Space}`
  property as word boundaries, ensuring that non-breaking spaces and
  typographic whitespace in French, Arabic, and other scripts are correctly
  treated as delimiters. The ASCII bitset preserves the `main`-branch O(1)
  punctuation lookup for the common (English/ASCII) case; codepoint
  decoding is triggered only by a non-ASCII lead byte.

The `LanguageRegistry` singleton holds one shared, immutable `Language`
instance per enum value — these provide the per-language defaults
(punctuation string, stop-word list, normalization form, locale, stemmer
algorithm). At index creation time, `CreateLanguage()` wraps the registry
singleton in a `CustomizedLanguage` that carries the effective punctuation
and stop words (either user-supplied overrides from FT.CREATE or the
language's defaults). Each index therefore owns its own immutable
`CustomizedLanguage` instance — heavyweight members (`PunctuationSet`,
stop-word hash set, normalizer, `SnowballStemFilter`) are built once at
construction and never modified. The per-instance overhead is small (~1-2 KB
depending on the language's stop-word list size); the actual `sb_stemmer*`
used by `SnowballStemFilter` is thread-local and shared across all instances
of the same language. There is no mutable language state, so
cross-contamination between indexes is impossible by construction.

Stemming is deliberately **not** part of the ingestion segmentation pass —
tokens are indexed in their normalized-but-unstemmed form; the stemmer is
invoked separately when a stem-map (ingestion) or query expansion is
needed. During ingestion, `TokenizeWithStemMap()` first collects all tokens
via `SegmentInternal`, then invokes `SnowballStemFilter::BuildStemMap()` as
a second pass over the collected token vector. This two-pass design
resolves the stemmer once (avoiding per-token thread-local hash lookups)
and allocates only for unique stems rather than every token.

### Ingestion pipeline

```text
Input text
    │
    ▼
┌────────────────────────────┐
│ SnowballLanguage::Tokenize │  UTF-8 validation, then:
│  (SegmentInternal loop)    │   • NormalizeInPlace on the full input text
│                            │     (per-language normalization form + casefold:
│                            │     NFC for most languages, NFKC for Arabic;
│                            │     locale-aware casefold for Turkish) —
│                            │     ensures that compatibility mappings
│                            │     (e.g. NFKC: U+FF0C fullwidth comma →
│                            │     U+002C) are visible to the delimiter
│                            │     scanner. Normalization and tokenization
│                            │     are not commutative for NFKC: tokenizing
│                            │     first would treat compatibility characters
│                            │     as word content instead of delimiters.
│                            │   • codepoint-aware segmentation loop:
│                            │     - ASCII fast path (lead < 0x80): byte-level
│                            │       bitset punctuation check, no UTF-8 decode
│                            │     - non-ASCII (lead >= 0x80): decode one
│                            │       codepoint via Scanner, check against the
│                            │       per-language non-ASCII punctuation set
│                            │       (includes Unicode whitespace: U+00A0,
│                            │       U+2000–200A, U+202F, U+3000, etc.)
│                            │     - stop-word filter per token
└────────────────────────────┘
    │
    ▼
Indexed tokens  ──┐
                  │  (ingestion-with-stemming path)
                  ▼
        TokenizeWithStemMap(): BuildStemMap() second pass
                  ▼
        stem_root → {surface_forms}   (one stemmer resolution,
                                       allocates only for unique stems)
```

### Query pipeline

Query grammar characters (`@`, `(`, `)`, `|`, `"`, `*`, `%`, `-`, `\`) are
handled directly by `FilterParser`, whose overall flow is kept as close to
upstream `main` as possible. The only language-aware hooks are:

- punctuation checks via a cached `PunctuationSet&` obtained once at
  function entry (`language.GetPunctuationSet()`), used directly in the
  byte-level inner loop — no virtual dispatch per character;
- non-ASCII word-boundary detection via `IsNonAsciiDelimiter()`, a helper
  that decodes the codepoint at the current position and checks it against
  the language's `PunctuationSet`, invoked only when a lead byte `>= 0x80`
  is encountered;
- normalization via `language.NormalizeInPlace()`;
- stop-word checks via `language.IsStopWord()`.

```text
                                Query string
                                     │
                                     ▼
          ┌──────────────────────────────────────────────────┐
          │ FilterParser::ParseTextTokens                    │
          │   byte-level scan → tokens                       │
          │   PunctuationSet bitset · IsNonAsciiDelimiter    │
          │   escape handling → \<char>                      │
          └───────────────────────┬──────────────────────────┘
                                  │  per token
                                  ▼
                      ┌────────────────────────┐
                      │  NormalizeInPlace()     │  ◄── every token
                      └───────────┬────────────┘
                                  ▼
                          ◇ token shape? ◇
                                  │
          ┌───────────────┬───────┴────────┬─────────────────────┐
          │ word          │ word*  *word   │ %word%              │ "w1 w2 …"
          │ (regular)     │ *word* (wild)  │ (fuzzy)             │ (phrase)
          ▼               ▼                ▼                     ▼
    ┌───────────┐   (normalize only) (normalize only)  normalize each word
    │IsStopWord?│───drop→✗                             (no stop-word filter)
    └─────┬─────┘         │                │                     │
       keep▼              │                │             TermPredicate per word
          │               │                │                (exact=true)
    ┌────────────────┐    │                │                     │
    │ VERBATIM?      │    │                │        ┌────────────┴──────────────┐
    │  yes → exact   │    │                │        │ ComposedPredicate         │
    │  no  → stem    │    │                │        │  (AND, slop=0, inorder)   │
    │  (NOSTEM gates │    │                │        └────────────┬─────────────┘
    │   at iterator) │    │                │                     │
    └──┬─────────┬───┘    │                │                     │
       │         │        │                │                     │
       ▼         ▼        ▼                ▼                     │
 TermPredicate  TermPredicate  Prefix/Suffix/  FuzzyPredicate    │
 (exact=true)  (exact=false)  InfixPredicate   (distance)        │
       │         │             │                │                │
       └─────────┴─────────────┴────────────────┴────────────────┘
                                  │   all are TextPredicate
                                  ▼
          ┌──────────────────────────────────────────────────┐
          │ TextPredicate::BuildTextIterator() → walk Rax    │
          │   • Term (exact=false) Stemmer.GetStemRoot() +   │
          │     stem-variant expansion via stem tree          │
          │   • Term (exact=true)  single-term lookup         │
          │   • Prefix/Suffix/     expand terms, union        │
          │     Infix/Fuzzy                                   │
          │   • ComposedAND        per-term iters + slop      │
          │     (phrase)                                       │
          └───────────────────────┬──────────────────────────┘
                                  ▼
                             TextIterator
```

**Stop words in exact phrases (pre-existing).** The exact-phrase path
applies only normalization — stop words are **not** filtered from phrase
terms. This is pre-existing behavior from the original English-only FTS
implementation, not a change introduced by multi-language support. Because
the ingestion pipeline *does* remove stop words, a phrase containing a
stop word (e.g. `"the quick brown"`) will not match any documents.
RediSearch behaves the same way — empirically, exact phrases containing
stop words return 0 results on RediSearch as well. Users who need stop
words to be searchable in phrases can supply `NOSTOPWORDS` at index
creation time.

`Fuzzy` DP is codepoint-aware so edit distance is computed correctly
for multi-byte scripts (Arabic, Russian, Turkish). This includes
buffering across `Rax` edge boundaries so a codepoint split by radix-tree
edge compression decodes correctly. `Proximity` and `OrProximity` operate
on word-level positions in an already-tokenized stream and are unaffected
by multi-byte handling.

### Per-language data ownership

Each language's punctuation set and stop-word list live in that language's
own header (`languages/french.h` defines French punctuation and French
stop words). There is no shared composition with common constants — each
language defines its complete, flat data set independently. Duplication
across languages is intentional: each language is self-contained and
independently modifiable, which is also what future non-Snowball languages
(CJK) require.

Stop-word sets are not built by a shared helper: `SnowballLanguage`
normalizes each stop word through its own `NormalizeCaseFoldFilter` at
construction (applying the language's normalization form and casefold
locale in that order), so the set matches the exact normalization applied
to tokens.

### Version gating and cluster safety

Multi-language support ships behind a `MetadataVersion` bump to `1.3`.
Non-English languages are gated by a version check so mixed-version clusters
fail closed: nodes running a module version < 1.3 will reject index metadata
carrying a non-English `LANGUAGE`. English and unspecified language values
remain accepted on all versions for backward compatibility. Additionally,
the registry rejects unrecognized language enum values at index creation
time rather than silently falling back to English, ensuring that older
module versions cannot incorrectly index data they cannot tokenize.

The version gating plugs into the existing per-index-schema versioning
machinery (see `version.h` and the RDB format RFC for the full design).
Each `Language` subclass declares a `MinRequiredVersion()` — English
returns `0.0.0` (always supported), non-English languages return
`1.3.0`. `IndexSchema::GetMinVersion()` resolves the language and
returns `max(kRelease12, language.MinRequiredVersion())` for any schema
with text indexes. This per-schema version then flows through three
rejection points:

1. **FT.CREATE**: The `ParseLanguage()` path checks
   `language->IsSupported()` (i.e., `kModuleVersion >=
   MinRequiredVersion()`) before accepting the command. A node running
   < 1.3 rejects creation of a non-English index outright.
2. **Cluster metadata broadcast**: Each metadata message carries a
   `top_level_min_version` computed as the max across all index schemas.
   A receiving node whose `kModuleVersion < top_level_min_version` drops
   the entire message (logged + counted, not silent).
3. **RDB save/load**: The RDB header embeds a `min_version` computed the
   same way. A loading node whose `kModuleVersion < rdb_version` fails
   the RDB load with an explicit error message directing the operator to
   check feature compatibility before downgrading.

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

### 1. Snowball 3.0.1 vs Snowball 2.1.0

Valkey Search uses Snowball 3.0.1, whereas RediSearch uses Snowball [2.1.0](https://redis.io/docs/latest/operate/oss_and_stack/stack-with-enterprise/release-notes/redisearch/redisearch-2.0-release-notes/).
Between these two versions the Snowball project made behavioral changes
to several stemming algorithms.

**Dutch.** Snowball 3.0.0 switched the default Dutch algorithm
from Martin Porter's Dutch Porter stemmer to the Kraaij-Pohlmann
algorithm. Kraaij-Pohlmann strips grammatical prefixes; the Porter
variant does not. The result is that Valkey Search reduces some Dutch
words to the same stem where RediSearch reduces them to different stems.

```text
FT.CREATE idx ON HASH PREFIX 1 doc: LANGUAGE dutch
    SCHEMA content TEXT
HSET doc:1 content "geschilderd"

FT.SEARCH idx "schilder" DIALECT 2
  → RediSearch: 0 results  (Porter: "geschilderd" is not reduced, no match)
  → Valkey:     1 result   (K-P: "geschilderd" stems to "schilder", matches)
```

Apache Lucene's Dutch analyzer uses Kraaij-Pohlmann-style prefix
stripping, matching Valkey Search's behavior.

**Additional stemmer changes.** Snowball 3.0.0 also introduced
targeted improvements to other languages' stemming algorithms:

| Language   | Change summary |
| ---------- | -------------- |
| English    | Extra condition on consonant undoubling (avoids conflating `add`/`ad`, `egg`/`eg`, `off`/`of`); exception handling to prevent conflation of word pairs such as `emerge`/`emergency`, `evening`/`even`, `lateral`/`later`, `universe`/`universal`/`university`, `past`/`paste`, `organ`/`organic`/`organize`; `-ogist` → `-og` (conflates `geologist` with `geology`); `-eed`/`-ing` exception handling restructured. |
| French     | Elisions (`l'`, `d'`, etc.) removed as a first step; `-aise`/`-aises` suffix removal added; conflation fixes (`mauvais`/`mauve`, `ni`/`niais`); `-oux` → `-ou`. |
| German     | Replaced with the "german2" variant (normalises umlauts: `ä`→`ae`, `ö`→`oe`, `ü`→`ue`); new suffix rules for `-erin`/`-erinnen`, `-ln`/`-lns`, `-et`; avoids overstemming `-system` words. |
| Spanish    | `-acion` handled like `-ación`, `-ucion` like `-ución` (accent-less forms now stemmed equivalently). |
| Italian    | Elision handling added; overstemming of "divano" (sofa) fixed (no longer conflated with "diva"). |
| Swedish    | `-öst`→`-ös` rule broadened to more preceding consonants; `-et`/`-ets` suffix removal added. |
| Turkish    | Proper-noun suffixes now removed (e.g. `Türkiye'dir` → `Türkiye`). |

### 2. ICU `utf8Fold` vs simple Unicode lowercasing

`NormalizeCaseFoldFilter` uses ICU's `CaseMap::utf8Fold` during ingestion
and querying. `utf8Fold` decomposes characters like `ß` to `ss` and
ligatures like `ﬁ` to `fi` so searches match across these equivalent
forms. RediSearch uses simple Unicode lowercasing, which preserves the
original characters.

```text
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

```text
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

`PunctuationSet` applies per-language default punctuation sets
sourced from Unicode CLDR v46 (defined in each `languages/*.h` header).
RediSearch's tokenization [documentation](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/escaping/) does not indicate that language-specific
default punctuation is applied.

For Spanish, for example, Valkey Search uses the inverted punctuation
characters `¡` (U+00A1) and `¿` (U+00BF) as default token separators:

```text
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

```text
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

```text
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

### 7. Unicode whitespace treated as word boundaries

`BuildPunctuationSet()` seeds Unicode White_Space codepoints (U+00A0
NO-BREAK SPACE, U+2000–200A general-punctuation spaces, U+202F NARROW
NO-BREAK SPACE, U+3000 IDEOGRAPHIC SPACE, etc.) as token delimiters via
ICU's `\\p{White_Space}` property. RediSearch only splits on ASCII
whitespace (space, tab, newline) despite documentation stating "all
whitespace separates tokens."

This matters most for French typography (NBSP before `:` `;` `!` `?` and
inside `« »`), Arabic text with non-breaking spaces, and CJK text with
ideographic spaces.

```text
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA body TEXT NOSTEM
HSET doc:space body "hello world"              # ASCII space (U+0020)
HSET doc:nbsp body "hello\xc2\xa0world"        # NO-BREAK SPACE (U+00A0)
HSET doc:enquad body "hello\xe2\x80\x80world"  # EN QUAD (U+2000)
HSET doc:ideographic body "hello\xe3\x80\x80world"  # IDEOGRAPHIC SPACE (U+3000)

FT.SEARCH idx "@body:hello" NOCONTENT
  → RediSearch: 1 result (doc:space only — NBSP, EN QUAD, IDEOGRAPHIC SPACE
                           are NOT treated as delimiters)
  → Valkey:     4 results (all Unicode whitespace splits tokens)
```

We choose to break on all Unicode whitespace because in
practice, NBSP and typographic spaces appear in web-sourced content (French
typography, copy-pasted HTML, CMS output) as incidental formatting rather
than intentional word-joining. Treating them as delimiters means users do not
need to know whether their source data contains U+0020 or U+00A0 for queries
to match, which is the more useful default for a search system.

### Future extension for multi-language indexes

An index in this release holds text in a single language, and there is no
query-time `LANGUAGE` argument because a query-time language would only
be meaningful if a single index could contain fields in multiple
languages. Two forward-compatible extensions are envisioned, as discussed in
the original [FTS RFC](https://github.com/valkey-io/valkey-rfc/pull/24/changes#diff-48ff4af40094c1df661c73561650315e01d872a043921be3a1a45eb2ffa39638R431)
and related [comments](https://github.com/valkey-io/valkey-rfc/pull/24/changes#r2210835085):

1. **`LANGUAGE_FIELD` in `FT.CREATE`.** A future release can add a
   `LANGUAGE_FIELD` clause to `FT.CREATE` naming a document field (e.g.
   a HASH field called `lang`) whose value specifies the language for
   that document. This would be a per-document setting: all text fields within
   a single document share the same language. Ingestion would then route
   each document through the corresponding `Language` implementation
   based on the value of its `LANGUAGE_FIELD`. Queries would need a
   `LANGUAGE` argument on `FT.SEARCH` to select the language used at
   query time.
2. **`FILTER` in `FT.CREATE`.** Rather than allowing multiple
   languages into one index, the `FILTER` clause on `FT.CREATE`
   restricts index membership so that each index still holds exactly one
   language, even when the underlying corpus is mixed. `FILTER` accepts
   an expression in the aggregation expression language (the same
   language used by the `FILTER` step in `FT.AGGREGATE`). The expression
   is evaluated against each document's fields at ingestion time; only
   documents for which it is truthy are added to the index. If a
   document is later mutated so that it no longer satisfies the filter,
   it is removed from the index. For example, a French index would
   include only documents where `@lang == "french"`. This approach
   avoids the semantic complexity of a mixed-language index while
   covering the same use case. See
   [Usage example 8](#8-separating-languages-with-filter-in-ftcreate)
   for a concrete illustration.


## Usage examples

### 1. Create a French index and search across morphological variants

```text
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

```text
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

```text
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

```text
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

**Scope of Arabic normalization.** NFKC unifies Arabic Presentation
Forms with their basic-letter equivalents, which is the primary encoding
inconsistency in legacy data. It does **not** strip harakat (short-vowel
diacritics such as fathah, dammah, kasrah) or unify alef/hamza variants
(أ/إ/آ→ا) and teh marbuta (ة→ه). These are distinct Unicode codepoints,
not compatibility mappings, so no standard Unicode normalization form
merges them. Full Arabic text normalization (diacritic removal, letter
folding) would require custom normalization logic outside of ICU's
standard normalizers — similar to Apache Lucene's hand-coded
`ArabicNormalizer`. This is intentionally left as a future enhancement;
the Snowball Arabic stemmer provides partial compensation by reducing
diacritized variants to a common stem during query expansion.

### 5. Override default stop words

Every non-English language ships with a default Apache Lucene stop-word
list. Use `NOSTOPWORDS` to disable it, or `STOPWORDS` to provide a
custom set. The following contrasts the default and overridden behavior
for Turkish, where `için` ("for") is in the default list:

```text
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

Without `NOSTEM`, the index stores normalized surface tokens and builds a
separate stem map that enables query expansion — a search for one Italian
conjugation matches documents containing any other conjugation that stems
to the same root. With `NOSTEM`, no stem map is built and queries match
only the exact indexed surface form:

```text
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
(or by `FILTER` on a language field — see
[example 8](#8-separating-languages-with-filter-in-ftcreate)):

```text
FT.CREATE en_idx ON HASH PREFIX 1 doc:en: LANGUAGE english SCHEMA body TEXT
FT.CREATE ru_idx ON HASH PREFIX 1 doc:ru: LANGUAGE russian SCHEMA body TEXT

HSET doc:en:1 body "The quick brown fox."
HSET doc:ru:1 body "Быстрая коричневая лиса."

FT.SEARCH en_idx "quick" DIALECT 2   # matches doc:en:1
FT.SEARCH ru_idx "быстрый" DIALECT 2 # stems to "быстр", matches doc:ru:1
```

### 8. Separating languages with `FILTER` in `FT.CREATE`

The key-prefix approach in example 7 requires the application to
partition documents into separate key namespaces. With `FILTER`,
documents can share a single prefix and carry a field that indicates
their language — each index self-partitions at ingestion time without
requiring distinct key prefixes.

```text
# All documents share the "product:" prefix and carry a "lang" field.
# Create one index per language, each restricted by FILTER.
FT.CREATE fr_products ON HASH PREFIX 1 product:
    FILTER '@lang == "french"'
    LANGUAGE french
    SCHEMA title TEXT lang TAG

FT.CREATE es_products ON HASH PREFIX 1 product:
    FILTER '@lang == "spanish"'
    LANGUAGE spanish
    SCHEMA title TEXT lang TAG

# Ingest a mixed-language corpus under a common prefix.
HSET product:1 title "chaussures de sport" lang french
HSET product:2 title "zapatos deportivos"  lang spanish
HSET product:3 title "chapeau élégant"     lang french
HSET product:4 title "sombrero elegante"   lang spanish

# The FILTER ensures fr_products contains only product:1 and product:3
# (lang == "french"), and es_products contains only product:2 and
# product:4 (lang == "spanish").

# French stemming: "chaussure" (singular) matches "chaussures" (plural)
# because the French Snowball stemmer reduces both to the same root.
FT.SEARCH fr_products "chaussure" DIALECT 2
  → 1 result: product:1

# Spanish stemming: "zapato" (singular) matches "zapatos" (plural).
FT.SEARCH es_products "zapato" DIALECT 2
  → 1 result: product:2

# Each index only contains its own language's documents, so French
# queries never see Spanish documents and vice versa — no TAG filter
# needed at query time.
FT.SEARCH fr_products "sombrero" DIALECT 2
  → 0 results  (product:4 was never indexed in fr_products)
```

Compared to the key-prefix approach (example 7), `FILTER` removes the
need for the application to encode the language in the key name. This is
especially useful when migrating an existing dataset that already uses a
uniform key prefix, or when multiple dimensions (language, region,
tenant) need to coexist — each can be a separate `FILTER` expression
without combinatorial key-prefix schemes.
