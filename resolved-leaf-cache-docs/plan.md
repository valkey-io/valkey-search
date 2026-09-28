# Generalize `ResolvedLeaves` from a scoring cache into a general resolution cache

## Context

A text/tag query walks the predicate tree **three times per document**, and two of those walks
re-derive rax results the third already cached. For an unsolved query (`IsUnsolvedQuery`,
[search.cc:201](src/query/search.cc#L201)):

1. **Per candidate** — `EvaluatePrefilteredKeys` builds a fresh `PrefilterEvaluator` *inside* the
   candidate loop ([search.cc:465-478](src/query/search.cc#L465-L478)) and walks the predicate
   against the candidate's **per-key** tree. Term leaves re-acquire `stem_tree_mutex_` and re-walk
   the global stem tree per candidate whenever the original word is absent
   ([predicate.cc:107](src/query/predicate.cc#L107)). Tag leaves allocate a `flat_hash_set` per
   candidate and do O(doc_tags × query_tags) string compares — but no rax work, which is why they are
   not a target here.
2. **Once per query** — `ResolveLeaves` walks the predicate over the *global* index, caching per
   leaf the posting lists, `dt`, and precomputed IDF ([search.cc:760](src/query/search.cc#L760)).
3. **Per candidate** — `ScoreNode` walks the predicate again, hits step 2's cache, and does one
   `GetPostingDocStats` probe per leaf.

Step 3 is the shape we want everywhere. Step 1 asks the same logical question — "does this
document match this leaf?" — with the expensive work left inside the per-document loop. The main
thread repeats the pattern: `VerifyFilter` evaluates against the per-key tree
([response_generator.cc:208-220](src/query/response_generator.cc#L208-L220)), then
`SingleDocumentScorer` resolves global leaves and walks `ScoreNode` per document.

**Change:** replace the eager `ResolveLeaves` walk with one lazily-filled `ResolvedLeafCache` keyed
on `const Predicate*`, read by **both** evaluation and scoring, so each leaf's rax traversal
happens once per query instead of once per document per phase.

**The organizing idea:** each leaf kind gets whichever of its work is *query-invariant* memoized,
and nothing more. What is invariant differs per kind, so the cache is not one uniform mechanism:

| Leaf kind | Filtering | Scoring | What the cache holds |
|---|---|---|---|
| Term (single-word) | cache | cache | the word's shared `Postings` + leaf `dt`/IDF |
| Term (stemmed) | cache **iff every enumerated variant resolved** (Rule 2) | cache | one `Postings` per resolved word + leaf `dt`/IDF |
| Expansion (prefix/suffix/fuzzy) | **per-key walk** | probe one cached term; per-key walk on miss | the single most-common matched term + its IDF |
| Tag (exact value) | cache **on the background path**; the record's tags on the main thread | same source as filtering | the value's `BagOfInternedStringPtrs` bits (global source only) + `dt`/IDF |
| Tag (prefix value) | TagInfo — the document's own tags | `dt` per candidate, as today | `dt`/IDF only, no membership data |
| Positional | cache **serves the iterator** for complete term leaves; per-key walk otherwise | as above, position-blind | same as the leaf's kind |

Each leaf owns its resolution outright — a list of `(word, shared Postings)` pairs held **by value**,
exactly the shape today's `ResolvedLeaf` already has. There is no shared word map underneath: see
*Rejected alternatives* for why cross-leaf word dedup isn't worth the indirection.

Four decisions this shape encodes, each argued below:

- **Expansion caches one term, not two hundred.** Scoring probes the most-common matched term and
  falls back to the document's own tree on a miss. See *Expansion*.
- **Tag caches the bag, like Text caches postings.** One `raxFind` per exact value per query on the
  background path, then `bag.contains(key)` per candidate — replacing a per-candidate set allocation
  and compare loop, and yielding `dt` for free. Prefix values use TagInfo; the main thread uses the
  fetched record on both filtering and scoring. See *Tag*.
- **Positional is served from the cache**, by handing back postings so the caller builds the
  `TermIterator`. See *Phase 1*.
- **A resolution is frozen per *word*, accumulated per *leaf*.** A word's postings are looked up once
  and never revised; a stemmed leaf's word set grows as later documents contribute their variants. See
  *The invariant*.

### Expansion — cache one term, not two hundred

Today an expansion leaf resolves globally into a vector of up to `max-term-expansions` (200) matched
terms, and `ScoreNode` probes that vector per candidate — up to 200 probes for a non-matching
document, ~100 for a matching one, because resolve order is rax-lexicographic, not
frequency-ordered ([1010-1020](src/query/search.cc#L1010-L1020)).

The key observation: an expansion leaf contributes **exactly one** matched term's BM25, never the sum,
and which term is unspecified by contract. So scoring only ever needs to establish that *some* term
matched. Cache the single term with the largest `GetKeyCount()` and probe that:

- **Hit** → score from it. One btree probe, no walk. Field-gated, since one `Postings` serves every
  TEXT field.
- **Miss** → the document matched some rarer term, so walk the document's own per-key tree, take its
  first match, and read `dt` from `GetKeyCount()` on the shared `Postings` — the true global `dt`,
  because per-key and global trees hand back the same object. **Not** a score of 0: the document passed
  filtering, so it matched something.

The global walk stays — you cannot know which term is most common without enumerating them — but it is
once per query and already happens today, so it costs nothing new. What goes away is the 200-entry
vector and the per-candidate probe loop.

**The bet is on expansion skew.** Hit rate is the most-common term's share of the expansion: skewed
(`pre*` where one term dominates) means most candidates skip the walk entirely; spread evenly across
200 terms means mostly misses and one wasted probe per candidate on top of the walk. Small loss when
wrong, large win when right, and strictly better than today either way.

Do **not** implement this by sorting the term vector — there is no vector any more, but the underlying
trap remains: iteration order *is* the scored representative, so ordering by key count would silently
make the lowest-IDF term the representative for every document rather than only for hits.

### Tag — cache the bag for exact values, TagInfo for prefixes, the record on the main thread

Tag gets the **same treatment as Text**: hoist the rax lookup to resolve time and cache what it
returns, so the per-candidate work becomes a pointer probe. The asymmetries fall out of the same two
rules Text obeys.

**What each path does today.** Filtering does zero rax work: background reads
`tracked_tags_by_keys_` + `ParseRecordTags` ([tag.cc:325-335](src/indexes/tag.cc#L325-L335)) via
`PrefilterEvaluator::EvaluateTags` ([vector_base.cc:152-157](src/indexes/vector_base.cc#L152-L157)),
which costs a hash find, a `StrSplit`, **a `flat_hash_set` allocation per candidate**, and then
O(doc_tags × query_values) `EqualsIgnoreCase` compares in `TagPredicate::Evaluate`
([predicate.cc:362-392](src/query/predicate.cc#L362-L392)). Scoring, by contrast, reads the index:
`ContainsKey(value, key)` per candidate per value ([search.cc:1109](src/query/search.cc#L1109)) —
a `Normalize` allocation plus a `raxFind` each time — and `GetPrefixMatchDocCount` for prefixes
([tag.cc:521-545](src/indexes/tag.cc#L521-L545)).

**Exact values, background: cache the bag.** One `raxFind(Normalize(value))` per exact query value at
resolve time yields the slot whose 8 bytes *are* a `BagOfInternedStringPtrs`. Cache those bits; every
candidate then answers with `bag.contains(key)` — single compare, linear scan of ≤8, or one hash
probe depending on representation ([string_interning.h:523-527](src/utils/string_interning.h#L523-L527)),
and **no allocation**. That replaces the per-candidate parse + set allocation + compare loop
outright. The `raxFind` is hoisted *out* of the candidate loop, not pushed into it.

Three facts make the verdict identical to today's:

- Rax keys are stored `Normalize`d ([tag.cc:95-100](src/indexes/tag.cc#L95-L100)) and `Normalize` is
  per-byte `absl::ascii_tolower`, which is the same folding `absl::EqualsIgnoreCase` applies — so
  normalized-key equality and the compare loop agree, for ASCII and for UTF-8 alike (both fold ASCII
  bytes only).
- Both sides are already whitespace-stripped: `ParseRecordTags` for doc tags, `ParseSearchTags` for
  query values.
- Query values are **already unescaped** at `TagPredicate` construction
  ([predicate.cc:354](src/query/predicate.cc#L354)), which is exactly what `Tag::Search`'s exact path
  feeds `raxFind` ([tag.cc:446-451](src/indexes/tag.cc#L446-L451)). Same input, same lookup.

Untracked and missing keys agree too: a key absent from every bag answers false, matching
`GetValue` → `nullopt` → `Evaluate(nullptr)` → false.

**Prefix values: fall back to TagInfo.** A prefix matches an unbounded slice of the rax, so there is
no single bag to cache — and `GetPrefixMatchDocCount` already documents the reason to prefer the
document's own tags ("a doc carries a handful of tags while a prefix can match an unbounded slice of
the index"). So prefixes keep today's `GetValue` + prefix-compare path verbatim.

Order the two: probe the cached bags first, and only call `GetValue` if no exact value hit **and** the
predicate carries at least one prefix value. An exact-only predicate — the common case — then never
allocates. A mixed `{red|foo*}` that misses on `red` pays the bag probes on top of today's work; a
small, bounded loss.

**Main thread: always the record, never the tag rax.** The index holds **pre-mutation** tags while the
filter reads the fetched record, so a cached bag would answer from stale data — this is Rule 1 in tag
form. So no bag is cached when the source is per-key: membership comes from the record's parsed tags
on both filtering and scoring, which also fixes the standing bug that a revalidated document is
*filtered on its new tags and scored on its old ones*.

Concretely, scoring's membership read splits by source rather than being one mechanism:

- Background (global source): probe the cached bag per value. Scoring needs **no** published tag set
  at all — and this also covers a solved query, where no filter ran to publish one.
- Main thread (per-key source): `ScoreContext` carries the per-document tag set the evaluator parsed,
  and `ContainsKey` is replaced by a compare against it — the same O(doc_tags × query_values)
  `EqualsIgnoreCase` loop the filter runs, **not** a hash probe, since the set holds raw tags.

Per-value bags give scoring something the filter's verdict cannot: `TagPredicate::Evaluate` returns on
the first matching value ([predicate.cc:381-388](src/query/predicate.cc#L381-L388)) while a union
`{red|blue}` must sum **every** matched value. Bags answer per value directly.

**`dt` comes free on the background path.** `GetTagValueDocCount` is exactly `raxFind` + `bag.size()`
([tag.cc:506-519](src/indexes/tag.cc#L506-L519)), so the bag already cached for membership yields `dt`
with no second lookup — one `raxFind` per exact value per query now serves both. On the main thread
there is no cached bag, so `dt` keeps its own `raxFind` per value per query; that read of the rax is
fine because a corpus-wide count has no document-local substitute. Prefix values keep resolving `dt`
per candidate, since which value represents the document is per-document by construction.

**Clamp `dt` to at least 1.** Once membership comes from the record, a mutation that *adds* a tag
makes a document match a value the index has not indexed yet, so `GetTagValueDocCount` returns 0 and
today's `if (dt == 0) continue` ([search.cc:945](src/query/search.cc#L945)) would silently drop the
term. `dt = max(1, count)` makes it scoreable. Safe to apply unconditionally: on the background path
`dt == 0` means no document carries the value, so membership never hits and the clamp is unreachable.

**Why caching raw bag bits is safe, and its exact limit.** The bag's storage lives *in* the rax slot
and is **not** ref-counted — `Adopt`/`Release` borrow it
([string_interning.h:669-690](src/utils/string_interning.h#L669-L690)) — and it changes
representation as it grows (single → array4 → array8 → set), `delete`ing the old block on each
transition ([497-519](src/utils/string_interning.h#L497-L519)). So cached bits survive exactly as long
as no mutation touches that value. That is guaranteed for a background query and only there: `Search()`
holds one reader lock across every candidate, the lock is never revoked mid-hold (`TimeSlicedMRMWMutex`
exposes only `Lock`/`Unlock`; the quota governs *waiting*, not preemption), and the tag index is not
mutated in read mode ([tag.cc:317, 327-328, 340](src/indexes/tag.cc#L317)). Precedent: `Tag::Search`'s
`EntriesFetcher` already caches raw slot pointers and adopts them lazily during iteration
([tag.cc:425-437](src/indexes/tag.cc#L425-L437)). Every probe must `Adopt` … `Release` and never let
the local bag destruct, or it frees storage the rax owns.

**The consequence for the cache's shape:** a tag leaf's entry is per exact value `{bag bits, dt, idf}`
with the bag bits populated **only from a global source**, plus the list of prefix values carrying
`dt`/`idf` alone. That is Rule 1 again — a global source gives you a reusable answer, a per-key source
gives you scoring inputs only.

## Verified facts

- **A `Postings` object holds every key, not one document's.** `key_to_positions_` is a btree over
  all keys ([posting.h:175](src/indexes/text/posting.h#L175)); `GetKeyCount()` is the global total
  ([109](src/indexes/text/posting.h#L109)); `GetPostingDocStats(key, mask)` probes any key
  ([122-123](src/indexes/text/posting.h#L122-L123)).
- **Per-key and global trees hand back the *same* `Postings`.** `CommitKeyData` plants one
  `updated_target` in both ([text_index.cc:274-297](src/indexes/text/text_index.cc#L274-L297)); both
  `Rax::FindPostingsTarget` ([rax_wrapper.h:97](src/indexes/text/rax_wrapper.h#L97)) and
  `WordIterator::GetPostingsTarget` ([173](src/indexes/text/rax_wrapper.h#L173)) return the shared
  `InvasivePtr`.
- **⇒ The linchpin: a leaf resolved via *any* tree answers for *every* candidate.** A per-key tree
  is merely the route by which a global object is discovered. This is what makes one query-scoped
  cache correct on both paths.
- **Cached `Postings` cannot dangle** — `InvasivePtr` is ref-counted.
- **A per-key `TextIndex` is immutable once published** (built as a local,
  [text_index.cc:237](src/indexes/text/text_index.cc#L237), emplaced only after
  [325](src/indexes/text/text_index.cc#L325)) **but its lifetime is not guaranteed across an
  unlocked read** — `DeleteKeyData` destroys it ([339](src/indexes/text/text_index.cc#L339)) and
  `GetPerKeyTextIndex` drops its mutex before the caller dereferences
  ([460-475](src/indexes/text/text_index.cc#L460-L475)). Pre-existing.
- **Stem-variant lists are `string_view`s into stem-tree memory**
  ([text_index.cc:452](src/indexes/text/text_index.cc#L452)) and `DeleteKeyData` can `Clear()` the
  `StemParents` vector ([389-407](src/indexes/text/text_index.cc#L389-L407)). A cache outliving a
  lock release must own copies.
- **`stem_tree_mutex_` is a plain `absl::Mutex`, unrelated to the time-sliced mutex.** So
  `GetAllStemVariants` keeps `lock_needed=true` ([search.cc:874](src/query/search.cc#L874)) even
  where `GetPerKeyTextIndex(lock=false)` is safe. Both read as "already locked" and only one is.
- **`Rax::WordIterator` self-bounds to its prefix** — `raxSeekSubTree` sets `RAX_ITER_SUB_TREE`
  ([rax.c:1623-1627](src/indexes/text/rax/rax.c#L1623-L1627)), so `SuffixPredicate::Evaluate`'s
  `starts_with` guard ([predicate.cc:226](src/query/predicate.cc#L226)) is defensive, not
  load-bearing. Noted so nobody "fixes" it.

## Design

### The invariant

**A word is resolved at most once per query and never revised.** A leaf's resolution is a set of
`(word → shared Postings)` plus `dt` and precomputed IDF. Because a `Postings` contains every key,
a resolution obtained from any tree is valid for every candidate — so the cache is **query-scoped
on both paths, with no `Clear()`**. IDF is stable within a reply for every leaf except a stemmed one
still accumulating variants, per the next paragraph.

**Frozen per *word*, accumulated per *leaf*.** Once a word's postings are cached they are never
looked up again — that is the traversal saving. But a stemmed leaf sourced from per-key trees only
sees the variants *that document* carries, so the leaf's word set **grows** as later documents
contribute theirs, and `word_set_complete`, `dt` and IDF are recomputed on each add. Without
accumulation a per-key-sourced stemmed leaf would never reach completeness and so would never serve
membership from the cache — which is the whole win on the main-thread path.

The cost is that documents scored earlier in a reply see a higher IDF than later ones, since `dt`
converges upward toward the true global value. That is smaller than the mixed-scale effect the plan
already accepts below (mutated documents use per-key IDF while un-mutated ones keep shard-side global
IDF), and revalidation usually touches few documents. Freezing instead would leave every document on
the same wrong (too high) IDF *and* forfeit cache-answered membership, so accumulation is the better
trade.

**One cache, one thread, one lock scope.** The cache is unsynchronized, which is sound because a whole
`Search()` — candidate loop included — runs as a single scheduled task
([search.cc:1734-1752](src/query/search.cc#L1734-L1752)). The rule that follows: the background cache
must **never** be handed to `ResolveContent`/`VerifyFilter` to "save work" on the main thread. Its tag
bag bits are borrowed from rax slots and die with the background reader lock, so the main thread builds
its own cache. Anything that parallelizes candidate evaluation breaks this silently.

**The cache is not a snapshot.** Entries hold `InvasivePtr<Postings>` so they never dangle, and
every *probe* happens under a reader lock, so no probe races an ingestion mutation. What is *not*
guaranteed is that an entry reflects the index at probe time — a key deleted between document 1 and
document 2 is gone from the shared `Postings` the cache already holds. Freshness is per-probe, not
per-cache, which is right for revalidation. Anything added later that assumes a stable snapshot
across documents breaks this.

**What laziness is for.** Not skipping: today's `ResolveLeaves` recurses only through AND/OR
([765-772](src/query/search.cc#L765-L772)) with `default: break`
([937](src/query/search.cc#L937)), so it stops at a negation exactly where `ScoreNode` stops, and
`ScoreNode`'s OR sums every child rather than short-circuiting
([980-988](src/query/search.cc#L980-L988)) — so essentially every leaf resolved today is read by
some candidate. Laziness buys two other things:

1. **Sharing.** Whichever phase reaches a leaf first pays its walk; the other gets a hash hit. This
   is the whole win. Direction flips per shape: scoring visits *more* branches than filtering (it
   visits AND text children the prefilter skips,
   [predicate.cc:453-458](src/query/predicate.cc#L453-L458)), so for `@tag:{x} @text:word` scoring
   is the text leaf's only visitor.
2. **Preserving "scoring off costs nothing."** `ScoreTextQuery` and `SingleDocumentScorer` both
   return before `ResolveLeaves` today, so the kill switch means no resolve at all. An eager
   filter-driven resolve would newly pay the global expansion walk for entries nobody reads.
   Laziness plus the kind check in Phase 2 keeps that at zero.

### Two rules that depend on the resolve source

Everything that differs between the two paths follows from these; there is one resolve
implementation and one membership function.

Both filter paths use per-key trees **today**. Sourcing the cache globally on the background path is
a choice that buys cacheable negatives (Rule 1) and complete stemmed word sets (Rule 2); where those
don't pay, the background filter keeps walking the per-key tree exactly as it does now.

**Rule 1 — a negative is cacheable only from a global source.** Empty-from-global means "absent
from the corpus" (cacheable). Empty-from-per-key means "absent from *this document*" — caching it
would poison every later candidate that does contain the word. On a per-key source a miss answers
"no match for this key" and caches **nothing**.

**Rule 2 — cache-answered membership requires a *complete* word set.** An incomplete set can
false-negative a later candidate, so it may supply scoring inputs but must not answer membership. A
single-word leaf is complete as soon as it resolves, from either source.

For a **stemmed** leaf, completeness is *measured, not inferred from the source*. The variant list
comes from the **global stem tree on both paths** (`GetAllStemVariants`,
[predicate.cc:118-120](src/query/predicate.cc#L118-L120)) — what differs is only whether each
enumerated word yields postings: the global prefix tree yields all of them, a per-key tree only the
words *this document* carries. So set

```
word_set_complete = (resolved_words == enumerated_words) && !stem_variants_truncated
```

counting the original, the stemmed form and every variant. From a global source this is
unconditionally true at first resolve. From a per-key source it is **re-evaluated on every add**: the
first document may cover every corpus variant (the common single-variant case) and answer membership
immediately, or it may take several documents to fill the set in. Either way the answer is *sound* for
every later candidate, because each resolved word's `Postings` is the shared global object. The
truncation term is necessary because stem-variant enumeration applies its own
`max-term-expansions` cap ([text_index.cc:446](src/indexes/text/text_index.cc#L446)); a truncated
list is incomplete no matter which source filled it.

|  | Background (`Search()` holds one reader lock across all candidates, [search.cc:1676](src/query/search.cc#L1676)) | Main thread (`VerifyFilter`, one lock per mutated document) |
|---|---|---|
| Source | global prefix/suffix tree | document's per-key prefix/suffix tree |
| Stem tree | global | global |
| Negatives cached | yes | no (Rule 1) |
| Single-word leaf (`IsExact() \|\| stem_field_mask == 0`) | membership + scoring from cache | membership + scoring from cache, shared across all documents |
| Multi-word (stemmed) leaf | membership + scoring from cache (set complete at first resolve) | scoring from cache; membership from cache once `word_set_complete` turns true, else **from the key's own tree** (Rule 2). The set accumulates across documents, so this flips on partway through a reply. |
| Expansion leaf | membership from the key's own tree; scoring probes the cached most-common term, falling back to that tree on a miss | same |
| Tag leaf, exact value | bag bits + `dt`/IDF cached; membership from `bag.contains(key)` on both filtering and scoring | no bag cached (Rule 1); membership from the fetched record's tags on both; `dt`/IDF still cached |
| Tag leaf, prefix value | `dt`/IDF cached; membership from the document's own tags (TagInfo) | same, from the fetched record |
| Positional leaf | cache hands back postings, caller builds the `TermIterator` | same |

**A single-word leaf's `dt` is the true global `dt` on both paths**, because that one word's
`Postings` is the shared object. "Single-word" is a property of the *leaf*
(`IsExact() || stem_field_mask == 0`, per the guard at [search.cc:868](src/query/search.cc#L868)),
not of which word a document matched — do not read the guarantee more broadly.

So the accepted IDF drift is **multi-word (stemmed) leaves on the main thread only**: early in a reply
the accumulated word set covers only the variants seen so far, so `dt` runs low and IDF high, and both
converge as documents contribute. Deliberate; the summation is already flagged wrong (`TODO` at
[search.cc:851](src/query/search.cc#L851)).

**The enumerated variant *list* never grows** — it comes from the global stem tree on both paths, so a
stem root absent from the stem map when the query starts will never appear for any candidate. What
accumulates is only which of those enumerated words have yielded postings, which is why
`word_set_complete` has a fixed denominator to compare against.

**One consequence to measure, not assume.** Recompute is per-document, so a single reply can mix
scales — mutated documents get drifted per-key IDF while un-mutated ones keep shard-side global
IDF, and `ProcessNeighborsForReply` re-ranks across both. Today's `SingleDocumentScorer` resolves
globally, so this mixing is **new**. See Verification 4.

### Phase 1 — extract the shared module

New `src/query/resolved_leaves.{h,cc}` carrying `ResolvedLeaf`, `ScoringFieldMask`, and the resolve
bodies out of `search.cc:658-940`. The recursive walk
([765-772](src/query/search.cc#L765-L772)) is **deleted**; its `kText`
([773-892](src/query/search.cc#L773-L892)) and `kTag`
([893-936](src/query/search.cc#L893-L936)) bodies survive nearly verbatim as the private miss
handler. `resolved.contains(tag_pred)` ([898](src/query/search.cc#L898)) goes away — the hash
lookup subsumes it. The global expansion walk stays, but `AddExpansionTerm` collapses to "keep the
term with the largest `GetKeyCount()`" instead of appending to a vector (see *Expansion*).

```
struct LeafResolveSource {
  const indexes::text::TextIndex* trees;  // global, or the document's
  bool is_global;                         // Rules 1 and 2
  bool own_stem_variants;                 // copy variant strings (main thread)
};

class ResolvedLeafCache {
 public:
  ResolvedLeafCache(uint32_t total_docs, const indexes::scoring::Scorer*,
                    LeafResolveSource);
  // Resolves on miss. Returns nullptr when nothing was cached (a per-key
  // negative under Rule 1). Called from BOTH evaluation and scoring.
  const ResolvedLeaf* GetOrResolve(const Predicate*);
  void SetSourceTrees(const indexes::text::TextIndex*);  // main thread, per document

 private:
  // This is the cache's ONLY map. Each leaf owns its own (word,
  // InvasivePtr<Postings>) pairs by value, exactly as today's ResolvedLeaf does
  // (search.cc:669-671) — there is no shared word → Postings map, so no entry
  // ever points into another growable container. See Rejected alternatives.
  //
  // node_hash_map, NOT flat: GetOrResolve hands out a ResolvedLeaf* and the
  // positional path holds it across sibling GetOrResolve calls, which a flat map
  // would invalidate on rehash. Also cheaper here — ResolvedLeaf is a
  // several-hundred-byte payload, so flat rehashing would move all of it.
  absl::node_hash_map<const Predicate*, ResolvedLeaf> leaves_;
};

// A cache answer. Membership alone is not enough: under require_positions the
// caller must build a TermIterator, so hand back the postings that would have
// been walked for it.
struct LeafMatch {
  enum class Verdict { kMatch, kNoMatch, kFallback } verdict;
  // Set on kMatch: the words that matched, with the mask each was gated by.
  absl::InlinedVector<MatchedWord, kStemVariantsInlineCapacity> matched;
};
LeafMatch MatchResolvedTextLeaf(const ResolvedLeaf&, const InternedStringPtr& key,
                               bool require_positions);
```

Additions to `ResolvedLeaf`:

- `enum class LeafKind { kTerm, kPrefix, kSuffix, kFuzzy, kTag, kUnscoreable }` — the expansion
  kinds map to distinct pausepoint names, and both rules key off kind.
- `bool word_set_complete` — Rule 2, **measured at resolve time**: `resolved_words ==
  enumerated_words && !stem_variants_truncated`. Always true for a single-word term leaf and for any
  leaf from a global source; true from a per-key source when the document carried every corpus
  variant.
- Tag: `tag_index` + two lists. Exact values carry `{bag storage bits, dt, idf}`, the bits set **only**
  when `is_global` (Rule 1); prefix values carry `{dt, idf}` alone. The bits are a bare `uintptr_t`
  borrowed from the rax slot, so every probe must `Adopt` … `Release` and the field must be documented
  as valid only for the lifetime of the background reader lock.
- Expansion: **no `expansion_terms` vector** — one `{word, postings, idf}` for the most-common matched
  term, plus the leaf's field mask. A word found by the fallback walk computes its IDF on the spot:
  the walk already holds the shared `Postings`, so it is `PrecomputeIDF(total_docs, GetKeyCount())` —
  one `log`, not worth memoizing.
- Every cached word is stored **as a string alongside its `Postings`**. Needed for `word_set_complete`
  (which enumerated words have resolved) and, later, to name the `RaxTargetMutexPool` bucket for a
  lock-scoped probe. On the main thread these are the `owned_stem_variants` copies below.
- `std::vector<std::string> owned_stem_variants`, populated when `own_stem_variants` is set.
  Deliberately *not* an `InlinedVector`: `kStemVariantsInlineCapacity` is 20
  ([text_index.h:46](src/indexes/text/text_index.h#L46)) and 20 inline `std::string`s would add
  ~640 bytes to every leaf, including tag leaves. `ResolvedLeaf` is a by-value hash-map payload.

No raw-mask fields are needed. `ScoringFieldMask` ([search.cc:725](src/query/search.cc#L725))
collapses an all-fields mask to `~0ULL`, which would be wrong to feed `TermIterator` (its
`QueryFieldMask()` is intersected across AND children,
[predicate.cc:468](src/query/predicate.cc#L468)) — but membership never builds one and
`GetPostingDocStats(key, ~0ULL)` short-circuits correctly.

`MatchResolvedTextLeaf` does one `GetPostingDocStats(key, mask)` per posting — original word gated
by `field_mask`, variants by `stem_field_mask`, the same asymmetry `ScoreNode` applies at
[1036-1045](src/query/search.cc#L1036-L1045). It returns `kFallback` — caller must walk the key's
own tree — for an expansion kind, `!word_set_complete`, or infix.

**`require_positions` is served from the cache, not fallen back on.** A leaf does **no positional
work at all**: it collects `KeyIterator`s and returns a `TermIterator` as `filter_iterator`
([predicate.cc:82-86, 138-143](src/query/predicate.cc#L82-L86)), and the parent `ComposedPredicate`
builds the `ProximityIterator` that does the slop/inorder checks
([477-497](src/query/predicate.cc#L477-L497)). So the leaf's contract there is "produce an iterator,"
not "answer yes/no" — and a cached entry holds the *same* shared `Postings` the walk would have found,
so the iterator can be built from it: `postings->GetKeyIterator()` + `SkipForwardKey(key)` +
`ContainsFields`, exactly what `TryAddWordKeyIteratorForPrefilter` does
([predicate.cc:74-88](src/query/predicate.cc#L74-L88)), minus the rax lookup. For a term leaf that is
strictly cheaper than today.

This is why `LeafMatch` returns the matched words and their masks rather than a bare verdict. A
bool-only answer would be **actively wrong** under `require_positions`: it deprives the AND of the
`filter_iterator` counted at [477](src/query/predicate.cc#L477), `childrenWithPositions` drops below
2, control falls to [499-500](src/query/predicate.cc#L499-L500), and the AND returns true **with no
proximity check** — a phrase query silently degrades to a conjunction. Positional OR has the same hole
at [516-518](src/query/predicate.cc#L516-L518). Comment this at the return type, since the failure is
a silent false-positive rather than a crash.

Two kinds still fall back under `require_positions`, for the same reason they fall back generally:
expansion leaves (no cached term list at all now) and `!word_set_complete` stemmed leaves — a missing
key iterator would silently narrow the proximity check rather than fail it.

The four `TextPredicate::Evaluate` bodies therefore **stay** — they are the `kFallback` path
anyway.

**`ScoreNode` reads the cache and nothing else.** On a hit it touches zero trees; on a miss
`GetOrResolve` does one walk. It needs no fallback: `nullopt` is "this leaf didn't match" for AND/OR
composition ([971](src/query/search.cc#L971), [984](src/query/search.cc#L984)) and never drops the
document, since `ScoreTextQuery` does `score.value_or(0.0f)`
([1180](src/query/search.cc#L1180)). A wrong score costs one document's relevance; a wrong *match*
would change the result set, which is why only the evaluators fall back.

**Cache-miss semantics, preserved exactly.** Today the distinction is accidental —
`ResolveLeaves` inserts a term leaf with empty postings but *skips* insertion for a fruitless
expansion leaf ([822-828](src/query/search.cc#L822-L828)) — and `ScoreNode` reads two answers out
of it:

| Situation | Today | `GetOrResolve` must |
|---|---|---|
| Term leaf, absent from corpus (global source) | inserted → leaf `nullopt` at [1022](src/query/search.cc#L1022) | cache `kTerm`, empty postings |
| Term leaf, absent from this document (per-key source) | n/a | cache nothing, answer `kNoMatch` (Rule 1) |
| Expansion leaf, no corpus matches (global source) | not inserted → miss → `0.0f` at [1000](src/query/search.cc#L1000) | cache a `kUnscoreable` negative |
| Expansion leaf, matches exist but not the cached term | `nullopt` at [1019](src/query/search.cc#L1019) → `0.0f` | **fall back to the per-key walk**, score its first match — `0.0f` only if that walk finds nothing |
| Infix / non-`TextPredicate` `kText` / `default` | miss → `0.0f` | cache `kUnscoreable` |

The fourth row is the one real scoring **fix**: today a document matching only a term the probe loop
gave up on scores 0. Reaching the fallback walk means it scores its actual term.

`ScoreContext::resolved` changes from `const ResolvedLeaves&` to a mutable `ResolvedLeafCache*`.

**One cancellation pausepoint has to move.** `integration/test_cancel.py:324-390` sets
`search_term_predicate`, `search_prefix_predicate`, `search_suffix_expansion`,
`search_fuzzy_search` on non-positional queries and asserts the pausepoint is hit and the query
times out. The three expansion names stay put — expansion membership *and* scoring now go through
`TextPredicate::Evaluate`'s per-key walk, so those bodies still fire. `search_term_predicate` **must gain a copy**
in the term resolve path, because a term leaf is answered from the cache and
`TermPredicate::Evaluate` may never run; keep the existing one too. Trigger frequency drops from
O(candidates) to 1, which is harmless since `PausePoint` blocks
([vmsdk/src/debug.h:38-50](vmsdk/src/debug.h#L38-L50)). The risk is **zero** hits, not fewer: the
test queries all carry a negation and the prefilter's AND-text skip excludes `kContainsNegate`
([predicate.cc:453-456](src/query/predicate.cc#L453-L456)), so evaluation reaches the leaf.
Re-confirm if those queries change.

CMake: mirror the `predicate` / `predicate_header` split in
[src/query/CMakeLists.txt](src/query/CMakeLists.txt). The header needs only `predicate_header` plus
`posting.h` / `invasive_ptr.h` and forward-declares `indexes::Tag`; the `.cc` pulls `tag.h`,
`text_index.h`, `fuzzy.h`, `scoring/scorer.h`. Verified acyclic — `vector_base` links only
index-layer libs, nothing under `src/query/`.

### Phase 2 — background: one query-scoped global cache

`PrefilterEvaluator` ([vector_base.h:446](src/indexes/vector_base.h#L446)) takes a
`query::ResolvedLeafCache*` instead of a pre-fetched `TextIndex*`:

- `EvaluateText`: `GetOrResolve` → `MatchResolvedTextLeaf`; on `kFallback`,
  `predicate.Evaluate(*per_key_index, key, require_positions)` with the per-key index fetched
  **lazily** on first need — a win by itself, since today `GetPerKeyTextIndex` runs for every
  candidate even when no text leaf reaches it. Check kind **before** calling `GetOrResolve` so an
  expansion leaf skips it entirely; expansion resolves nothing up front now.
- `EvaluateTags`: now cache-first. `GetOrResolve` gives the leaf's exact-value bags; probe each with
  `bag.contains(key)` and return true on the first hit. Only if every exact value misses **and** the
  predicate carries a prefix value does it fall back to today's `GetValue` + `TagPredicate::Evaluate`
  path. Verdict is unchanged (see *Tag* for why the three equality facts make that exact); what goes
  away is the per-candidate `flat_hash_set` allocation for exact-only predicates. No tag set needs
  publishing here — scoring probes the same bags.
- `EvaluateNumeric`: unchanged — `Numeric::GetValue` is a map lookup, no rax.

`PredicateEvaluator` ([response_generator.cc:84-147](src/query/response_generator.cc#L84-L147)) has
the identical shape and takes the same change for **text**. Its tag/numeric overrides stay, reading the
fetched `RecordsMap`: no bag is cached from a per-key source, so tag membership there is the record's
tags for filtering *and* for scoring, and the tag set it publishes is the **post-mutation** one — which
is the point of the fix.

Build sites, all inside the reader lock `Search()` already holds: `DoSearchNonVector` (construct
before the `requires_prefilter_evaluation` branch, pass to `EvaluatePrefilteredKeys` then
`ScoreTextQuery`) and `DoSearchVector` (construct before `CalcBestMatchingPrefilteredKeys` /
`PerformVectorSearch`, pass to `InlineVectorFilter`
([search.cc:108](src/query/search.cc#L108)) then `ApplyHybridTextScore`). Hoist evaluator
construction out of the per-candidate loops while there. `ScoreTextQuery` / `ApplyHybridTextScore`
gain a trailing `ResolvedLeafCache* = nullptr` and build their own when null, so existing callers
and tests keep working.

Construct the cache whenever prefilter evaluation runs and the predicate has a text or tag leaf —
**not** gated on `options::IsScoringDisabled()`; with scoring off the IDF fields go unused but the
cache still pays for itself on filtering. `ScoreTextQuery` keeps its own early return.

The expansion **fallback** needs the per-key index at scoring time, so the lazy fetch above must also
be reachable from `ScoreNode` — including on shapes where the prefilter skipped the text child
(`@tag:{x} @text:pre*`). Only misses pay it; hits touch no tree at all.

Global expansion keeps **today's** `max-term-expansions` cap, so which terms are candidates for
"most common" is unchanged.

### Phase 3 — main thread: same cache, per-key source

`VerifyFilter` ([response_generator.cc:165](src/query/response_generator.cc#L165)) owns **one
`ResolvedLeafCache` for the whole reply**, and for each document needing revalidation
(`db_seq != n.sequence_number`) takes one `ReaderMutexLock` and inside it:

1. `SetSourceTrees(GetPerKeyTextIndex(key, /*lock=*/false))` — `lock=false` is safe because the
   reader lock excludes mutators, matching the prefilter. `is_global=false`,
   `own_stem_variants=true`.
2. Runs `PredicateEvaluator`. Text goes through `GetOrResolve` + `MatchResolvedTextLeaf`; tags and
   numerics keep reading the fetched `RecordsMap` — revalidation exists *because* the record
   changed, so the index is not authoritative there. This is the one place the paths genuinely
   diverge. The tag set parsed here is published on the `ScoreContext` for step 3.
3. Scores from the same cache; `GetOrResolve` fills whatever evaluation short-circuited past.

Single-word and tag leaves resolved for document 1 are reused for every later document. Stemmed
leaves **accumulate** across documents — each contributes the variants it carries, and once the set is
complete the leaf starts answering membership from cache for the rest of the reply. Expansion leaves
walk per document by design, for both membership and scoring. The per-document `GetAllStemVariants`
walk drops out either way: the variant list is query-invariant, so it is enumerated on the first
document that needs it.

`SingleDocumentScorer` **shrinks**: `State` drops `resolved`
([search.cc:1220-1231](src/query/search.cc#L1220-L1231)), the ctor's `ResolveLeaves` call
([1266](src/query/search.cc#L1266)) goes away, and it becomes a per-reply holder of corpus stats
plus the scorer, with `Score` taking the cache. Its ctor and `Score` each take their own reader lock
today, documented "callers must NOT already hold it"
([search.h:396-404](src/query/search.h#L396-L404)); invert both to caller-locked. The time-sliced
mutex is non-reentrant, so the lock must live in exactly one place and `VerifyFilter` is it —
content fetch happens before it and no Valkey API call occurs inside. Net: one acquire per mutated
document, replacing the ctor acquire, the per-`Score` acquire, and the currently-unlocked per-key
tree read.

**Audit every `Score()` and ctor call site before flipping the contract.** `VerifyFilter` holds
**no** time-sliced lock today — `GetPerKeyTextIndex(key, true)` takes
`per_key_text_indexes_mutex_`, a plain `std::mutex`, and drops it before returning
([text_index.cc:460-475](src/indexes/text/text_index.cc#L460-L475)). So nothing is stacked today
and nothing could be; the inversion *adds* the lock that unlocked read should always have had.

### Follow-on goal — retiring the main-thread time-sliced lock

**The goal, and why it matters more than the lock count.** Get the main-thread revalidation path off
`TimeSlicedMRMWMutex` entirely. The problem is not contention, it is **phase shift**: the mutex grants
read and write access in alternating time-sliced phases, so a main-thread reader arriving during a write
phase waits out the writer's quota and grace period before it can enter
([time_sliced_mrmw_mutex.h:85-102](vmsdk/src/time_sliced_mrmw_mutex.h#L85-L102)) — a stall measured in
whole quota intervals, on the thread that is serving a client reply. Every fine-grained alternative
below is held for the duration of one btree probe or one map lookup, so the worst case stops being
"wait for the ingestion phase to end" and becomes "wait for one `InsertKey`". That is the whole point;
reducing the acquire *count* is incidental.

Phase 3 deliberately takes it *once per mutated document* rather than twice-plus-unlocked, which is the
setup for removing it.

**What this change does buy.** The global rax traversal disappears from the main thread — the ctor's
`ResolveLeaves` walk over the global prefix/suffix trees (`FindPostingsTarget` / `GetWordIterator`) is
gone, and term resolution reads the document's own per-key tree. That removes the largest and
longest-held reason the lock was needed.

**The mechanism: take the fine-grained locks the writers already take.** The time-sliced mutex is not
the only thing protecting this state — the write paths guard each piece with a narrower lock, and a
reader can take the same one. Two suffice for the text state:

1. **The per-word bucket lock, for postings.** `RaxTargetMutexPool` hashes a word to one of N
   `absl::Mutex`es ([rax_target_mutex_pool.h:32-53](src/indexes/text/rax_target_mutex_pool.h#L32-L53)),
   and **every** mutation of a `Postings` happens inside it: `CommitKeyData`'s `AddKeyToPostings`
   ([text_index.cc:276-294](src/indexes/text/text_index.cc#L276-L294)) and `DeleteKeyData`'s
   `RemoveKeyFromPostings` ([358-379](src/indexes/text/text_index.cc#L358-L379)). So a reader that takes
   `rax_target_mutex_pool_.Get(word)` around `GetPostingDocStats` / `GetKeyCount` is fully serialized
   against `InsertKey` / `RemoveKey` — which closes both the stale-`dt` read *and* the
   `FlatPositionMap` use-after-free (`RemoveKey` destroys the map inside that same lock). Each cache
   entry stores the word next to its `Postings`, so the bucket to take is already in hand — nothing has
   to be looked up to name it.
2. **`per_key_text_indexes_mutex_`, for the per-key tree map.** `DeleteKeyData` extracts the node under
   it ([text_index.cc:336-346](src/indexes/text/text_index.cc#L336-L346)), so holding it blocks the
   extract and the returned `TextIndex*` cannot be pulled out from under a walk.

Two details this needs, neither hard:

- **The per-key lock must be held across the *use*, not just the lookup.** `GetPerKeyTextIndex` drops
  its guard before returning ([text_index.cc:460-475](src/indexes/text/text_index.cc#L460-L475)), which
  is fine when the caller is inside a time-sliced read phase and useless once that goes away. It needs
  a shape that keeps the guard alive for the walk — return a guard alongside the pointer, or take a
  callback. Note `DeleteKeyData` destroys the extracted `TextIndex` *outside* the lock (the node dies at
  end of scope), so "held across the walk" is exactly the requirement, not "held during find".
- **Lock ordering is safe but newly nested.** A reader would hold `per_key_text_indexes_mutex_` and take
  word buckets inside it. Writers never hold both at once — `CommitKeyData` releases its word lock
  before emplacing into the map, and `DeleteKeyData` releases the map lock before taking word locks — so
  the reader's nesting introduces no cycle. Worth asserting with a lock-order check rather than leaving
  implicit, since the invariant lives in two functions that don't reference each other.

Also `stem_tree_mutex_` for the variant enumeration, which `GetAllStemVariants` already takes with
`lock_needed=true` — no change, and the plan's `own_stem_variants` copy already covers the string
lifetime.

**The one leftover is outside the text index:** `GetDocumentScore` reads `index_key_info_`
([index_schema.h:201-208](src/index_schema.h#L201-L208)) per document, and that map has no lock but the
time-sliced one. `N` needs no answer — it is captured once at cache construction, not per document — so
this is a single per-document hash read to re-home. Cheapest option is to fold the document score into
the same fetch that already reads the record, but it should be decided rather than assumed.

**Sequencing:** land Phase 3 with the lock cheap and singly-held, then remove it by swapping that one
acquire for the two fine-grained ones above. Keeping them separate keeps the scoring change reviewable
on its own and means a regression in either is attributable.

**The tag bags add nothing to this work.** They are cached only from a global source, i.e. only on the
background path, which keeps its single long-held reader lock either way. The main thread never holds
bag bits, so tag membership there is already lock-free in the relevant sense — it reads the fetched
record.

## Behavior changes to expect

1. **Expansion scoring picks a different representative term.** Today it is the first match in global
   rax order; now it is the most-common term when the document has it, otherwise the document's own
   first match. Both are unspecified by contract, but scores will move, so golden values in
   `ScoreTextQueryTestBase` need re-baselining.
2. **A document matching only a term late in the expansion now scores instead of scoring 0.** Today the
   probe loop can give up; the fallback walk always finds the document's actual term. A fix, but it
   changes result scores.
3. **Expansion scoring reads the per-key tree on a probe miss**, including on shapes where the
   prefilter skipped the text child. Hits touch no tree. This is the one place the change *adds* a tree
   access, so measure it — it replaces up to 200 probes, so it should still win.
4. **Background tag filtering resolves exact values through the tag rax** instead of parsing the
   document's tag string per candidate. The verdict is identical (normalized rax keys fold ASCII the
   same way `EqualsIgnoreCase` does; query values are already unescaped; both sides are
   whitespace-stripped) and untracked/missing keys still answer false — so nothing user-visible, but it
   is a new read path through `raxFind` at resolve time and deserves the equivalence test in
   Verification 3. A mixed `{red|foo*}` predicate whose exact values all miss pays the bag probes on top
   of today's work.
5. **Tag scoring on the main thread uses the fetched record's tags** instead of the stale indexed ones.
   This is the required correctness fix: today a revalidated document is filtered on its new tags and
   scored on its old ones. Scores change for revalidated documents whose tags were mutated, and a
   newly-added tag now scores (via the `max(1, dt)` clamp) where it previously contributed 0.
   Background tag scoring is unchanged — filter and index agree there under one lock.
6. **Stemmed-leaf IDF drifts high on main-thread recompute and converges** as documents accumulate
   variants. Single-word leaves are unaffected.
7. **`SingleDocumentScorer`'s lock contract inverts.** Its one direct test caller,
   `RecomputePathMatchesExtraStepAtNonZero`
   ([search_test.cc:1878-1900](testing/search_test.cc#L1878-L1900)), constructs it outside a lock
   *because* it takes its own; it needs a `ReaderMutexLock` wrapper.
8. **Pre-existing asymmetry, still unresolved:** on the main thread text membership comes from the
   index while tag/numeric come from the fetched record (`Improvements.md:145-148`). Item 5 fixes the
   exact-value half of the tag case; **prefix values (`{foo*}`) are still open** — see Open questions.

## Files

| File | Change |
|---|---|
| `src/query/resolved_leaves.{h,cc}` | **New.** `ResolvedLeaf`, `ResolvedLeafCache`, `MatchResolvedTextLeaf`; the moved resolve bodies. |
| `src/query/search.cc` | Delete the moved block (658-940) and the recursive walk; build + thread the cache through `DoSearchNonVector`, `DoSearchVector`, `EvaluatePrefilteredKeys`, `InlineVectorFilter`; `ScoreNode` reads via `GetOrResolve`. |
| `src/query/search.h` | Optional cache param on `ScoreTextQuery`; `ScoreContext` gains the per-document tag set; `SingleDocumentScorer` loses its resolve step and both locks. Its class comment's lock contract and "ResolveLeaves over the global posting lists" description both become wrong. |
| `src/indexes/vector_base.{h,cc}` | `PrefilterEvaluator` takes the cache; cache-first `EvaluateText`; lazy per-key index lookup; cache-first `EvaluateTags` (bag probe per exact value, `GetValue` fallback only for prefixes). |
| `src/query/response_generator.cc` | `VerifyFilter` owns a reply-scoped cache, one reader lock per document, `SetSourceTrees` per document; `PredicateEvaluator` consults it for text and publishes the record's tag set. |
| `src/indexes/tag.{h,cc}` | `dt` clamped to `max(1, count)`. `GetPrefixMatchDocCount` takes the caller's tag set **if** prefix staleness is in scope. |
| `src/query/CMakeLists.txt` | New `resolved_leaves` library; link from `vector_base`, `search`, `response_generator`. |

## Verification

Success criteria: existing suites unchanged modulo the list above, and measurably fewer rax
traversals per candidate.

1. **Build:** `./build.sh` (confirm the target it uses).
2. **No regressions:** `search_test.cc` (`ScoreTextQueryTest` pins the BM25 formula end to end;
   `ScoreTextQueryTestBase` pins tag/expansion/NaN semantics),
   `testing/query/response_generator_test.cc`, `filter_test.cc`, `tag_index_test.cc`,
   `text_test.cc`, `text_index_schema_test.cc`, `testing/scoring/bm25std_scorer_test.cc`, plus
   `integration/test_cancel.py` for the pausepoints.
3. **New unit tests:**
   - **The counting test that pins the goal:** a debug counter on `Rax::FindPostingsTarget` /
     `GetWordIterator` shows one walk per leaf per query across filter + score. Run with candidate
     counts of 1 and 100 and assert the count is identical — nothing else in the suite would catch
     a regression to per-candidate walking.
   - **Rule 1:** with a per-key source, a document lacking the term caches nothing and a later
     document containing it still matches. The false-negative regression to guard.
   - **Rule 2, both directions:** a stemmed leaf resolved from document A's tree, where A lacks a
     corpus variant, is `word_set_complete == false` and does not answer membership for document B —
     B still matches via `kFallback`. And where A *does* carry every corpus variant, the leaf is
     complete and B is answered from the cache with the same verdict `TermPredicate::Evaluate` gives.
     Plus a truncated variant list (`max-term-expansions` lowered below the variant count) is
     incomplete even from a global source.
   - **Accumulation:** documents A and B each carry a different corpus variant. After A the leaf is
     incomplete and falls back; after B it is complete and answers document C from cache with the same
     verdict `TermPredicate::Evaluate` gives. Assert `dt` rises across the two adds and that no word's
     postings are looked up twice.
   - **Positional from cache:** a positional AND over two single-word term leaves is served from the
     cache and still rejects a document whose terms are too far apart — the false-positive regression to
     guard, since a bool-only answer would drop the proximity check entirely. Repeat with the cache
     warm from a prior query. And a positional AND over two expansion leaves, plus one over an
     incomplete stemmed leaf, both fall back.
   - `MatchResolvedTextLeaf` agrees with `TextPredicate::Evaluate` for term, exact and stemmed,
     `require_positions=false`; and returns `kFallback` — with a byte-identical `Evaluate` result —
     for every expansion kind and for infix.
   - **Expansion probe and fallback:** a document carrying the most-common term scores from it with no
     tree access (assert the walk counter is unchanged). A document carrying only a rarer term falls
     back, scores from its own first match, and gets `dt == GetKeyCount()` for that term — this is the
     case that scores 0 today. Also assert the probe is field-gated: a document carrying the
     most-common term in a field outside the leaf's mask must not score from it.
   - **Tag *filtering* equivalence, the new risk:** for every combination of exact / union / prefix /
     mixed exact+prefix, case-sensitive and case-insensitive index, escaped separators in the query
     value, leading-and-trailing whitespace in the document's tag string, a non-ASCII UTF-8 tag, an
     untracked key and a key missing the field — assert the bag-probe verdict is **byte-identical** to
     `TagPredicate::Evaluate(GetValue(...))`. This is the test that guards the three equality facts in
     *Tag*; a divergence changes the result set, not just a score.
   - **Bag caching is lock-scoped:** assert an exact-only predicate performs one `raxFind` per value per
     query regardless of candidate count (the tag analogue of the counting test), that no
     `flat_hash_set` is allocated per candidate for an exact-only predicate, and that a mixed
     `{red|foo*}` predicate falls back to `GetValue` only when every exact value misses. Also assert no
     bag bits are retained when the source is per-key (Rule 1) — a main-thread cache must answer tag
     membership from the record even for a value it has `dt` for.
   - **Tag scoring from the tag set:** matches `TagPredicate::Evaluate(GetValue(...))` for exact, union,
     prefix, case-sensitive and case-insensitive indexes, plus an untracked key. Then the fix itself:
     mutate a document to **add** a tag, revalidate, and assert the added value scores with `dt == 1`
     rather than contributing 0; and mutate one to **remove** a tag and assert the removed value no
     longer scores. Both fail today.
   - **IDF stability:** a single-word leaf scores identically from a global and a per-key source; a
     multi-variant stemmed leaf yields the documented higher-IDF result from per-key, converging as
     variants accumulate.
   - A leaf skipped during evaluation is resolved on demand by scoring, and one visited by both
     resolves exactly once. Cover all three skip routes: an OR sibling past the first match, a text
     child of AND the prefilter `continue`d past, and a leaf under a negation.
   - **Match-all (`*`):** `root_predicate == nullptr`, so filtering never runs and `ScoreTextQuery`
     takes the constant-wildcard branch ([1169-1178](src/query/search.cc#L1169-L1178)) with an
     untouched cache. Assert it explicitly so a future call-site move doesn't start resolving leaves
     that don't exist.
4. **End-to-end:** `@tag:{x} @text:word`, `-@text:word` (universal-set scan, hottest case),
   `@num:[0 10] @text:word`, `@text:(a|b) @tag:{x}`, hybrid `text=>[KNN k @vec $q]` on both the
   prefilter and inline-filter plans. Compare `WITHSCORES` before and after.

   **Mixed-scale reply (the drift measurement):** index a corpus with a common stemmed term, issue a
   query, mutate one matched key mid-reply so it revalidates while its neighbours do not, and
   compare `WITHSCORES` ordering against the un-mutated run. This is the one place new per-key
   stemmed IDF meets shard-side global IDF inside a single ranking. If ordering moves materially,
   escalate rather than accepting the trade-off.
5. **Race check:** mutate matched keys while a reply is generated to confirm the per-document reader
   lock closes the unlocked per-key tree read. Use `BACKGROUND_PAUSEPOINT` / `PAUSEPOINT` to
   interleave deterministically.
6. **Perf spot-check:** `-@text:word` with stemming enabled — the shape the change is for, since it
   currently takes `stem_tree_mutex_` once per candidate. Also time `-@text:pre*`, which should get
   *faster*: scoring drops up to 200 probes per candidate for one probe plus a walk only on misses. Run
   it on both a skewed expansion (one dominant term — best case) and an evenly-spread one (worst case,
   where every candidate pays the wasted probe), and `@tag:{x} @text:pre*`, the shape where the fallback
   walk is genuinely new work.

## Open question — prefix tag values on the main thread

The one thing still undecided. `GetPrefixMatchDocCount` finds a prefix value's representative by
scanning `tracked_tags_by_keys_` ([tag.cc:521-545](src/indexes/tag.cc#L521-L545)) — the **index**, not
the fetched record. So the staleness fix in *Tag* covers exact values only; `{foo*}` on a revalidated
document still picks its representative from the pre-mutation tags.

Fixing it means passing the caller's tag set into `GetPrefixMatchDocCount` (or splitting it into
"find the representative in this set" plus "`dt` for that value", keeping its one `raxFind`). Small
change, but it widens the tag work, so it is called out rather than assumed.

## Rejected alternatives

Recorded because the reasoning is non-obvious and someone will re-propose them.

- **Keeping the full 200-entry expansion term list.** Superseded: the leaf scores one term, so one
  cached term plus a fallback walk answers every candidate. Related trap — reordering the list by
  `GetKeyCount()` instead of caching a single term would have made the lowest-IDF term the
  representative for *every* document, not just probe hits, depressing all expansion scores.
- **Always walking the per-key tree for expansion scoring** (i.e. caching nothing). Strictly worse than
  the single-term probe whenever the expansion is skewed, and never better: the global walk that
  identifies the most-common term happens once per query and already happens today.
- **Widening expansion past `max-term-expansions` to build a complete global key set**, so a miss could
  be trusted for membership. Needs a `truncated` flag and changes which documents score at all; the
  fallback walk gets the same `dt` accuracy without it, since a per-key `WordIterator` hands back the
  **shared global** `Postings` and `GetKeyCount()` off it is the true global `dt`.
- **A shared query-scoped `word → Postings` map under the leaves,** so no word's postings are "stored
  twice." Considered and dropped: an `InvasivePtr<Postings>` copy is 8 bytes and a refcount bump on the
  *same* shared object, so "twice" was only ever two pointers. The one real saving is skipping a
  duplicate rax walk when two leaves name the same word (`@title:run @body:run`) — once per query,
  marginal, and paid for with a `std::string` key per word plus a second growable container that cache
  entries would point into. Each leaf owning its own `(word, Postings)` pairs matches today's
  `ResolvedLeaf` exactly and covers every job the map had: per-leaf resolved-word tracking for
  `word_set_complete`, the word string for the follow-on bucket lock, and expansion-fallback IDF (one
  `log` off a `Postings` already in hand). It also removes the structural half of the pointer-stability
  problem — with one map, nothing can dangle when a stemmed leaf accumulates.
- **Freezing a stemmed leaf's word set at first resolve.** Rejected in *The invariant*: it forfeits
  cache-answered membership on the main thread and leaves every document on the same too-high IDF.
- ~~**Bag-testing tags for filtering.**~~ **Adopted** — see *Tag*. This was rejected in an earlier
  draft on the reasoning that "filtering touches no rax today, so this would move `raxFind`s into the
  per-candidate loop," which is simply wrong: caching the bag hoists the `raxFind` **out** of the loop,
  one per exact value per query, and replaces a per-candidate `flat_hash_set` allocation plus compare
  loop with a pointer probe. The correct scoping is by *source*, not by phase — a global source gives a
  reusable bag, a per-key source gives none (Rule 1), and prefixes give none regardless because they
  span an unbounded rax slice.
- **Sorting candidates and streaming `KeyIterator::SkipForwardKey` monotonically.**
  `InternedStringPtrLess` compares raw *pointers*
  ([string_interning.h:262-268](src/utils/string_interning.h#L262-L268)), so this means ordering
  candidates by allocation address — valid, but it perturbs candidate iteration for no gain.

One fact worth keeping from the rejected work: any cache-fed membership must apply `field_mask` gating
(`ContainsFields`, [posting.h:148](src/indexes/text/posting.h#L148)) — one `Postings` serves every TEXT
field ([120-121](src/indexes/text/posting.h#L120-L121)), and a stemmed leaf's original and variants use
*different* masks ([1036-1045](src/query/search.cc#L1036-L1045)).
