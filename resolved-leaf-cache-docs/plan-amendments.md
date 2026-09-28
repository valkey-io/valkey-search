# Amendments to plan.md

Companion to [plan.md](plan.md) and
[plan-walkthrough.md](plan-walkthrough.md).
Where this doc disagrees with those, **this doc wins** — it records decisions made after they were
written. [top-level-read-first.txt](top-level-read-first.txt) wins over everything.

## Decision log

1. **Stemmed leaves: complete-or-walk.** A stemmed leaf answers membership and scoring from the
   cache only when every enumerated variant has resolved; otherwise the caller walks. With
   decision 5 the leaf is complete at first resolve on both paths, so the walk is never taken for
   term leaves in practice. See §Stemming.
2. **The main thread never acquires the time-sliced mutex.** Not a follow-on; it lands as Phase 4
   of this task. See §Phase 4.
3. ~~Prefix tag staleness fix: in scope.~~ **Reversed by decision 7.**
4. **Prerequisite: [PR #1354](https://github.com/valkey-io/valkey-search/pull/1354)** (stemming
   scoring; rewrites `ResolvedLeaf`/`ResolveLeaves`/`ScoreNode`) must land first. No
   implementation before then; rebase, then re-audit plan anchors by **symbol name, not line
   number** — they have already drifted (`ResolvedLeaf` is now a `std::variant` over
   `TermLeaf`/`ExpansionLeaf`/`TagLeaf`, with `TermLeaf` a vector of `TermGroup`s;
   `GetPostingDocStats` kept its name).
   [Issue #1439](https://github.com/valkey-io/valkey-search/issues/1439) (tag-only recompute
   skips the contention check) is **independent**, not a gate: every unlocked per-key walk in
   this design is for an expansion leaf, i.e. a text query, which already gets the contention
   check today; tag-only queries touch only lock-guarded reads (`Tag::index_mutex_`,
   `per_key_text_indexes_mutex_`). Until #1439 lands, tag-only recompute may score stale — that
   is #1439's bug, neither caused nor fixed here — so verification must not assert tag-only
   recompute freshness.
5. **Main-thread text source is the global tree, not the per-key tree.** Term leaves (original
   word, stem root, every variant) resolve via `FindPostingsTarget` on the global prefix tree
   under a `text_index_mutex_` reader lock — one short acquire per word, once per reply, lazily
   from `GetOrResolve`. This is the read protocol `CommitKeyData` already uses on itself. The leaf
   is complete at first resolve, so on the main thread the per-document work is `GetPostingDocStats` on
   the cached shared `Postings`, never a tree. A word or variant absent at first resolve stays
   absent for every revalidated key: the contention check applies each such key's mutation
   before the reply loop starts, and the loop runs on the main thread without yielding, so no
   revalidated key's word set can change mid-reply. Consequently the plan's **Rule 1, Rule 2,
   `word_set_complete`, `LeafResolveSource.is_global`, `SetSourceTrees`, and
   `own_stem_variants` are removed from the design.** Background and main thread now differ only
   in which locks wrap the resolve and the probes (§Phase 4), not in the resolve source.
   The per-key tree is used only for expansion walks (decision 6), on both paths.
6. **Expansion representative is found opportunistically, not by a global walk.** The
   once-per-query global expansion walk in `ResolveLeaves` / `AddExpansionTerm` is **deleted**.
   The leaf's single cached entry `{word, postings, idf}` is populated from whatever per-key walk
   reaches the leaf (filtering, or the scoring fallback), replacing it whenever a term with a
   larger `GetKeyCount()` is found. Scoring probes the cached entry first (field-gated) and falls
   back to the document's per-key walk on a miss; the walk's first match scores and also updates
   the entry. Identical mechanism on both paths; no global tree traversal anywhere for expansion.
   Accepted consequence: the representative can change mid-query, so two documents carrying the
   same terms may score against different representatives depending on candidate order. The
   representative is already unspecified by contract (today it is rax order). Early candidates
   pay the walk until an entry exists; for `@tag:{x} @text:pre*` the scoring fallback is the only
   populator, since the prefilter skips the text child.
7. **Prefix-tag fix and `max(1, dt)` clamp: dropped.** Both were workarounds for tag-only
   recompute reading a not-yet-applied mutation, which is #1439's bug and is fixed at the root
   there by waiting for the mutation. With a text predicate the contention check already makes
   the index authoritative for the revalidated key. `GetPrefixMatchDocCount` keeps its
   signature; `dt == 0` keeps skipping the value. Plan §Tag "Clamp `dt`", behavior change 5's
   "newly-added tag now scores", behavior change 8, the "Open question", and the `tag.{h,cc}` row
   of the Files table are superseded.
8. **Main-thread tag membership stays on the fetched record.** The bag bits are borrowed from the
   rax slot and die on any mutation of that value; on the background path they survive because
   one reader lock spans all candidates. On the main thread the only guard is `Tag::index_mutex_`
   (exclusive, held by ingestion), so bits cannot be cached across documents without holding it
   for the whole reply. The record is already parsed for `PredicateEvaluator::EvaluateTags`, so
   membership comes from it for both filtering and scoring (published on `ScoreContext` as the
   plan describes). Only `dt`/IDF per exact value is shared with the cache. Background path:
   unchanged from the plan (bag bits + `dt`/IDF, `bag.contains(key)` per candidate).
9. **`SingleDocumentScorer` collapses into the reply-scoped cache object** (corpus stats, scorer,
   leaves). Its ctor and `Score()` locks go away with it (decision 2).
10. **Verification is trimmed** to: the walk-counter test (one tree walk per term leaf per query,
    identical at 1 and 100 candidates, on both paths), behavioral equivalence of
    `MatchResolvedTextLeaf` vs `TextPredicate::Evaluate` and of bag-probe vs
    `TagPredicate::Evaluate(GetValue(...))`, the positional-from-cache false-positive guard,
    expansion probe-hit / fallback / field-gating, the match-all guard, and the existing suites.
    Drop the "no `flat_hash_set` allocated" assertion (not practically testable) and everything
    listed under §Superseded.
11. **#1354 merged 2026-09-28 (`cc6cb5e`).** Step 0 is now a fast-forward of local `main` onto
    `origin/main` (9 commits, also bringing #1083 FT.Hybrid, #1394 WITHCURSOR, #967 INKEYS,
    #1400) followed by the symbol-name re-audit.
12. **Reader lock protocol in Phase 4 is non-nesting** — see the corrected §Phase 4 lock ordering
    paragraph. The earlier claim that writers never hold bucket + tree lock together was wrong.
13. ~~Two PRs.~~ **Reversed by decision 14.**
14. **One PR, one signed-off commit for the initial implementation.** Phases 1-4 land together.
    Planning docs stay untracked; they are not part of the PR.
15. **Main-thread expansion walks take the word bucket lock per probe.** The §Phase 4 table's
    "none" for the per-key expansion walk was wrong: the walk probes the *shared* `Postings`
    btree (`GetKeyIterator` + `SkipForwardKey`), which ingestion of *other* keys carrying the
    same word mutates under `rax_target_mutex_pool_.Get(word)`. The contention check only covers
    the revalidated key. So `TextPredicate::Evaluate` walks on the main thread lock `Get(word)`
    around each word's `SkipForwardKey`/`ContainsFields`, exactly like the cached-term probe.
    `per_key_text_indexes_mutex_` is still held only for the `find`.
16. **Positional (slop/inorder) main-thread revalidation locks the whole bucket pool.** A
    `TermIterator`/`ProximityIterator` reads several words' btrees at once, and two words can
    hash to one non-reentrant bucket, so per-word locking cannot be held across the iterator.
    Instead, lock every bucket in index order (default pool size 256, each a short
    `absl::Mutex`) for the duration of that document's predicate evaluation, then release. Still
    never the time-sliced mutex. Non-positional documents use decision 15's per-probe locking.
17. **`N` and `GetDocumentScore` on the main thread read under a short `mutated_records_mutex_`
    hold.** Every `index_key_info_` mutation already takes it (`index_schema.cc`). No new atomic
    counter and no change to the record fetch; the §Phase 4 table rows for those two reads are
    superseded.
18. **Fine-grained implementation choices are the implementer's** and are recorded in
    `implementation-choices.md`, not here.
19. **The expansion fallback walk stops at the first matching word** (reaffirming decision 6
    after an implementation had it take the most common). Expansion scoring's guarantees are
    deliberately vague about which matched term scores, so be as lazy as they allow: one match,
    early exit, offered to the cache as-is. `OfferExpansionTerm` still only promotes a more common
    term. Hardening the guarantees is a separate, later decision.

## Stemming — after PR #1354

PR #1354 makes a stemmed query term up to **3 summed BM25 leaves** (exact word, stem-root literal,
inflection group), with `ResolvedLeaf` holding a vector of `TermGroup{postings, idf, field_mask}`.
The cache entry adopts this shape. The inflection group's `dt` is a `distinct_docs` counter
maintained at ingestion on the stem root — read O(1). So there is no leaf-level `dt` to accumulate
and no converging IDF: the plan's accumulation machinery is obsolete. Do not build it.

The variant *list* (`GetAllStemVariants`, under `stem_tree_mutex_`) is query-invariant: enumerate
once per leaf per query and cache it. Variant strings are views into stem-tree memory, so the
cache copies them (a `std::vector<std::string>`, not inline — `ResolvedLeaf` is a by-value map
payload). The word strings are also what name the `RaxTargetMutexPool` bucket in Phase 4.

#1354 also touches tag scoring (commit e59ee0f) — re-audit the plan's tag-scoring references
after rebase.

## Main thread: which tree, when

Point lookups go to the global tree; walks go to the per-key tree; tags and numerics go to the
fetched record.

| Leaf | Source on main thread | Lock |
|---|---|---|
| Term (exact or stemmed) | global prefix tree, one `FindPostingsTarget` per word, once per reply | `text_index_mutex_` reader, per lookup |
| Term probe per document | cached shared `Postings`, `GetPostingDocStats(key, mask)` | `RaxTargetMutexPool` bucket for that word, per probe |
| Expansion (prefix/suffix/fuzzy) | document's per-key tree, walked per document | none (see Phase 4 on why) |
| Tag membership | fetched record | none |
| Tag `dt` | tag rax, once per exact value per reply; prefix per document | `Tag::index_mutex_` |
| Numeric | fetched record | none |

## Phase 4 — the main thread never takes the time-sliced mutex

The problem is phase shift, not contention: a main-thread reader arriving during a write phase
waits out the writer's quota. Every replacement below is held for one probe or one map lookup.

Reads to re-home, and the lock each takes — all already taken by the writer of that state:

| Read | Guard |
|---|---|
| `FindPostingsTarget` on the global tree (resolve) | `text_index_mutex_` reader |
| `GetPostingDocStats` / `GetKeyCount` on a `Postings` | `rax_target_mutex_pool_.Get(word)` — every `Postings` mutation (`AddKeyToPostings`, `RemoveKeyFromPostings`) is inside it, which also closes the `FlatPositionMap` use-after-free |
| `GetPerKeyTextIndex` + per-key walk (expansion) | `per_key_text_indexes_mutex_` for the `find` only (today's `lock=true`). **Not** held across the walk: the revalidated key has no in-flight mutation (contention check), and `per_key_text_indexes_` is a `node_hash_map` so other keys' emplace/extract cannot move it. The plan's "guard-returning API" item is dropped. |
| `GetTagValueDocCount`, `GetPrefixMatchDocCount` | `Tag::index_mutex_` |
| `GetDocumentLength` (`per_key_scoring_info_`, a `flat_hash_map`) | `per_key_text_indexes_mutex_` |
| `GetDocumentScore` (`index_key_info_`) | decide at implementation; cheapest is folding it into the record fetch |
| `N` (`index_key_info_.size()`) | **new atomic counter** maintained beside `index_key_info_` insert/erase |
| total doc length | already atomic (`TextIndexMetadata::total_doc_len`) |

`stem_tree_mutex_` for variant enumeration: unchanged, already taken with `lock_needed=true`.

Lock ordering: **the reader never nests these locks.** Writers take the `RaxTargetMutexPool`
bucket *first* and `text_index_mutex_` *inside* it (`CommitKeyData` and `DeleteKeyData`, both on
`origin/main` after #1354). A reader holding `text_index_mutex_` (reader) and then waiting on a
bucket would deadlock against a writer holding that bucket and waiting for `text_index_mutex_`
(writer). So the reader's protocol is: take `text_index_mutex_` (reader) for `FindPostingsTarget`
only, drop it holding the `InvasivePtr`, then take the word's bucket separately per probe. Same
for `per_key_text_indexes_mutex_`: hold it for the `find` only, never while taking a bucket.
Assert this (at most one of these held at a time) rather than leaving it implicit.

## Superseded plan sections

Read these as deleted:

- Plan §The invariant — "Frozen per word, accumulated per leaf" and the IDF-drift paragraphs;
  §Two rules (Rule 1, Rule 2, the per-path source table's main-thread column); §Phase 3 steps
  1-2 (`SetSourceTrees`, per-key source); §Expansion "The global walk stays"; §Tag "Clamp `dt`";
  §Follow-on goal (replaced by §Phase 4 above); §Open question; behavior changes 6 and 8 and the
  clamp half of 5; Verification items "Rule 1", "Rule 2", "Accumulation", "IDF stability",
  "Mixed-scale reply", "Bag caching is lock-scoped" (Rule 1 half), and the `dt == 1` half of
  "Tag scoring from the tag set".
- Walkthrough §4.1-4.3, §5.2 (accumulation), §5.3 "The global walk stays", §6.3 steps 1-2, §6.4,
  §10.
- The earlier version of this doc's §Prefix tag fix.

Still valid from the plan: the linchpin (per-key and global trees share one `Postings`), the
cache shape (`node_hash_map<const Predicate*, ResolvedLeaf>`, leaf owns its `(word, Postings)`
pairs), positional served from the cache via `LeafMatch` returning matched words + masks (the
bool-only hazard), the cache-miss semantics table, the `search_term_predicate` pausepoint copy,
Phase 2's lazy per-key index fetch and cache-first `EvaluateTags`, and the CMake split.

## Post-merge re-audit (2026-09-28, `origin/main` = `cc6cb5e`)

Anchors verified by symbol on the merged tree. The plan holds; the items below are the corrections
and constraints that fall out of the merge.

**Shapes on merged main (search.cc):**
- `ResolvedLeaf = std::variant<TermLeaf, ExpansionLeaf, TagLeaf>`; `ResolvedLeaves =
  absl::flat_hash_map<const Predicate*, ResolvedLeaf>`.
- `TermLeaf{InlinedVector<TermGroup, 3> groups}`, `TermGroup{InlinedVector<InvasivePtr<Postings>>
  postings, float idf, uint64_t field_mask}`. Leaf 1 exact word (`field_mask`), leaf 2 stem root
  literal (`stem_field_mask`, only if `stemmed != word`), leaf 3 inflection group (all variants,
  `dt` = `distinct_docs` from `GetAllStemVariants(..., &stem_distinct_docs)`).
- `ExpansionLeaf{field_mask, InlinedVector<ExpansionTerm{postings, idf}, 8> expansion_terms}` —
  this vector is what decision 6 replaces with one entry.
- `TagLeaf{tag_index, InlinedVector<pair<string,float>> tag_values, InlinedVector<string_view>
  tag_prefixes}`; scoring calls `tag_index->ContainsKey(value, key)` per value per candidate
  (`Normalize` + `raxFind` + bag `Adopt`/`contains`/`Release`) and `GetPrefixMatchDocCount` per
  prefix.
- The cache keeps the variant. `kUnscoreable` becomes a `std::monostate` alternative so
  `GetOrResolve` always inserts and `ScoreNode` needs no `find()`-miss branch. The map becomes
  `node_hash_map` per the plan (pointer stability across sibling `GetOrResolve` calls).

**Main-thread lock state is worse than the plan describes (post-#1400).** `VerifyFilter` now takes
the time-sliced reader lock around `GetPerKeyTextIndex` + `PredicateEvaluator`, explicitly releases
it before `recompute()`, and then `SingleDocumentScorer`'s ctor (first mutated doc) and `Score()`
(every mutated doc) each take it again. So a mutated document costs two acquires today (three for
the first). Plan §Phase 3 "VerifyFilter holds no time-sliced lock today", behavior change 7, and
walkthrough §2.4 are stale. This only strengthens Phase 3/4; PR B removes all of them.

**#1439 root cause confirmed independent:** `SearchParameters::GetContentProcessing` returns
`kContentionCheckRequired` only when `QueryHasTextPredicate`; tag-only queries return
`kContentRequired` and skip `PerformKeyContentionCheck`. Nothing here touches that path.

**Positional-from-cache constraints (Phase 1/2, from `TermPredicate::Evaluate` +
`TermIterator` ctor):**
- The plan's "no raw-mask fields are needed" is true for the *cache*, but the `TermIterator` built
  from it needs the predicate's raw `field_mask_` and `stem_field_mask` (not the collapsed
  `ScoringFieldMask`), plus `has_original`. All three come from the `TermPredicate` and schema at
  call time, so `MatchResolvedTextLeaf` takes the predicate, not just the leaf.
- `TermIterator` partitions its key iterators by index (exact / root / stem, per #1354), so the
  iterators handed back must be in the same order `TermPredicate::Evaluate` produces: original
  word first (only if it matched → `has_original`), then stem root, then variants. `TermGroup`
  order in `TermLeaf.groups` already matches; the cache must not reorder it.
- `TryAddWordKeyIteratorForPrefilter` gates on `SkipForwardKey(key) && ContainsFields(mask)`;
  the cache path does the same on `postings->GetKeyIterator()`, skipping only the rax lookup.

**Expansion leaves always get a cache entry.** The plan's "check kind before `GetOrResolve` so an
expansion leaf skips it" is unnecessary: the first `GetOrResolve` does the one-time
`dynamic_cast` and inserts an `ExpansionLeaf` whose representative slot is empty. Every later
visit is a hash hit that says "walk". `ScoreContext` gains the `TextIndexSchema*` so the scoring
fallback can fetch the per-key index lazily (shared with the evaluator's lazy fetch where both run).

**Evaluator seam is as the plan assumes.** `Evaluator::EvaluateText(const TextPredicate&, bool
require_positions)` is the virtual; `PrefilterEvaluator(text_index, query_operations)` +
`Evaluate(predicate, key)` sets `key_`; `PredicateEvaluator` in response_generator has the same
shape plus `RecordsMap`. `PrefilterEvaluator::EvaluateTags` is `GetValue` (parse +
`flat_hash_set` alloc) → `TagPredicate::Evaluate(tags, case_sensitive)`. Both construction sites
(`EvaluatePrefilteredKeys`, `InlineVectorFilter::operator()`) build the evaluator and call
`GetPerKeyTextIndex(key, false)` per candidate — the lazy-fetch win is confirmed.

**Bag borrow pattern to reuse:** `BagOfInternedStringPtrs::Adopt(SlotToStorage(slot))` …
`(void)bag.Release()` — exactly `Tag::ContainsKey` / `GetTagValueDocCount`. `Tag::index_mutex_`
is an exclusive `absl::Mutex`; the background path's safety is the documented read-side invariant
(tag index not mutated while the time-sliced mutex is in read mode).

**Pausepoints:** `test_cancel.py` queries unchanged — `search_term_predicate` uses
`-@num:[-inf 0] @text:ran` (negation present → prefilter reaches the leaf). The copy into the
term resolve path is still required.

**New since the plan (no design impact):** FT.HYBRID (#1083) calls `VerifyFilter` with a
`recompute_score_override` per arm and shares `ApplyHybridTextScore`; INKEYS (#967) adds a set
check inside `EvaluatePrefilteredKeys` / `InlineVectorFilter` and a predicate-less
`CalcBestMatchingInkeys` (no cache needed). `ScoreTextQuery`'s trailing `ResolvedLeafCache* =
nullptr` keeps all of these callers compiling.

**CMake acyclicity re-confirmed:** `vector_base` links `predicate` (full lib) which links
`tag`/`numeric`; `text`, `tag`, `scoring` link nothing under `src/query/`. `resolved_leaves` →
{`predicate`, `tag`, `text`, `scoring`}; `vector_base` → `resolved_leaves`. No cycle.

## Task sequencing

**PR A**

0. Fast-forward `main` to `origin/main` (#1354 is merged). Re-audit anchors by symbol name.
1. **Phase 1** — extract `src/query/resolved_leaves.{h,cc}` (`TermGroup`-shaped `ResolvedLeaf`,
   `ResolvedLeafCache::GetOrResolve`, `MatchResolvedTextLeaf`); delete the recursive
   `ResolveLeaves` walk and the global expansion walk; `ScoreNode` reads via `GetOrResolve`;
   opportunistic expansion entry; pausepoint copy; CMake.
2. **Phase 2, background** — `PrefilterEvaluator` takes the cache; lazy per-key index; positional
   from cache; cache-first `EvaluateTags` with bag bits; hoist evaluator construction out of the
   candidate loops. Re-baseline expansion golden scores. `SingleDocumentScorer` keeps working
   unchanged on top of the new cache (it builds its own) so PR A is self-contained.

**PR B**

3. **Phase 3, main thread** — `VerifyFilter` owns one reply-scoped cache; term leaves resolve
   from the global tree; expansion per-key; tags/numerics from the record;
   `SingleDocumentScorer` folded into the cache. Lands **with** Phase 4's locks from the start —
   there is no intermediate "one time-sliced acquire per document" step, since the ctor/`Score`
   acquires being removed are the only ones the main thread has today.
4. **Phase 4** — the table above: bucket locks around probes, `text_index_mutex_` reader around
   resolve, `Tag::index_mutex_` around tag `dt`, `per_key_text_indexes_mutex_` around
   `GetDocumentLength`, atomic `N`, `GetDocumentScore` re-homed. Non-nesting assertion.
5. **End-to-end + perf** — `WITHSCORES` comparisons on the plan's query shapes; `test_cancel.py`
   pausepoints; perf on `-@text:word` (stemmed), `-@text:pre*` skewed vs even, and
   `@tag:{x} @text:pre*`.
