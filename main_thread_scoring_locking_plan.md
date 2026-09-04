# Main-thread scoring without the time-slice mutex

**Status: pushed; the open correctness item is now fixed in the working tree.** Commits on
`BCathcart/valkey-search:brennan-scoring-locking` (base `zackcam:search-comments2`):

```
67db658 Return only scoring fields from LookupKeyPosting
d4cd54e Tidy up main-thread scoring internals
5e454db Score on the main thread without the time-slice mutex
```

**Do not amend or force-push these** — a PR is open against them; follow-ups go in new commits.

The `no_content` FT.AGGREGATE path (see "Resolved item" below) is fixed in an uncommitted follow-up. Still
outstanding, perf-only: main-thread per-key cost, and tag-locking coarseness. Also filed for later:
`ADDSCORES` is a parsed no-op (see "ADDSCORES" below).

**Read "Claim reliability" below before trusting any statement in this document about FT.AGGREGATE
semantics.** Several were asserted from partial reads and turned out wrong.

## Context

Main-thread score recompute (`response_generator.cc VerifyFilter` → `SingleDocumentScorer::Score`) did two
things it shouldn't:

1. **Corpus-level work on the main thread.** `Search()` pre-builds `parameters.recompute_scorer` on a
   background thread, but in CME the coordinating `SearchParameters` never runs `Search()` — the local shard
   runs a separate `LocalResponderSearch` ([fanout.cc:410-418](src/query/fanout.cc#L410-L418)).

   For FT.SEARCH **with** content this is harmless: the local responder resolves its own content, so its
   neighbors reach the coordinator already populated and `ProcessNeighborsForReply` skips them
   ([response_generator.cc:466-469](src/query/response_generator.cc#L466-L469)). The recompute runs on the
   responder, off the scorer it pre-built.

   The gap is FT.AGGREGATE whose LOAD resolves to no record attributes
   ([ft_aggregate.cc:91](src/commands/ft_aggregate.cc#L91) sets `no_content`). That propagates to every
   responder, so nothing carries content and the coordinator fetches it for the keys it owns — while the
   responder, being `no_content`, skipped the pre-build. The coordinator then hit the lazy `kAcquireLock`
   fallback and did `ResolveLeaves` — rax lookups, stem expansion, per-term IDF — on the main thread. That
   also snapshots a *later* corpus than the one the carried scores came from, so recomputed scores land on a
   slightly different scale.

2. **A time-slice mutex acquisition on the main thread.** `Score()` took `ReaderMutexLock` on
   `time_sliced_mutex_`, which can stall behind an active writer phase.

Goal: the scorer is **always** built on a background thread and holds all corpus-level state; the main
thread reads only key-level state, under fine-grained locks, and never touches the time-slice mutex.

### Why fine-grained locks are needed (not just "no mutation in flight")

`PerformKeyContentionCheck` ([index_schema.cc:1918-1942](src/index_schema.cc#L1918-L1942)) runs once before
the neighbor loop and only guarantees that *the result keys* have no queued mutation. Mutation-pool threads
keep draining **other** keys throughout the loop, and `CommitKeyData` `emplace`s into the *shared* `Postings`
btree ([text_index.cc:264-275](src/indexes/text/text_index.cc#L264-L275) — the per-key tree and the main tree
point at the same object). So `key_to_positions_.find()` races a concurrent `emplace()` for an unrelated key.

Also note `GetContentProcessing()` only returns `kContentionCheckRequired` for queries with a text
predicate; tag/numeric-only filters skip the check entirely.

Every key-level read already has a lock **the writer takes**, so the time-slice mutex is replaceable:

| Read | Lives in | Writer's lock | Branch |
|---|---|---|---|
| `tf`, `doc_len` | `PostingValue` in shared `Postings::key_to_positions_` | `rax_target_mutex_pool_.Get(word)` ([text_index.cc:254](src/indexes/text/text_index.cc#L254), [:337](src/indexes/text/text_index.cc#L337)) | `kText` |
| tag membership | `Tag::tree_` rax | `Tag::index_mutex_` (already taken by `GetTagValueDocCount`) | `kTag` |
| `doc_len` | `per_key_scoring_info_` (`flat_hash_map`) | `per_key_text_indexes_mutex_` | `kTag` only |
| document score | `index_key_info_` (`flat_hash_map`) | `mutated_records_mutex_` | composition, only if `has_score_field` |

Position maps are never read by scoring — `tf`/`doc_len` are mirrored into `PostingValue` precisely to avoid
that ([posting.h:73-81](src/indexes/text/posting.h#L73-L81)).

### Why `Score()` must reach postings via the per-key tree

`SingleDocumentScorer`'s constructor pins `InvasivePtr<Postings>` via `ResolveLeaves`. Those pins can go
**stale**: if the re-evaluated key was the word's last holder, `RemoveKeyFromPostings` clears the tree's ref
([text_index.cc:55-67](src/indexes/text/text_index.cc#L55-L67)) and `DeleteKeyData` erases the word from the
rax ([:349-352](src/indexes/text/text_index.cc#L349-L352)); the re-add then finds no word and
`AddKeyToPostings` builds a **fresh** `Postings`. The pinned object stays alive (refcount) but orphaned and
empty, so `LookupKey` → nullopt → `tf == 0` → `Score()` → nullopt → `VerifyFilter` degrades to `0.0f`. Safe,
but a silent mis-ranking in exactly the case recompute exists for. This bug predates the change on the
`Search()` pre-built path; Part 1 would have made it universal.

So `Score()` resolves per-key data through the key's own tree, which `CommitKeyData` rebuilds wholesale
([text_index.cc:298-302](src/indexes/text/text_index.cc#L298-L302)) and which therefore always points at the
*current* shared `Postings`. It is stable for the duration because the key is quiesced. Corpus-level state
(IDF, `avg_doc_len`, `total_docs`) still comes from the background snapshot — that is what keeps the
recomputed score on the same scale as the carried neighbor scores. Bonus: membership becomes correct too
(key lost the word → tree lacks it → true non-match instead of a stale hit).

Background `ScoreTextQuery` keeps the pinned-postings route: it runs inside the read phase against the same
snapshot, so it has no staleness window and no reason to pay for a rax walk.

**The quiescence premise — and where the original claim was WRONG.** Walking the per-key tree without a lock
relies on the key having no in-flight mutation, which `PerformKeyContentionCheck` only guarantees for
`kContentionCheckRequired` queries.

This document previously claimed the two conditions coincide, because `QueryHasTextPredicate` is what makes
`GetContentProcessing()` return `kContentionCheckRequired`. **That is false.** `GetContentProcessing()` tests
`no_content` *first* and returns `kNoContent` before it ever looks for a text predicate:

```cpp
if (no_content) return kNoContent;                       // <-- short-circuits
if (query::QueryHasTextPredicate(*this)) return kContentionCheckRequired;
return kContentRequired;
```

So a text-predicate query with `no_content` set gets `kNoContent`, never runs `ResolveContent`, and never runs
the contention check — yet could still reach the `kText` scoring branch. Only `no_content`
FT.AGGREGATE ever did this, and it no longer fetches content at all. See "Resolved item".

Tag/numeric-only queries do skip the contention check but never touch a per-key tree — their reads go through
`Tag::index_mutex_`, `per_key_text_indexes_mutex_`, and `mutated_records_mutex_`, none of which depend on
quiescence. That part still holds.

---

## Review ordering

The two parts are independent and worth reviewing separately. Part 2 (per-key-tree routing + fine-grained
locks) is the correctness-critical, TSAN-sensitive half and stands alone. Part 1 is a behavioral change
touching FT.AGGREGATE and fanout. Part 2 alone does not reach the stated goal — the lazy `VerifyFilter`
path's `kAcquireLock` **constructor** still took the reader lock; Part 1 is what removed the last
acquisition.

---

## Part 1 — Always build the scorer on a background thread

**[search.h:282](src/query/search.h#L282)** — new virtual on `SearchParameters`:

```cpp
// Gates the background pre-build of recompute_scorer, matching
// GetContentProcessing() != kNoContent. Deliberately NOT virtual: every
// operation that fetches content on the main thread does so under a
// no_content == false parameter set, and a subclass that forced this to true
// anyway would enable a recompute on a query that never ran the contention
// check the recompute's per-key text index walk depends on.
bool WillFetchContentOnMainThread() const { return !no_content; }
```

**[search.cc:1534](src/query/search.cc#L1534)** — pre-build gate switched from
`GetContentProcessing() != kNoContent` to `WillFetchContentOnMainThread()`.

This is **behavior-identical**, not a widening: `GetContentProcessing()` returns `kNoContent` iff
`no_content`, so `!no_content` ≡ `GetContentProcessing() != kNoContent`. The named accessor exists to state
the property the pre-build actually depends on, and it is **non-virtual on purpose** — overriding it is what
produced the use-after-free below, so the invariant is now structural rather than comment-enforced.

> Earlier revisions of this plan added two overrides returning `true` — on `AggregateParameters` and on
> `LocalResponderSearch` — plus a scorer borrow chain in `VerifyFilter`. All three are **removed**; see
> "Resolved item" below. The only thing they served was `no_content` FT.AGGREGATE, which no longer fetches
> content at all.

**[response_generator.cc:203-210](src/query/response_generator.cc#L203-L210)** — the lazy construction is
replaced by a straight read of the pre-built scorer:

```cpp
const query::SingleDocumentScorer *scorer = parameters.recompute_scorer.get();
if (scorer == nullptr) {
  recompute_scorer_missing.Increment();   // dev counter, expected to stay 0
  return {true, std::nullopt};            // keep the carried score
}
```

No borrow is needed: `ProcessNeighborsForReply` skips any neighbor that already has
`attribute_contents` ([response_generator.cc:462](src/query/response_generator.cc#L462)), so a neighbor
resolved by another shard never reaches `VerifyFilter`. Every neighbor that does reach it belongs to the
`SearchParameters` that ran `Search()` and therefore pre-built the scorer under its own reader lock.

A `DCHECK` was considered and rejected: it would abort debug builds on a path that cannot be proven
unreachable, and it makes the fall-through untestable. The counter is observable from `INFO SEARCH` as
`search_recompute_scorer_missing` ([response_generator.cc:158](src/query/response_generator.cc#L158)) and is
what the integration test asserts on.

`local_responder_` ([search.h:313](src/query/search.h#L313)) is **retained** — it still anchors the
`RecordsMap` `string_view` lifetimes ([fanout.cc:277-280](src/query/fanout.cc#L277-L280)). Only the scorer
borrow through it is gone.

The `document_scorer` out-parameter is deleted from `VerifyFilter` / `GetContentNoReturnJson` /
`GetContent` / `ProcessNeighborsForReply`. `LockPolicy::kAcquireLock` now has no production caller —
audited: the only construction sites are the ctor itself, and the sole production user of the default
argument was the lazy `VerifyFilter` path. The enum is retained for tests.

## Part 2 — Fine-grained locks in `Score()`

**`src/query/search.cc`**

- [`ScoreContext`:820-821](src/query/search.cc#L820-L821) — new `per_key_text_index` pointer and
  `lock_per_key_reads` flag. Unset on the background path, so `ScoreTextQuery` keeps its hot loop unchanged.
- [`ResolvedLeaf`](src/query/search.cc#L647) — carries the resolved `words` (original plus each stem variant,
  populated by the existing `add_word` lambda) and the `TextIndexSchema*`. `postings` is kept for the
  background path.
- [`Score()`:1139](src/query/search.cc#L1139) — fetches the per-key tree once per key via the existing
  `TextIndexSchema::GetPerKeyTextIndex(key, /*lock=*/true)`. Null (key gone) → no text leaf matches.
- [`ScoreNode` `kText`:878-897](src/query/search.cc#L878-L897) — in main-thread mode, resolves each word via
  `per_key_prefix.FindPostingsTarget(word)` and takes that word's bucket mutex around `LookupKey`.
- [`ScoreNode` `kTag`:936-950](src/query/search.cc#L936-L950) — `GetDocumentLengthLocked` and the locking
  `ContainsKey` overload.
- [Composition:1164](src/query/search.cc#L1164) — `GetDocumentScoreLocked`.
- The `ReaderMutexLock` is **removed** from `Score()`.

**`src/indexes/text/text_index.h`**

- [`GetWordMutex(word)`:162](src/indexes/text/text_index.h#L162) — exposes `rax_target_mutex_pool_.Get(word)`.
- `GetKeyDocLen(key, bool lock)` overload, mirroring the existing `bool lock` pattern on
  `GetPerKeyTextIndex` and `GetTrackedKeyCount(bool)`.
- `GetPerKeyTextIndex(key, lock)` reused as-is — unchanged.

**`src/indexes/tag.h` / `tag.cc`** — locking `ContainsKey(value, key, bool lock)` overload taking
`index_mutex_` (`GetTagValueDocCount` is the pattern).

**[index_schema.h:208-220](src/index_schema.h#L208-L220)** — `GetDocumentScoreLocked` (takes
`mutated_records_mutex_`) and `GetDocumentLengthLocked` (passes `lock=true` through). The
`ABSL_SHARED_LOCKS_REQUIRED(time_sliced_mutex_)` originals are retained for background callers so the
compiler still enforces which path may skip the lock.

---

## Verification results

**Unit tests — all 1365 pass across 24 binaries, no failures.** New/changed:

| Test | File | What it pins |
|---|---|---|
| `RecomputeSurvivesPostingsRecreation` | [search_test.cc:1867](testing/search_test.cc#L1867) | Staleness fix. **Fails without the per-key-tree routing** (`Score()` returns nullopt off the orphaned pin). |
| `ScoreIsSafeAgainstCommitsOnTheSameWord` | [search_test.cc:1901](testing/search_test.cc#L1901) | Word-bucket lock, under TSAN. |
| `ScoreDoesNotTakeTimeSlicedMutex` | [search_test.cc:1845](testing/search_test.cc#L1845) | `Score()` is callable with the reader lock held (would deadlock before). |
| `RecomputeScorerGateTest.DefaultsToContentFetchingQueries` | [search_test.cc:1946](testing/search_test.cc#L1946) | Base gate ≡ `GetContentProcessing() != kNoContent`. |
| `AggregateTest.NoContentDoesNotFetchContentOnMainThread` | [ft_aggregate_parser_test.cc:314](testing/ft_aggregate_parser_test.cc#L314) | FT.AGGREGATE uses the base gate — `no_content` claims no fetch. |
| `RecomputeUsesPreBuiltScorer` | [response_generator_test.cc:399](testing/query/response_generator_test.cc#L399) | The pre-built scorer replaces the stale carried score. |
| `RecomputeKeepsCarriedScoreWithoutScorer` | [response_generator_test.cc:428](testing/query/response_generator_test.cc#L428) | Fall-through keeps the score, no crash. |

Stale lock comments in `RecomputePathMatchesExtraStepAtNonZero` and
`RecomputeScorerConstructibleUnderHeldLock` were corrected; both still assert scale equality with the
extra-step path.

**TSAN — clean, and the race test provably fails without the fix.** `./build.sh --tsan`, then all 157
`query_test` cases: 0 warnings. With the word-bucket lock removed, TSAN reports exactly the predicted race —
`btree::internal_emplace` (mutation thread) against `btree_node::finish()` (main thread) on
`btree_map<InternedStringPtr, PostingValue>`, plus an `operator delete` race. This was verified before
merging: a race test that cannot fail without the fix proves nothing.

**Integration — [test_scoring_recompute_cluster.py](integration/test_scoring_recompute_cluster.py), 3 pass.**
Uses the existing `block_mutation_queue` pausepoint (as `test_postfilter.py` does) to park a mutation
deterministically, and a numeric/tag index so revalidation runs synchronously instead of parking the query.
The mutation touches a field *outside* the filter, so revalidation still matches and actually reaches the
recompute.

- `test_aggregate_no_content_skips_main_thread_fetch` — asserts `search_predicate_revalidation` does **not**
  move, which proves the fetch was skipped: `VerifyFilter` is only reachable from
  `ProcessNeighborsForReply`. Before the fix this same shape incremented the counter and, without the (now
  removed) overrides, also drove `search_recompute_scorer_missing` to 1 — which is what established the CME
  gap was FT.AGGREGATE-specific in the first place.
- `test_aggregate_with_load_still_recomputes` — a real LOAD clears `no_content`, so revalidation and the
  recompute still run. Pins that the fix did not narrow the content path.
- `test_search_recomputes_on_local_responder` — passed with or without the overrides, confirming FT.SEARCH
  with content is handled entirely on the responder, off its own scorer.

**Build.** `ninja -C .build-release` clean; `clang-format` clean on all changed files (the single violation
in `ft_aggregate_parser.h` is pre-existing).

## Resolved item — `no_content` FT.AGGREGATE

**The bug.** `AggregateParameters::WillFetchContentOnMainThread() -> true` enabled the score recompute on a
path where the contention check never ran. `Score()` then walks the key's per-key `TextIndex` unlocked while a
mutation thread can free it:

- `GetPerKeyTextIndex(key, /*lock=*/true)` returns `&it->second` and releases the mutex — the lock covers the
  *map lookup*, not the lifetime of the pointee.
- `DeleteKeyData` does `per_key_text_indexes_.extract(key)`; the `node_type` destructs at end of scope,
  destroying that `TextIndex` and its `Rax`. `node_hash_map` gives stability across rehash, not against erase.
- Nothing parks the query, so those can overlap. Use-after-free.

**Why the main-thread fetch is pointless there anyway** (verified by reading `ft_aggregate.cc`):

- With no LOAD, `return_attributes` is empty, so `GetContent` fetches *every* field of every key — one
  `ValkeyModule_OpenKey` per neighbor.
- Then all of it is discarded: `if (n.attribute_contents.has_value() && !parameters.no_content)` at
  [ft_aggregate.cc:289](src/commands/ft_aggregate.cc#L289).
- `n.score` is written into the record only under `if (parameters.IsVectorQuery())`, and vector queries never
  recompute (`recompute_score = parameters.IsNonVectorQuery()`), so no aggregate reads a recomputed score *as a
  value*. It is not entirely inert on the content path, though: a recompute sets `any_score_recomputed`, which
  re-ranks neighbors via the `stable_sort` at
  [response_generator.cc:538](src/query/response_generator.cc#L538) and so changes record order.
- The only other live effects under `no_content` were `VerifyFilter` dropping stale matches,
  `CheckSlotOwnership`, and the erase-neighbors-without-content step.

**Fix applied.** Guard the `ProcessNeighborsForReply` call in `ProcessNeighborsForProcessing` on
`!no_content` — the guard FT.SEARCH already has, via `HandleEarlyReplyScenarios` at
[ft_search.cc:344](src/commands/ft_search.cc#L344). With the fetch gone, `AggregateParameters`' and
`LocalResponderSearch`' overrides and the `VerifyFilter` borrow are all dead and were removed.

The guard wraps the **call only**, not the whole function: the `AddRecordAttribute` calls above it establish
`key_index` / `scores_index` and must still run.

**The content path is unchanged.** `FT.AGGREGATE ... LOAD @f` has `no_content == false`, so it still fetches,
revalidates, and recomputes exactly as FT.SEARCH does — pinned by
`test_aggregate_with_load_still_recomputes`.

**Accepted behaviour change, matching FT.SEARCH NOCONTENT.** Without the fetch, keys that were deleted,
expired, mutated out of the filter, or that live in a slot this shard no longer owns are no longer pruned, so
`FT.AGGREGATE idx "<query>" GROUPBY 0 REDUCE COUNT 0` can over-count. FT.SEARCH NOCONTENT already returns
exactly this staleness: `HandleEarlyReplyScenarios` returns before `ProcessNeighborsForQuery`, so it never
prunes, never revalidates, and never checks slot ownership either.

**This also closes what an earlier draft called a separate pre-existing UAF.** `VerifyFilter`'s own predicate
revalidation ([response_generator.cc:224-231](src/query/response_generator.cc#L224-L231)) uses the same
fetch-pointer-then-release pattern on `GetPerKeyTextIndex` and walks it in `TermPredicate::Evaluate`. It is not
independent: `ProcessNeighborsForReply` now only runs when `GetContentProcessing() != kNoContent`, and any
query with a text predicate in that set is `kContentionCheckRequired`, so the key is quiesced. The aggregate
override was the only thing that broke that invariant. Removing it closes `VerifyFilter`'s walk and
`SingleDocumentScorer::Score()`'s walk together, and makes the comment at
[search.cc:1133-1139](src/query/search.cc#L1133-L1139) true as written.

**Second, separate aggregate bug found while investigating.** `no_content` is derived solely from `loads_`,
but `MakeReference` registers a record slot for every `@field` a stage references *without* adding it to
`loads_`. So `FT.AGGREGATE idx "<query>" FILTER "@n > 5"` with no LOAD sets `no_content = true`, and the
field's slot is never populated (line 289 refuses to copy contents in) — the stage evaluates against an empty
value. Suspected already-broken on `main`, independent of locking. **Still unverified — confirm with an
integration test before acting.** Note the ADDSCORES probe below shows a stage referencing a field *absent
from the schema* is rejected at compile time, so this bug (if real) is narrower than first described: it needs
a field that **is** in the schema but absent from `loads_`.

---

## ADDSCORES is a parsed no-op (found while investigating; pinned by test, not fixed)

`ADDSCORES` is meant to expose each document's relevance score to the aggregation pipeline, the way FT.SEARCH
`WITHSCORES` exposes it to the reply. It parses into `AggregateParameters::addscores_`
([ft_aggregate_parser.cc:245](src/commands/ft_aggregate_parser.cc#L245), via `GENERATE_FLAG_PARSER`) and that
field is **read nowhere else in the codebase**. The keyword silently succeeds and does nothing.

Combined with the point above — `n.score` reaching a record only under `IsVectorQuery()` — there is no route at
all from a non-vector relevance score to aggregate output. The score is computed, and recomputed on the main
thread when a document mutated mid-query, and then dropped.

Observed on a running server (text+numeric index, `FT.AGGREGATE idx "hello" ...`):

| Command | Result |
|---|---|
| `ADDSCORES LOAD 1 @n` | reply **byte-identical** to the same command without `ADDSCORES` |
| `ADDSCORES LOAD 2 @n @__score` | `Index field `__score` does not exist` |
| `ADDSCORES ... SORTBY 2 @__score DESC` | `Error parsing value for the parameter `SORTBY` - Index field `__score` does not exist` |

So `@__score` is resolved as an ordinary schema attribute; there is no synthetic score field for a stage to
reference.

Not fixed here: it is a feature gap, not a locking issue, and implementing it means choosing a field name
(Redis uses `@__score`) and wiring it through `record_indexes_by_alias_`. Pinned instead by
[integration/test_aggregate_addscores.py](integration/test_aggregate_addscores.py), which asserts the current
no-op behaviour and carries a `TODO` plus the assertions that should replace it once implemented, so the
change is caught rather than shipped silently.

Note this cuts *against* removing the recompute from the content path: once `ADDSCORES` works, the recomputed
score becomes directly observable there.

---

## Claim reliability

Two confident claims in earlier drafts of this document were wrong. Verify before relying on anything here
about FT.AGGREGATE.

| Claim | Verdict |
|---|---|
| The CME scorer gap affects *every* clustered content query | **Wrong.** FT.SEARCH-with-content is handled on the local responder, which resolves its own content; its neighbors reach the coordinator populated and are skipped. The gap is FT.AGGREGATE-with-`no_content` only. Caught because the integration test passed with the fix reverted. |
| `kText` branch implies `kContentionCheckRequired`, so the unlocked per-key walk is safe | **Was wrong, now true.** `GetContentProcessing()` tests `no_content` first and returns `kNoContent` before it ever calls `QueryHasTextPredicate`, so a `no_content` text aggregate reached the `kText` branch with no contention check. The fix above removes the only path that did this, so the invariant now holds. |
| The recompute is unobservable for *all* FT.AGGREGATE | **Overstated.** The recomputed *value* never reaches output (no route for non-vector; `ADDSCORES` is a no-op), but a recompute sets `any_score_recomputed` and re-ranks neighbors via the `stable_sort`, which does change record order on the content path. |
| `LocalResponderSearch` needs `WillFetchContentOnMainThread() -> true` for CME | **Only for `no_content` FT.AGGREGATE**, which no longer fetches. For FT.SEARCH-with-content the responder resolves its own content and recomputes off its own scorer; populated neighbors are then skipped at [response_generator.cc:462](src/query/response_generator.cc#L462). Override removed. |
| Aggregate stages are value-independent under `no_content` | **Wrong.** Stages can reference `@field` via `MakeReference`; those references are simply never populated. |
| `SendReply` / aggregate stages run on a background thread | **Wrong.** `async::Reply` is the blocked-client callback, so all of `SendReplyInner` — including FILTER/APPLY/SORTBY/GROUPBY — runs on the main thread. |

Verified and trustworthy: everything in "Verification results" (test outcomes, TSAN behaviour with and
without the word lock), the lock-substitution table, and the stale-`Postings` analysis — each was checked
against code or demonstrated by a test that fails without the fix.

**Suggested next step:** explore FT.AGGREGATE's main-thread pipeline from scratch — how `loads_`,
`return_attributes`, `record_indexes_by_*` and the stage expressions actually relate — rather than extending
the assumptions above.

---

## Outstanding

**Tag locking from the main thread is a new paradigm and needs optimization thought.** `Tag::ContainsKey`
was *deliberately* lock-free before this change, relying on the read-phase invariant. Now that `Score()` runs
outside the read phase it takes `Tag::index_mutex_` — and that is **one mutex for the entire tag index**,
guarding `tree_`, `tracked_tags_by_keys_` and `untracked_keys_` together, with no per-value or per-key
sharding. Consequences:

- It is the same lock tag writers take (`AddRecord` / `RemoveRecord` / `ModifyRecord`), so main-thread tag
  scoring now contends with tag ingestion across the *whole* index, not just the queried value.
- It is acquired once **per tag value in the query**, so `@color:{red|blue|green}` is three acquire/release
  cycles for one document.
- `Normalize(value)` (a string allocation, plus `AsciiStrToLower` on case-insensitive indexes) currently runs
  *inside* the lock, because the locking overload wraps the existing function wholesale.

This is structurally coarser than the text path, which shards via `RaxTargetMutexPool` so two different words
never contend. Cheap improvements, deliberately not applied without measurement: hoist the lock around the
whole `tag_values` loop (one acquisition instead of N, and a consistent snapshot), and move `Normalize` out of
the critical section. The structural fix, if it proves to matter, is to shard `Tag::index_mutex_` the way text
shards its word locks.

Note the tag path gains nothing from the quiescence argument and needs nothing from it — it never walks a
per-key structure, so `kContentRequired` tag/numeric queries skipping `PerformKeyContentionCheck` is harmless.
The question here is purely throughput, not correctness.

**Main-thread per-key perf measurement.** Part 2 trades a stale-but-O(1) pinned lookup for a correct
`GetPerKeyTextIndex` (mutex + hash lookup, hoisted once per key) plus a rax walk per word, so per-document
main-thread cost goes up even though time-slice contention goes away. Benchmark a reply where many neighbors
are mutated (worst case: every neighbor mutated, multi-term query with stem variants) and confirm the removed
contention dominates the added walk.

If it does not, the pinned postings can serve as a fast path with the per-key tree consulted only when
`LookupKey` misses — sound because an orphaned `Postings` is always empty, so a hit implies the object is
still live. Measure before optimizing.

The background `ScoreTextQuery` loop needs no measurement beyond confirming it still takes the unlocked
pinned-postings path, which `lock_per_key_reads == false` guarantees structurally.
