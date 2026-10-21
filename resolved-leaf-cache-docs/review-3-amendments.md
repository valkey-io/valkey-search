# Review amendments — PR #1472, round 3 (review 3)

Continues [review-2-amendments.md](review-2-amendments.md) (decisions numbered from 34). Where
this doc disagrees with any earlier doc, **this doc wins**.
[top-level-read-first.txt](top-level-read-first.txt) wins over everything.

Source: the review threads of 2026-10-07 to 2026-10-21 (boda26, Frank-Gu-81, coderabbit,
greptile), applied in commits `21c9d9f8` through `236bda40`, then the merge of `origin/main`
(`c3666a47`, bringing in #1263 multi-language stemming).

## Decision log

34. **`ResolvedLeafCache` is `QueryContext`.** It owns everything a query reads from the index
    under one `LockMode`: the leaf cache, `CorpusStats`, `PerKeyTextIndex`, `DocLen`, `DocScore`,
    `TagPrefixDocCount`, `ScoreLeaf`. Every lock choice by mode lives in this class; new inputs
    are added here rather than read from the scorer or the evaluators. `ScoreContext` in
    `search.cc` shrinks to `{query_ctx, records, per-key memo}`. Files `resolved_leaves.{h,cc}`
    become `query_context.{h,cc}`; `resolved_leaf.h` (structs only) is unchanged.

35. **`LockMode` names the locks, not the thread:** `kReadPhase` (caller holds the time-sliced
    read phase) and `kShortLocks` (per-structure mutexes only). `MainThread()` becomes
    `ShortLocks()`. Comments in `resolved_leaf.h` keep saying "main thread" / "background";
    that distinction is the design intent there and stays.

36. **`ScoreLeaf` takes `indexes::scoring::LeafScoreInput`**, the struct `Scorer` already
    documents as its extension point, instead of four loose floats. Callers use designated
    initializers; `QueryContext` fills `avg_doc_len`.

37. **Thread-safety annotations stay as function-wide opt-outs.** `DocScore` and
    `ReadCorpusStats` keep `ABSL_NO_THREAD_SAFETY_ANALYSIS`. A per-branch
    `AssertTimeSlicedReaderHeld()` on `IndexSchema` was tried and reverted: it covered two of
    fourteen `ShortLocks()` branches and looked arbitrary. A pattern that covers all of them is
    an open question, not for this PR.

38. **Record tags are parsed per call, not memoized.** `RecordTags` (a per-reply memo keyed by
    identifier) is replaced by the free function
    `ReadRecordTags(const RecordsMap&, const TagPredicate&)` returning
    `std::optional<flat_hash_set<string_view>>`. Revalidation is rare and the memo had produced
    two bugs (wrong tokenizer, flat_hash_map rehash dangling pointer) for a saving below noise.
    Parsing uses `Tag::ParseRecordTags`, as the index does; the `ParseSearchTags` call it
    replaces was inherited from main.

39. **The free `EvaluateText` is `EvaluateTextLeaf`** so `Evaluator::EvaluateText` calling it
    no longer reads like recursion. "WithCache" was rejected because the cache is now part of
    `QueryContext`.

40. **`MainThreadLockModesMatchBackground` runs over every registered scorer**
    (`indexes::scoring::kScorerByStr`) with `SCOPED_TRACE`, not one hard-coded scorer.

41. **Expansion scoring is order-dependent by design.** The representative is the first
    matching word scoring reaches, so the same query on the same data can pick a different word
    on a replica. Raised by Frank-Gu-81, kept as a deliberate trade-off; the user replies on
    the thread.

42. **Sync-path revalidation is a fast follow-up, not in this PR.** `ExecuteSyncLocal`
    (MULTI/EXEC, Lua, `--reader-threads 0`) reaches `VerifyFilter` without the key contention
    check the async path runs in `ResolveContent`. Since this PR removed the time-sliced read
    lock from `VerifyFilter`, that path can read a per-key tree a writer is freeing. Fix: run
    the contention check on the sync path and block the main thread until the result keys'
    in-flight mutations are indexed (bounded: nothing new is enqueued while the main thread is
    inside the command), with an aggressive timeout. A `SearchLocked`/held-lock stopgap was
    written, built green and dropped as having no long-term benefit. Filed as
    [#1602](https://github.com/valkey-io/valkey-search/issues/1602).

    The cache's correctness rests on the same guarantee: a cached `Postings` can only go stale
    for a result key if that key is removed from it and re-added under a replacement object,
    which needs an in-flight mutation of that key. The async path has none by construction
    (contention check, then a fresh `QueryContext`); the sync path gets the same once #1602
    lands. Its regression test should remove and re-add a result key's final word while a
    synchronous query is entering revalidation.

## Merge of `origin/main` (c3666a47)

#1263 replaced `Lexer` with `Language` + `Stemmer` and made fuzzy distance per-language
(`LengthUnit`). No design interaction with the cache; the index structures and the writer
lock order (word bucket → tree → stem tree) are unchanged, so the short-lock helpers hold.

- `TextIndexSchema::StemWord(word)` replaces `GetLexer().StemWord(word)`; it forwards to
  `language_->GetStemmer()->GetStemRoot(word)` (word unchanged when no stemmer), which is what
  main's own query path did. Query-side stemming uses `min_stem_size = 0` while ingestion uses
  the configured size, the same asymmetry main has.
- `FuzzySearch::Search` keeps our `Sink` template and gains main's `LengthUnit` parameter
  before the sink. `Expansion` is gone; main's new `text_test.cc` fuzzy tests collect key
  iterators through a test-local `FuzzyExpand` helper.
- `lexer.{h,cc}` deleted. Main's `GetAllStemVariants` body dropped (replaced by
  `WithStemTreeLock` reads); its one test ported to `StemWord` + `WithStemTreeLock`.
- `ForcedMultiArmFailure` (#1529) and the fuzzy compat counter auto-merged.

## Follow-ups for the PR description

1. Align main-thread and read-phase scoring paths so only `ReadRecordTags` is per-document.
2. Move tag-value parsing (prefix split, case-dedup) from `ResolveTag` into `TagPredicate`.
3. Decision 42: onboard the sync search path to the key contention check.

## Post-merge review findings (Sol, 2026-10-21)

Verified against `c3666a47`; disposition decided per item below.

**F1. Group field mask lost at position time.** `TermPredicate::Evaluate(leaf, …)` probes each
word with its `TermGroup::field_mask` (key-level check) but hands every position map to
`SingleKeyTermIterator` with the predicate-wide `field_mask_`, so an inflection admitted via a
stemmed field contributes positions from a NOSTEM field during SLOP/INORDER. Pre-existing on
main in a different form: main's `TermIterator::InsertValidPositionIterator` also filters
positions by `query_field_mask_` only, and applies `stem_field_mask_` at the key level only.
Same user-visible behaviour, so not a regression. Fix is real and small (per-map mask in the
iterator); disposition: TBD.

**F2. Case-insensitive `Tag::ModifyRecord` diffs raw values.** `Red` → `red` indexes the
normalized bag then de-indexes it, leaving the key absent. The diff and the normalize-on-mutate
are byte-identical on `origin/main`; pre-existing, unrelated to the cache. Disposition: TBD
(separate issue).

**F3. Cached `Postings` identity.** A `kShortLocks` `QueryContext` holds an `InvasivePtr` to
the postings object a word resolved to; a writer that removes the word's last key detaches it
and a later add installs a new one, so later probes in the same context miss keys in the new
object. Only reachable through an in-flight mutation of a result key, which only the sync path
allows. Disposition: a consequence of #1602 (decision 42), not a separate blocker; no
generation or identity machinery.
