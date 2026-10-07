# Review amendments — PR #1472, round 1

Companion to [plan-amendments.md](plan-amendments.md), which it continues (decisions are numbered
from 20). Where this doc disagrees with plan-amendments.md, plan.md or plan-walkthrough.md,
**this doc wins**. [top-level-read-first.txt](top-level-read-first.txt) wins over everything.
Fine-grained choices made while applying these decisions go in
[implementation-choices.md](implementation-choices.md) §Review round 1, not here.

Source: the review threads on [PR #1472](https://github.com/valkey-io/valkey-search/pull/1472)
as of 2026-10-04 (head `b955c12`). File:line anchors below are against that commit.

**Review scope.** The round-1 pass covered everything *except* `resolved_leaves.{h,cc}`,
`response_generator.{h,cc}` and `search.{h,cc}` (reviewer summary, 2026-10-04). Those files hold
most of the new cache and locking logic, so a further round on them is expected after these
decisions land; this doc is not a statement that review is complete.

**Sign-off.** Decisions 24, 25, 26 and 28 chose among options the reviewer left open or went
beyond what was proposed; all four were signed off on 2026-10-04 and are marked as such. The
rest follow directly from the review comments.

## Lock domains

The single view asked for in
[r4179154126](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179154126).
After decisions 20-24. "Background" means inside the time-sliced read phase, which excludes all
ingestion; "main thread" means outside it.

| Lock | Protects | Writers | Background reader | Main-thread reader |
|---|---|---|---|---|
| word bucket (`RaxTargetMutexPool`, private to `TextIndexSchema`) | a `Postings` btree (`key_to_positions_`) | `CommitKeyData` / `DeleteKeyData`, per word, around `AddKeyToPostings` / `RemoveKeyFromPostings` | none | `WithWordLock(word, lock, fn)`; `fn` seeks one key and copies out `PostingValue`; no `KeyIterator` survives it (20, 21) |
| `text_index_mutex_` (rw) | the global prefix/suffix rax | writer, nested *inside* the word bucket, for `MutateTarget` | none | reader, `LookupGlobalPostings`, for the `find` only |
| `per_key_text_indexes_mutex_` | the per-key `node_hash_map` | writer, for emplace/extract | none | for the `find` only; the walk runs unlocked (no in-flight mutation on the revalidated key, 23) |
| `stem_tree_mutex_` (rw) | the stem rax and every `StemParents` | writer, taken *after* bucket and tree are released | none | reader, `WithStemParents(word, lock, fn)`, held across the variant lookups and their `KeyCount` probes (24) |
| `Tag::index_mutex_` | the tag rax and its bags | ingestion | none (read-side invariant) | `GetTagValueDocCount(lock=true)` / `GetPrefixMatchDocCount(lock=true)`; membership comes from the fetched record, never a bag |
| `mutated_records_mutex_` | `index_key_info_` | every mutation | none | short hold for `N` and `GetDocumentScore` (plan-amendments 17) |
| time-sliced phase mutex | everything above, by phase | writer phase | reader phase, held for the whole query | **never** (plan-amendments 2) |

Allowed reader nesting: `stem_tree_mutex_` (reader) → `text_index_mutex_` (reader) and
`stem_tree_mutex_` (reader) → word bucket, both inside `WithStemParents`' callback (the first
from `LookupGlobalPostings`; the second from `ResolveText`'s `KeyCount` of the stem root and
from every `ProbeWord` in `TermPredicate::Evaluate`), and nothing else. Writers never hold the stem lock while waiting on bucket or tree, so both pairs are
deadlock-free; every other pair is forbidden because writers take bucket → tree nested.

## Decision log

20. **Word locks are owned by the text layer.** `RaxTargetMutexPool` is never exposed outside
    `TextIndexSchema`. Readers get one entry point, `TextIndexSchema::WithWordLock(word, lock, fn)`,
    which runs `fn` under the word's bucket on the main thread and calls `fn` directly in the
    background (where the time-sliced read phase already excludes writers). Removes
    `GetWordLocks()` (text_index.h:182), `WordPostings::lock` (resolved_leaves.h), the
    `word_locks` parameter on `TextPredicate::Evaluate` and its four overrides (predicate.h:209),
    and `ProbePostings`' lock plumbing (predicate.cc:72-91). The `LockMode` enum shrinks to
    `kBackground` / `kMainThread`. Keeping the locking domain in one place is the point; a
    callback is also what the writers effectively do (`CommitKeyData`, `DeleteKeyData` scope a
    `MutexLock` around their postings mutation).
21. **The bucket covers only the btree access; nothing that reads the btree outlives it.**
    What the bucket protects is `Postings::key_to_positions_` (writers of *other* keys carrying
    the word rebalance it). What comes out of a lookup, `PostingValue{FlatPositionMap*, tf,
    doc_len}` (16 trivially-copyable bytes, posting.h:82), is safe after unlocking: rebalancing
    copies the value but never moves the map, and only `RemoveKey(target)` frees it, which cannot
    be in flight for a revalidated key (decision 23). What is *not* safe after unlocking is a
    `Postings::KeyIterator`, which holds btree iterators (posting.h:163-167). So on the main
    thread every probe seeks the target key inside `WithWordLock` and copies out its
    `PostingValue`; no `KeyIterator` is retained. Positional revalidation builds a
    `SingleKeyTermIterator` over the copied `PostingValue`s' position maps (decision 29). The
    entries-fetcher path keeps its multi-key `KeyIterator`s unchanged. This deletes
    `RaxTargetMutexPool::LockAll`, `LockMode::kMainThreadWordLocksHeld`, the positional branch in
    `VerifyFilter` (response_generator.cc:181-197), and the `CHECK` at resolved_leaves.cc:349.
    **Reverses decision 16.** Decision 12's non-nesting rule for the word path is unchanged:
    `WithWordLock` is never entered while holding `text_index_mutex_` or
    `per_key_text_indexes_mutex_`.
22. **The OR + INORDER/SLOP crash is fixed by decision 21, not by the parser flag.** The `CHECK`
    that fires (resolved_leaves.cc:349) exists only because `LockAll` exists; with per-probe
    locking and no retained iterator there is nothing for `kContainsProximity` to gate, so the
    flag's value cannot cause a crash. The suggested fix (set `kContainsProximity` on OR nodes
    built with slop/inorder, filter_parser.cc) is **not adopted**: it patches the symptom and
    the flag would then drive nothing but the proximity metric (search.cc:1582). Whether OR with
    slop should count toward that metric is an open question, separate from this PR. Verify by
    re-running the reported repro (`HSET` during `background_search_completing` pause, OR query
    with `INORDER`, then `SLOP` and prefix/suffix/fuzzy variants) after 21 lands.
23. **The #1489 miss is closed on main by 20/21, and the dependency it rests on is made
    explicit.** The miss ([#1489 thread](https://github.com/valkey-io/valkey-search/pull/1489#discussion_r4157873450)):
    reading a `Postings` btree on the main thread without the word lock. Every main-thread
    btree read now goes through `WithWordLock`. The remaining assumption, that the revalidated
    key itself has no in-flight mutation, holds because `SearchParameters::GetContentProcessing`
    returns `kContentionCheckRequired` whenever `QueryHasTextPredicate` (search.cc:1864) and
    `ResolveContent` then runs `PerformKeyContentionCheck` (content_resolution.cc:39); FT.HYBRID
    returns the same (ft_hybrid.cc:602). `VerifyFilter` gets a `DCHECK` (or, if the parameters
    do not carry enough to assert it, a comment at the top) stating that a text predicate
    implies the contention check ran, so the next person to add a `VerifyFilter` caller sees
    the contract. The 1.2 backport in #1489 is its own PR and not touched here.
24. **Stem variants are read under the stem lock, not copied** (*signed off 2026-10-04*; the
    reviewer listed three options without choosing, this was the implementer's pick). `GetAllStemVariants`
    returns `string_view`s into `StemParents::parents` and releases `stem_tree_mutex_` on return;
    the main-thread resolve then reads them unlocked (resolved_leaves.cc:240-259, use-after-free
    confirmed by ASAN). Fix: `TextIndexSchema::WithStemParents(word, lock, fn)` enumerates the
    variants and runs `fn(stemmed, variants, distinct_docs)` with `stem_tree_mutex_` held
    (reader) on the main thread and unlocked in the background; `ResolveText` does its
    `Lookup`s and `KeyCount`s inside `fn`. One call site, both modes, no copies. **Amends decision
    12**: the reader now nests `stem_tree_mutex_` (reader) → `text_index_mutex_` (reader) inside
    `LookupGlobalPostings`, and `stem_tree_mutex_` (reader) → word bucket from `ResolveText`'s
    `KeyCount` of the stem root and from `TermPredicate::Evaluate`'s probes of the stemmed word
    and each parent. Neither can deadlock because writers take
    `stem_tree_mutex_` only after releasing the bucket and `text_index_mutex_`
    (text_index.cc:277-302 and 364-403), so no writer ever waits on the tree lock while holding
    the stem lock. Supersedes §Stemming's "the cache copies them" in plan-amendments.md, which
    the implementation had already drifted from. Why not the other two: option 1 (copy) adds an
    allocation per variant per leaf for the sake of a lock that is held for a handful of tree
    finds anyway; option 3 (per-target stem locking) adds a second mutex pool and a second
    lock-ordering rule for the same handful of finds. `WordPostings::word` stays: with decision 20
    removing `WordPostings::lock`, the word is what names the bucket when the main-thread scorer
    probes a cached leaf's `Postings` per candidate through `WithWordLock`.
25. **`FuzzySearch::Search` is templated on a match sink; `Expansion::words` is deleted**
    (*signed off 2026-10-04*). This is the reviewer's "template `Search`" option
    ([r4150745090](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4150745090))
    with one addition from the callback idea: the sink may return `false` to stop the walk. The
    sink is a callable, so lambdas pass straight through and `Expansion` gets an `operator()`.

    ```cpp
    template <class Sink>  // bool(absl::string_view word, const InvasivePtr<Postings> &)
    static void Search(const Rax &tree, absl::string_view pattern,
                       size_t max_distance, uint32_t max_words, Sink &&sink);
    // SearchRecursive, at the one emit point:
    if (!sink(new_word, child_iter.GetPostingsTarget()) || ++word_count >= max_words)
      return false;
    ```

    Each caller supplies the sink it needs:
    - The entries fetcher (text.cc) passes `Expansion`, now `{key_iterators, per_term_dt}`
      (`words` and the unread `postings` are gone), which always returns `true`.
    - Per-key evaluation (`FuzzyPredicate::Evaluate`, both modes) passes a lambda that probes
      each match for the target key through `ProbePostings` and returns `true`.
    - `FindExpansionMatch` passes a lambda that probes under `WithWordLock` and returns `false`
      on the first hit.

    Why the sink rather than a plain callback or two plain structs: a template means no indirect
    call per match on the background hot path; `new_word` is alive during the call, so the main
    thread never copies a word (the `std::vector<std::string> words` field, and both fuzzy.h
    comments about it, go away); and the early return gives `FindExpansionMatch` its
    first-match exit (decision 19) without walking up to `max_words` matches first. The
    `words_only` flag goes with the field. The prefix/suffix walks in `TextPredicate::Evaluate`
    / `FindExpansionMatch` already iterate the rax directly and take the same `WithWordLock`
    probe inline, with no struct change.
26. **`Tag::ValueHandle` becomes a bag view beside `BagOfInternedStringPtrs`.** It is
    `BagOfInternedStringPtrs` in borrowed form, so it is defined in `string_interning.h` next to
    the type it views, not in tag.h:182, and `Tag::LookupValue` returns it. The tag.h:199
    comment is corrected to say the read is lock-free only under the background read-side
    invariant and takes `Tag::index_mutex_` otherwise. **Naming** (*signed off 2026-10-04*):
    the new type is `BorrowedBagOfInternedStringPtrs`, matching its sibling
    `BorrowedInternedStringPtr`. The reviewer's preferred `View` convention is applied to both in
    a follow-up PR; the sibling has 74 uses across 14 files of unrelated main code, too much
    churn for this one, and renaming only the new type now would leave the two siblings on
    different conventions, which defeats the consistency the comment asked for.
27. **Planning docs stay in the branch during review and leave before merge.** Decision 14 said
    they stay untracked; they are in the PR diff (five files under `resolved-leaf-cache-docs/`).
    They stay through review so reviewers can read them, and are removed in the final squash
    before merge, once the top-level file's final approval is given.
28. **`PrefilterEvaluator` always has a cache; the null-cache mode is removed from the contract**
    (*signed off 2026-10-04*, including the test change: no code path exists only to serve a
    test). The reviewer's point at vector_base.cc:167 and
    :191 is a design stance, not a tidy-up: no code path exists that does not have a cache, so
    maintaining the pre-cache "walk everything" branch alongside it is exactly the duplication
    the PR was meant to remove. `PrefilterEvaluator` takes `ResolvedLeafCache&`; both
    `Evaluate*` overrides call `GetOrResolve` themselves and hand the leaf to `EvaluateTagLeaf` /
    `EvaluateTextLeaf`, so tag and text resolve at the same level. The `ResolvedLeafCache
    *cache = nullptr` defaults on `PerformVectorSearch` (search.h:388),
    `CalcBestMatchingPrefilteredKeys` (search.h:395) and `ScoreTextQuery` (search.h:422) go
    too; otherwise a test can still construct a null-cache evaluator through them. The one
    consumer of null mode, `filter_test`'s cached-vs-uncached equivalence check
    (filter_test.cc:162), is rewritten to compare the cache path against a direct
    `predicate.Evaluate(text_index, key, require_positions)` call, which is what it was really
    asserting. The main-thread evaluator, `PredicateEvaluator` in response_generator.cc, had the
    same null-cache branch (reachable only for a text predicate on an index without a text
    schema, which the parser never produces) and takes `ResolvedLeafCache&` too.

29. **`TermIterator` is split in two, not templated** (*decided 2026-10-05, after the round-1
    commit*). The first implementation of decision 21 made `TermIterator` a template
    `TermIteratorT<KeyIt>` instantiated over `Postings::KeyIterator` (entries fetcher) and over a
    new `KeyPosting`, one copied-out `PostingValue` wearing the multi-key cursor interface
    (`NextKey` → done, `SkipForwardKey` → compare). It worked and the diff was small, but it
    compiles two full copies of term.cc's key merge, position merge and scoring, on the path whose
    code layout already moved the pure-text benchmark in this PR; and on the per-key side the key
    heap always has every cursor on the same key and `GetScore` is never called (per-key
    evaluation scores through `ResolvedLeafCache::Probe`). The single-key case is the many-key
    case degenerate, so running it through the general machinery is pure overhead, and the only
    thing that genuinely differs between the two paths is the cursor type (live btree iterators
    vs. a copied value), which no non-template merge can span without a branch per step.

    Instead, the position merge becomes the per-key iterator, and the entries-fetcher iterator
    composes it:

    ```
    TextIterator
    ├── TermIterator                       entries fetcher, many keys
    │     key heap over Postings::KeyIterator[]; scoring (tf / doc_len off the cursors)
    │     SingleKeyTermIterator positions_;      by value, concrete type: direct calls (see result)
    │       on each new key: positions_.Reset(current_key, lambda yielding the active cursors' maps)
    │       the five TextIterator position methods forward to positions_
    └── SingleKeyTermIterator (final)            per-key evaluation, one key
          const Key* key_; bool done_;     trivial key API
          position heap over PositionIterator[]: today's seven position methods, moved verbatim
          Reset(key, maps); field mask fixed at construction
    ```

    Rules: `SingleKeyTermIterator` is `final`; the field mask is a constructor argument (it never
    changes; `InsertValidPositionIterator` filters on it alone); `Reset` reuses the inlined
    vectors it already clears today and takes the maps through a template producer callback,
    not a span, so `TermIterator` hands over its active cursors' maps without an intermediate
    array (a 1.6 KB scratch member cost -3% to -5% on the pure-text benchmark before it was
    removed; see implementation-choices.md) and the pure-text path allocates exactly as it does
    now;
    `TermIterator::IsIteratorValid` consults `positions_` only when `require_positions_` is set,
    since `Reset` is never called otherwise. `SingleKeyTermIterator` is only ever built positional
    (every per-key `Evaluate` returns a plain bool first when positions are not required), so it
    drops `require_positions`, `stem_field_mask` and `has_original`: those drove key-level mask
    selection and leaf partitioning for scoring, and `ProbePostings` already applied the mask via
    `GetPostingValue`. `KeyPosting` is deleted; `ProbePostings` and `EvaluateTermLeaf` collect
    `PostingValue::map` pointers. `TermIterator`'s constructor and interface are unchanged, so
    text.cc, search.cc and the tests do not move. Churn: ~115 lines of term.cc re-homed to the
    other class, ~95 added, ~85 deleted; the only new logic is the map gather in
    `FindMinimumValidKey` step 4 and `SingleKeyTermIterator::Reset`.

    Why not the alternatives: a runtime key-source variant adds a branch per key step on the hot
    path; a shared abstract base implementing the position half is the same footprint but leaves
    the per-key path holding a half-iterator nobody can hand out; wrapping through the
    `TextIterator` interface (holding the inner by pointer) adds a virtual hop per position step
    on the proximity path. Why now rather than a follow-up: the template has not been reviewed
    yet, and showing the reviewer one design beats showing the template and then replacing it.

    Lands as its own commit after the round-1 commit so a benchmark shift is attributable:
    baseline on the round-1 commit, refactor with `INORDER`/`SLOP` cases added to
    `MainThreadLockModesMatchBackground` (unreachable there before `LockAll` went), benchmark
    against the baseline, result noted on the PR. The harness is boda26's `search_benchmark.sh`
    from the PR's perf comment, plus a positional scenario set (phrase, `INORDER`, `SLOP 0`,
    `SLOP 2`, OR and prefix with `INORDER`, phrase+num+tag) since none of the eight original
    queries reaches the position merge.

    Result, in two parts. On the eight non-positional scenarios the split is inside the A/A
    noise envelope against the round-1 commit, and against the PR's merge base it reproduces
    the comment (pure text within noise, mixed queries +7% to +34%). On the positional set the
    first build of the split was 1.7% to 6.6% slower than the round-1 commit. `perf stat`
    showed +4.0% instructions at unchanged IPC, so extra code rather than layout: the
    "direct calls" above were only half true. `TermIterator`'s position forwards called the
    concrete `SingleKeyTermIterator` method, but GCC compiled each forward to `add $0x1f68,%rdi; jmp
    SingleKeyTermIterator::X` with the body out of line in term.cc, so a proximity step went
    virtual call → thunk → jump → body where the template went virtual call → body. The
    follow-up commit defines `DonePositions`, `CurrentPosition` and `CurrentFieldMask` in the
    class body so the forwards inline them (`NextPosition` and `SeekForwardPosition` are the
    heap operations and stay in term.cc; the profile attributed the loss to the accessors).
    After the fix: +1.0% instructions, within the A/A spread; every positional row is inside
    the noise envelope against the round-1 commit and 1.5% to 6.4% faster than the merge base.
    Tables and the per-run counters in implementation-choices.md §Verification (decision 29).

## Comment → resolution map

Every review thread on #1472 as of 2026-10-04, once. See §Review scope above for what has not
been reviewed yet.

| Thread | Anchor | Resolution |
|---|---|---|
| [r4148984400](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4148984400) boda26, OR+INORDER crash | response_generator.cc:186 | Decision 22 (via 21) |
| [r4179248252](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179248252) BCathcart, revalidate after locking rework, see #1489 | response_generator.cc:186 | Decisions 22, 23 |
| [r4149195918](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4149195918) boda26, stem variant UAF | resolved_leaves.cc:240 | Decision 24 |
| [r4179196234](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179196234) BCathcart, three stem options | resolved_leaves.cc:240 | Decision 24 (option 2) |
| [r4150745090](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4150745090) `Expansion` field roles / template `Search` | fuzzy.h:27 | Decision 25 |
| [r4150745424](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4150745424) "main thread revalidation only" | fuzzy.h:38 | Moot after decision 25 (field deleted) |
| [r4150745694](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4150745694) `words` as `InlinedVector` | fuzzy.h:39 | Moot after decision 25 (field deleted) |
| [r4151050010](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4151050010) skeptical of `LockAll` | rax_target_mutex_pool.h:55 | Decision 21 |
| [r4157085170](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4157085170) don't expose the pool; `WithWordLock` | text_index.h:182 | Decision 20 |
| [r4178971533](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4178971533) comment suggestion on main-thread reads | text_index.h:176 | implementation-choices.md §Review round 1: the suggested framing ("we avoid stalling the main thread" as a choice, not "cannot wait" as a constraint) is kept; only the `GetWordLocks()` reference is updated for decision 20 |
| [r4179154126](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179154126) hide locks in the text layer; discuss holistically | predicate.h:209 | Decision 20 and §Lock domains |
| [r4157245842](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4157245842) `ValueHandle` placement; `View` convention | tag.h:182 | Decision 26 |
| [r4179081795](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179081795) "now conditionally lock free" | tag.h:199 | Decision 26 (comment) |
| [r4178992577](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4178992577) comment suggestion on Adopt/Release | tag.cc:355 | implementation-choices.md §Review round 1 (accepted as written) |
| [r4179117341](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179117341) `EvaluateTags` duplication | vector_base.cc:167 | Decision 28 |
| [r4179118667](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4179118667) `EvaluateText` duplication; `GetOrResolve` level | vector_base.cc:191 | Decision 28 |
| [Perf analysis](https://github.com/valkey-io/valkey-search/pull/1472#issuecomment-5904523663) BCathcart, 2026-09-30 | `DoSearchNonVector` | implementation-choices.md §Review round 1 (lazy cache when `score_in_drain`); the `-falign-functions=64` build flag is a separate PR |

## Resolution order

Dependencies, not a task list. Each step compiles and passes the existing suites on its own.

1. Decision 20 (`WithWordLock`, remove the pool from every public surface). Mechanical; the
   main-thread positional path temporarily keeps `LockAll` behind the schema.
2. Decision 21 (copy-out `PostingValue`, single-key `TermIterator` source, delete `LockAll` and
   `kMainThreadWordLocksHeld`). Then re-run the decision 22 repro.
3. Decision 24 (`WithStemParents`). Re-run boda26's ASAN stem repro.
4. Decision 25 (inline expansion probes, delete `Expansion::words`).
5. Decision 23 (`VerifyFilter` DCHECK / contract comment).
6. Decision 26 (`ValueHandle` → view type in string_interning.h; comment fix).
7. Decision 28 (evaluator contract; drop null-cache defaults; rewrite the `filter_test` check).
8. implementation-choices.md §Review round 1 cleanups.
9. Verification per plan-amendments.md decision 10 plus the two repros above, then
   `WITHSCORES` spot checks on the main-thread positional path (phrase, `INORDER`, `SLOP` with a
   mutated document), since that path's iterator source is new.
10. Decision 29 (`TermIterator` / `SingleKeyTermIterator` split), second commit: benchmark baseline on
    the round-1 commit first, then the refactor, then benchmark again.

## Superseded

- plan-amendments.md decision 16 (reversed by 21); decision 12's "never nests" (amended by 24
  to "nests only stem → tree"); §Stemming "the cache copies them" (24); §Phase 4 table row
  "`GetPostingDocStats` / `GetKeyCount` on a `Postings`" guard column now reads "via
  `WithWordLock`" (20); decision 14's "planning docs stay untracked" (27).
- Decision 21's "single-key source" sentence as first implemented (`TermIteratorT<KeyIt>` +
  `KeyPosting`): replaced by 29.
- implementation-choices.md §Expansion representative, the `collect_words` bullet (25);
  §Background filtering, the "a null cache is allowed and means walk everything" bullet (28);
  §Tag bags, the `ValueHandle` placement bullet (26).
