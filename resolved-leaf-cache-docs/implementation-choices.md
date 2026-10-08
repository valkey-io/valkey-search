# Implementation choices — resolved-leaf cache

Fine-grained decisions made while implementing
[plan-amendments.md](plan-amendments.md)
(which wins over this doc, as [top-level-read-first.txt](top-level-read-first.txt)
wins over it). Only choices that a reviewer might reasonably question are recorded; anything that
follows directly from the amendments is not repeated.

## Choices

### Expansion representative

- **Only the scoring fallback populates the representative; the filter walk does
  not.** Amendment 6 allows either walk to populate. Populating from
  `TextPredicate::Evaluate` would mean threading a sink through the four
  `Evaluate(text_index, key, require_positions)` overrides and every caller; the
  saving is one extra per-key walk per query (the first scored candidate, before
  any representative exists). Not worth the plumbing.
- The fallback returns the first matching word and stops (`FindExpansionMatch`);
  see amendment 19. It returns the document's `PostingValue` and the word's key
  count with it, read in the one locked section, and `ExpansionLeaf::Term` keeps
  its key count: round 2 review found the match being probed again for tf and
  doc_len and both counts re-read, four btree lookups where one does.
- `FuzzySearch::Search` gained an opt-in `collect_words` so the fallback can name
  the matched word (needed to store `WordPostings` and, in Phase 4, to pick the
  bucket lock). Off by default: 200 inline `std::string`s would have been ~6KB
  on the in-iterator hot path, so the words live in a plain `std::vector` that
  stays empty unless asked for.
- `TextPredicate::GetTextString()` became a pure virtual: all five subclasses
  already defined it identically and kind-generic code needs it.
- No golden re-baseline was required: the existing expansion tests either use
  single-match documents or accept any matched term.

### Background filtering through the cache

- **`EvaluateTextLeaf` is a free function taking a `FunctionRef` for the per-key
  index**, shared by `PrefilterEvaluator` and (Phase 3) `PredicateEvaluator`,
  rather than a method on the cache. The cache should not know how each caller
  fetches or locks the per-key tree; the callback keeps that at the call site and
  keeps the fetch lazy.
- **`PrefilterEvaluator` owns the lazy per-key fetch** (reset per `Evaluate`),
  taking a `TextIndexSchema*` + `ResolvedLeafCache*` instead of a pre-fetched
  `TextIndex*`. A null cache is allowed and means "walk everything", which is
  what `filter_test` uses to check cached vs uncached verdicts agree on all 150
  cases (including the positional ones), so no separate equivalence fixture was
  written.
- `TextIndexSchema::GetPerKeyTextIndex` became `const`: it is a pure read and
  the guard mutex was already `mutable`.
- `EvaluateTermLeaf` uses the predicate's raw masks, not the cached
  `ScoringFieldMask`-collapsed ones, because `TermIterator::QueryFieldMask()` is
  intersected across AND children.
- The cache is constructed unconditionally in `DoSearchNonVector` and
  `DoSearchVector` (an empty `node_hash_map`), not gated on the query having
  text/tag leaves. Gating would save nothing measurable.

### Tag bags

- **`Tag::ValueHandle` wraps the bag storage bits behind `Contains`/`DocCount`**
  rather than exposing `uintptr_t` + `Adopt`/`Release` to query code. The
  borrow-then-release dance stays in tag.cc, and the existing `ContainsKey` /
  `GetTagValueDocCount` now go through it, so there is one implementation of it.
- **The handle is stored in `TagLeaf` unconditionally** rather than gated on a
  background/main-thread mode flag. In Phase 3 the main-thread cache will still
  resolve `dt` through `LookupValue` (one lookup per value per reply under
  `Tag::index_mutex_`), but its evaluator and scorer read membership from the
  record and never call `handle.Contains`; the handle is simply unused there.
  A mode flag that only removes an 8-byte field is not worth the branch.
- `EvaluateTagLeaf` runs the prefix fallback through the existing
  `TagPredicate::Evaluate(tags, case_sensitive)`, which re-checks the exact
  values too. They already missed, so the redundant compares cost nothing and
  the prefix logic is not duplicated.
- The tag equivalence matrix from the plan is covered by extending
  `filter_test`'s parameterized cases (each already runs cached and uncached)
  rather than a dedicated fixture: prefix-only, exact-miss/prefix-hit, UTF-8
  case folding, and key-without-field were the missing rows.
- `PrefilterEvaluator::EvaluateTags` falls back to the uncached parse when the
  resolved leaf is `monostate` (a `kTag` predicate that is not a `TagPredicate`,
  which exists in tests).

### Main-thread lock primitives

- **`LockMode` has three values, fixed per cache**: `kBackground`,
  `kMainThread` (per-probe bucket locks), `kMainThreadWordLocksHeld` (caller
  holds every bucket via `RaxTargetMutexPool::LockAll`, for positional queries).
  A per-query mode is simpler than a runtime "all held" toggle, since whether a
  query is positional is known before any document is revalidated.
- **Holding all buckets while taking `text_index_mutex_` as a reader is safe**
  even though amendment 12 says the reader never nests: writers take bucket
  first and tree second, so a reader holding every bucket then the tree follows
  the writers' order and no writer can be inside the tree lock at the time.
  `stem_tree_mutex_`, `per_key_text_indexes_mutex_`, `Tag::index_mutex_` and
  `mutated_records_mutex_` are all leaf locks for their writers (never held
  together with a bucket), so taking them while holding all buckets is also
  safe. Verified by reading `CommitKeyData`/`DeleteKeyData`/`index_schema.cc`.
- **The bucket pointer is stored on `WordPostings`** (`absl::Mutex *lock`,
  null unless `kMainThread`), computed once at resolve, so a probe does not
  re-hash the word. `KeyCount()` / `ProbeDocStats()` are the only two places
  that take it.
- **Per-key walks receive a nullable `RaxTargetMutexPool*`** threaded through
  `TextPredicate::Evaluate` and `FindExpansionMatch`; one shared
  `ProbePostings` helper in predicate.cc does the probe and the lock. Retaining
  an iterator under a per-word lock is a `CHECK` failure: positional queries
  must use `kMainThreadWordLocksHeld`, where no per-probe lock is passed.
- **`FuzzySearch::Search(words_only)`** returns words and postings but no key
  iterators: opening one reads the btree, which on the main thread must happen
  under the word's bucket. Background fuzzy evaluation also moved to building
  its iterators from `postings` (three pointer copies each) so there is one
  loop instead of two.
- **`SingleDocumentScorer`'s corpus stats moved into the cache** (`CorpusStats`
  + `NeedsDocLen`/`AvgDocLen`/`Scorer`) as the first step of amendment 9;
  `ScoreContext` shrank to the cache plus per-document state, and the
  main-thread branches for doc length / doc score / N live there. Task 6 deletes
  the class.
- `IndexSchema::GetTotalDocumentLength` lost its `SHARED_LOCKS_REQUIRED`
  annotation: it reads an atomic.
- `Tag::GetTagValueDocCount` / `GetPrefixMatchDocCount` gained a `lock`
  parameter (default false) rather than `*Locked` twins, matching the existing
  `GetKeyDocLen(key, lock)` convention.

### Main thread off the time-sliced mutex

- **`SingleDocumentScorer` is replaced by a free function
  `RecomputeDocumentScore(schema, root, key, cache, record_tags)`** (amendment
  9). The cache already carries corpus stats and the scorer, so there was
  nothing left for a class to hold; `VerifyFilter`'s out-parameter is now the
  `unique_ptr<ResolvedLeafCache>` itself.
- **Main-thread tag membership is read from `RecordTags`**, a small
  memoizing parser over the fetched `RecordsMap` shared by the filter and the
  rescore, so each field is parsed at most once per document and scoring does
  not depend on which leaves the filter happened to visit (an OR short-circuits
  past later tag siblings). Membership is an `EqualsIgnoreCase` loop, the same
  one the filter runs. A field absent from the record scores its leaf 0.
- **Prefix tag `dt` on the main thread still comes from
  `GetPrefixMatchDocCount`**, which picks the representative from the index's
  tags, not the record's. Amendment 7 dropped the prefix-tag fix as #1439's
  concern; only the lock (`Tag::index_mutex_`) was added.
- **Positional queries hold `LockAll` across both the evaluation and the
  rescore** in one scope, rather than acquiring 256 buckets twice.
- The positional decision is per query (`kContainsProximity`), so a query with
  only tag/numeric leaves still gets `kMainThread`; the mode only matters for
  text leaves anyway.
- Tests: `MutatedTextDocumentRevalidatedUnderWriterLock` runs the reply path
  while a writer holds the time-sliced mutex, which is the direct proof the
  main thread no longer waits on it (the old protocol would deadlock).
  `MutatedTagDocumentFollowsRecord` shows a document whose record dropped the
  queried tag is now rejected on revalidation even though the index still lists
  it, and `RecomputeScoresTagsFromRecord` pins the rescore to the record.

### Verification (Task 7)

- clang-format applied to all 26 changed/new C++ files via the devcontainer.
- clang-tidy (CI's thread-safety config) on the touched TUs: three new
  findings in `ScoreContext::DocLen/DocScore` and `ReadCorpusStats`, where the
  analyzer cannot see that the `LockMode` branch encodes the caller's lock, are
  suppressed with `ABSL_NO_THREAD_SAFETY_ANALYSIS` and one comment. Two
  remaining findings in `search.cc` (`ComposeDocumentScore` drain at line
  ~1217, `PopulateIndexMutationSequenceNumbers` at ~1501) are on unchanged
  main code and left alone. CI pipes tidy through `tail`, so it is
  informational there anyway.
- Unit tests: all 13 binaries pass in the container build.
- Integration (devcontainer, `-k` over cancel, fulltext, scoring,
  stale_score_after_mutation, non_vector, filter_expressions, ft_hybrid,
  vector_mutation_rescore, postfilter, untracked_key_in_reply, query_parser,
  aggregate_addscores): 239 passed, 8 skipped, 3 failed. The failures are all
  `TestFtHybridCosineNegativeDistance`, which asserts the sign of a cosine
  self-distance rounding residue (`5.96e-08` here vs the expected negative
  epsilon) from the vector kernel on this host's `-march=nehalem` build;
  nothing in this change touches vector distance.
- Ad-hoc server smoke (scratch, not committed): term/stem/prefix/fuzzy/tag/
  tag-prefix/phrase/negation/OR-with-tag `WITHSCORES`, then HSET mutations and
  re-search through revalidation; scores monotone and sane, mutated tag doc
  dropped from `@color:{red}`, no CHECK failures in the server log.
- Single signed-off commit `116989c` on `brennan-caching-and-locking-improvement`.

### Review round 1

Cleanups from the PR #1472 review that are not design decisions. The design decisions are in
[review-1-amendments.md](review-1-amendments.md), which wins over this section.

- **`PrefilterEvaluator` contract change is decision 28** in review-1-amendments.md, not here.
  Mechanical consequences recorded for the implementer: `cache_` becomes a reference, the
  `dynamic_cast` in `ResolveTag` stays (it is the one-time resolve, not the per-key path), and
  `EvaluateTextLeaf`'s `FunctionRef` for the per-key index is unchanged.
- **`ResolvedLeafCache` in `DoSearchNonVector` is built lazily** (search.cc:1086): when
  `score_in_drain` is true the drain scores through the `TermIterator` and the cache is never
  touched, yet it was constructed eagerly on the pure-text hot path. Reverses the "constructed
  unconditionally, gating would save nothing" bullet in §Background filtering for this one site;
  the perf analysis on the PR measured the eager construction as a real (if small) cost.
  `DoSearchVector` (search.cc:1267) stays eager: the vector path always filters through it.
- **Comment fixes.** text_index.h:172-176 takes the suggested framing ("we avoid stalling the
  main thread ... and instead take the same short locks the writers take" — a design choice, not
  a constraint); only the `GetWordLocks()` reference changes, to `WithWordLock`, per decision 20.
  tag.cc:354 takes the suggested wording verbatim. tag.h:199 per decision 26. The two fuzzy.h
  comments are deleted with the field they describe (decision 25).
- **Not done here:** `-falign-functions=64` in `cmake/Modules/valkey_search.cmake` (benchmark
  reproducibility across code-size shifts), and the `Borrowed*` → `*View` rename. Both are
  follow-up PRs.

#### Choices made while implementing decisions 20-28

- **`WordPostings::word` is kept** (decision 24 says it goes). On the main thread the scorer
  probes a cached leaf's `Postings` per candidate, and `WithWordLock` needs the word to pick the
  bucket. The lock went (decision 20); the name stays.
- **The main-thread reader also nests `stem_tree_mutex_` (reader) → word bucket**, not only
  stem → `text_index_mutex_` as decision 24's lock table says: `ResolveText` does its `Lookup`s
  and `KeyCount`s inside `WithStemParents`' callback. Deadlock-free by the same argument:
  writers take the stem lock only after releasing both the bucket and the tree lock. The lock
  table in review-1-amendments.md is left for its author to amend.
- **Per-key `TextPredicate::Evaluate` uses the single-key iterator in both modes.** The
  overrides build a `SingleKeyTermIterator` over the copied-out position maps in the background too,
  so there is one per-key probe path instead of a `KeyIterator` variant beside it. The
  entries-fetcher path (`BuildTextIterator`) keeps its multi-key `TermIterator`, which embeds a
  `SingleKeyTermIterator` for the positions of the current key (decision 29).
- **`lock` is a plain `bool`** on `WithStemParents` and `TextPredicate::Evaluate`, following
  `GetKeyDocLen(key, lock)` / `GetPerKeyTextIndex(key, lock)` / `GetTagValueDocCount(value, lock)`.
  No default, so every caller states its mode. `WithWordLock(word, fn)` takes no flag and always
  locks; its three callers (`ResolvedLeafCache::Probe`, `::KeyCount`, `ProbePostings`) already
  hold the decision and branch themselves. Round 2 review: the earlier
  `WithWordLock(word, lock, fn)` read as a contradiction when `lock` was false.
- **`WithStemParents` is a non-template taking `absl::FunctionRef`** (one call per leaf resolve);
  `WithWordLock` is a template (per-probe path). The unused `stem_enabled_mask` parameter of
  `GetAllStemVariants` has no counterpart.
- **`ResolveText` `CHECK`s the predicate's schema is the cache's.** The cache now owns the schema
  pointer for `WithWordLock`; a predicate from another schema would silently probe
  the wrong trees.
- **Decision 23 is a comment, not a `DCHECK`.** FT.HYBRID arm `SearchParameters` report
  `kNoContent` because the contention check ran on the enclosing `ArmGate`, so the parameters
  passed to `VerifyFilter` cannot assert it.
- **The fuzzy sink is a callable, not a struct with `Add`**: `sink(word, postings) -> bool`, so
  lambdas pass straight through and `Expansion` just gets an `operator()`. `Expansion::postings`
  is deleted with `words`: it served the pre-cache scoring path and nothing read it.
- **`PrefilterEvaluator::EvaluateTags` is `std::get<TagLeaf>`**, no monostate fallback: a
  `TagPredicate` with an index always resolves to a `TagLeaf`, and `TagPredicate::Evaluate` is
  the only caller. `ResolveTag`'s monostate stays for a `kTag` predicate that is not a
  `TagPredicate` (a mock), which the scorer skips.
- **`PredicateEvaluator` (response_generator.cc) takes `ResolvedLeafCache&` too.** Its
  null-cache branch was the main-thread twin of the one decision 28 removes, reachable only for
  a text predicate on an index without a text schema, which the parser never produces.
- **`DoSearchVector` builds the cache before the no-predicate branch.** `ReadCorpusStats` is a
  few atomic reads; gating it there would be noise. `DoSearchNonVector` gates on
  `!score_in_drain`, which implies `!requires_prefilter_evaluation`, so neither
  `EvaluatePrefilteredKeys` nor `ScoreTextQuery` runs without it.
- **`filter_test`'s equivalence check** compares the cache path against a direct
  `TextPredicate::Evaluate(per_key_index, key, false, /*lock=*/false)` only when the root is a
  `TextPredicate`; composed roots keep the warm-cache re-evaluation assertion. A composed walk
  would need a second evaluator, which is what decision 28 deletes.

#### Verification (review round 1)

- Unit tests: all 21 host binaries pass (`.build-debug`, host gcc).
- Repro, committed as `test_revalidation_complex_text_queries_match_steady_state` in
  `test_fulltext_inflight_blocking.py` (one server, one mutation, eight queries): a document
  mutated while the query sits at `background_search_completing` is revalidated on the main
  thread (`search_predicate_revalidation` asserted to increment) for OR + `INORDER`, `INORDER`
  and `SLOP` phrases, prefix/suffix/fuzzy ORs and a stem query; keys and the mutated document's
  `WITHSCORES` value equal the steady-state query. Passes on the debug build and on a fresh
  `--asan` build (`halt_on_error=1`, no sanitizer output), which covers decision 24's
  use-after-free; the stem add/delete race itself has no deterministic hook, so ASAN is its
  check.
- `SingleKeyTermIterator` holds `const Key*`, not a `Key`: the target key outlives every per-key
  iterator, and a copy would cost an atomic pair per retained match.
- Integration (devcontainer, `-k` over cancel, fulltext, scoring, stale_score_after_mutation,
  non_vector, filter_expressions, ft_hybrid, vector_mutation_rescore, postfilter,
  untracked_key_in_reply, query_parser, aggregate_addscores, inflight, text): 250 passed, 10
  skipped, 3 failed, the same `TestFtHybridCosineNegativeDistance` rounding residue as before.
- clang-format (devcontainer, `--dry-run --Werror`) clean on every changed file. clang-tidy on
  the touched TUs reports only the findings recorded under Task 7 plus brace style the file
  already uses.
- Not run: the full integration suite and the C++ `testing/integration` harness. The known tag
  rescore gap (#1439, PR #1491) is out of scope and follows this PR.

#### Choices made while implementing decision 29

- **`Reset` takes a producer, not a span.** The first cut gathered the active cursors' maps into
  a `PositionMaps` member of `TermIterator` and passed it as a span; that added an
  `InlinedVector<ptr, 200>` (1.6 KB) to every `TermIterator`, which is zero-filled per query,
  and the pure-text benchmark moved -3% to -5% against the template commit. `Reset(key,
  for_each_map)` is a template over a callable that yields `const FlatPositionMap&`; the
  entries-fetcher path passes a lambda over `current_key_indices_` and the per-key constructor a
  lambda over its span. No array, no indirect call; `sizeof(TermIterator)` is 29,792 bytes
  against 29,760 at the template commit.
- **`Postings::KeyIterator::GetPositionMap()`** is the new primitive; `GetPositionIterator()`
  wraps it. `textinfocmd.cc` and `posting_test.cc` still use the latter.
- **`SingleKeyTermIterator::NextKey` clears position state** so a per-key iterator that is advanced
  reports `DonePositions()` like an exhausted `TermIterator`. `TermIterator::ClearKeyState`
  calls it only when positions were required; otherwise `positions_` was never reset.
- **`MainThreadLockModesMatchBackground` rows all use the `-@rating:[500 600]` form.** The
  fixture's numeric index holds no records, so `@rating:[0 100]` never matches and the five
  rows that used it only ever compared scores of non-matching documents. With the negated range
  the positional rows (`INORDER`, `SLOP 0`/`2`, prefix, suffix and fuzzy with `INORDER`/`SLOP`,
  OR with `INORDER`) produce true verdicts with scores to compare.
- **`SingleKeyTermIterator::GetScore` stays a constant stub with a comment**, not a `DCHECK(false)`:
  nothing calls it today, but a per-key iterator is returned through `EvaluationResult` and a
  composite asking its children for scores would crash rather than get the pre-scoring
  fallback.

#### Verification (decision 29)

- Unit tests: all 21 host binaries pass (`.build-debug`).
- Benchmark: boda26's `search_benchmark.sh` (the method behind the PR's perf comment), release
  builds in the devcontainer, both sides compiled with `-falign-functions=64 -falign-loops=32`
  as the comment did, 1000 docs, 500 connections, median of 7 interleaved paired runs, server
  pinned to one core, client to two others, nothing else running. Three binaries: `main`
  (cc6cb5e, the PR's merge base), `template` (7595926, round-1 commit), `refactor` (this
  commit's text/query files on 7595926). An A/A run of `template` against itself gives the
  noise floor on this host: rps -2.0% to +0.1%, with the candidate side consistently low (it
  runs second in each pair).

  | Scenario | main rps | template rps | Δ | refactor rps | Δ vs main | Δ vs template |
  |---|---|---|---|---|---|---|
  | single word | 9707 | 9846 | +0.9% | 9878 | +1.3% | -0.3% |
  | two words (AND) | 5890 | 6081 | +1.6% | 5986 | +1.8% | -1.8% |
  | three words (AND) | 3886 | 4011 | +3.0% | 4010 | +2.9% | +0.6% |
  | OR of two | 6514 | 6556 | +2.3% | 6612 | +0.3% | -0.7% |
  | OR of three | 4732 | 4814 | +3.1% | 4791 | +1.5% | -1.4% |
  | text+num+tag | 6270 | 6915 | +10.6% | 6768 | +8.9% | -0.3% |
  | 2w AND+num+tag | 4694 | 5007 | +7.6% | 4975 | +7.2% | -1.2% |
  | 2w OR+num+tag | 1868 | 2539 | +35.9% | 2560 | +34.2% | +1.5% |

  Δ columns are the median paired delta of their own run (template vs main, refactor vs main,
  refactor vs template), so they do not equal the ratio of the displayed medians; refactor rps
  shown is from the refactor-vs-main run. Template vs main reproduces the PR comment (pure text
  within noise, mixed queries +8% to +36%). Refactor vs template is inside the A/A envelope on
  every row. `libsearch.so` shrinks by 133 KB (one fewer `TermIterator` instantiation).

  None of those eight queries is positional, so a second scenario set makes every text leaf
  positional (`search_benchmark_pos.sh`, same method, `EXTRA_ARGS` per row for the command
  flags). That set found a regression the first did not:

  | Scenario | template rps | refactor rps | Δ vs template | Δ vs main |
  |---|---|---|---|---|
  | phrase `"apple banana"` | 4182 | 4002 | -4.7% | -3.0% |
  | two words INORDER | 3839 | 3724 | -3.1% | +0.2% |
  | two words SLOP 0 | 3089 | 2979 | -3.9% | -4.7% |
  | two words SLOP 2 | 3505 | 3263 | -6.6% | -5.7% |
  | OR of two INORDER | 6647 | 6441 | -1.8% | +2.0% |
  | prefix+word INORDER | 4789 | 4699 | -1.7% | -4.1% |
  | phrase+num+tag | 3862 | 3732 | -4.3% | -2.1% |

  `perf stat` over the server threads for 200K phrase requests, three runs each: template
  165.3G cycles / 554.3G instructions, refactor 173.5G / 576.3G. Instructions +4.0%, IPC
  unchanged (3.35 vs 3.32), branch misses flat: extra code executing, not layout. The profile
  showed the `ProximityIterator` methods up by 1-2 points each and a new
  `TermIterator::DonePositions` at 1.1% self time. `TermIterator`'s position forwards had
  compiled to `add $0x1f68,%rdi; jmp SingleKeyTermIterator::X`, a thunk to the out-of-line body in
  term.cc, so every proximity step paid virtual call → thunk → jump → body where the template
  had virtual call → body. Fixed in the commit after the split by defining `DonePositions`,
  `CurrentPosition` and `CurrentFieldMask` in the class body so the forwards inline them:
  559.9G instructions (+1.0% vs template, within the A/A spread). Positional set after the fix,
  same method, fresh runs against both baselines:

  | Scenario | template rps | fixed rps | Δ vs template | main rps | fixed rps | Δ vs main |
  |---|---|---|---|---|---|---|
  | phrase `"apple banana"` | 4120 | 4120 | +0.3% | 4014 | 4168 | +4.8% |
  | two words INORDER | 3865 | 3829 | -1.3% | 3749 | 3845 | +3.2% |
  | two words SLOP 0 | 3130 | 3110 | +0.5% | 2990 | 3123 | +4.5% |
  | two words SLOP 2 | 3450 | 3472 | +0.4% | 3363 | 3467 | +3.3% |
  | OR of two INORDER | 6557 | 6683 | +1.5% | 6632 | 6672 | +1.5% |
  | prefix+word INORDER | 4747 | 4849 | +0.6% | 4684 | 4889 | +4.0% |
  | phrase+num+tag | 3840 | 3791 | -1.4% | 3662 | 3884 | +6.4% |

  Against the template every row is inside the A/A envelope. Against main every positional
  row is faster, by about the same margin the template showed before the split, so the PR's
  positional path carries no regression relative to the merge base.

### Review round 2

Decisions 30 and 31 of [review-2-amendments.md](review-2-amendments.md), which wins over this
section.

- **`resolved_leaf.h` includes `text_index.h`** for `kStemVariantsInlineCapacity`, and
  `predicate` links `text`. `text` does not link `predicate` or `resolved_leaves`, so no cycle.
  `indexes::Tag` is forward-declared; `TagLeaf` only holds a pointer to it.
- **The `search_term_predicate` pausepoint fires once per query**, in `ResolveText`, not per key.
  It used to fire on both routes; the per-key one went with the tree-walk overload.
  `test_cancel.py` only needs the query to pause somewhere mid-search, which the resolve does.
- **`ProbeWord` is deleted.** It existed to look a word up in a tree before probing; a `TermLeaf`
  already holds the postings, so `TermPredicate::Evaluate` calls `ProbePostings` directly.
- **`EvaluateText(cache, predicate, key, …)` in `resolved_leaves.h` is the shared dispatch**, so
  the two evaluators do not repeat the leaf-type branch. It fetches and dispatches only; the
  verdict is the predicate's.
- **The dispatch `static_cast`s on the leaf type.** `ResolveText` returns a `TermLeaf` only for a
  `TermPredicate`, and every other `TextPredicate` is an `ExpansionPredicate` (checked: no other
  subclass exists in `src` or `testing`). The `dynamic_cast` chain in `ResolveText` stays until
  decision 32 gives the predicate a kind.
- **The `filter_test` cross-check against the tree walk is gone** with the tree walk. Main vs
  background equivalence is `MainThreadLockModesMatchBackground` in `search_test.cc`.

### Merge with main (#985 Vector Range)

- **`SearchVectorRangeQuery` gets its own `ResolvedLeafCache`**, built in `Search()` and shared
  by its prefilter drain and `ApplyRelevanceScore`, the same shape as `DoSearchNonVector`.
  #985 added a third drain loop that builds a `PrefilterEvaluator` per key; the merge hoists
  that evaluator as the other two loops do. The loop itself is not folded into
  `EvaluatePrefilteredKeys` here because the VR distance rides on the `EvaluationResult` and
  the appender there only sees the key; that is the one design change drain unification needs,
  and it belongs with the existing `DoSearchNonVector` TODO in its own PR.
- **`PrefilterEvaluator::Evaluate` now routes through `EvaluateFull`.** #985's `EvaluateFull`
  set `key_` without clearing `per_key_index_fetched_`; `Evaluate` had its own reset.
  Harmless with #985's per-key evaluator, wrong once the merge hoisted the evaluator in
  `SearchVectorRangeQuery`, the only `EvaluateFull` caller: every later candidate of a
  compound VR query would evaluate expansion leaves (prefix/suffix/fuzzy) against the first
  candidate's tree. Terms, and phrases of terms, are answered from the shared postings and
  never reach that tree. A stale tree is wrong only when the key matches through a word the
  first key lacks, since the tree's words are probed against the shared postings
  (`ProbePostings` → `GetPostingValue`); a first draft of the tests used one shared matching word
  and passed against the bug for that reason. Neither parent had the bug; the merge would
  have. The reset now lives in `EvaluateFull` and protects both entry points.
  `EvaluateFullRefetchesPerKeyTreeForEachKey` (unit) and `test_vector_range_and_prefix`
  (integration, one matching word per candidate, VR + negated prefix) both fail without it
  and pass with it.
- **VR leaves still score 0.** `ScoreNode` is the scoring walk under `ScoreDocument` on both
  the background (`ApplyRelevanceScore`) and main-thread (`RecomputeDocumentScore`) paths, and
  #985's `kVectorRange → 0.0f` case merged into it. `ResolvedLeafCache::GetOrResolve` returns
  `monostate` for a VR predicate but is never asked.
- **Evaluator constructors take the union**: `(TextIndexSchema*, ResolvedLeafCache&,
  QueryOperations, const IndexSchema*)`; the last is #985's and resolves a VR alias to its
  vector index.

