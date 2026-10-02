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
  see amendment 19.
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
