# Review amendments — PR #1472, round 2 (review 2)

Continues [review-1-amendments.md](review-1-amendments.md) (decisions numbered from 30). Where
this doc disagrees with any earlier doc, **this doc wins**.
[top-level-read-first.txt](top-level-read-first.txt) wins over everything.

Source: review threads
[r4214394254](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4214394254) and
[r4214397791](https://github.com/valkey-io/valkey-search/pull/1472#discussion_r4214397791)
(2026-10-08) and the discussion that followed.

## The problem

On main every predicate follows one rule: the evaluator fetches the key's data, the predicate
decides. The cache broke that for Term and Tag: free functions in `resolved_leaves.cc`
(`EvaluateTermLeaf`, `EvaluateTagLeaf`, `EvaluateTextLeaf`) decide, and
`TermPredicate::Evaluate(tree, key, …)` is dead in production.

```
main       evaluator fetches tree/tags   →  predicate.Evaluate(data, key)
PR today   evaluator fetches leaf        →  EvaluateTermLeaf(leaf, key)      (free fn)
                                            TermPredicate::Evaluate(tree)    (dead)
```

## Decision log

30. **Evaluation stays in the predicate; the cache is what the evaluator fetches.**
    - `TermPredicate::Evaluate(const TermLeaf&, key, require_positions, lock)` replaces the
      tree-walk overload, which is deleted. Body is today's `EvaluateTermLeaf`, probing through
      the existing `ProbePostings`.
    - `TagPredicate::Evaluate(const TagLeaf&, key)` replaces `EvaluateTagLeaf`: bag probe, then
      the existing `Evaluate(tags)` for prefix values. Only `PrefilterEvaluator` uses it; the
      main-thread `PredicateEvaluator::EvaluateTags` keeps reading tags off the fetched record.
    - New `class ExpansionPredicate : public TextPredicate` owns the tree-walk virtual
      `Evaluate(tree, key, require_positions, lock)`; Prefix, Suffix, Infix and Fuzzy derive from
      it. `TextPredicate` has no `Evaluate(data, …)` of its own, so no predicate carries a method
      it cannot honor.
    - Each evaluator's `EvaluateText` does `leaf = cache.GetOrResolve(pred)`, then
      `TermLeaf → term.Evaluate(leaf, …)`, otherwise `expansion.Evaluate(per_key_tree, …)`.
      `EvaluateTextLeaf` is deleted.
    - The leaf structs (`WordPostings`, `TermGroup`, `TermLeaf`, `ExpansionLeaf`, `TagLeaf`,
      `ResolvedLeaf`) move to a structs-only header `src/query/resolved_leaf.h` so `predicate.h`
      can include them without depending on the cache. `resolved_leaves.{h,cc}` keeps
      `ResolvedLeafCache`.
    - The `filter_test` cross-check against the deleted tree walk is removed;
      `MainThreadLockModesMatchBackground` covers main/background equivalence.

    End state, one rule for all four kinds:
    ```
    Numeric    value = GetValue(key)              →  NumericPredicate::Evaluate(value)
    Tag        leaf  = cache.GetOrResolve(pred)   →  TagPredicate::Evaluate(leaf, key)
    Term       leaf  = cache.GetOrResolve(pred)   →  TermPredicate::Evaluate(leaf, key)
    Expansion  tree  = PerKeyTree(key)            →  ExpansionPredicate::Evaluate(tree, key)
    ```
    Filtering only reads. The one lazily built value, the expansion representative, has one
    writer: scoring (`ScoreLeaf`).

31. **"Leaf" is not overloaded.** A `TermLeaf` sums `TermGroup`s; the comments in `ResolveText`
    and on `TermGroup` say "group", not "Leaf 1/2/3".

32. **One traversal per expansion kind** (second commit, after 30/31).
    `ExpansionPredicate::ForEachMatch(tree, FunctionRef<bool(word, postings)>)` is implemented
    once each by Prefix (`WordIterator`), Suffix (suffix tree, forward word handed to the sink)
    and Fuzzy (`FuzzySearch::Search`). `Evaluate`, `ResolvedLeafCache::FindExpansionMatch` and
    `BuildTextIterator` become sinks over it, so each walk exists once instead of three times and
    the cache no longer knows how to walk a tree. `BACKGROUND_PAUSEPOINT` names move inside
    `ForEachMatch` unchanged (`integration/test_cancel.py` keys on them). `ExpansionLeaf::Kind`
    is deleted with the switch that read it; `ResolveText` casts once to `ExpansionPredicate`.
    The suffix `starts_with(reversed_term)` guard is dropped: `WordIterator` is bounded to its
    prefix's subtree, so it could never fire, and the other two suffix walks never had it.

33. **Seeding the representative during filtering is optional, last, and benchmark-gated.**
    The filter walk sees every matching word a key carries, so it is the only place the
    highest-dt word can be chosen; scoring's `FindExpansionMatch` stops at the first hit. Shape
    if adopted: `ExpansionPredicate::Evaluate(ExpansionLeaf&, tree, …)` probes the representative
    first (one probe on a hit when no positions are needed), otherwise walks and calls
    `leaf.Offer(word, key_count)` on the struct; scoring uses the same `Offer`. Cost: one
    `GetKeyCount()` read per matched word, under the word lock on the main thread, paid by
    queries that never score text and by keys dropped before scoring. Not done unless a benchmark
    shows the scoring fallback gets cheaper by more than filtering gets slower.

## Order

1. Decisions 30 and 31, one commit. The three small cleanups (ResolveTag `MainThread()`,
   posting.h NOTE dedupe, `ProbeWord`) went in their own commit first. *Done.*
2. Decision 32, second commit, with query/text_index unit tests and `test_cancel` /
   `test_scoring` / `test_fulltext*` integration runs. *Done.*
3. Decision 33 only after measurement, as its own commit if at all.

Fine-grained choices made while applying these go in
[implementation-choices.md](implementation-choices.md) §Review round 2.
