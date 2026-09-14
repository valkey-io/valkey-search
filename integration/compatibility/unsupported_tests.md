# Unsupported Tests

This is the list of every compatibility case that is not compared against the
Redis reference answer, and why.

Two markers are used, and they mean different things:

- **excluded** — the command is still run against Valkey as a no-crash check,
  but no answer is recorded and nothing is compared. Used where the two engines
  differ by design, or where the difference lives outside the feature under
  test.
- **xfail** — the reference answer *is* recorded and the comparison *is* run;
  a mismatch is the documented state of an open gap and keeps the suite green.
  If one starts matching, the run prints a loud `XPASS` banner: the gap has
  been closed, and both the marker and the entry here should be removed.

Sections 1-4 cover the text-search suite (`generate_text.py`); section 5 covers
the FT.HYBRID suite (`generate_hybrid.py`).

## 1. Exact Phrase Query

Exact phrase query tests are unsupported due to different behavior. In Valkey, the query string should exist in the same field and should not be breaked to different fields in the document.

   Example:

   ```
   127.0.0.1:6379> hset hash:1 title "one two three" body "four five six"
   (integer) 2
   127.0.0.1:6379> ft.search idx '"three four"'
   1) (integer) 0
   ```

## 2. Group Queries with INORDER and SLOP

Some special cases of the group queries with INORDER and SLOP are not supported in the test:

1. **Parsing difference** — Some group queries are excluded due to parsing difference. In Valkey, all the group queries are directly parsed as it is, without flattening any groups.

   Example:

   ```
   a (b c) -> AND(a, AND(b c))
   (ab)(cd) -> AND(AND(ab), AND(cd))
   ```

2. **Nested OR queries** — Some group queries are excluded due to different behaviors in the search phase. This includes specific ordering in the OR clause, and same word appearing in both sides of an OR clause. In these cases Valkey correctly returns the expected answers.

   Example:

   ```
   # specific ordering in the OR clause
   # document: "silent puzzle lemon window movie apple melon"
   "(((melon | window) (puzzle | bright)))" SLOP 2

   # same word appearing in both sides of an OR clause
   "(((shark | cold) (river | cold)))"
   ```

3. **Slop calculation in a group** — Some queries are excluded due to different behaviors in slop calculation. Valkey uses the leftmost position in the document of any word in the group to calculate the slop value.

   Example:

   ```
   127.0.0.1:6379> hset hash:2 body "ocean carrot jump quiet build shark onion"
   (integer) 1
   127.0.0.1:6379> ft.search idx "(((carrot quiet) | (quiet desert)) ((shark | forest)))" SLOP 2
   1) (integer) 0
   ```

   Valkey does not return the document, because "carrot", "shark" is picked from two groups, and the distance is 3, which is larger than 2 in the query

## 3. Fuzzy Search with Levenshtein > 1
Some queries are excluded due to different behavior in fuzzy search. ValkeySearch uses Damerau-Levenshtein to calculate the distance in fuzzy search. The results produced by Valkey is consistent with the Damerau-Levenshtein rules.

## 4. Escaped character search in JSON
Some queries are excluded due to different behavior in escaped character search. Any punctuation followed by a backslash will be escaped. This is the standard format in both ingesting documents and performing search.
   
   Example:

   ```
   127.0.0.1:7001> ft.create idx on json prefix 1 json: schema $.body as body text
   OK
   127.0.0.1:7001> json.set json:1 $ '{"body":"black\\,white"}'
   OK
   127.0.0.1:7001> ft.search idx 'black\\,white'
   1) (integer) 1
   2) "json:1"
   3) 1) "$"
      2) "{\"body\":\"black\\\\,white\"}"
   127.0.0.1:7001> ft.search idx "black\\\\,white"
   1) (integer) 1
   2) "json:1"
   3) 1) "$"
      2) "{\"body\":\"black\\\\,white\"}"
   ```

## 5. FT.HYBRID

### 5.1. Loading a field the index does not have — TODO, marked `xfail`

**Status:** open, and not FT.HYBRID-specific.

Redis lets a `LOAD` clause name a field the index does not have and simply
returns no column for it. valkey-search rejects the whole command:

```
127.0.0.1:6379> FT.HYBRID idx SEARCH @title:alpha VSIM @vec $q KNN 2 K 10 LOAD 1 @nosuchfield ...
(error) Index field `nosuchfield` does not exist
```

The same input produces the same error from FT.AGGREGATE, so this belongs to
the aggregate pipeline's LOAD handling rather than to FT.HYBRID. A JSON path
against a HASH index (`LOAD 1 $.price` on a hash index) names nothing in the
same way and is rejected the same way. `test_load_unknown_field` sweeps both
shapes, marked `xfail`.

When the aggregate pipeline accepts an unknown field, these will start
matching, the run will print `XPASS`, and both the `xfail=True` in
`generate_hybrid.py::test_load_unknown_field` and this section should be
removed.

Every other `LOAD` form is compared normally, in `test_load_clause`: field
subsets, `@__key`, `AS` renames of an existing field, a rename onto another
field's name, two renames at once, and renames referenced by a following
`SORTBY`, `APPLY` or `GROUPBY`.

### 5.2. The suite sweeps HASH keys only — waiting on PR 1381

**Status:** open, fix identified and measured, not yet merged.

`generate_hybrid.py` runs against HASH indexes and does not sweep JSON. Naming
an indexed field in a pipeline stage — `GROUPBY 1 @color`, `SORTBY 2 @price
ASC` — does not resolve under `LOAD *` on a JSON index, because the document
arrives as a single `$` column. That is an FT.AGGREGATE limitation, not an
FT.HYBRID one: the equivalent FT.AGGREGATE query has the same problem, and
FT.HYBRID has no key-type-specific code of its own.

The fix is PR 1381, "Ask for content with `all_content`, not by leaving the
list empty". Today an empty `return_attributes` means two different things,
"fetch nothing" and "fetch the whole record", so a stage that names a field
cannot ask for it alongside `LOAD *`. 1381 splits them with an explicit flag.

**FT.HYBRID does not get that fix for free.** `FusedResolver` in
`src/commands/ft_hybrid.cc` copies `return_attributes` and `no_content` but
not `all_content`, and `WantsNoDatabaseContent` still tests `loadall_`.
Landing 1381 without that three-line follow-up silently *empties* `LOAD *`
replies rather than fixing them — a JSON `LOAD *` returns no `$` column at
all, and a HASH `LOAD * SORTBY 2 @price ASC` returns only `price`. There is no
compile error to catch it; the flag just stays false.

Measured on a merged build (hybrid + 1381 + that follow-up) against a redis:8
reference, over the 15 cases in `test_loadall_feeding_a_stage`:

| build | hash | json |
|---|---|---|
| `hybrid` alone | 10 pass / 1 fail | 4 pass / 7 fail |
| merged + follow-up | 11 pass / 0 fail | 11 pass / 0 fail |

(Four of the 15 record a reference error rather than an answer, so they pass
unconditionally: Redis auto-loads for SORTBY and GROUPBY but not for APPLY or
FILTER. They are kept to pin that, as 1381 does for FT.AGGREGATE.)

`test_loadall_feeding_a_stage` carries those cases now, marked `skip`. When
1381 and the follow-up land: drop the skip, delete this section, and
parametrize `TestHybridCompatibility` over both key types — after gating
`test_load_unknown_field`'s `LOAD 1 $.price` case to HASH, because that path
resolves on a JSON index and would otherwise XPASS for the wrong reason.

Note 1381 does **not** address 5.1: it never touches the schema lookup that
produces "Index field `x` does not exist", and `LOAD 1 @nosuchfield` is
rejected identically before and after the merge.

### 5.3. An incomplete KNN block — valkey accepts, Redis rejects

**Status:** deliberate leniency, not swept.

Redis requires the `KNN` block, when present, to carry a positive even
argument count and to include `K`:

```
KNN 0                  (error) Invalid argument count: 0
KNN 2 EF_RUNTIME 40    (error) Missing required argument K
```

valkey-search accepts both and applies the same default K of 10 that either
engine applies when the block is left out entirely. Nothing in an answer
differs — an accepted command returns what the equivalent complete command
returns — so recording these would only pin a permanent error-message
mismatch. `testing/ft_hybrid_parser_test.cc` covers them instead, and the
sweep in `generate_hybrid.py` stays to the forms both engines accept.

### 5.3b. `LOAD 0` — valkey accepts, Redis rejects

**Status:** deliberate leniency, not swept.

```
... LOAD 0 ...
Redis:  (error) SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected n
Valkey: each row carries __key, as with no LOAD clause at all
```

An empty LOAD names no columns, which is what omitting the clause means, and
valkey-search treats it that way. Redis requires a positive count. Nothing in
an answer differs -- the accepted command returns what the omitted clause
returns -- so recording it would pin an error-message mismatch and no more.
Noted here because it is the same shape of leniency as 5.3.

### 5.4. Per-arm score aliases in a pipeline stage — TODO, marked `xfail`

**Status:** open.

A per-arm `YIELD_SCORE_AS` alias reaches the reply on both engines, but only
Redis lets a later stage refer to it:

```
FT.HYBRID idx SEARCH @title:alpha YIELD_SCORE_AS ts
              VSIM @vec $q KNN 2 K 10 YIELD_SCORE_AS vs
              COMBINE RRF 2 YIELD_SCORE_AS hs
              LOAD 1 @price SORTBY 2 @vs DESC ...
Redis:  sorts by the VSIM arm's score
Valkey: (error) Index field `vs` does not exist
```

Redis resolves such an alias in a `SORTBY` under every `LOAD` clause except
`LOAD *`, where it rejects it as "Property `vs` not loaded nor in schema".
valkey-search rejects it under every `LOAD` clause, at parse time, because the
stage parser resolves `@name` against the index schema and a score alias is
not a field. The `COMBINE` alias is reachable in stages on both engines; only
the per-arm ones are not.

`test_sortby_per_arm_score_is_reachable` sweeps the shapes Redis accepts,
marked `xfail`. `test_sortby_every_kind_of_column` and
`test_pipeline_stages_over_scores` sweep the rest, including the `LOAD *`
forms both engines reject and the `APPLY`, `FILTER` and `GROUPBY` references
neither resolves.

Two Redis behaviours here are not worth matching and are deliberately not
swept. Under a non-`LOAD *` clause, an `APPLY` or `FILTER` over a per-arm
alias returns an empty result set with a `SEARCH_VALUE_NOT_FOUND` warning
rather than an error, and a `GROUPBY` reducer over one returns `-inf` for most
groups. Those are soft failures, not a capability; recording them would pin
Redis's degraded answer as the target.

When the aliases become reachable these will start matching, the run will
print `XPASS`, and both the `xfail=True` in `generate_hybrid.py` and this
section should be removed.

### 5.4b. `POLICY` — valkey accepts and discards, Redis rejects

**Status:** deliberate leniency, not swept.

```
... VSIM @vec $q KNN 2 K 20 POLICY local ...
Redis:  (error) SEARCH_PARSE_ARGS POLICY: Unknown argument
Valkey: answers as if the clause were not there
```

The Redis 8.4 query engine has no `POLICY` clause on FT.HYBRID and refuses the
token outright, for every value tried. valkey-search parses it and throws the
value away (ft_hybrid_parser.cc, the top-level walk), erroring only when the
value is missing. Accepted so a command written for a coordinator dialect that
does carry `POLICY` is not rejected here; it does not change an answer, so
sweeping it would record an error-message mismatch and nothing else.

Both engines take a `FILTER` inside the VSIM clause, where it pre-filters the
vector search. It goes after the `KNN`/`RANGE` block and *before*
`YIELD_SCORE_AS` -- placed after the alias it ends the clause and becomes the
aggregate stage instead -- and it is written in the FT.SEARCH query language,
not the aggregate FILTER's expression language. The count is optional. Note the
command reference has the order the other way round, and its structured
argument list omits `POLICY` entirely while the syntax line shows it; both were
resolved by measurement.

### 5.4c. A pipeline stage naming a field no LOAD clause asked for

**Status:** open, and not FT.HYBRID-specific.

valkey-search implicitly loads a field that a pipeline stage references
(#919), so a `FILTER` resolves whether or not a LOAD clause names the field.
Redis requires the field to have been loaded by an earlier stage, and when it
has not the `FILTER` passes every row through rather than failing:

```
... KNN 2 K 20 FILTER @price<5 ...               Redis 20 rows, Valkey 5
... KNN 2 K 20 LOAD 1 @price FILTER @price<5 ... Redis  5 rows, Valkey 5
... KNN 2 K 20 FILTER @price<5 LOAD 1 @price ... Redis 20 rows, Valkey 5
... KNN 2 K 20 LOAD * FILTER @price<5 ...        Redis errors, Valkey 5
```

So the engines agree exactly when the field is loaded before the stage that
names it, and disagree in three ways otherwise -- Redis silently not filtering,
Redis caring about stage order where valkey-search does not, and the `LOAD *`
case of 5.4. This belongs to the aggregate pipeline rather than to FT.HYBRID:
the same shapes behave the same way under FT.AGGREGATE. Not swept here, because
a sweep of it would be testing the aggregate pipeline's loading rules through
FT.HYBRID, and because two of the four rows above are Redis soft failures that
are not a target worth recording.

### 5.10. A text conjunct is dropped from a KNN pre-filter — skipped

**Status:** open in valkey-search, and NOT an FT.HYBRID defect. Tracked here
only because this is where it was found.

A pre-filter that ANDs a text predicate with anything loses the text conjunct
when it is used for a vector search. A conjunction of non-text predicates is
correct, and so is every single predicate. Written parenthesized, which is the
spelling the reference requires for more than one predicate:

```
prefilter                       matches   redis   valkey
@title:epsilon                        1       1        1
@price:[0 10]                         6       6        6
@price:[0 30] @price:[20 52]          5       5        5
@color:{red} @price:[0 30]            4       4        4
@title:alpha @body:river             12      12       24
@price:[0 30] @title:alpha           10      10       14
@color:{red} @title:alpha              0       0        6
@color:{red} @title:delta             2       2        6
```

With both conjuncts text nothing survives, so the query degrades to an
unfiltered KNN -- the 24 above is the whole corpus.

FT.SEARCH and FT.AGGREGATE produce identical counts and identical wrong rows
for every expression above, so this is one defect in the shared pre-filter
path, not one per command; FT.HYBRID's VSIM `FILTER` inherits it by routing
through the same place. `test_vsim_filter_conjunction` is therefore `skip`
rather than `xfail`: an FT.HYBRID divergence register is the wrong place to
track a shared-path defect, and the sweep would only re-report it.

Note the parentheses are required to ask the reference the question at all --
it refuses `@a:x @b:y=>[KNN ...]` as a syntax error -- but they change nothing
on our side: bare, spaced, parenthesized and both give byte-identical rows.

Every single-predicate filter -- text, tag, numeric, negation, distributed
union -- matches the reference exactly, and those are swept normally.

### 5.5. Referencing `@__score` under a LOAD clause — valkey accepts, Redis rejects

**Status:** open, permissive, and narrow.

The fused score's own name and emission now match the reference exactly. The
rule both engines follow, measured across every combination of LOAD shape,
COMBINE method and per-arm alias, on HASH and on JSON:

| shape | column in the reply |
|---|---|
| no LOAD clause | `__score` |
| any LOAD clause (`LOAD *` or a named list) | none |
| `COMBINE ... YIELD_SCORE_AS name`, any LOAD shape | `name` |

So a LOAD clause replaces the default projection outright, and an explicitly
named score is an explicit request that survives it. `COMBINE ...
YIELD_SCORE_AS __score` with no LOAD names the column the default projection
already generates, and both engines reject it.

What still differs is whether the column is *referenceable* from a pipeline
stage once a LOAD clause has hidden it:

```
... COMBINE RRF 0 APPLY "@__score * 2" AS dbl            both engines: ok
... COMBINE RRF 0 LOAD 1 @price APPLY "@__score * 2" ...  Redis rejects,
                                                          valkey accepts
... COMBINE RRF 0 LOAD *       APPLY "@__score * 2" ...  Redis rejects,
                                                          valkey accepts
```

Redis answers `SEARCH_PROP_NOT_FOUND Property not loaded nor in pipeline`,
because for it the score column ceases to exist when the default projection is
replaced. valkey-search keeps the column registered so the post-fusion
pipeline can still sort and filter on it, and only hides it from the reply.
This is the same permissive-loading difference as 5.4c, reached through the
score column rather than through a database field, and it is left as-is for
the same reason. Not swept: the two divergent rows are Redis errors, so there
is no reference answer worth recording.

### 5.6. COMBINE FUNCTION — a valkey-search extension, deliberately not swept

**Status:** permanent divergence, by design.

The Redis 8.4 query engine accepts only `RRF` and `LINEAR` as `COMBINE`
methods. `FUNCTION` is refused exactly as a nonsense method name is:

```
127.0.0.1:6379> FT.HYBRID idx ... COMBINE FUNCTION 4 EXPR "@s + @v" ...
(error) SEARCH_PARSE_ARGS COMBINE: Invalid value for argument
```

valkey-search implements it, binding each arm's score to a reference -- its
`YIELD_SCORE_AS` alias, the positional `@__arm<i>_score`, or
`@__search_score` / `@__vector_score` for the two-arm shape. It is therefore an
extension with no reference behaviour to be compatible with, and sweeping it
would only record a reference error, which this harness passes unconditionally.
It is covered by `integration/test_ft_hybrid.py` instead.

### 5.7. VSIM RANGE — parsed, not implemented, not swept

**Status:** open, in valkey-search.

Redis implements `VSIM ... RANGE <count> RADIUS <r> [EPSILON <e>]`.
valkey-search parses the clause for shape and then refuses it:

```
(error) VSIM RANGE is not yet supported; use KNN
```

Not swept, for the same reason as 5.6 in reverse: the answers would be
valkey-search errors against real Redis results, which the harness reports as a
failure rather than a documented gap, and an `xfail` entry per shape would
document an unimplemented feature rather than a behavioural difference.
`testing/ft_hybrid_parser_test.cc` pins the parse-then-refuse behaviour. When
RANGE is implemented it should be swept like KNN and this section removed.

### 5.8. Reference engine image — TODO

**Status:** temporary.

`generate_hybrid.py` overrides `BaseCompatibilityTest.DOCKER_IMAGE` to
`redis:8`, because FT.HYBRID does not exist in `redis/redis-stack-server`
(RediSearch 2.x) and was added in the Redis 8.4 query engine. A separate PR
moves the shared image to `redis:latest` for every generator; once that lands,
the `DOCKER_IMAGE` override marked `TODO(reference-image)` can be dropped and
this generator can inherit the shared image again.

### 5.9. Cluster replay — blocked on shard-local text scoring

**Status:** open. The mechanical blockers are fixed; the substantive one is not.

`GENERATORS` marks this generator `"cluster": False`, so the answers are
replayed against a standalone server only. Two things used to make cluster
replay impossible at all, and both are now fixed:

* `load_data_cluster` hardcoded the vector corpora, so the `hybrid text` data
  set raised `KeyError` before a single query ran. It now dispatches on the
  data set the way `load_data` does.
* The cluster client routes a keyless command only if it recognizes the name.
  Its `SEARCH_COMMANDS` list carries FT.SEARCH and FT.AGGREGATE but predates
  FT.HYBRID, so the command raised "No way to dispatch this command" instead of
  reaching a node. `cluster_routing()` in compatibility_test.py sends any
  unrecognized `FT.` command to the default node, which is what the client does
  for the two it knows.

What remains is not mechanical. With the flag on, 33 of 202 answers match. The
text arm is the reason: each shard scores BM25 from its own corpus, so the
document frequency of a term is a third of what a standalone server sees and
the inverse document frequency rises to match. For `@title:beta` the reference
top score is 0.938 and a three-shard cluster answers 1.340. Every sweep whose
fused ranking depends on a text score follows it, which is nearly all of them.
`generate_text.py` is excluded from cluster replay for the same reason.

So the options are to align distributed text scoring with the single-node
computation, or to capture a second set of reference answers from a Redis
cluster. Until one of those happens, flipping the flag would record the
divergence 169 times rather than test anything. Note the reference engine's own
cluster behaviour is unmeasured here: this generator runs one container.
