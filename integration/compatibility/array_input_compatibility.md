# ARRAY values as input to a later aggregate stage

PR #932 adds `TOLIST`, whose result is a new ARRAY value. The compatibility
suite covered ARRAY only as the *last* thing a pipeline produces. This adds
coverage for ARRAY as **input** to SORTBY, GROUPBY and APPLY, records the
result, and diagnoses every failure.

## What was added

| File | Change |
|---|---|
| `data_sets.py` | `array inputs` dataset: 3 groups × 3 keys; `ga`/`gc` collect equal arrays, `n2` repeats inside a group so array/array ops see both matching and mismatched lengths, `n3` holds timestamps, `t3` a parseable date |
| `generate_array.py` | 158 queries per key type (316 answers) — every dyadic operator in 5 operand shapes, unary `!`, every registered function, the four unregistered array functions, 10 SORTBY tails, 6 GROUPBY tails, 8 reducer tails |
| `__init__.py` | registers the generator → `array-input-answers.pickle.gz` |
| `compatibility_test.py` | three harness fixes needed before these queries could run at all (below) |

Every query has the same shape: `groupby 1 @t1 reduce tolist ... <stage under test>`,
since TOLIST is the only way to obtain an ARRAY.

Run with `./build.sh --run-integration-tests=array`; regenerate the reference
answers with `./integration/compatibility/regenerate.sh`.

### Harness fixes required

1. **`groupby` + `sortby` in one command was an unconditional `assert False`.**
   Row-ordering keys now come from the *last* GROUPBY/SORTBY in the pipeline —
   an earlier GROUPBY's key is gone from the reply once a later stage regroups.
2. **Rows keyed on an ARRAY were compared against the wrong rows.** The two
   engines return TOLIST elements in different orders (see "TOLIST element
   order" below), so sorting rows by the raw list ordered each reply
   differently. Row ordering now canonicalizes list values and breaks ties on
   the whole row.
3. **`parse_value` crashed on RESP nil**, which an APPLY returning nothing
   produces.

Fix 2 alone turned 7 spurious SORTBY failures into passes (13 → 6).

## Result

316 answers: **184 reported pass, 132 fail**. The pass count overstates
compatibility, because the harness passes any case where RediSearch itself
errored, whatever valkey did:

| | count | meaning |
|---|---|---|
| genuine match | 54 | same answer from both engines |
| **mismatch** | **132** | both answered, answers differ |
| RediSearch rejects, valkey answers | 120 | reported as a pass; **untested** |
| both reject | 10 | agreed error (the unregistered array functions) |

The aggregate and text-search suites still pass, so none of the harness changes
regressed existing coverage.

## Failures by root cause

### F1 — nil APPLY result: RediSearch emits a nil field, valkey drops it (46)

```
apply dayofweek(@tsitems) as result
  RL: t1=ga tsitems=[...] result=(nil)
  VK: t1=ga tsitems=[...]                 <- no `result` field at all
```

`ReplyWithValue` (`src/commands/ft_aggregate.cc:163`) returns false for a Nil
value and the caller then omits the name/value pair. **This is not
array-specific** — `apply lower(@n1) as result` over a scalar numeric drops the
field too, while RediSearch replies `result → nil`.

Two things land here: all 11 date/time functions (the `TIME_FUNCTION` macro at
`src/expr/value.cc:872` calls `AsDouble()`, which is `nullopt` for an ARRAY, so
they never broadcast) and `lower`/`upper` over a numeric array (ApplyToElements
turns a single non-string element into a whole-value Nil).

**Fix.** Distinguish a field *computed* by APPLY/REDUCE from a field *loaded*
from the document: computed fields always appear in the reply, replying Null
when the value is Nil; loaded fields keep today's drop-on-missing behavior
(confirm that is RediSearch's rule for a missing loaded field). Needs a flag on
`record_info_by_index_` / `Attribute`, set by the APPLY and REDUCE parsers, and
`GenerateResponse` (`ft_aggregate.cc:365`) already uses a postponed array
length, so emitting an extra pair is free.
*Effort: medium. Risk: low. Fixes 46 of 132.*

Registering the existing-but-unused `generate_expr.py` in `GENERATORS` would
have caught this class at scalar level long ago; worth doing in the same change.

### F2 — math functions broadcast instead of collapsing (28)

```
apply abs(@items) as result     RL: result="nan"      VK: result=["1","2","3"]
apply sqrt(@sitems) as result   RL: result="nan"      VK: result=["nan","nan","nan"]
```

`FuncAbs`/`Ceil`/`Floor`/`Log`/`Log2`/`Exp`/`Sqrt` each have an `IsArray()`
branch calling `ApplyToElements` (`src/expr/value.cc:595-680`). RediSearch
accepts these queries and answers with a scalar `nan`.

### F3 — `lower`/`upper` over a string array (4)

```
apply lower(@sitems) as result  RL: result=(nil)      VK: result=["apple","banana","cherry"]
```

Same cause as F2 (`value.cc:787`, `:816`).

**Fix for F2+F3.** This is the policy decision (see below). To be compatible,
the `IsArray()` branch of every *unary* function must collapse — nan for the
math functions, nil for the string ones — rather than broadcast. If broadcast
is wanted as a feature, it has to be opt-in, because RediSearch accepts these
queries today and answers differently.
*Effort: small. Risk: low (mechanical). Fixes 32 of 132.*

### F4 — an array is falsy in valkey, truthy in RediSearch (18)

```
apply (@items)&&(2) as result   RL: result="1"   VK: result="0"
apply !(@items) as result       RL: result="0"   VK: result="1"
```

`Value::AsBool()` (`src/expr/value.cc:104`) falls through to the Nil branch for
an ARRAY and returns false, so `IsTrue()` is false and `&&`/`||`/`!` all invert.

**Fix.** Return true for an ARRAY (empty array → false is the natural reading,
though RediSearch's behavior there is unverified), gated by
`VALKEY_SEARCH_COMPATIBILITY_FIX` the same way the 1.2.1 string-truthiness fix
is.
*Effort: trivial. Risk: low. Fixes 18 of 132.*

### F5 — `==` / `!=` against an array (12)

```
apply (@items)==(2) as result   RL: result="0"   VK: result="1"
```

`Compare(array, scalar)` returns `kUNORDERED` (`value.cc:310`) and
`operator==` (`value.h:147`) counts `kUNORDERED` as equal — deliberate, for
legacy nan/nil semantics.

**Fix.** Handle the array-vs-scalar case in `FuncEq`/`FuncNe` rather than
changing the shared `operator==`: an array is never equal to a scalar.
*Effort: trivial. Risk: low. Fixes 12 of 132.*

### F6 — ordered comparison of a string array against a string literal (4)

```
apply (@sitems)<("a") as result   RL: result="1"   VK: result="0"
```

RediSearch stringifies the array to the literal `(null)` and compares strings;
`"(null)" < "a"`. valkey returns `kUNORDERED`, so `<` is false. Note the
related inconsistency: `Value::AsString()` returns `""` for an array while
`AsStringView()` returns `nullopt` (`value.cc:178`, `:161`).

**Fix.** Either give an ARRAY a defined string form and let the existing string
fallback in `Compare` run (matching RediSearch exactly means using the string
`(null)`), or accept the divergence and mark these two queries excluded. Make
`AsString`/`AsStringView` agree either way.
*Effort: small. Risk: low. Recommend documenting rather than emulating `(null)`.*

### F7 — GROUPBY on an array (10)

```
groupby 1 @items reduce count 0 as cnt
  RL: 6 groups — 1→2, 2→2, 3→2, 4→1, 0.5→1, -1→1     (expands the array)
  VK: 2 groups — [1,2,3]→2, [-1,0.5,4]→1              (keys on the whole array)
groupby 2 @items @sitems ...
  RL: 18 groups (cross product of both expansions)     VK: 2
```

`GroupBy::Execute` (`ft_aggregate_exec.cc:197-204`) puts the ARRAY into the
`GroupKey` whole; `AbslHashValue` (`value.h:97`) hashes it element-wise, so two
groups that collect equal arrays also merge into one. RediSearch treats an
array as a multi-value field and emits one group per distinct element.

This is the sharpest semantic incompatibility here: a query migrated from
RediSearch returns a different group set *and* different counts.

**Fix.** Expand array-valued key components into the cartesian product of their
elements and process the record into each resulting group. Needs a cap on the
product to bound the blowup.
*Effort: medium. Risk: medium (perf). Fixes 10 of 132.*

### F8 — SORTBY on an array with LIMIT / MAX (6)

```
sortby 2 @items asc limit 0 2
  RL: rows gc[3,2,1], ga[3,2,1]      VK: rows gb[-1,0.5,4], ga[1,2,3]
```

Without LIMIT these now pass — the row *sets* agree and the harness normalizes
order. With LIMIT/MAX the surviving rows differ: RediSearch keys on the *first
element* of the collected list, whose order is its hash-table layout (below),
while valkey compares arrays lexicographically over scan order.

**Fix.** Matching would mean reproducing RediSearch's hash layout *and* its
ingest order — see "TOLIST element order". Mark these three queries `excluded`
in the generator (the harness then runs them for a crash check only) and state
the difference in `COMPATIBILITY.md`.
*Effort: trivial. Removes 6 of 132 from the failure list.*

### F9 — reducers fed an array (4)

```
reduce min 1 @items / max 1 @items   RL: 0 / 0        VK: the array itself
reduce stddev 1 @items               RL: 1, 2.56580.. VK: 0
```

`Min`/`Max` (`ft_aggregate_exec.cc:244`, `:270`) keep an `expr::Value`, and
array-vs-scalar comparison is `kUNORDERED`, so the first array seen becomes the
result — an ARRAY leaks out of a numeric reducer. `Stddev` (`:311`) uses
`AsDouble()`, `nullopt` for an array, so it counts nothing and returns 0;
RediSearch expands the array and computes over its elements (stddev of
{1,2,3} = 1). Note RediSearch does *not* expand for SUM/AVG — those return 0 in
both engines, which is why they pass.

**Fix.** MIN/MAX: treat an ARRAY argument as the non-numeric 0 RediSearch
produces (restrict the change to `IsArray()` so scalar behavior is untouched);
at minimum never return an ARRAY from MIN/MAX. STDDEV: expand an ARRAY argument
into its elements, with a comment noting SUM/AVG deliberately do not.
*Effort: small. Risk: low. Fixes 4 of 132.*

### F10 — 120 answers the suite cannot check

Every arithmetic operator (`+ - * / ^`), every ordered comparison against a
scalar, and every string function over an array makes RediSearch reject the
whole query (`Could not convert value to a number`, `Error converting string
'(null)' to number`). valkey answers instead — with a broadcast result:

```
apply (@items)+(2) as result     RL: error       VK: result=["3","4","5"]
apply (@items)+(@items2)         RL: error       VK: ["11","22","33"] for the equal-length group,
                                                 field dropped where the lengths differ
```

The harness passes any case where the reference errored, so none of this is
actually verified. Under "RediSearch rejects it, so it is free space" these are
legitimate extensions — but they need their own expectations.

**Fix.** Record valkey's answers for these 120 in a valkey-side expectation
file (or as assertions in `integration/test_tolist.py`), and document the
extension in `COMPATIBILITY.md`. Separately, the harness's "reference errored →
pass" rule should at least assert no crash and record what valkey did.
*Effort: medium. Risk: none.*

### F11 — array functions are unreachable (10, currently "passing")

`arraylen`, `arrayat`, `isarray` and `flatten` are implemented and unit-tested
in `value.cc:1055-1100` but never registered in `expr::function_table`
(`src/expr/expr.cc:192`), so APPLY rejects them: *"Function arraylen is
unknown"*. RediSearch rejects them too, so the tests pass by agreement — on
dead code.

**Fix.** Register them (RediSearch rejects the names, so this is free space) and
document them, or delete the implementations and their unit tests.
*Effort: trivial.*

## TOLIST element order

Measured against `redis/redis-stack-server` and Redis 8.8.1's bundled query
engine, collecting a single group:

| Probe | Result |
|---|---|
| Same data, same query, twice | identical order |
| Fresh server process | identical order — the layout is not seeded randomly |
| redis-stack vs Redis 8.8.1 | identical order across both engine builds |
| `0..9` loaded ascending | `7 5 1 4 2 0 8 9 3 6` |
| Same ten values, loaded in a different document order | `9 7 5 1 4 0 2 8 3 6` — different |
| Two groups, same values, same load order | same order in both |
| Same group, but three extra documents repeating a value already present | `a c d b e` instead of `d c b a e` |

So the order is **deterministic, not random** — but it is hash-table layout, not
a contract: it is not sorted, not insertion order, not the reverse of either,
and it shifts when the ingest order changes, when a duplicate document is added
(no change to the distinct set), or when the element count crosses a growth
boundary (`0..8` returns the `0..7` order with `8` appended).

Reproducing it in valkey would mean reimplementing RediSearch's hash function,
table growth and iteration *and* feeding the reducer in RediSearch's document
order — valkey collects in index-scan order. That is why the comparison is
order-insensitive rather than exact, and why the SORTBY-on-array cases (F8)
cannot be made to agree.

## Policy decision this needs

The failures split cleanly by one rule:

> If RediSearch **accepts** a query, valkey must return the same answer. Where
> RediSearch **rejects** it, valkey may extend.

Under that rule F1, F4, F5, F7, F9 are plain bugs, F8 is undefined on both
sides, F10/F11 are extensions to document — and **F2/F3 (32 failures) are the
open question**: element-wise broadcast for unary functions changes the answer
to queries RediSearch already accepts. Either broadcast becomes opt-in (config
or emulate-release gate) and the default collapses to nan/nil, or the PR
declares this an intentional divergence and `COMPATIBILITY.md` says so. The
same question applies to `Compare`'s lexicographic array ordering used by
SORTBY.

## Suggested order of work

| Step | Fixes | Failures cleared |
|---|---|---|
| 1. Emit nil for computed fields (F1) | reply shape | 46 |
| 2. Array truthiness (F4) | `AsBool` | 18 |
| 3. `==` / `!=` against an array (F5) | `FuncEq`/`FuncNe` | 12 |
| 4. Reducer handling (F9) | MIN/MAX/STDDEV | 4 |
| 5. Decide the broadcast policy (F2/F3) | unary functions | 32 |
| 6. GROUPBY multi-value expansion (F7) | `GroupBy::Execute` | 10 |
| 7. Mark SORTBY+LIMIT excluded, document (F8, F6) | generator + docs | 10 |
| 8. Register or delete the array functions (F11) | `expr.cc` | — |
| 9. Cover the 120 extension answers (F10) | new expectations | — |

Steps 1–4 are small, low-risk, and clear 80 of the 132 failures.
