# Known Differences vs. Redis

Open behavioral differences between valkey-search and the Redis reference
engine, found by the compatibility generators. This is the backlog behind the
one compatibility test that is currently red:

```
integration/compatibility_test.py::TestAnswersCMD::test_answers[expr-answers.pickle.gz]
```

It fails at **12325 correct out of 13156**. There is no CME counterpart because
`generate_expr.py` is registered with `cluster: False`. `aggregate-answers` and
`text-search-answers` pass in both CMD and CME.

For gaps in the *text-search* tests specifically, see `unsupported_tests.md`.

## 1. Differences the red test measures

All 831 failures fall into five classes. Counts are the number of failing
answers, not the number of distinct defects — each class is a single
underlying difference.

| Count | Difference |
| ----- | ---------- |
| 396 | An `APPLY` that evaluates to Nil: Redis emits the output field with a `NULL` value, valkey-search omits the field entirely |
| 165 | Comparison between an empty string and a number |
| 125 | Sign of `NaN` |
| 77 | Floating point formatting |
| 68 | Negative zero |

### 1.1 `APPLY` producing Nil (396)

```
FT.AGGREGATE idx * LOAD * APPLY 'upper(3.14)' AS result

Redis:          ... result NULL      (every row)
valkey-search:  ... <no result field at all>
```

Note this is *not* the same as a loaded field the document does not have.
There, Redis also omits the field, and valkey-search already matches — so a
blanket "emit NULL for every Nil" would trade one incompatibility for another.
The two cases have to be told apart.

The distinction is available statically: an `APPLY` output column exists for
every row in the pipeline, whereas an absent document field is a slot that was
simply never written. A sketched fix is to add a `computed_` flag to
`AttributeRecordInfo` (`src/commands/ft_aggregate_parser.h`), set it where the
`APPLY` output attribute is created (`MakeReference(name, /*create=*/true)`,
called from `ConstructApplyParser`), and consult it in `ReplyWithValue`
(`src/commands/ft_aggregate.cc`) where the `value.IsNil()` early return
currently drops the pair. The reply already uses a postponed array length, so
the field-count bookkeeping needs no change.

This is an observable change in reply shape, so it wants a
`search.emulate-release` gate at 1.3.0 and a COMPATIBILITY.md row like the
other fixes. Evaluate the gate once per reply rather than per field per row —
`VALKEY_SEARCH_COMPATIBILITY_FIX` reads config and bumps its counter on each
legacy invocation, and a per-field call would both be hot and make the INFO
counter report field count instead of query count.

### 1.2 Empty string compared against a number (165)

```
APPLY '(0)==("")'    Redis: 0    valkey-search: 1
APPLY '("")<(@n1)'   Redis: 1    valkey-search: 0
```

### 1.3 NaN sign (125), float formatting (77), negative zero (68)

```
APPLY '(+inf)+(-inf)'   Redis: -nan   valkey-search: nan
APPLY '(-1)-(3.14)'     Redis: -4.14  valkey-search: -4.140000000000001
APPLY '(0)*(-1)'        Redis: 0      valkey-search: -0
```

These three may share one cause and are best investigated together.
`FormatDouble` (`src/expr/value.cc`) already returns `-nan` when
`std::signbit` is set, so the sign is being lost before formatting is reached.
These files are compiled with `-ffast-math` (`cmake/Modules/valkey_search.cmake`),
which implies `-ffinite-math-only`; the same flag is why `value.cc` hand-rolls
`IsNan`/`IsInf` instead of using the `std::` versions. That is a suspicion, not
a confirmed diagnosis.

The float formatting difference is a deliberate choice rather than a bug:
`FormatDouble` uses `std::to_chars`, which produces the shortest
round-trippable form, while Redis formats more loosely. Matching Redis means
changing that formatting.

## 2. Differences no test covers

Found by direct comparison against Redis, but not exercised by any generator,
so nothing would catch a regression in them.

### 2.1 GROUPBY key absent on some documents

```
FT.AGGREGATE idx * LOAD 2 @__key @n1 GROUPBY 1 @n1 REDUCE COUNT 0 AS c

Redis:          n1 NULL  c 2
valkey-search:  c 2                 (n1 omitted)
```

The same mechanism as 1.1, and nearly free once that lands. It cannot be added
to the suite until then: `compatibility_test.py` sorts rows by the groupby key
via `itemgetter`, which raises `KeyError` on rows that lack it.

### 2.2 FIRST_VALUE over a group with no values

```
Redis:          fv NULL
valkey-search:  <fv omitted>
```

Also the same mechanism as 1.1. Note the other reducers do *not* want NULL
here — `TOLIST` returns `[]` and `COUNT_DISTINCT` returns `0` in both engines,
and MIN/MAX/SUM/AVG return fold identities (fixed; see
`test_aggregate_groupby_missing_field_reducers`).

### 2.3 APPLY referencing a missing input field

Redis does not emit NULL and does not omit the field — it **truncates the
result stream**. Rows before the offending one are returned; that row and every
row after it are not, even when later rows have the field:

```
docs: e:1(t1=a) e:2(t1=b) e:3(no t1) e:4(t1=d)

LOAD 2 @__key @t1                    -> 4 rows, e:3 simply has no t1 key
LOAD 2 @__key @t1 APPLY '@t1' AS r   -> 2 rows (e:1, e:2); e:4 never appears
```

`total_results` still reports the full match count. RESP3 carries a warning,
`SEARCH_VALUE_NOT_FOUND ... consider using EXISTS if applicable`; RESP2 gives
no signal at all, so a client just receives a short result set.

valkey-search instead returns every row with the output field absent. Whether
to match Redis here is an open question — reproducing it means deliberately
returning incomplete results.

## 3. Notes on the harness and the reference engine

* **`contains()` with a vector needle hangs Redis.** `contains(<any
  string-valued operand>, @v1)` never returns and pins the Redis server at 100%
  CPU: `contains` scans the needle as a C string, and a hash vector field is a
  binary blob containing NUL bytes. valkey-search answers all of these
  normally, so this is a defect in the reference engine. `generate_expr.py`
  skips the needle position for vector operands; there is no reference answer
  to capture. Worth reporting upstream to Redis.

* **`integration/run.sh` zaps every valkey-server on the host.** Its `zap`
  helper matches on process name and `kill -9`s all of them, so two integration
  runs on one machine kill each other's servers. Failures look like
  `Connection closed by server` or `Error 111 connecting`, and they clear on a
  re-run.

* **The `kText` fix is ungated.** Allowing a TEXT field as an `APPLY`/`FILTER`
  operand turns a hard error into a result, so no client can have depended on
  the old behavior and no `search.emulate-release` gate was added. That is
  inconsistent with how the other fixes on this branch are handled, and may
  warrant a gate plus a COMPATIBILITY.md row.

* **Intra-group ordering of `FIRST_VALUE` and `TOLIST` differs** between the
  engines. Without a `BY` clause the order is unspecified, so this is probably
  noise rather than a defect.
