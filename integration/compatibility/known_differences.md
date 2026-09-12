# Known Differences vs. Redis

Open behavioral differences between valkey-search and the Redis reference
engine, found by the compatibility generators. The reference is `redis:latest`
(Redis 8, which bundles Redisearch natively). Where a difference below cites a
measurement, it was taken against that image, and against
`redis/redis-stack-server` where the two engines disagree with each other.

For gaps in the *text-search* tests specifically, see `unsupported_tests.md`.
For the array-input sweep, see `array_input_compatibility.md`.

## 1. Differences no test covers

Found by direct comparison against Redis, but not compared by any generator --
either no generator exercises them, or the case is captured and marked
`excluded`, which replays the command only to check that valkey-search does not
crash. Either way nothing would catch a regression in them.

### 1.1 APPLY over an absent field truncates the result stream

Redis 8 does not just drop the offending record — it stops there. Rows before
it are returned; that row and every row after it are not, even when later
documents have the field. `total_results` still reports the full match count.
RESP3 carries a warning, `SEARCH_VALUE_NOT_FOUND ... consider using EXISTS if
applicable`; RESP2 gives no signal at all, so a client just receives a short
result set.

valkey-search filters the record out and carries on, so a query whose absent
field sits in the middle returns more rows than Redis 8 does. Matching this
means deliberately returning incomplete results, which is why it has not been
done.

`&&` is the one operator exempt from the rule, and that part *is* implemented:
a missing operand there is falsy, so the record is kept and the alias replies
`0`, whichever side the reference sat on. Measured on Redis 8 over three
documents whose middle one lacks `@n2`:

```
APPLY (@n2)&&(1)   -> rows 1 and 2 returned, x = 1 then 0   (row 3 truncated)
APPLY (@n2)&&(0)   -> all three returned,    x = 0, 0, 0
APPLY (1)&&(@n2)   -> rows 1 and 2 returned, x = 1 then 0   (row 3 truncated)
APPLY abs(@n2)     -> row 1 only                            (rows 2, 3 truncated)
```

The value is matched; the truncation is not.

### 1.2 GROUPBY key absent on some documents

```
FT.AGGREGATE idx * LOAD 2 @__key @n1 GROUPBY 1 @n1 REDUCE COUNT 0 AS c

Redis:          n1 NULL  c 2
valkey-search:  c 2                 (n1 omitted)
```

Cheap now that a nil records whether it is a missing field or a computed
result, but the harness sorts rows by the groupby key, so a row lacking it
needs the placeholder handling that `row_sort_key` now provides.

### 1.3 FIRST_VALUE over a group with no values

```
Redis:          fv NULL
valkey-search:  <fv omitted>
```

Note the other reducers do *not* want NULL here: `TOLIST` returns `[]` and
`COUNT_DISTINCT` returns `0` in both engines, and MIN/MAX/SUM/AVG return fold
identities (fixed; see `test_aggregate_groupby_missing_field_reducers`).

### 1.4 Float formatting, negative zero, and the sign of NaN

`FormatDouble` uses `std::to_chars`, which produces the shortest
round-trippable form, while Redis formats more loosely and disagrees on the
rendering of negative zero and on the sign it prints for NaN. This is a
deliberate choice rather than a defect: matching Redis means giving up shortest
round-trip formatting. The harness absorbs it instead, comparing two numeric
values that differ byte-for-byte through `compare_number_eq`, which uses
`math.isclose` and treats `nan` and `-nan` as equal.

### 1.5 Equality between two arrays

Redis reads only the first element of each array and compares that.
valkey-search compares element by element and then by length
(`Compare` in `src/expr/value.cc`). Measured on `redis:latest`:

```
FT.AGGREGATE idx @n1:[-inf inf] LOAD 3 @n1 @n2 @t1 GROUPBY 1 @t1
  REDUCE TOLIST 1 @n1 AS items REDUCE TOLIST 1 @n2 AS items2
  APPLY (@items)==(@items2) AS result

items      items2       Redis   valkey-search
[5]        [5,7]        1       0
[1,9]      [1,2]        1       0
[1,2]      [1,3,2]      1       0
[2]        [1,9]        0       0
```

Matching Redis means discarding every element after the first, so
valkey-search keeps its own rule and `generate_array.py` marks
`test_array_vs_array_compare` excluded.

Adopting the Redis rule would not make the case comparable anyway. TOLIST's
element order is unspecified and the two engines produce different orders, so
under a first-element rule the recorded answer depends on whichever element
Redis's hash table happened to yield first. The harness already sorts TOLIST
arrays before comparing them (`compare_row` in `compatibility_test.py`), which
hides the order in the array fields but cannot hide it in a scalar computed
from them.

That ordering is also why the divergence went unnoticed until the pickles were
regenerated: every earlier generation landed on orders whose first elements
differed, where the two rules agree. The `array compare` dataset exists to tell
the rules apart and did so once the order changed.

The same expression under FILTER (`test_filter_array`) is left comparing,
because the `array inputs` dataset gives `@n1` and `@n2` disjoint values in
every group. The first elements can never coincide there, so both rules answer
"not equal" under any order.

## 2. Where the two reference engines disagree

These are not valkey-search defects. They are places where `redis:latest` and
`redis/redis-stack-server` answer differently, so the choice of reference
decides what "compatible" means. Recorded because several rules in
`COMPATIBILITY.md` were originally measured against redis-stack and adjusted
when the reference moved.

Empty fold, and non-numeric input to MIN/MAX:

| | redis:latest | redis-stack |
| --- | --- | --- |
| `MIN` over nothing | `inf` | `0` |
| `MAX` over nothing | `-inf` | `0` |
| `SUM` over nothing | `nan` | `0` |
| `AVG` over nothing | `nan` | `0` |
| `MIN` over a TAG field | `inf` | `0` |
| `MIN` over an array | `inf` | `0` |

Redis 8 folds only numbers, so a non-numeric input contributes nothing and the
group answers the reducer's identity. redis-stack reads such an input as `0`.
`APPLY (@absent)&&(1)` likewise keeps the record on Redis 8 and drops it on
redis-stack.

## 3. Notes on the harness and the reference engine

* **`contains()` with a vector needle hangs Redis.** `contains(<any
  string-valued operand>, @v1)` never returns and pins the server at 100% CPU:
  `contains` scans the needle as a C string, and a hash vector field is a binary
  blob containing NUL bytes. valkey-search answers these normally, so this is a
  defect in the reference engine. `generate_expr.py` skips the needle position
  for vector operands. Worth reporting upstream to Redis.

* **Hash numeric fields are typed inconsistently by Redis.** `@n1 = 0` is falsy
  in `@n1&&@n2` but truthy in `(-1)&&(@n1)`, where the stored value `"0"` reads
  as a non-empty string. The only thing that changed is whether the other
  operand is a schema-typed field or a literal, so no single rule reproduces
  both. `generate_expr.py` excludes these on hash indexes; JSON indexes carry
  real types and agree.

* **`integration/run.sh` zaps every valkey-server on the host.** Its `zap`
  helper matches on process name and `kill -9`s all of them, so two integration
  runs on one machine kill each other's servers. Failures look like
  `Connection closed by server` or `Error 111 connecting`, and clear on a re-run.

* **The `kText` fix is ungated.** Allowing a TEXT field as an `APPLY`/`FILTER`
  operand turns a hard error into a result, so no client can have depended on
  the old behavior and no `search.emulate-release` gate was added. That is
  inconsistent with how the other fixes here are handled, and may warrant a gate
  plus a COMPATIBILITY.md row.

* **Intra-group ordering of `FIRST_VALUE` and `TOLIST` differs** between the
  engines. Without a `BY` clause the order is unspecified, so this is probably
  noise rather than a defect.
