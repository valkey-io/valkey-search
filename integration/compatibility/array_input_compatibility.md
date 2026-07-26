# ARRAY values as input to a later aggregate stage

PR #932 adds `TOLIST`, whose result is a new ARRAY value. The compatibility
suite covered ARRAY only as the *last* thing a pipeline produces. This adds
coverage for ARRAY as **input** to SORTBY, GROUPBY and APPLY, and fixes what
that coverage found: 132 of 362 answers differed from Redisearch to begin with,
and none do now.

## The tests

| File | Contents |
|---|---|
| `data_sets.py` | `array inputs` — 3 groups × 3 keys; `ga`/`gc` collect equal arrays, `n2` repeats inside a group so array/array operations meet both matching and mismatched lengths, `n3` holds timestamps, `t3` a parseable date |
| | `array inputs empty` — group `ge` has no `n2`/`t2` on any key, so TOLIST collects nothing and yields an empty array beside a populated one |
| | `array compare` — four group shapes that tell apart the rules for comparing two arrays (identical, differing second element, prefix, differing first element) |
| `generate_array.py` | every dyadic operator in five operand shapes, unary `!`, every function in `expr::function_table`, the unregistered array functions, SORTBY, a second GROUPBY, every reducer, the empty array, and array-vs-array comparison |

Every query has the shape `groupby 1 @t1 reduce tolist ... <stage under test>`,
since TOLIST is the only way to obtain an ARRAY.

Run with `./build.sh --run-integration-tests=array`; regenerate the reference
answers with `./integration/compatibility/regenerate.sh`.

### Harness fixes these needed

1. **`groupby` + `sortby` in one command was an unconditional `assert False`.**
   Row-ordering keys now come from the *last* GROUPBY/SORTBY in the pipeline —
   an earlier GROUPBY's key is gone from the reply once a later stage regroups.
2. **Rows keyed on an ARRAY were compared against the wrong rows.** The two
   engines return TOLIST elements in different orders (below), so sorting rows
   by the raw list ordered each reply differently. Row ordering now
   canonicalizes list values and breaks ties on the whole row.
3. **`parse_value` crashed on RESP nil**, which an APPLY returning nothing
   produces.

## What the coverage found, and how each was resolved

| | Answers | Resolution |
|---|---|---|
| Comparison against a scalar returned `kUNORDERED`, which `operator==` counts as equal, so `@array == 2` was true and `@array < "a"` false | 16 | ARRAY compares as the empty string, which is what Redisearch does — `@array == ""` is true there for an empty and a populated array alike. Array-vs-scalar now falls through to the string comparison. |
| An array evaluated as false, inverting `&&`, `\|\|` and `!` | 18 | `AsBool` reads a populated array as truthy and an empty one as falsy, matching Redisearch on both. |
| `abs`/`ceil`/`exp`/`floor`/`log`/`log2`/`sqrt` and `lower`/`upper` mapped over the elements | 34 | Redisearch accepts these and collapses the array to a scalar — `nan` for the numeric ones, nil for the case-folding ones. Their `IsArray` branch is gone; the ordinary non-numeric / non-string paths already produce exactly that. |
| A nil-valued APPLY or REDUCE output vanished from the reply | 50 | Redisearch names the field and replies nil. A `Nil` already carries a reason string clients never see, so it now records *which* nil it is: the default (an unwritten record field) reads as `kMissing` and stays out of the reply; every expression-produced nil is named with a nil value. Not array-specific — `APPLY lower(@n1)` over a plain numeric field dropped the field the same way. |
| GROUPBY keyed on the whole array, merging groups that collected equal arrays | 12 | Redisearch treats an array key as a multi-value field. `GroupBy::Execute` expands array-valued key components into the cartesian product of their elements; an empty array keys as nil. Reducer arguments are *not* expanded — they still see the whole array, as in Redisearch. Bounded by `--max-group-key-expansion` (default 65536 keys per record). |
| `MIN`/`MAX` returned the array itself; `STDDEV` returned 0 | 4 | Redisearch reads an array as the number 0 for MIN/MAX, and spreads it across the sample for STDDEV (unlike SUM and AVG, which read it as one unconvertible value and so contribute nothing — both engines return 0 there). |

No compatibility gate guards any of this: ARRAY is new in this PR, so there is
no released behavior to preserve.

## Deliberately not tested: ordering of the collected list

`TOLIST`'s element order is **deterministic but arbitrary**. Measured against
`redis/redis-stack-server` and Redis 8.8.1's bundled query engine:

| Probe | Result |
|---|---|
| Same data, same query, twice | identical order |
| Fresh server process | identical — the layout is not seeded randomly |
| redis-stack vs Redis 8.8.1 | identical across both engine builds |
| `0..9` loaded ascending | `7 5 1 4 2 0 8 9 3 6` |
| Same ten values, loaded in a different key order | `9 7 5 1 4 0 2 8 3 6` — different |
| Same group plus three keys repeating a value already present | `a c d b e` instead of `d c b a e` |

It is hash-table layout, not a contract: not sorted, not insertion order, not
the reverse of either, and it shifts when the ingest order changes, when a
duplicate key is added (no change to the distinct set), or when the element
count crosses a growth boundary. Reproducing it would mean reimplementing
Redisearch's hash function, table growth and iteration *and* feeding the
reducer in its key order — valkey collects in index-scan order.

So the comparison of TOLIST contents is order-insensitive, and two families of
query are left out of the generator with a comment saying why:

- `SORTBY @array` with `LIMIT`/`MAX`, where the ordering decides which rows
  survive. Redisearch keys on the first element of that hash-ordered list.
- Ordered comparison of two arrays (`<`, `<=`, `>=`, `>`). Both engines compare
  element by element, so the answer follows whichever element each engine holds
  first. Equality is unaffected for these shapes and is still tested.

## Extensions: what valkey answers where Redisearch rejects the query

124 of the 362 answers are queries Redisearch fails outright — arithmetic
(`Could not convert value to a number`), ordered comparison against a numeric
scalar, and `strlen`/`startswith`/`contains` over an array. valkey answers
these element-wise instead. Since no query Redisearch accepts can disagree,
this broadcast is a superset rather than an incompatibility, but the harness
passes those cases without checking anything, so they have no regression
coverage of their own yet.

`arraylen`, `arrayat`, `isarray` and `flatten` are implemented in `value.cc`
but not registered in `expr::function_table`, so APPLY rejects them — as does
Redisearch, which is why those 10 answers agree. They are either extensions to
register and document or dead code to delete.

## Divergences found on the way that are out of this PR's scope

- Redisearch **drops the whole row** when an APPLY references a field the key
  does not have; valkey keeps the row without the field. A row-count
  difference, untested here.
- Redisearch requires a property to be LOADed before an APPLY can reference it
  (`Property 's' not loaded nor in pipeline`); valkey resolves it anyway.
- `LOAD` of a field that is in no schema is accepted and ignored by Redisearch,
  rejected by valkey (`Index field 'nosuch' does not exist`).
- `MIN`/`MAX` over a group where every record lacks the field: Redisearch
  returns 0, valkey leaves the field out.
