# RediSearch probe recipes

Ready-made command sequences for measuring RediSearch reference behavior. Run each
against the Docker container (`scripts/redisearch-docker.sh cli <ARGS...>`), then run
the same shape against `valkey-server` + `libsearch.so` and diff the replies.

Before running any of these, classify the question against the repo's `COMPATIBILITY.md`
(see the skill's step 0): if it is an explicit non-goal — most commonly error-message
*wording* — no measurement is needed.

Always capture the version first so the result is reproducible:

```bash
scripts/redisearch-docker.sh cli MODULE LIST
```

## Unused / undefined PARAMS (issue #1372)

RediSearch tolerates a declared `PARAMS` entry that the query never references.

```
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA n NUMERIC name TEXT
HSET doc:1 n 5 name alice
HSET doc:2 n 9 name bob

# One param used (n via @n), one unused (unused) — RediSearch: succeeds.
FT.SEARCH idx "@n:[5 5]" PARAMS 4 n 5 unused 99 DIALECT 2

# All params unused — RediSearch: succeeds.
FT.SEARCH idx "*" PARAMS 2 foo bar DIALECT 2

# FT.AGGREGATE with an unused param — RediSearch: succeeds.
FT.AGGREGATE idx "*" PARAMS 2 foo bar DIALECT 2
```

Expected finding: no error on any of the three. valkey-search before the fix errored
`Parameter \`X\` not used.` on FT.SEARCH; FT.AGGREGATE already tolerated it.

## DIALECT differences

Some query constructs parse differently across dialects. Probe the same query at each
dialect the code path supports:

```
FT.SEARCH idx "@name:alice" DIALECT 1
FT.SEARCH idx "@name:alice" DIALECT 2
FT.SEARCH idx "@name:alice" DIALECT 3
```

Compare which dialects accept the syntax and whether reply shape changes.

## SORTBY / LIMIT / MAX retention bounds

Measure how many results are retained through SORTBY + LIMIT over a larger dataset.
Seed enough documents to exceed any internal cap, then vary LIMIT and MAX:

```
# seed, e.g., 10000 docs via a loop in the interactive shell or a pipe, then:
FT.SEARCH idx "*" SORTBY n ASC LIMIT 0 10
FT.AGGREGATE idx "*" SORTBY 2 @n ASC MAX 100 LIMIT 0 10
```

Record the count and ordering RediSearch returns as the retention target.

## ADDSCORES / WITHSCORES

Confirm how and where scores are exposed:

```
FT.SEARCH idx "@name:alice" WITHSCORES DIALECT 2
FT.AGGREGATE idx "*" ADDSCORES SORTBY 2 @__score DESC
```

## RESP2 vs RESP3 reply shape

Reply structure (arrays vs maps, field ordering) can differ by protocol version. Probe
both when the change touches reply format:

```
scripts/redisearch-docker.sh cli    FT.SEARCH idx "*" DIALECT 2   # RESP2
scripts/redisearch-docker.sh cli -3 FT.SEARCH idx "*" DIALECT 2   # RESP3
```

## Diffing against valkey-search

For each probe, run the identical command against a local valkey-server loading
`libsearch.so` (build/run per the `valkey-search-contrib` skill) and compare replies.
Per `COMPATIBILITY.md`, a difference in an *expected-compatibility* area (command syntax,
query semantics, reply shape, index types) is a bug; a difference in a *non-goal* area
(error wording, performance, persistence format, ACL strictness) is not. Note that
result ordering for queries **without** a defined sort order is explicitly allowed to
differ — compare the result *set*, not the sequence, in that case.
