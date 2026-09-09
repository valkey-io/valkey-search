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
field's name, two renames at once, renames referenced by a following `SORTBY`,
`APPLY` or `GROUPBY`, and — on a JSON index — loads and renames written as a
JSON path.

### 5.2. Field references under `LOAD *` on a JSON index — `excluded`

**Status:** open, tracked outside this suite.

Naming an indexed field in a pipeline stage — `GROUPBY 1 @color`,
`SORTBY 2 @price ASC` — does not resolve under `LOAD *` on a JSON index,
because the document arrives as a single `$` column. Writing the field into the
LOAD clause explicitly (`LOAD 1 @color GROUPBY 1 @color`) works; only the
implicit `LOAD *` form does not.

This is an FT.AGGREGATE limitation rather than an FT.HYBRID one: the equivalent
FT.AGGREGATE query has the same problem. The JSON variants of
`test_groupby_reduce` and the field-sorting cases of `test_sortby` are
therefore recorded `excluded` — comparing them here would test that gap instead
of FT.HYBRID.

### 5.3. Reference engine image — TODO

**Status:** temporary.

`generate_hybrid.py` overrides `BaseCompatibilityTest.DOCKER_IMAGE` to
`redis:8`, because FT.HYBRID does not exist in `redis/redis-stack-server`
(RediSearch 2.x) and was added in the Redis 8.4 query engine. A separate PR
moves the shared image to `redis:latest` for every generator; once that lands,
the `DOCKER_IMAGE` override marked `TODO(reference-image)` can be dropped and
this generator can inherit the shared image again.
