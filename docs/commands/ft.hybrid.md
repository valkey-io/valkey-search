The `FT.HYBRID` command runs two searches over one index and combines their results into a single ranked list.

The first search is the `SEARCH` arm: a text, tag or numeric query, scored for relevance the way `FT.SEARCH` scores one. The second is the `VSIM` arm: a vector similarity search, scored by distance. Each arm ranks its own matches independently.

The two ranked lists are then fused into one. Fusion is what makes this a hybrid search rather than two searches: a document that both arms found is rewarded for appearing in both, and the `COMBINE` clause decides how the two arms' scores are weighed against each other.

The fused list is then fed through the same processing stages as `FT.AGGREGATE`, so the result can be projected, filtered, grouped, sorted and trimmed before it is returned.

Unlike `FT.AGGREGATE`, which returns every record, `FT.HYBRID` returns at most 10 records when the command writes no `LIMIT` clause. An explicit `LIMIT` stays where it is written in the pipeline; only the default is appended, so it runs after every other stage.

```
FT.HYBRID <index-name>
    SEARCH <query> [SCORER <scorer>] [YIELD_SCORE_AS <alias>]
    VSIM @<field> $<param> [KNN <count> [K <k>] [EF_RUNTIME <ef>] [SHARD_K_RATIO <ratio>]]
                           [FILTER [<count>] <expression>
                             [POLICY (ADHOC | BATCHES [BATCH_SIZE <n>])]]
                           [YIELD_SCORE_AS <alias>]
    [COMBINE
      ( RRF      <count> [CONSTANT <c>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
      | LINEAR   <count> [ALPHA <a> BETA <b>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
      | FUNCTION <count> EXPR <expression> [WINDOW <w>] [YIELD_SCORE_AS <alias>]
      )]
    [DIALECT <dialect>]
    [LOAD * | LOAD <count> <field> [AS <alias>] [<field> [AS <alias>] ...]]
    [PARAMS <count> <name> <value> [ <name> <value> ...]]
    [TIMEOUT <timeout>]
    [WITHCURSOR [COUNT <count>] [MAXIDLE <maxidle>]]
    (
      | APPLY <expression> AS <field>
      | FILTER <expression>
      | GROUPBY <count> [<field> ... ] [[REDUCE <reducer> <count> [<expression> [<expression> ...]]] [ REDUCE ...]]
      | LIMIT <offset> <count>
      | SORTBY <count> <expression> [ASC | DESC] [<expression> [ASC | DESC] ...] [MAX <num>]
    )*
```

- `<index-name>` (required): The index to query. Both arms search the same index.
- `SEARCH <query>` (required): The non-vector arm. `<query>` is any query the search parser accepts, see [Search - query language](../topics/search-query.md). A vector query is rejected here — the vector search is the `VSIM` clause's job.
  - `SCORER <scorer>` (optional): The relevance scorer for this arm. The only supported scorer now is `BM25STD`, which is also the default.
  - `YIELD_SCORE_AS <alias>` (optional): Emits this arm's score under `<alias>`, making it available to `COMBINE FUNCTION` and to the processing stages.
- `VSIM @<field> $<param>` (required): The vector arm. `@<field>` names a vector attribute of the index; the `@` is required. `$<param>` names a `PARAMS` entry that holds the query vector as a binary blob, which must be exactly the size the field's type and dimension call for; the `$` is required. The vector cannot be written inline.
  - `KNN <count> [K <k>] [EF_RUNTIME <ef>] [SHARD_K_RATIO <ratio>]` (optional): The vector search parameters. `<count>` is a count of the arguments that follow within the block, not a count of parameters. `K` is the number of nearest neighbors to retrieve, between 1 and `search.max-vector-knn` (default 10000), and defaults to 10. The `VSIM` arm never contributes more than `K` documents to fusion, whatever the `WINDOW`. Omitting the whole block, or writing `KNN 0`, is the same as taking every default. `EF_RUNTIME` tunes the HNSW search breadth. `SHARD_K_RATIO` is accepted for compatibility and ignored.
  - `FILTER <expression>` or `FILTER <count> <expression> [POLICY ...]` (optional): A pre-filter, written in the [query language](../topics/search-query.md). The vector search considers only documents that match it. The filter decides membership only: this arm's score is still the vector similarity, never a text score.
    - `<count>` counts every token after it, the expression included, as in `FILTER 3 "@cat:{shoes}" POLICY ADHOC`. It may be left out when the filter is a bare expression, and is required when `POLICY` is given.
    - `POLICY ADHOC`: filter first, then compute exact distances. Cannot be combined with `EF_RUNTIME`.
    - `POLICY BATCHES [BATCH_SIZE <n>]`: run the vector search first and filter inline. `BATCH_SIZE` is allowed only here, and its value is ignored.
    - Without `POLICY` the query planner chooses. With a wildcard expression (`FILTER 3 * POLICY ...`) there is nothing to filter, so either policy runs a plain KNN search, as Redis does.
    - The policy values are literal tokens, not `PARAMS` references.
    - A `FILTER` that directly follows the `VSIM` clause is always this pre-filter, in query syntax, even when it was meant as a processing stage. `VSIM @vec $q FILTER "@price > 10"` is therefore an error. To filter the fused results with an expression, put the `FILTER` stage after `COMBINE`, `LOAD` or another stage. If more than one `FILTER` follows `VSIM`, only the last one is used.
  - `YIELD_SCORE_AS <alias>` (optional): As for the `SEARCH` arm. The value is this arm's similarity, as described in [Fusion methods](#fusion-methods), not the raw distance `FT.SEARCH` reports.
- `COMBINE` (optional): How the two arms' results are fused. Defaults to `RRF` with its own defaults when the clause is absent. In every form, `<count>` is a count of the arguments that follow within the clause, not a count of sub-arguments; sub-arguments may appear in any order. See [Fusion methods](#fusion-methods) below.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `LOAD * | LOAD <count> <field> [AS <alias>] [...]` (optional): Which fields of the matched keys are loaded into the working set, exactly as for `FT.AGGREGATE`. Without a `LOAD` clause the result carries the key, the fused score, and any per-arm scores that were named. `AS <alias>` is always honored: `FT.HYBRID` is new in 1.3.0 and does not depend on `search.emulate-release`.
- `PARAMS <count> <name> <value> [...]` (optional): `<count>` is the number of arguments, i.e. twice the number of name/value pairs. Used to supply the `VSIM` query vector. A `$name` reference inside either arm's query text is not substituted — the same limitation `FT.SEARCH` and `FT.AGGREGATE` have.
- `TIMEOUT <timeout>` (optional): A timeout for the command, in milliseconds. It must not exceed `search.max-timeout-ms` (default 60000). The default is the value of `search.default-timeout-ms` (50000).
- `WITHCURSOR [COUNT <count>] [MAXIDLE <maxidle>]` (optional): Returns at most `<count>` records and saves the remaining records in a cursor, which is read with [`FT.CURSOR`](ft.cursor.md), exactly as for [`FT.AGGREGATE`](ft.aggregate.md). `<count>` must be between 1 and `search.cursor-max-count`, the default is 1000. `<maxidle>` is the number of milliseconds the cursor may go unread before it is destroyed; it must be between 1 and `search.cursor-max-idle-ms`, the default is 300000. `WITHCURSOR` may appear anywhere among the clauses that follow `SEARCH`, `VSIM` and `COMBINE`, and ends any of those three clauses the way `LOAD` does. If it is given more than once, the last one is used. A cursor is no exception to the cancellation rules: a query cancelled by its `TIMEOUT` is an error, as it is without `WITHCURSOR`. The cursor pages through the records the command would have returned without `WITHCURSOR`, so the default `LIMIT` of 10 described above still applies.
- `APPLY`, `FILTER`, `GROUPBY`, `LIMIT`, `SORTBY` (optional): The `FT.AGGREGATE` processing stages, applied to the fused list in the order written. See [FT.AGGREGATE](ft.aggregate.md#processing-stages) for what each stage does.

## Fusion methods

Fusion sees each arm's results ranked best-first. The `VSIM` arm's distance, defined in [Vector Distance](../topics/search-scoring.md#vector-distance), is converted to a similarity before fusion:

- `L2`: the similarity is `1 / (1 + distance)`, so a nearer document scores higher.
- `COSINE`: the similarity is `1 - distance / 2`, so a nearer document scores higher.
- `IP`: the similarity is `(1 + distance) / 2`, so a nearer document scores lower.

`RRF` uses only the ranks and is not affected. `LINEAR` and `FUNCTION` use the similarity value directly, so on an `IP` index a positive `BETA` favors farther documents.

### RRF

```
COMBINE RRF <count> [CONSTANT <c>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
```

Reciprocal Rank Fusion scores a document by its _rank_ in each arm rather than by the arm's score, which makes it insensitive to the two arms producing scores on entirely different scales. A document at position `r` in an arm, counting from 1 for the arm's best result, contributes `1 / (c + r)`, and the contributions from both arms are summed. With the default constant, a document that is first in both arms scores `2/61`.

- `CONSTANT <c>` (optional, default `60`): The `c` above. A larger constant flattens the difference between ranks. Must be a non-negative finite number; fractional values are honored.
- `WINDOW <w>` (optional, default `20`): How many of each arm's top results take part in fusion.

This is the default method: a command with no `COMBINE` clause fuses with `RRF` using these defaults.

### LINEAR

```
COMBINE LINEAR <count> [ALPHA <a> BETA <b>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
```

A weighted sum of the arms' raw scores: `a * search_score + b * vector_similarity`. A document absent from an arm contributes nothing from it.

- `ALPHA <a>` (optional, default `0.3`): The weight applied to the `SEARCH` arm.
- `BETA <b>` (optional, default `0.7`): The weight applied to the `VSIM` arm.
- `WINDOW <w>` (optional, default `20`): As for `RRF`.

The two weights are optional together, not individually. Writing neither takes the defaults above, so `COMBINE LINEAR 0` fuses exactly as `COMBINE LINEAR 4 ALPHA 0.3 BETA 0.7` does. Writing exactly one is an error: the other is not defaulted for it, because a half-written pair is much more likely to be a typo than a request for a default.

Any finite value is accepted, including negative values and values greater than 1.

The scores are used as they stand, with no per-arm normalization. Normalizing would make a document's fused score depend on which _other_ documents happened to come back in the same arm, so the same document against the same query would score differently as the corpus around it changed. Use `ALPHA` and `BETA` to balance the arms instead.

### FUNCTION

```
COMBINE FUNCTION <count> EXPR <expression> [WINDOW <w>] [YIELD_SCORE_AS <alias>]
```

The fused score is whatever your expression computes from the arms' scores. This is a valkey-search extension.

- `EXPR <expression>` (required): An expression over the per-arm scores. See [Search - expressions](../topics/search-expressions.md) for the syntax.
- `WINDOW <w>` (optional): Defaults to the widest window allowed rather than to 20, because a user expression is normally expected to see every candidate.

Each arm's score is reachable from the expression three ways: by the arm's own `YIELD_SCORE_AS` alias, by position as `@__arm0_score` and `@__arm1_score`, and — for the standard two-arm shape — as `@__search_score` and `@__vector_score`.

```
FT.HYBRID idx
  SEARCH "@title:running shoes" YIELD_SCORE_AS s
  VSIM @embedding $q KNN 2 K 10 YIELD_SCORE_AS v
  COMBINE FUNCTION 4 EXPR "@s * 10 + @v" YIELD_SCORE_AS score
  PARAMS 2 q <blob>
```

An arm that did not return a given document has no score for it, and an expression that reads a missing arm score evaluates to 0 for that document as a whole — not merely for that term. `@v * 0 + 100` yields 0, not 100, for a document the vector arm did not return. Guard with `exists(@alias)`, which is 0 for a missing score and 1 otherwise, if such a document should keep a non-zero fused score.

### Defaults

| Setting | Default | Notes |
|---|---|---|
| Fusion method | `RRF` | Used when there is no `COMBINE` clause. |
| `RRF CONSTANT` | `60` | Non-negative, finite; fractional values allowed. |
| `RRF` and `LINEAR` `WINDOW` | `20` | `WINDOW 0` means as wide as allowed. |
| `FUNCTION` `WINDOW` | As wide as allowed | See [WINDOW](#window). |
| `LINEAR ALPHA` / `BETA` | `0.3` / `0.7` | Give both or neither. |
| Fused score name | `__score` | Renamed by `COMBINE ... YIELD_SCORE_AS`. |

### WINDOW

`WINDOW` bounds how many of each arm's top results take part in fusion. It is a per-arm count, so with two arms the fused list can hold up to twice the window before the processing stages run. The `VSIM` arm returns at most `K` results, so a window larger than `K` does not bring in more vector results: raise `K` together with `WINDOW`.

`WINDOW 0` means "as wide as allowed" and resolves to the ceiling set by the `search.max-combine-window` configuration, which defaults to 1,000,000. Values above that ceiling are rejected.

The ceiling bounds a window the command asks for, and the window `FUNCTION` takes when it asks for none. It does not clamp the RRF and LINEAR default of 20: lowering the configuration below 20 rejects an explicit `WINDOW 20` while leaving the default untouched.

## Score naming

The fused score is emitted under `__score` unless `COMBINE ... YIELD_SCORE_AS` names it otherwise.

`__key` is reserved and cannot be used as a score alias anywhere.

An alias cannot be empty.

`__score` cannot be used by `COMBINE ... YIELD_SCORE_AS` when the command has no `LOAD` clause, because that is already the name of the default score column; give the score a different name, or add a `LOAD` clause.

A per-arm `YIELD_SCORE_AS` alias must differ from the fused score's name: `__score`, or the `COMBINE ... YIELD_SCORE_AS` alias if one is given. For example, `SEARCH ... YIELD_SCORE_AS fs ... COMBINE RRF 2 YIELD_SCORE_AS fs` is rejected.

A score alias, per-arm or fused, cannot be the same as a column that a named `LOAD` list emits, including a name given with `AS`. Under `LOAD *`, a document field with the same name as a score alias is replaced by the score.

A per-arm alias is missing from a record when that arm did not return the document. Under `LOAD *`, per-arm aliases cannot be referenced by the processing stages.

## Reply

The reply has the same structure as the [`FT.AGGREGATE` reply](ft.aggregate.md#reply), with and without `WITHCURSOR`; see that page for details. As there, N is the number of records returned, not the total number of matches.

Without a `LOAD` clause every record carries `__key` and the fused score, under `__score` or under the alias given by `COMBINE ... YIELD_SCORE_AS`. A `LOAD` clause replaces those two implicit columns with the fields it names: load `@__key` to keep the key, and either name the fused score with `COMBINE ... YIELD_SCORE_AS` or list `@__score` in the `LOAD` clause to keep it. Per-arm scores appear under their own `YIELD_SCORE_AS` aliases either way.

Unless a `SORTBY` stage orders them otherwise, records are returned by fused score, highest first, with ties broken by key name in ascending order. The fused score is stored as a 32-bit float.

## Example

An index with a text field and a 2-dimensional `FLOAT32` vector field, and three documents:

```
FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT vec VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2
HSET doc:1 title "red running shoes" vec "\x00\x00\x00\x00\x00\x00\x00\x00"
HSET doc:2 title "blue sandals"      vec "\x00\x00\x80\x3f\x00\x00\x00\x00"
HSET doc:3 title "running shorts"    vec "\x00\x00\xa0\x40\x00\x00\xa0\x40"
```

The vectors are `[0, 0]`, `[1, 0]` and `[5, 5]`. The query matches `shoes` in the text arm and asks for the two nearest neighbors of `[0, 0]` in the vector arm, fusing with the default `RRF`:

```
FT.HYBRID idx
  SEARCH "@title:shoes" YIELD_SCORE_AS ts
  VSIM @vec $q KNN 2 K 2 YIELD_SCORE_AS vs
  PARAMS 2 q "\x00\x00\x00\x00\x00\x00\x00\x00"
```

```
1) (integer) 2
2) 1) "__key"
   2) "doc:1"
   3) "__score"
   4) "0.0327868834138"
   5) "ts"
   6) "1.75636863708"
   7) "vs"
   8) "1"
3) 1) "__key"
   2) "doc:2"
   3) "__score"
   4) "0.0161290317774"
   5) "vs"
   6) "0.5"
```

`doc:1` is first in both arms, so it scores `1/61 + 1/61`. `doc:2` is second in the vector arm only, so it scores `1/62` and has no `ts`. Its `vs` is the L2 similarity `1 / (1 + 1)`. `doc:3` is in neither arm.

## Cluster behavior

- `FT.HYBRID` takes no `SOMESHARDS`, `ALLSHARDS`, `CONSISTENT` or `INCONSISTENT` arguments. Partial and consistent results follow the `search.enable-partial-results` and `search.enable-consistent-results` configurations.
- A shard where either arm fails is dropped for both arms, so no document is fused from one arm's results only. An arm that no shard answered is an error, even when partial results are enabled. A cluster consistency failure is always an error.
- Each shard is asked for up to `max(WINDOW, 10)` results for the `SEARCH` arm and `max(K, WINDOW)` for the `VSIM` arm. A `COMBINE FUNCTION` with no `WINDOW` asks each shard for up to the widest window allowed, which can be costly.
- In cluster mode each arm is checked against concurrent writes on its own shard, so on a given shard the two arms can see different write states.
- Text scores are computed per shard; see [Per-Shard Values](../topics/search-scoring.md#per-shard-values).

## Common errors

- `VSIM vector field must start with '@', got ...` or `VSIM vector parameter must start with '$', got ...`: write `VSIM @<field> $<param>`.
- `Parameter <name> not found.`: the `$<param>` named by `VSIM` has no `PARAMS` entry.
- `Error parsing vector similarity parameters: query vector blob size (...) does not match index's expected size (...).`: the blob does not match the field's type and dimension.
- `A vector query is not supported in the SEARCH clause; use VSIM`: the vector search belongs in the `VSIM` clause.
- `Invalid filter expression: ...`: often an expression-language `FILTER` placed directly after `VSIM`, where it is read as the vector pre-filter. See `VSIM` `FILTER` above.
- `COMBINE LINEAR: missing value for BETA` (or `ALPHA`): give both weights or neither.
- `YIELD_SCORE_AS ... collides with ...`: see [Score naming](#score-naming).

## Notes

- **`VSIM RANGE` is not implemented.** The clause parses, so a command written for another engine is checked rather than misread, but executing one returns an error. Use `KNN`.
- **`POLICY` is active inside a counted VSIM `FILTER`.** `ADHOC` forces exact filter-first execution and `BATCHES` forces vector-search-first execution with inline filtering. Other values, duplicate declarations, and placement outside the counted FILTER block are rejected.
- **`BATCH_SIZE` is accepted but ignored.** It is valid only after `POLICY BATCHES`; fixed-size batch iteration is tracked separately.
- **`NOCONTENT` is rejected.** `FT.HYBRID` always returns records; a query wanting keys only can ask for no `LOAD` clause.
- **`FT.HYBRID` cannot run inside `MULTI`/`EXEC` or a Lua script**, and is unavailable when the reader thread pool is disabled. All three force synchronous execution, which cannot revalidate the two arms' results against concurrent writes.
