The `FT.HYBRID` command runs two searches over one index and combines their results into a single ranked list.

The first search is the `SEARCH` arm: a text, tag or numeric query, scored for relevance the way `FT.SEARCH` scores one. The second is the `VSIM` arm: a vector similarity search, scored by distance. Each arm ranks its own matches independently.

The two ranked lists are then fused into one. Fusion is what makes this a hybrid search rather than two searches: a document that both arms found is rewarded for appearing in both, and the `COMBINE` clause decides how the two arms' scores are weighed against each other.

The fused list is then fed through the same processing stages as `FT.AGGREGATE`, so the result can be projected, filtered, grouped, sorted and trimmed before it is returned.

```
FT.HYBRID <index-name>
    SEARCH <query> [SCORER <scorer>] [YIELD_SCORE_AS <alias>]
    VSIM <field> <vector> [KNN <count> [K <k>] [EF_RUNTIME <ef>] [SHARD_K_RATIO <ratio>]]
                          [FILTER <expression>] [YIELD_SCORE_AS <alias>]
    [COMBINE
      ( RRF      <count> [CONSTANT <c>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
      | LINEAR   <count> [ALPHA <a> BETA <b>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
      | FUNCTION <count> EXPR <expression> [WINDOW <w>] [YIELD_SCORE_AS <alias>]
      )]
    [DIALECT <dialect>]
    [LOAD * | LOAD <count> <field> [AS <alias>] [<field> [AS <alias>] ...]]
    [PARAMS <count> <name> <value> [ <name> <value> ...]]
    [TIMEOUT <timeout>]
    (
      | APPLY <expression> AS <field>
      | FILTER <expression>
      | GROUPBY <count> <field> [<field> ... ] [[REDUCE <reducer> <count> [<expression> [<expression> ...]]] [ REDUCE ...]]
      | LIMIT <offset> <count>
      | SORTBY <count> <expression> [ASC | DESC] [<expression> [ASC | DESC] ...] [MAX <num>]
    )*
```

- `<index-name>` (required): The index to query. Both arms search the same index.
- `SEARCH <query>` (required): The non-vector arm. `<query>` is any query the search parser accepts, see [Search - query language](../topics/search-query.md). A vector query is rejected here — the vector search is the `VSIM` clause's job.
  - `SCORER <scorer>` (optional): The relevance scorer for this arm. The only supported scorer is `BM25STD`, which is also the default.
  - `YIELD_SCORE_AS <alias>` (optional): Emits this arm's score under `<alias>`, making it available to `COMBINE FUNCTION` and to the processing stages.
- `VSIM <field> <vector>` (required): The vector arm. `<field>` is a declared vector attribute and `<vector>` is a binary blob, supplied through `PARAMS`.
  - `KNN <count> [K <k>] [EF_RUNTIME <ef>] [SHARD_K_RATIO <ratio>]` (optional): The vector search parameters. `<count>` is a count of the arguments that follow within the block, not a count of parameters. `K` is the number of nearest neighbors to retrieve, between 1 and 10000, and defaults to 10. Omitting the whole block, or writing `KNN 0`, is the same as taking every default. `EF_RUNTIME` tunes HNSW's search breadth. `SHARD_K_RATIO` is accepted for compatibility and ignored.
  - `FILTER <expression>` (optional): Restricts which documents the vector search considers. The filter decides membership only; this arm's score remains the vector distance. See [Search - query language](../topics/search-query.md)
  - `YIELD_SCORE_AS <alias>` (optional): As for the `SEARCH` arm.
- `COMBINE` (optional): How the two arms' results are fused. Defaults to `RRF` with its own defaults when the clause is absent. In every form, `<count>` is a count of the arguments that follow within the clause, not a count of sub-arguments; sub-arguments may appear in any order. See [Fusion methods](#fusion-methods) below.
- `DIALECT <dialect>` (optional): Specifies your dialect. The only supported dialect is 2.
- `LOAD * | LOAD <count> <field> [AS <alias>] [...]` (optional): Which fields of the matched keys are loaded into the working set, exactly as for `FT.AGGREGATE`. Without a `LOAD` clause the result carries the key, the fused score, and any per-arm scores that were named. `AS <alias>` requires `search.emulate-release` to be at least `1.3.0`; below that the `AS` keyword is read as another field name and the load fails.
- `PARAMS <count> <name> <value> [...]` (optional): `<count>` is the number of arguments, i.e. twice the number of name/value pairs. Used to supply the `VSIM` query vector. A `$name` reference inside either arm's query text is not substituted — the same limitation `FT.SEARCH` and `FT.AGGREGATE` have.
- `TIMEOUT <timeout>` (optional): A timeout for the command, in milliseconds, between 1 and 60000.
- `APPLY`, `FILTER`, `GROUPBY`, `LIMIT`, `SORTBY` (optional): The `FT.AGGREGATE` processing stages, applied to the fused list in the order written. See [FT.AGGREGATE](ft.aggregate.md#processing-stages) for what each stage does.

# Result

The output is an array. The first element is a scalar that repeats the number of records returned and carries no other information — in particular it is not the total number of matches. The remainder is one element per record.

Each record is an array of field/value pairs. Without a `LOAD` clause every record carries `__key` and the fused score, under `__score` or under the alias given by `COMBINE ... YIELD_SCORE_AS`. A `LOAD` clause replaces those two implicit columns with the fields it names: load `@__key` to keep the key, and name the fused score with `COMBINE ... YIELD_SCORE_AS` to keep it. Per-arm scores appear under their own `YIELD_SCORE_AS` aliases either way.

Unlike `FT.AGGREGATE`, which returns every record, `FT.HYBRID` returns at most 10 records when the command writes no `LIMIT` clause. An explicit `LIMIT` stays where it is written in the pipeline; only the default is appended, so it runs after every other stage.

# Fusion methods

Fusion sees each arm's results ranked best-first, and every arm's score in the same direction: higher is better. The `VSIM` arm's distance is converted to a similarity before fusion, so a nearer document scores higher.

## RRF

```
COMBINE RRF <count> [CONSTANT <c>] [WINDOW <w>] [YIELD_SCORE_AS <alias>]
```

Reciprocal Rank Fusion scores a document by its _rank_ in each arm rather than by the arm's score, which makes it insensitive to the two arms producing scores on entirely different scales. A document at rank `r` in an arm contributes `1 / (c + r + 1)`, and the contributions from both arms are summed.

- `CONSTANT <c>` (optional, default `60`): The `c` above. A larger constant flattens the difference between ranks. Must be a non-negative finite number; fractional values are honoured.
- `WINDOW <w>` (optional, default `20`): How many of each arm's top results take part in fusion.

This is the default method: a command with no `COMBINE` clause fuses with `RRF` using these defaults.

## LINEAR

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

## FUNCTION

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

## WINDOW

`WINDOW` bounds how many of each arm's top results take part in fusion. It is a per-arm count, so with two arms the fused list can hold up to twice the window before the processing stages run.

`WINDOW 0` means "as wide as allowed" and resolves to the ceiling set by the `search.max-combine-window` configuration, which defaults to 1,000,000. Values above that ceiling are rejected.

The ceiling bounds a window the command asks for, and the window `FUNCTION` takes when it asks for none. It does not clamp the RRF and LINEAR default of 20: lowering the configuration below 20 rejects an explicit `WINDOW 20` while leaving the default untouched.

# Score naming

The fused score is emitted under `__score` unless `COMBINE ... YIELD_SCORE_AS` names it otherwise.

`__key` is reserved and cannot be used as a score alias anywhere.

`__score` cannot be used by `COMBINE ... YIELD_SCORE_AS` when the command has no `LOAD` clause, because that is already the name of the default score column; give the score a different name, or add a `LOAD` clause. A per-arm `YIELD_SCORE_AS` cannot use `__score` at all, with or without a `LOAD` clause, since it would collide with the fused score's own column.

# Notes

- **`VSIM RANGE` is not implemented.** The clause parses, so a command written for another engine is checked rather than misread, but executing one returns an error. Use `KNN`.
- **`POLICY` and `BATCH_SIZE` are accepted and ignored.** Both select how the vector search executes rather than what it answers, and this implementation does not expose that choice. Both belong to the `VSIM` clause and must sit outside the `KNN` block, after it; neither value is validated.
- **`NOCONTENT` is rejected.** `FT.HYBRID` always returns records; a query wanting keys only can ask for no `LOAD` clause.
- **`FT.HYBRID` cannot run inside `MULTI`/`EXEC` or a Lua script**, and is unavailable when the reader thread pool is disabled. All three force synchronous execution, which cannot revalidate the two arms' results against concurrent writes.
