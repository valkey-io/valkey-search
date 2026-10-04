---
title: "Valkey Search - Scoring"
description: How Valkey Search computes relevance scores and uses them to order query results
---

A score is a number that Valkey Search computes for each key that matches a query.
A higher score means a better match.
By default, `FT.SEARCH` uses the score to order the results of text, tag, and numeric queries, and of KNN queries with a text filter.
Other KNN queries order results by vector distance.
A `SORTBY` clause overrides both orders.

This page first explains what a score is and how each field type is scored.
It then describes where scores appear, how to modify and combine them, score fusion, and scoring in cluster mode.
The last section gives a worked example that you can check by hand, and recipes to tune the ranking.

# Version Requirement

Relevance scoring requires Valkey Search 1.3.0 or later.
In earlier releases:

- `FT.SEARCH` has no `WITHSCORES` or `SCORER` option.
- `FT.CREATE` rejects `SCORE_FIELD` and accepts only `SCORE 1.0`.
- The `FT.HYBRID` command does not exist.

# What Is a Score?

A score is a value that shows how relevant a key is to a query.
Valkey Search measures relevance in three ways:

- Text and tag clauses get a relevance score. A higher score is a better match. See [Text Fields](#text-fields).
- For KNN queries, the vector distance measures relevance. A smaller distance is a better match. See [Vector Fields](#vector-fields).
- `FT.HYBRID` fuses the results of a text search and a vector search into one score. See [Score Fusion in FT.HYBRID](#score-fusion-in-fthybrid).

The rest of this section is about the relevance score.
It compares a key with the other keys in the index.
It has no upper limit, and its value depends on the contents of the index as well as on the key and the query.
Thus a score is useful to compare keys in the result of one query, but not to compare results across queries.

The score of a key can change when the key itself does not change:

- Another key is added, updated, or deleted. This changes the number of keys, the number of keys that contain each term, and the average key length. See [Text Fields](#text-fields) for how the formula uses them.
- The query changes. Each matched clause adds to the score, so a query with more clauses usually gives higher scores.
- The query uses `$weight`, or the index uses `SCORE` or `SCORE_FIELD`.

For example:

```
> FT.CREATE r ON HASH PREFIX 1 r: SCHEMA title TEXT
OK
> HSET r:1 title "rain jacket"
(integer) 1
> HSET r:2 title "wool coat"
(integer) 1
> HSET r:3 title "red hat"
(integer) 1
> HSET r:4 title "blue scarf"
(integer) 1
> FT.SEARCH r jacket WITHSCORES NOCONTENT
1) (integer) 1
2) "r:1"
3) "1.20397281647"
> HSET r:5 title "jacket"
(integer) 1
> FT.SEARCH r jacket WITHSCORES NOCONTENT
1) (integer) 2
2) "r:5"
3) "1.0700173378"
4) "r:1"
5) "0.837404847145"
> DEL r:5
(integer) 1
> FT.SEARCH r jacket WITHSCORES NOCONTENT
1) (integer) 1
2) "r:1"
3) "1.20397281647"
```

The score of `r:1` decreases from 1.204 to 0.837 after `r:5` is added, although `r:1` did not change.
After `r:5` is deleted, the score of `r:1` is 1.204 again.

Valkey Search updates these statistics when it indexes a change.
A deleted key stops counting when Valkey Search removes it from the index.
There is no separate cleanup step.

A fixed score threshold, for example "show only results with a score above 1.0", can behave differently for different queries and as the index changes.
If you use a threshold, test it with the queries and the data that you expect.

# How Each Field Type Is Scored

Each clause of a query contributes a score that depends on the type of the field that it matches:

| Field type | Contribution to the score |
| :--- | :--- |
| `TEXT` | A BM25 score for each matched term. |
| `TAG` | A BM25 score for each matched tag value, with a term frequency of 1. |
| `NUMERIC` | 0. A numeric clause filters keys. It does not change their order. |
| `VECTOR` | 0. A vector clause has a distance, not a relevance score. |

## Text Fields

The `BM25STD` scorer gives each matched text term this score:

$$
\text{term score} = \text{weight} \cdot \text{IDF} \cdot \frac{f \cdot (k_1 + 1)}{f + k_1 \cdot \left(1 - b + b \cdot \frac{dl}{avgdl}\right)}
$$

$$
\text{IDF} = \ln\left(1 + \frac{N - n + 0.5}{n + 0.5}\right)
$$

In the formulas, `k1` is written with a subscript 1.

| Symbol | Value |
| :--- | :--- |
| `N` | The number of keys in the index. |
| `n` | The number of keys that contain the term. |
| `f` | The number of times that the term occurs in the key, in the `TEXT` fields that the clause queries. |
| `dl` | The number of words in the key, in all of its `TEXT` fields, after stop word removal. |
| `avgdl` | The average `dl` of the keys in the index. |
| `k1` | 1.2. This constant limits how much repeated occurrences of a term increase the score. |
| `b` | 0.75. This constant sets how much the length of a key decreases the score. |
| `weight` | The `$weight` query attribute of the clause. The default is 1.0. |

The formula has three effects:

- A rare term contributes more than a common term. `IDF` decreases as `n` increases.
- More occurrences of a term increase the score, but each additional occurrence adds less than the previous one.
- A long key gets a lower score than a short key with the same number of occurrences. A search for `jacket` thus ranks the title "rain jacket" above the title "lightweight waterproof rain jacket with hood".

Stop words are not indexed, so they do not count in `dl`.
See [Text Fields](search-data-formats.md#text-fields) for how Valkey Search splits text into words, removes stop words, and finds word stems.

### Stemming Adds an Exact-Match Bonus

If the queried field uses stemming and the query does not use `VERBATIM`, one term can match several words that have the same stem.
For example, `running` matches `running`, `runs`, and `run`.
The score of the term is then the sum of up to three parts.
Each part has its own `f` and its own `IDF`:

1. The word exactly as written in the query.
2. The stem itself, if it is a different word that occurs in the index. For `running`, this part is `run`.
3. All indexed words that have the stem, other than the stem itself. For `running`, this part is `running` and `runs` together. Its `f` is the sum of the occurrences of these words in the key. Its `n` is the number of keys that contain at least one of them.

The word as written in the query is in part 1 and also in part 3.
Thus a key that contains the exact query word gets a higher score than a key that contains only a different form of the word.
Assume that `running` is the only indexed word with the stem `run`.
Then a query for `running` scores twice as high as the same query with `VERBATIM`.

A word that is its own stem, such as `jacket`, is never in part 3.
For a query for `jacket`, a key that contains `jacket` gets part 1, and a key that contains `jackets` gets part 3.

### Prefix, Suffix, and Fuzzy Terms

A prefix term (`run*`), a suffix term (`*ing`), or a fuzzy term (`%runing%`) can match many indexed words.
For each key, the term contributes the score of one matched word, with the `f` and `IDF` of that word.
The scores of the other matched words are not added.
If a key contains more than one matched word, the word that is used is not specified.

### Phrases, Slop, and Match-All

A quoted phrase (`"red running"`) is scored as an AND of its words.
The quoted words are not stemmed, so each word contributes only part 1.
`SLOP` and `INORDER` change which keys match, but not their scores.

The match-all query `*` gives each key one term score with `IDF` = 1 and `f` = 1.
Thus `*` ranks short keys above long keys.

## Tag Fields

A tag clause such as `@color:{red}` is scored with the same `BM25STD` formula, with these changes:

- `f` is always 1.
- `n` is the number of keys that have the tag value.
- `dl` and `avgdl` are the lengths of the `TEXT` fields of the key and the index.

If the index has no `TEXT` field, `avgdl` is 0 and every tag clause scores 0.

A clause that matches more than one tag value, such as `@color:{red|blue}`, adds the scores of all the matched values that the key has.
A tag prefix, such as `@color:{bl*}`, is an exception. It contributes the score of one matched value, as a text prefix term does.
A tag prefix needs at least `search.tag-min-prefix-length` characters before `*`. The default is 2.

## Numeric Fields

A numeric clause such as `@price:[0 100]` contributes 0.
It removes the keys that are outside the range and does not change the order of the other keys.

## Vector Fields

A vector query measures the distance between the query vector and each indexed vector.
A smaller distance is a better match.
See [`FT.CREATE`](../commands/ft.create.md) for the distance formulas of `L2`, `IP`, and `COSINE`.

A KNN query (`*=>[KNN ...]`) sorts its results by distance and returns the distance in the `__<field>_score` field.
`WITHSCORES` reports 0 for each result of a pure KNN query.
A KNN query can have a filter that contains a text clause, such as `(shoes)=>[KNN ...]`.
`WITHSCORES` then reports the score of the filter, and the results are sorted by that score.

A vector range clause (`@<field>:[VECTOR_RANGE ...]`) contributes 0 to the score.
See [Vector Range Match](search-query.md#vector-range-match) for how to return its distance.

## Clauses That Score 0

A clause contributes 0 to the score in these cases:

- The clause is a numeric range, a vector range, or a negation.
- The clause is a tag clause and the index has no `TEXT` field.
- The query is a pure KNN query. `WITHSCORES` then reports 0, and the distance is returned separately.

# Where Scores Appear and How They Are Used

These options return scores to the client:

| Command | Option | Result |
| :--- | :--- | :--- |
| [`FT.SEARCH`](../commands/ft.search.md) | `WITHSCORES` | The score follows each key name in the reply. |
| [`FT.AGGREGATE`](../commands/ft.aggregate.md) | `ADDSCORES` | Each record contains a `__score` field. Later stages can use it as `@__score`. |
| [`FT.HYBRID`](../commands/ft.hybrid.md) | `COMBINE ... YIELD_SCORE_AS` | Each record contains the fused score, under `__score` or under the alias that you name. |
| `FT.SEARCH` with a KNN query | `AS <name>` | Each result contains the vector distance, under `__<field>_score` or under `<name>`. A smaller distance is a better match. See [Vector Fields](#vector-fields). |

`SCORER <scorer>` selects the scoring function.
The only supported value is `BM25STD`, which is also the default. Any other value returns an error.

## How Scores Order Results

`FT.SEARCH` sorts results in this order:

1. If the command has a `SORTBY` clause, the results are sorted by the `SORTBY` field.
2. If the query is a KNN query and its filter has no text clause, the results are sorted by vector distance, nearest first. A KNN query without a filter is in this case.
3. All other results are sorted by score, highest first. This includes a KNN query with a text filter, such as `(shoes)=>[KNN 10 @vec $v]`. The KNN clause selects the nearest keys, and the text score then orders them.

The sort occurs before `LIMIT`.
Thus, when results are sorted by score, `LIMIT 0 10` returns the 10 keys with the highest scores.
The order of keys with equal scores is not defined, and it can change in a future release.
A query that contains only numeric clauses gives every key a score of 0, so the order of its results is not defined.
To get an order that does not change, see [Make the Order of Equal Scores Repeatable](#make-the-order-of-equal-scores-repeatable).

`FT.AGGREGATE` does not sort by score automatically.
To sort its records by score, use `ADDSCORES` and `SORTBY 2 @__score DESC`.

# How Scores Can Be Modified

The final score is the score of the whole query tree, multiplied by the document score of the key.
The document score is a number for each key that does not depend on the query:

$$
\text{score} = \text{document score} \cdot \text{query score}
$$

The query score combines the scores of the clauses in the query.
See [How Scores Combine](#how-scores-combine).

## Query Weights

The query `jacket (@color:{red}) => {$weight: 2.0}` gives this score.
In this formula, the jacket score is the score of `jacket`, and the red score is the score of `@color:{red}`:

$$
\text{score} = \text{document score} \cdot \left(\text{jacket score} + 2 \cdot \text{red score}\right)
$$

The `$weight` attribute multiplies the score of the clause that it is attached to.
The weight must be greater than 0.
A weight on a term is the `weight` in the term formula.
A weight on a group multiplies the sum of the group.

## Document Score

The document score comes from the index definition:

- `SCORE_FIELD <field>` sets the document score of each key to the value of that field.
- `SCORE <value>` sets the document score of the keys that do not have a valid `SCORE_FIELD` value. If the index has no `SCORE_FIELD`, `SCORE` applies to all keys.
- If the index definition has neither option, the document score is 1.0.

Use the document score to promote or demote keys independently of the query, for example to rank in-stock products above other products.

# How Scores Combine

A query is a tree of clauses.
Valkey Search computes the score of a key from the bottom of the tree to the top:

- An AND of clauses (`shoes @color:{red}`) adds the scores of all its clauses.
- An OR of clauses (`shoes | jacket`) adds the scores of the clauses that match the key. A clause that does not match the key contributes nothing.
- A negated clause (`-socks`) contributes 0.

# Score Fusion in FT.HYBRID

The [`FT.HYBRID`](../commands/ft.hybrid.md) command runs two searches on one index: a text, tag, or numeric search and a vector search.
It then fuses the two result lists into one list.
The `SEARCH` arm computes scores as `FT.SEARCH` does, with the `BM25STD` scorer.
The `VSIM` arm converts each vector distance into a similarity, so that a higher value is a better match in both arms.

The `COMBINE` clause selects how the two arms fuse:

- `RRF` (the default) uses the rank of a key in each arm, not its score. The two arms can use scores on different scales.
- `LINEAR` adds the scores of the two arms with weights that you set.
- `FUNCTION` computes the fused score with an expression that you write.

See [Fusion methods](../commands/ft.hybrid.md#fusion-methods) for the formulas and the options.

# Scores in Cluster Mode

In cluster mode, each shard computes scores from its own keys.
`N`, `n`, and `avgdl` are the values for the keys on that shard, not for the whole index.
Thus the same key can get a different score in cluster mode than on a single node.
The results from all shards are then sorted together by these scores, so keys whose scores come from different statistics are compared.

# Examples

## Read and Check Scores

This example creates a small index, runs scored queries, and computes one score by hand.

1. Create an index with a text field, a tag field, a numeric field, and a document score field:

   ```
   > FT.CREATE products ON HASH PREFIX 1 product: SCORE_FIELD boost SCHEMA title TEXT color TAG price NUMERIC
   OK
   ```

2. Add four products:

   ```
   > HSET product:1 title "red running shoes" color red price 80
   (integer) 3
   > HSET product:2 title "trail running shoes" color blue price 120
   (integer) 3
   > HSET product:3 title "blue rain jacket" color blue price 150
   (integer) 3
   > HSET product:4 title "wool socks" color red price 15
   (integer) 3
   ```

3. Search for `jacket` and return the scores:

   ```
   > FT.SEARCH products jacket WITHSCORES NOCONTENT
   1) (integer) 1
   2) "product:3"
   3) "1.16080248356"
   ```

   The reply contains one key and its score.

4. Compute the score by hand.
   The index has `N = 4` keys. One key contains `jacket`, so `n = 1`.
   `product:3` contains `jacket` once, so `f = 1`. Its title has 3 words, so `dl = 3`.
   The titles have 3, 3, 3, and 2 words, so `avgdl = 11 / 4 = 2.75`.

   ```
   IDF        = ln(1 + (4 - 1 + 0.5) / (1 + 0.5))               = 1.20397
   TF part    = (1 * 2.2) / (1 + 1.2 * (0.25 + 0.75 * 3 / 2.75)) = 0.96414
   term score = 1.20397 * 0.96414                               = 1.16080
   ```

   The TF part is the part of the term score formula after `IDF`. The `weight` is 1.0.

   The document score of `product:3` is 1.0, because the key has no `boost` field and the index has no `SCORE` option.
   The final score is thus 1.16080, which agrees with the reply.

5. Search for `running` with and without `VERBATIM`:

   ```
   > FT.SEARCH products running WITHSCORES NOCONTENT
   1) (integer) 2
   2) "product:1"
   3) "1.33658659458"
   4) "product:2"
   5) "1.33658659458"
   > FT.SEARCH products running WITHSCORES NOCONTENT VERBATIM
   1) (integer) 2
   2) "product:1"
   3) "0.668293297291"
   4) "product:2"
   5) "0.668293297291"
   ```

   `running` is the only indexed word with the stem `run`, so the exact-match bonus doubles the score.

6. Combine a text clause with a tag clause and with a numeric clause, then run the text clause and the tag clause alone:

   ```
   > FT.SEARCH products "shoes @color:{red}" WITHSCORES NOCONTENT
   1) (integer) 1
   2) "product:1"
   3) "2.00487995148"
   > FT.SEARCH products "shoes @price:[0 100]" WITHSCORES NOCONTENT
   1) (integer) 1
   2) "product:1"
   3) "1.33658659458"
   > FT.SEARCH products shoes WITHSCORES NOCONTENT
   1) (integer) 2
   2) "product:1"
   3) "1.33658659458"
   4) "product:2"
   5) "1.33658659458"
   > FT.SEARCH products "@color:{red}" WITHSCORES NOCONTENT
   1) (integer) 2
   2) "product:4"
   3) "0.780193567276"
   4) "product:1"
   5) "0.668293297291"
   ```

   The first query scores `product:1` at 2.00487995148.
   This is the score of `shoes` (1.33658659458) plus the score of `@color:{red}` (0.668293297291).
   The second query scores `product:1` at 1.33658659458, the same as `shoes` alone, because the numeric clause contributes 0.

7. Set a document score of 0.5 on `product:1` and search again:

   ```
   > HSET product:1 boost 0.5
   (integer) 1
   > FT.SEARCH products running WITHSCORES NOCONTENT
   1) (integer) 2
   2) "product:2"
   3) "1.33658659458"
   4) "product:1"
   5) "0.668293297291"
   ```

   The score of `product:1` is half of its previous score, so `product:1` now sorts after `product:2`.

## Tune the Ranking

The recipes in this section use this index and data:

```
> FT.CREATE p ON HASH PREFIX 1 p: SCHEMA title TEXT body TEXT color TAG sales NUMERIC
OK
> HSET p:1 title "rain jacket" body "a light coat" color red sales 10
(integer) 4
> HSET p:2 title "wool coat" body "warm jacket for winter" color blue sales 900
(integer) 4
> HSET p:3 title "red hat" body "matches any jacket" color red sales 50
(integer) 4
> HSET p:4 title "blue scarf" body "soft wool" color blue sales 5
(integer) 4
```

The query `jacket` matches `p:1` with a score of 0.374, and `p:2` and `p:3` with a score of 0.341 each:

```
> FT.SEARCH p jacket WITHSCORES NOCONTENT
1) (integer) 3
2) "p:1"
3) "0.373659491539"
4) "p:2"
5) "0.341167360544"
6) "p:3"
7) "0.341167360544"
```

### Find Out Why a Key Has Its Score

Valkey Search does not return a breakdown of a score.
To find the contribution of each clause, run each clause as a separate query with `WITHSCORES`.
The scores of the clauses of an AND add up to the score of the full query.

For example, `jacket @color:{red}` gives `p:1` a score of 1.100.
`jacket` alone gives 0.374, and `@color:{red}` alone gives 0.726:

```
> FT.SEARCH p "jacket @color:{red}" WITHSCORES NOCONTENT
1) (integer) 2
2) "p:1"
3) "1.09981369972"
4) "p:3"
5) "1.00417780876"
> FT.SEARCH p "@color:{red}" WITHSCORES NOCONTENT
1) (integer) 2
2) "p:1"
3) "0.726154208183"
4) "p:3"
5) "0.663010418415"
```

### Make Matches in One Field Count More

To make a match in the title count more than a match in the body, put a `$weight` on the title clause:

```
> FT.SEARCH p "(@title:jacket) => {$weight: 3.0} | (@body:jacket)" WITHSCORES NOCONTENT
1) (integer) 3
2) "p:1"
3) "1.12097847462"
4) "p:2"
5) "0.341167360544"
6) "p:3"
7) "0.341167360544"
```

The score of `p:1`, which has `jacket` in its title, increases from 0.374 to 1.121.
The scores of `p:2` and `p:3`, which have `jacket` only in the body, stay at 0.341.

### Filter Without a Change to the Ranking

A numeric clause contributes 0, so it filters keys without a change to their scores.
A tag clause adds its own score, so it can change the order of the results.
A `$weight` of 0 is not supported.

To filter by a tag without a change to the scores, filter in an `FT.AGGREGATE` stage instead of in the query:

```
> FT.AGGREGATE p jacket ADDSCORES LOAD 2 @__key @color FILTER "@color == 'red'" SORTBY 2 @__score DESC
1) (integer) 2
2) 1) __key
   2) "p:1"
   3) __score
   4) "0.373659491539"
   5) color
   6) "red"
3) 1) __key
   2) "p:3"
   3) __score
   4) "0.341167360544"
   5) color
   6) "red"
```

The records for `p:1` and `p:3` keep the scores 0.374 and 0.341 that the query `jacket` gives them.
The `LOAD` stage reads the `color` field of each key that matches `jacket`.
A filter in the query does not read the keys.

### Promote Keys with a Stored Value

To rank some keys above others for every query, store a number in each key and name its field with `SCORE_FIELD`.
Valkey Search multiplies the query score of each key by this number.
A key without the field gets the `SCORE` value of the index, which is 1.0 by default.
Thus a value of 2 doubles the score of a key, and a value of 0.5 halves it.
See [Document Score](#document-score).

Use this method when the value changes less often than the queries run, for example a product rating.

### Combine Relevance with Popularity at Query Time

To combine the score with a numeric field at query time, compute a new value with `APPLY` and sort by it:

```
> FT.AGGREGATE p jacket ADDSCORES LOAD 2 @__key @sales APPLY "@__score * log(2 + @sales)" AS rank SORTBY 2 @rank DESC
1) (integer) 3
2) 1) __key
   2) "p:2"
   3) __score
   4) "0.341167360544"
   5) sales
   6) "900"
   7) rank
   8) "2.32151237533"
3) 1) __key
   2) "p:3"
   3) __score
   4) "0.341167360544"
   5) sales
   6) "50"
   7) rank
   8) "1.34803539034"
4) 1) __key
   2) "p:1"
   3) __score
   4) "0.373659491539"
   5) sales
   6) "10"
   7) rank
   8) "0.928508955282"
```

The records are in the order `p:2` (rank 2.322), `p:3` (rank 1.348), and `p:1` (rank 0.929).
`p:1` has the highest score, but `p:2` has many more sales.
The logarithm keeps a large sales value from hiding the score.

### Make the Order of Equal Scores Repeatable

The order of keys with equal scores is not defined.
To get a repeatable order, sort by the score and then by a second field in `FT.AGGREGATE`:

```
> FT.AGGREGATE p jacket ADDSCORES LOAD 1 @__key SORTBY 4 @__score DESC @__key ASC
1) (integer) 3
2) 1) __key
   2) "p:1"
   3) __score
   4) "0.373659491539"
3) 1) __key
   2) "p:2"
   3) __score
   4) "0.341167360544"
4) 1) __key
   2) "p:3"
   3) __score
   4) "0.341167360544"
```

`p:2` and `p:3` have the same score, so `@__key ASC` puts `p:2` before `p:3`.
Use the same sort when you page through results with `LIMIT`.
If the index does not change between the requests, a key then cannot appear on two pages.
A change to the index between requests can change scores and move keys to a different page.

### Combine Text Results with Vector Results

To rank keys by a text query and by vector similarity together, use [`FT.HYBRID`](../commands/ft.hybrid.md).
See [Score Fusion in FT.HYBRID](#score-fusion-in-fthybrid).
