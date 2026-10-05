---
title: "Valkey Search - Scoring"
description: How Valkey Search computes relevance scores and uses them to order query results
---

A score is a number that Valkey Search computes for each key that matches a query.
A higher score means a better match.
By default, `FT.SEARCH` orders results by score.
A KNN query whose filter has no text or tag clause orders results by vector distance instead.
A `SORTBY` clause overrides both orders.
For a worked example and recipes to tune the ranking, see [Search - Scoring Examples](search-scoring-examples.md).

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
- For KNN queries whose filter has no text or tag clause, the vector distance measures relevance. A smaller distance is a better match. See [Vector Fields](#vector-fields).
- `FT.HYBRID` fuses the results of a text search and a vector search into one score. See [Score Fusion in FT.HYBRID](#score-fusion-in-fthybrid).

The clauses of an `FT.SEARCH` query select the score measure, and that measure sets the default sort order:

| Query shape | Score measure | Default sort order |
| :--- | :--- | :--- |
| Text or tag clauses, with or without numeric clauses | `BM25STD` score | By score, highest first |
| Only numeric or vector range clauses, or only tag clauses in an index whose schema has no `TEXT` field | 0 for every key | By key name |
| KNN query without a filter | Vector distance, in `__<field>_score` | By distance, nearest first |
| KNN query with a text or tag clause in its filter | `BM25STD` score of the filter | By score, highest first |
| KNN query with only numeric clauses in its filter | Vector distance, in `__<field>_score` | By distance, nearest first |
| Any of the above with `SORTBY` | Unchanged | By the `SORTBY` field |

The relevance score compares a key with the other keys in the index.
It has no upper limit, and its value depends on the contents of the index as well as on the key and the query.
Thus a score is useful to compare keys in the result of one query, but not to compare results across queries.

The score of a key can change when the key itself does not change:

- Another key is added, updated, or deleted. Each of these changes the number of keys, the number of keys that contain each term, and the average key length. See [Text Fields](#text-fields) for how the formula uses them.
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

A new score takes effect as soon as Valkey Search indexes the added, updated, or deleted key.

A fixed score threshold, for example "show only results with a score above 1.0", can behave differently for different queries and as the index changes.
If you use a threshold, test it with the queries and the data that you expect.

# How Each Field Type Is Scored

Each clause of a query contributes a score that depends on the type of the field that it matches:

| Field type | Contribution to the score |
| :--- | :--- |
| `TEXT` | A `BM25STD` score for each matched term. |
| `TAG` | A `BM25STD` score for each matched tag value, with a term frequency of 1. |
| `NUMERIC` | 0. A numeric clause filters keys. It does not change their order. |
| `VECTOR` | The vector distance, returned in `__<field>_score`. It does not add to the relevance score. |

## Text Fields

The `BM25STD` scorer gives each matched text term a score. These factors change the score:

- A rare term contributes more than a common term.
- More occurrences of a term increase the score, but each additional occurrence adds less than the previous one.
- A long key gets a lower score than a short key with the same number of occurrences. A search for `jacket` thus ranks the title "rain jacket" above the title "lightweight waterproof rain jacket with hood".

Stop words are not indexed, so they do not count toward the length of a key.
See [Text Field Format](search-data-formats.md#text-fields) for how Valkey Search splits text into words, removes stop words, and finds word stems.
For the formula and its constants, see [Term Score](#term-score).

### Stemming Adds an Exact-Match Bonus

If the queried field uses stemming and the query does not use `VERBATIM`, one term can match several words that have the same stem.
For example, `running` matches `running`, `runs`, and `run`.
A key that contains the exact query word gets a higher score than a key that contains only a different form of the word.
A `$weight` on the term applies only to the exact word. The other forms of the word keep a weight of 1.

### Prefix, Suffix, and Fuzzy Terms

A prefix term (`run*`), a suffix term (`*ing`), or a fuzzy term (`%runing%`) can match many indexed words.
For each key, the term contributes the score of one matched word.
A term that matches many words in a key thus scores no higher than a term that matches one.

### Phrases, Slop, and Match-All

A quoted phrase (`"red running"`) is scored as an AND of its words.
The quoted words are not stemmed.
`SLOP` and `INORDER` change which keys match, but not their scores.
The match-all query `*` ranks short keys above long keys.

## Tag Fields

A tag clause such as `@color:{red}` gets a `BM25STD` score, as a text term does.
Each matched tag value counts once per key. A rare tag value scores higher than a common one, and a key with more words in its `TEXT` fields scores lower. Only fields that the schema declares as `TEXT` count.
If the index schema has no `TEXT` field, every tag clause scores 0.

For example:

```
> FT.CREATE t ON HASH PREFIX 1 t: SCHEMA title TEXT color TAG
OK
> HSET t:1 title "rain jacket" color red
(integer) 2
> HSET t:2 title "warm winter wool coat" color red
(integer) 2
> HSET t:3 title "blue hat" color blue
(integer) 2
> FT.SEARCH t "@color:{red}" WITHSCORES NOCONTENT
1) (integer) 2
2) "t:1"
3) "0.523548364639"
4) "t:2"
5) "0.390191704035"
> FT.SEARCH t "@color:{blue}" WITHSCORES NOCONTENT
1) (integer) 1
2) "t:3"
3) "1.09256923199"
> FT.CREATE tagonly ON HASH PREFIX 1 t: SCHEMA color TAG
OK
> FT.SEARCH tagonly "@color:{red}" WITHSCORES NOCONTENT
1) (integer) 2
2) "t:1"
3) "0"
4) "t:2"
5) "0"
```

`t:1` and `t:3` both have 2-word titles.
`t:3` scores higher because `blue` is on one key and `red` is on two.
`t:2` scores lower than `t:1` because its title is longer.
The `tagonly` index has no `TEXT` field, so every key scores 0.

A clause that matches more than one tag value, such as `@color:{red|blue}`, adds the scores of all the matched values that the key has.
A tag prefix, such as `@color:{bl*}`, is an exception. It contributes the score of one matched value, as a text prefix term does.

## Numeric Fields

A numeric clause such as `@price:[0 100]` contributes 0.
It removes the keys that are outside the range and does not change the order of the other keys.

## Vector Fields

A vector query measures the distance between the query vector and each indexed vector.
A smaller distance is a better match.
See [Vector Distance](#vector-distance) for the distance formulas of `L2`, `IP`, and `COSINE`.

A KNN query (`*=>[KNN ...]`) sorts its results by distance and returns the distance in the `__<field>_score` field.
A KNN query can have a filter with no text or tag clause, such as `(@price:[0 100])=>[KNN ...]`.
Then the score of each result is its distance, and the results stay sorted by distance.
`WITHSCORES` does not return the distance. It reports 0, so read the distance from `__<field>_score`.
A KNN query can have a filter that contains a text or tag clause, such as `(shoes)=>[KNN ...]` or `(@color:{red})=>[KNN ...]`.
`WITHSCORES` then reports the score of the filter, and the results are sorted by that score.

A vector range clause (`@<field>:[VECTOR_RANGE ...]`) does not add to the relevance score.
See [Vector Range Match](search-query.md#vector-range-match) for how to return its distance.

# Where Scores Appear and How They Are Used

These options return scores to the client:

| Command | Option | Result |
| :--- | :--- | :--- |
| [`FT.SEARCH`](../commands/ft.search.md) | `WITHSCORES` | The score follows each key name in the reply. |
| [`FT.AGGREGATE`](../commands/ft.aggregate.md) | `ADDSCORES` | Each record contains a `__score` field. Later stages can use it as `@__score`. |
| [`FT.HYBRID`](../commands/ft.hybrid.md) | `COMBINE ... YIELD_SCORE_AS` | Without a `LOAD` clause, each record contains the fused score, under `__score` or under the alias that you name. With a `LOAD` clause, the fused score appears only if you name it with `YIELD_SCORE_AS` or load `@__score`. |
| `FT.SEARCH` with a KNN query | `AS <name>` | Each result contains the vector distance, under `__<field>_score` or under `<name>`. A smaller distance is a better match. See [Vector Fields](#vector-fields). |

`SCORER <scorer>` selects the scoring function.
The only supported value now is `BM25STD`, which is also the default. Any other value returns an error.

## How Scores Order Results

For the default sort order of each query shape, see the table in [What Is a Score?](#what-is-a-score).
In a KNN query with a text or tag clause in its filter, such as `(shoes)=>[KNN 10 @vec $v]`, the KNN clause selects the nearest keys, and the score of the filter then orders them.

The sort occurs before `LIMIT`.
Thus, when results are sorted by score, `LIMIT 0 10` returns the 10 keys with the highest scores.
Keys with equal scores are sorted by key name, in byte order. For example, `doc:10` sorts before `doc:2`.
Redis Search sorts keys with equal scores in the order in which they were added, so the two can return ties in a different order.
To order ties by another field, or to get the same order in `FT.AGGREGATE`, see [Make the Order of Equal Scores Repeatable](search-scoring-examples.md#make-the-order-of-equal-scores-repeatable).

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
The weight must be 0 or greater. A clause with a weight of 0 still filters keys, but adds 0 to the score.
A weight on a single term multiplies the score of the word as written in the query.
If the term is stemmed, the other forms of the word keep a weight of 1. See [Stemming Adds an Exact-Match Bonus](#stemming-adds-an-exact-match-bonus).
A weight on a group of terms multiplies the sum of the group.

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
The `VSIM` arm converts each vector distance into a similarity.
For `L2` and `COSINE`, a higher similarity is a better match.
For `IP`, a higher similarity is a worse match.
`RRF` uses ranks, so the `IP` direction does not affect it.
`LINEAR` and `FUNCTION` use the similarity value directly.

The `COMBINE` clause selects how the two arms fuse:

- `RRF` (the default) uses the rank of a key in each arm, not its score. The two arms can use scores on different scales.
- `LINEAR` adds the scores of the two arms with weights that you set.
- `FUNCTION` computes the fused score with an expression that you write.

See [Fusion methods](../commands/ft.hybrid.md#fusion-methods) for the formulas and the options.

# Scores in Cluster Mode

In cluster mode, each shard computes scores from its own keys.
It uses the number of keys, the number of keys that contain each term, and the average key length on that shard, not in the whole index.
Thus the same key can get a different score in cluster mode than on a single node.
The results from all shards are then sorted together by these scores, so the merged order can compare scores that were computed from different statistics.

# How the Score Is Computed

## Term Score

The `BM25STD` scorer gives each matched text term this score:

$$
\text{term score} = \text{weight} \cdot \text{IDF} \cdot \frac{f \cdot (k1 + 1)}{f + k1 \cdot \left(1 - b + b \cdot \frac{dl}{avgdl}\right)}
$$

$$
\text{IDF} = \ln\left(1 + \frac{N - n + 0.5}{n + 0.5}\right)
$$

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

## Stemming Parts

A stemmed term ([Stemming Adds an Exact-Match Bonus](#stemming-adds-an-exact-match-bonus)) is the sum of up to three parts.
Each part has its own `f` and its own `IDF`:

1. The word exactly as written in the query.
2. The stem itself, if it is a different word that occurs in the index. For `running`, this part is `run`.
3. All indexed words that have the stem, other than the stem itself. For `running`, this part is `running` and `runs` together. Its `f` is the sum of the occurrences of these words in the key. Its `n` is the number of keys that contain at least one of them.

The word as written in the query is in part 1 and also in part 3.
Assume that `running` is the only indexed word with the stem `run`.
Then, with the default weight of 1, a query for `running` scores twice as high as the same query with `VERBATIM`.

A `$weight` on the term multiplies only part 1. Parts 2 and 3 use a weight of 1.

A word that is its own stem, such as `jacket`, is never in part 3.
For a query for `jacket`, a key that contains `jacket` gets part 1, and a key that contains `jackets` gets part 3.

## Prefix, Suffix, and Fuzzy Term Values

A prefix, suffix, or fuzzy term contributes the score of one matched word.
That word uses its own `f` and `IDF`.
If a key contains more than one matched word, the word that is used is not specified.

## Phrase and Match-All Values

Each word of a quoted phrase contributes only part 1 of [Stemming Parts](#stemming-parts).
The match-all query `*` gives each key one term score with `IDF` = 1 and `f` = 1.

## Tag Values

A tag clause such as `@color:{red}` is scored with the same `BM25STD` formula, with these changes:

- `f` is always 1.
- `n` is the number of keys that have the tag value.
- `dl` is the number of words in all of the key's `TEXT` fields, and `avgdl` is the average `dl` over the index.

If the index schema has no `TEXT` field, `avgdl` is 0, so every tag clause scores 0.

## Vector Distance

This table shows how Valkey Search computes the distance between two vectors `X` and `Y`:

| Metric | Distance | Range |
| :--- | :--- | :--- |
| `L2` | `sum((x[i] - y[i])^2)`, the squared Euclidean distance | 0 or greater |
| `IP` | `1 - dot(X, Y)` | Any value. It is negative when `dot(X, Y)` is greater than 1. |
| `COSINE` | `1 - dot(X, Y) / (magnitude(X) * magnitude(Y))` | 0 to 2 |

## Per-Shard Values

In cluster mode, `N`, `n`, and `avgdl` are the values for the keys on that shard, not for the whole index.
