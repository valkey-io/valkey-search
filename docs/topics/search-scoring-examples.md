---
title: "Valkey Search - Scoring Examples"
description: Worked scoring example and recipes to tune the ranking of Valkey Search results
---

These examples use the scores described in [Search - Scoring](search-scoring.md).
They require Valkey Search 1.3.0 or later.

# Read and Check Scores

In this example, you build a four-product index, check one score against the formula, and see how stemming, extra clauses, and a document score change the scores.

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

   The TF part is the part of the [term score formula](search-scoring.md#term-score) after `IDF`. The `weight` is 1.0.

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

# Tune the Ranking

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

## Find Out Why a Key Has Its Score

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

## Make Matches in One Field Count More

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

## Filter Without a Change to the Ranking

A numeric clause contributes 0, so it filters keys without a change to their scores.
A tag clause adds its own score, so it can change the order of the results.
To filter by a tag without a change to the scores, give the tag clause `$weight: 0`, for example `jacket (@color:{red}) => {$weight: 0}`.
You can also filter in an `FT.AGGREGATE` stage instead of in the query:

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
This costs more than a filter in the query, which does not read the keys.

## Promote Keys with a Stored Value

To rank some keys above others for every query, store a number in each key and name its field with `SCORE_FIELD`.
Valkey Search multiplies the query score of each key by this number.
A key without the field gets the `SCORE` value of the index, which is 1.0 by default.
Thus a value of 2 doubles the score of a key, and a value of 0.5 halves it.
See [Document Score](search-scoring.md#document-score).

Use this method when the value changes less often than the queries run, for example a product rating.

## Combine Relevance with Popularity at Query Time

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

## Make the Order of Equal Scores Repeatable

`FT.AGGREGATE` does not sort records by score automatically, so it does not order keys with equal scores.
To get a repeatable order, sort by the score and then by a second field:

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
If the index does not change between requests, no key appears on two pages.
A change to the index between requests can change scores and move keys to a different page.

## Combine Text Results with Vector Results

To rank keys by a text query and by vector similarity together, use [`FT.HYBRID`](../commands/ft.hybrid.md).
See [Score Fusion in FT.HYBRID](search-scoring.md#score-fusion-in-fthybrid).
