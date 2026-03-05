# Unsupported Tests

This is the list of all the unsupported tests in text-search compatibility tests.

## 1. Exact Phrase Query

Exact phrase query tests are unsupported because of a bug in Redis exact phrase search.
Redis sometimes returns a document when the query string is separated in two fields.

Example:

```
127.0.0.1:6379> ft.create idx on hash prefix 1 hash: schema title text body text
OK
127.0.0.1:6379> hset hash:1 title "one two three" body "four five six"
(integer) 2
127.0.0.1:6379> ft.search idx '"three four"'
1) (integer) 1
2) "hash:1"
3) 1) "title"
   2) "one two three"
   3) "body"
   4) "four five six"
```

## 2. Group Queries with INORDER and SLOP

Some special cases of the group queries with INORDER and SLOP are not supported:

1. **Parsing difference** — Valkey parses some queries differently than Redis. This leads to different search results. The compatibility tests check the parsing and filter out these queries.

2. **Nested OR queries** — Redis has bugs in some of the nested OR queries. The bugs include specific orderings in the words, and the same word appearing in both sides of an OR clause. The queries with bugs are extracted and added into an excluded query set in `generate_text.py`.

3. **Slop calculation in a group** — Currently, Valkey uses the leftmost position in the document of any word in the group to calculate the slop value. This holds true for most queries in Redis, however there are edge cases where this does not hold true and needs future investigation.

   Example:

   ```
   127.0.0.1:6379> hset hash:2 body "ocean carrot jump quiet build shark onion"
   (integer) 1
   127.0.0.1:6379> ft.search idx "(((carrot quiet) | (quiet desert)) ((shark | forest)))" SLOP 2
   1) (integer) 1
   2) "hash:2"
   3) 1) "body"
      2) "ocean carrot jump quiet build shark onion"
   ```

   In the rule described above, this document should be returned only when SLOP=3. Further investigation might be needed to solve this inconsistency.

## 3. Fuzzy Search with Levenshtein > 1

Both Valkey and Redis declare to use Damerau-Levenshtein in fuzzy search. However, testing indicates that in some fuzzy searches with distance > 1, Redis returns answers that are not consistent with Damerau-Levenshtein calculation.
