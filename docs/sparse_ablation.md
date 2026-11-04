# Sparse traversal and numeric IDs

This prototype builds on the isolated numeric-ID and prefilter experiments.
The HASH wire format, signed dot product, FLOAT32 final distances, overlap-only
candidate contract and key-ascending ties are unchanged.

Postings contain recycled uint32 document IDs and FLOAT32 weights. Swap deletion
and forward back-references support replacement/removal without tombstones.
Query-local score arrays avoid per-candidate string hashing; their memory scales
with the highest allocated slot count, not just current matches.

For a selective filter, the existing filter engine enumerates candidates and
sparse search scores their forward vectors. An adaptive plan compares estimated
forward work with the sum of query posting lengths. It retains posting traversal
when enumeration is more expensive. Negation and vector-range predicates bypass
estimation because constructing their fetchers can itself perform substantial
work. Their predicates and INKEYS are still evaluated on the posting path.

The cost estimate is a heuristic for execution time, not a relevance or recall
heuristic. Both traversals implement the same ranking and candidate contract.
Average vector length is maintained on updates/removals and rebuilt with the
index after RDB loading. A skewed corpus can make the cost estimate inaccurate;
that affects performance, not the requested result.

Tests cover signed/zero sums, replacement, deletion and ID reuse, Boolean and
numeric filters, negation with INKEYS, RDB restore and dense/sparse RRF.
Benchmark harnesses and private corpora remain outside this branch. This is an
experimental managed index, not a replacement for the standalone QP C command
on existing Valkey 9.0 deployments.
