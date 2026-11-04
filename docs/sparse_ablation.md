# Sparse ablation: both

This is an isolated experiment based on d08ad005ed82b1d188c47452b3d8714e343853b9,
not a production release or an automatic change to the default PR branch.

The HASH wire format, signed dot product, FLOAT32 final distances, overlap-only
candidate contract and key-ascending ties are unchanged.

`prefilter` enumerates matching keys with the existing filter planner and scores
their forward vectors. It is intentionally unconditional for filtered queries
in this ablation. A filter matching most documents is a severe performance
regression; a selectivity-aware plan chooser is required before general use.

`numeric` uses recycled uint32 document IDs, contiguous (ID, FLOAT32 weight)
postings, swap deletion with forward back-references, and query-local arrays
for score accumulation. Query memory scales with the highest allocated slot
count, not only the current number of matching documents.

`both` combines those two changes. See the comparison in PR #1 for measured
throughput, latency, memory and the broad-filter negative result. In each of
these branches the name identifies the enabled treatment; the unmodified
parent is the control. Numeric-only retains the candidate-list helper so the
representation can be tested independently of its use by the query planner.

Tests exercise signed/zero sums, deletion and ID reuse, Boolean and numeric
filters, INKEYS, mutations, RDB restore and independent dense/sparse RRF.
Benchmark harnesses and private corpora are intentionally outside the branch.
