# Managed sparse vectors

Use a SPARSE field to index variable-length weighted features in HASH documents. FT.SEARCH returns the top overlapping documents by dot product. A sparse field can share a schema with existing filter and dense vector fields.

```text
FT.CREATE items ON HASH PREFIX 1 item: SCHEMA embedding SPARSE region TAG
HSET item:1 embedding <blob> region 77
FT.SEARCH items '(@region:{77})=>[KNN 100 @embedding $q AS distance]' \
  PARAMS 2 q <query-blob> RETURN 1 distance LIMIT 0 100
```

Each blob is a sequence of little-endian `(uint32 dimension, float32 weight)`
pairs, without a header. Dimensions must be strictly increasing; weights must
be finite. Signed weights are supported, zero entries are ignored. An empty
document vector is treated as a missing field. A query may use a zero-weight
entry to represent an empty vector; FT.SEARCH rejects an empty PARAMS value.
A command accepts up to 64 KiB (8,192 encoded pairs) of sparse query data in
total. Each sparse arm counts separately, even when arms reuse a parameter.
Document vectors and dimension IDs are independent of this query limit.

The index subscribes to the normal HASH mutation lifecycle. HSET replaces the
old vector, HDEL removes the field, DEL/expiry removes its postings. Invalid
values follow the existing invalid-field policy. Schema metadata is persisted;
postings are rebuilt from the HASH values during RDB loading, like TAG/NUMERIC.

Filtered searches choose between scoring matching forward vectors and
accumulating the union of query posting lists. The planner compares the
filter cardinality estimate times the mean document/query vector lengths
with the number of query postings. Negated filters retain the posting path
because estimating their candidates can be expensive. KNN filters reject
`VECTOR_RANGE` predicates during parsing.
Unfiltered searches also use postings. Both select top-K with a bounded heap
and return only documents with nonzero-weight overlap. Accumulation uses double precision in increasing dimension order;
the existing query pipeline represents the final distance as FLOAT32. Distance
is **negative dot product**, sorted ascending with key-ascending ties. Overflow
outside FLOAT32 is an error. This is exact retrieval for that declared ranking
contract; it is not WAND, ANN or a quantized/pruned search algorithm.

Queries use the existing reader thread pool and cancellation token. Index
mutations and queries share the schema's reader/writer phase synchronization;
the sparse index owns its data instead of borrowing Valkey STRING buffers.
The index uses recycled numeric document IDs, contiguous posting arrays
and forward vectors with deletion back-references. Rare posting lists accumulate scores by matched ID. Direct score arrays are
used only when a posting list already covers at least one in sixteen slots.

## Dense + sparse RRF

Add a normal dense VECTOR field to the same schema, then run:

```text
FT.HYBRID items \
  VSIM @embedding $s KNN 2 K 100 YIELD_SCORE_AS sparse_score \
  VSIM @dense $d KNN 2 K 100 YIELD_SCORE_AS dense_score \
  COMBINE RRF 4 CONSTANT 60 WINDOW 100 \
  LOAD 1 @__key LIMIT 0 100 PARAMS 4 s <sparse-blob> d <dense-blob>
```

The parser accepts 2–5 SEARCH/VSIM arms, with at least one VSIM. New arm
combinations support RRF; LINEAR/FUNCTION retain the original SEARCH+VSIM shape.
Each VSIM can have its own existing FILTER clause. The fusion implementation,
result projection and per-arm dispatch are reused. Sparse arm scores exposed
by YIELD_SCORE_AS are dot products. RRF depends on ranks, not score calibration.
WINDOW limits contributions per arm, not necessarily the underlying scan work.

## Compatibility

This implementation targets Valkey 9.1 or later. Sparse fields support HASH
input and KNN queries. The stored schema uses a new protobuf field type;
restore it only on nodes running a module that supports SPARSE. Qualify
upgrades, AOF replay and replica failover before deploying it on a cluster.
