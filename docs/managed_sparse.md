# Experimental managed sparse vectors

This fork explores the managed-index alternative to #1568 / #1587 and the
dense+sparse RRF use case discussed in #1601. It is a prototype, not a release
or a supported migration path for existing deployments.

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

The index subscribes to the normal HASH mutation lifecycle. HSET replaces the
old vector, HDEL removes the field, DEL/expiry removes its postings. Invalid
values follow the existing invalid-field policy. Schema metadata is persisted;
postings are rebuilt from the HASH values during RDB loading, like TAG/NUMERIC.

Search accumulates dot products over the union of query posting lists, applies
filters and selects top-K with a bounded heap. It does not scan vectors with no
overlap. Accumulation uses double precision in increasing dimension order;
the existing query pipeline represents the final distance as FLOAT32. Distance
is **negative dot product**, sorted ascending with key-ascending ties. Overflow
outside FLOAT32 is an error. This is exact retrieval for that declared ranking
contract; it is not WAND, ANN or a quantized/pruned search algorithm.

Queries use the existing reader thread pool and cancellation token. Index
mutations and queries share the schema's reader/writer phase synchronization;
the sparse index owns its data instead of borrowing Valkey STRING buffers.
The initial implementation uses hash-map postings plus forward vectors to
support replacements. It trades memory for a simple mutable index. It has no
compressed posting blocks or filter-selectivity planner yet.

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

## Scope and differences from the QP pilot

The prototype supports HASH input and dot-product KNN. JSON input, sparse range
queries, SORTABLE sparse fields, EF_RUNTIME and sparse search policy hints are
not supported. Single-node behavior is tested; distributed correctness, mixed
versions, AOF and replica failover still need dedicated qualification before
production use. Sparse schema metadata is experimental: do not roll it back to
a release which does not know this field type.

QP's four-token intersection + location + full cell count is a different API.
It can be represented using TAG membership filters plus sparse scoring, but a
separate count query is required. The current QP command's FP64 scores and
item-ID-descending ties also differ from this pipeline's FLOAT32 distances and
key-ascending ties. Benchmark those differences explicitly; a faster generic
sparse query is not evidence of a faster equivalent QP implementation.
