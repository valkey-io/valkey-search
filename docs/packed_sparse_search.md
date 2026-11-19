# Packed sparse-vector search (draft API)

`FT.SPARSESEARCH` computes exact top-K sparse inner products over posting lists
stored in Valkey strings. This is a low-level interface for applications that
generate their inverted index externally, such as recommendation retrieval with
small sets of weighted features. It requires no `FT.CREATE` schema.

```text
FT.SPARSESEARCH K M key1 weight1 ... keyM weightM
FT.SPARSESEARCHBIN K M key1 weight1 ... keyM weightM
```

`K` is between 1 and 10000; `M` is between 1 and 64. Each key represents one
dimension. Query weights are finite, nonnegative doubles. Missing or empty keys
are empty posting lists; other key types return WRONGTYPE. Repeating a key adds
its weighted contribution again.

## Posting format and score

Each string contains consecutive six-byte records, without a header:

| Field | Encoding |
| --- | --- |
| Point ID | Little-endian uint32 |
| Stored weight | Little-endian IEEE binary16 (FP16) |

IDs must be strictly increasing within each list. Stored weights must have a
clear sign bit and be finite (positive zero and subnormal values are supported).
Malformed lengths, unordered or duplicate IDs, invalid weights, and score
overflow produce an error without a partial reply.

The score is `sum(query_weight[t] * stored_weight[point, t])`, accumulated in
double precision, in the supplied key/weight order. Multiplication and addition
round separately; fused multiply-add and regrouping sums are not used. Changing
the input order can therefore change rounding and the ranking of nearly tied
scores. Missing dimensions contribute zero. Exactness is relative to
the stored FP16 values, after any quantization performed by the producer.
Candidates are the union of the supplied lists, including zero-score candidates;
points absent from that union are not returned.

Results are ordered by decreasing score, then increasing point ID. The text
command returns an array of `[ID, score]` pairs. The binary command returns the
same results in a bulk string of twelve-byte little-endian `uint32 + binary64`
records, without a count or terminator. An empty result is an empty array or
empty bulk string, respectively.

```python
import struct

client.set("features:{catalog}:a", struct.pack("<IeIe", 1, 1.0, 2, 2.0))
client.set("features:{catalog}:b", struct.pack("<IeIe", 1, 2.0, 3, 3.0))
reply = client.execute_command(
    "FT.SPARSESEARCHBIN", 2, 2,
    "features:{catalog}:a", 2.0, "features:{catalog}:b", 3.0,
)
assert list(struct.iter_unpack("<Id", reply)) == [(3, 9.0), (1, 8.0)]
```

## Execution and limitations

The search sequentially scans all selected lists, accumulating scores in blocks
of 8192 IDs and maintaining a min-heap of K hits. The next block is selected from
the list cursors, skipping empty ID ranges. No WAND/MaxScore pruning is used.
For P postings, U candidate IDs, M lists, and B occupied blocks, time is
`O(P + M * B + U * log(K) + K * log(K))`. Scratch storage is 96 KiB for the block
accumulator and touched IDs, plus `O(M + K)`. It is allocated per invocation.

The command borrows string data and runs synchronously on the main thread.
Long posting lists can delay other commands; the command has no TIMEOUT option.
It does not use the reader pool, schema index, filters, or cluster coordinator
used by `FT.SEARCH`. Those features require a separate design for an index-owned
snapshot and query lifecycle.

Both commands declare their input keys for Valkey key-level ACL checks and
cluster routing. In Valkey Cluster all keys in one call must share a hash slot.
Cross-shard fanout, merging top-K, and mapping local point IDs to application IDs
are the caller's responsibility.

The index remains ordinary strings, with standard string persistence,
replication, and replacement semantics. Updating several lists atomically is the
producer's responsibility (for example, a transaction on keys in one slot or
switching between versioned index generations).
