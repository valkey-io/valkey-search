---
RFC: (PR number)
Status: Draft
---

# Background Index Rebuild for Vector Indexes

## Abstract

This RFC proposes a valkey-search framework for rebuilding a vector index in the background and swapping it in without blocking queries for more than a short, bounded time. A rebuild produces a new index of the same attribute from the live keyspace, with the same or different algorithm parameters, while the old index keeps serving queries and absorbing mutations. The framework is algorithm-independent. It covers learning-free algorithms (HNSW, SVS_VAMANA with FP32/FP16) and learning-based ones (scalar quantization, LVQ, LeanVec, and other compression that needs a view of the corpus), and it manages the CPU and memory a rebuild uses.

It was proposed in the review of the SVS_VAMANA RFC (#1256), which deferred its own compression transition to this framework.

## Motivation

- **Incremental edits degrade indexes.** Most vector index algorithms don't handle deletes and replacements perfectly: recall drops, query rates fall, or memory is stranded. Generally the only full repair is to build a new index and replace the old one. valkey-search has no way to do that today short of `FT.DROPINDEX` plus `FT.CREATE`, which makes the index unavailable during the rebuild.
- **Learning-based algorithms need the corpus.** Quantization codebooks, centroids, partitions and ranges are computed from the data. Building them from the first N inserted vectors trades quality (if the data isn't temporally uniform) against how long the index runs uncompressed. A rebuild can train on as much of the corpus as needed.
- **Configuration changes.** Changing index parameters (for example compression, graph degree or block size) currently means dropping and recreating the index. A rebuild could apply them in place.
- **Repeated per-algorithm solutions.** Without a shared framework, each algorithm would build its own transition machinery, each with its own CPU, memory and consistency behavior. The SVS_VAMANA RFC's deferred-compression section is one such design.

## Design considerations

**Requirements**

1. Queries keep being served from the old index during the rebuild. The time queries are blocked at cutover is bounded and small (target: ~10 ms, matching the SVS_VAMANA RFC).
2. Mutations are never lost. Every mutation applied to the old index during the rebuild is reflected in the new index before it serves queries.
3. Both learning-free and learning-based algorithms are supported. A learning-based rebuild can sample or scan as much of the corpus as it needs before building.
4. CPU and memory are bounded and controllable: rebuild work runs on a limited share of threads, and a rebuild doesn't start if the extra memory (up to a second copy of the index during overlap) would exceed a configured limit.
5. Behavior is consistent across RDB save/load, replication and cluster mode: a rebuild in progress doesn't change what gets persisted or replicated.
6. It fits valkey-search's concurrency model: queries assume no mutations run concurrently on the index; mutations on different keys may run in parallel; no operation spawns sub-threads.

**Approaches discussed in #1256**

- **Snapshot, journal and catch-up** (yairgott, and the SVS_VAMANA RFC's Transition Mechanics):
  1. Build the new index from a snapshot while queries and mutations go to the old index.
  2. Journal mutations made after the snapshot.
  3. Drain the journal without holding the index lock until the remaining tail is small.
  4. Replay the tail under the exclusive lock, then swap the index pointer.
  5. Free the old index outside the lock.
- **Dual-instance merge** (yairgott). Mutations go only to the new index. Queries run against both indexes and the results are merged; queries against the old index skip entries already mutated in the new one. This needs a per-candidate callback during search on the old index, the same mechanism as traversal-time filtering.

Both approaches can share the parts that matter here: the journal of mutations, the cutover, and the resource controls. This RFC proposes snapshot-journal-catch-up as the base mechanism, because it needs nothing from the index beyond today's add, remove and search, and leaves dual-instance merge as an option for indexes that support per-candidate callbacks.

**Comparison with other systems**

- Elasticsearch / OpenSearch: rebuild by reindexing into a new index behind an alias, then switching the alias. Consistency during the switch is the application's responsibility.
- Milvus: background compaction and index building over immutable segments.
- Redis: no in-place rebuild; indexes are dropped and recreated. (To verify.)

## Specification

### Commands

Not decided. Options for discussion:

1. **`FT.REBUILD <index> [FIELD <attribute>] [<algorithm parameters>]`.** An explicit rebuild of one vector attribute, optionally with new parameters.
2. **Policy keywords on `FT.CREATE`** for automatic rebuilds, for example a learning-based index that starts uncompressed and rebuilds once it has N vectors, or a rebuild when the deleted fraction passes a threshold.
3. **Status:** `FT.INFO` reports, per vector attribute, whether a rebuild is in progress, its phase, progress, and the extra memory in use.

### Internal machinery

- **Rebuild job.** It runs on the writer pool with a configured concurrency limit. Its phases are: snapshot (enumerate the tracked keys and their `VectorRecord`s from the `VectorRegistry`), optional training (learning-based algorithms), build, catch-up, cutover, and free.
- **Mutation journal.** Records (internal id, operation, vector record) for mutations that complete after the snapshot. Records reference `VectorRegistry` entries rather than copying vector bytes.
- **Cutover.** Takes the index's exclusive phase, replays the journal tail, swaps the index pointer, and releases. The old index is freed afterwards on the writer pool.
- **Algorithm interface.** A small extension of `VectorBase`: build a new index from a sequence of records, optionally preceded by a training pass over a sample or the whole corpus. Algorithms that can convert in place (for example `svs_index_convert` in the SVS C API) may do so without rebuilding the graph.
- **Cancellation.** `FT.DROPINDEX`, `FLUSHDB` and shutdown cancel a rebuild in progress and free its memory.

### RDB

A rebuild in progress isn't persisted: the RDB holds the old index (or the new one, if cutover has completed). After a restart, a policy-triggered rebuild starts again when its condition still holds. Whether an explicit `FT.REBUILD` resumes after a restart is an open question.

### Configuration

To be defined: the thread share for rebuild work, the memory limit for starting a rebuild, the maximum journal size, and the cutover latency target.

### Cluster mode

Each shard rebuilds its own index independently. Index metadata (and so a parameter change) is distributed with the existing metadata sync. Whether rebuilds are coordinated across shards is an open question.

### Replication

Replicas apply the same mutations to their own indexes; a rebuild runs on each node independently. Open question: whether a rebuild started on the primary is replicated as a command, or each node decides from policy.

### Observability

Per-attribute rebuild state in FT.INFO, plus INFO counters for rebuilds started, completed, cancelled and failed, rebuild duration, journal high-water mark, and cutover latency.

### Testing

- Correctness under concurrent mutations: every mutation during a rebuild is present after cutover.
- Bounded cutover latency under load.
- Memory limit enforcement.
- Cancellation on drop, flush and shutdown.
- RDB save during a rebuild.

## Open questions

1. Explicit command, policy keywords, or both?
2. How much of the corpus may a learning-based rebuild read, and at what cost to the main thread (keyspace access) and memory?
3. Is dual-instance merge needed in v1 of this framework, or is catch-up enough?
4. Should a rebuild resume after restart?
5. Cross-shard coordination, if any.

## Appendix

- SVS_VAMANA RFC review thread on index transitions: valkey-io/valkey-search#1256 (comments on the Deferred Compression section, 2026-08-20 to 2026-08-25).
- SVS C API index conversion: intel/ScalableVectorSearch#392 (`svs_index_convert`).
