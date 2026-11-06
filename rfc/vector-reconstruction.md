---
RFC: (PR number)
Status: Draft
---

# Serving Vectors from the Index (Approximate Vector Reconstruction)

## Abstract

This RFC proposes letting a vector index supply a field's vector value when the original bytes aren't kept: for example when returning the field in FT.SEARCH / FT.AGGREGATE results, or reading the key. An index that stores vectors compressed (scalar quantization, LVQ, LeanVec, FP16) can return an approximate vector instead of valkey-search keeping a full-precision copy. Done for all vector index types at once, this removes the duplicate raw copy for compressed indexes, which is often the largest share of their memory.

It was raised in the review of the SVS_VAMANA RFC (#1256), which left index-level vector retrieval out of its user-visible surface and deferred it to this RFC. As noted there, it needs changes in the Valkey core and likely the JSON module, and should apply uniformly to all vector indexes.

## Motivation

- **Duplicate storage.** Today the canonical vector bytes live in the keyspace (the hash field or JSON document), and the `VectorRegistry` (#1316) shares them with the indexes. A compressed index (FP16, SQ8, LVQ, LeanVec) stores its own reduced representation in addition, so the full-precision copy dominates memory, and much of the benefit of compression is lost.
- **The index already holds a usable value.** Many indexes can return an approximate vector from their own storage: SVS through `svs_index_reconstruct`, HNSW and FLAT exactly, quantized indexes by decompressing. If the application accepts an approximate value, the full copy isn't needed.
- **Consistency across algorithms.** If one algorithm (for example SVS_VAMANA's `RAW_VECTOR_STORAGE DROP`, eshenayo/valkey-search#14) offered this on its own, behavior would differ by index type, which is what #1256's review asked to avoid.

## Design considerations

1. **Who owns the value.** The keyspace is the source of truth today. Serving a vector from an index means the stored value of a field can come from somewhere other than the key, which is a core semantic change: HGET, HGETALL, DUMP / RESTORE, replication, AOF, keyspace notifications, and JSON.GET all observe it.
2. **Exactness.** A reconstructed vector may differ from what was written. The user must opt in per field, and the difference must be documented and observable.
3. **Multiple indexes.** One field can be indexed by several indexes, with different precisions. Which one supplies the value has to be well defined.
4. **Data types.** HASH fields are binary strings; JSON vectors are arrays of numbers, so reconstruction goes through the JSON module.
5. **Durability.** If the raw bytes aren't kept, the RDB and AOF must still persist the vector (or enough to rebuild it). An index that can't reconstruct must not be allowed to drop the raw copy.
6. **No silent behavior change** for existing indexes: the default stays the current behavior.

## Specification

Not decided; options for discussion.

### Levels of support

1. **Read-only, in query results.** FT.SEARCH / FT.AGGREGATE return the field's vector from the index when the raw bytes aren't available. Smallest change; no keyspace semantics change.
2. **Index-owned storage.** The key stores no vector bytes for the field; reads of the key (HGET, JSON.GET) are served by the index. This needs Valkey core support (a module hook to supply a field's value) and JSON module support.

### Interface

- **`VectorBase`:** a method to return the stored vector for an internal id (exact or approximate) and say which. HNSW and FLAT return exact values; SVS uses `svs_index_reconstruct`; quantized indexes decompress.
- **FT.CREATE:** a per-field option to opt in, for example `VECTOR_STORAGE INDEX` (name to be decided). It is only accepted for index types that can reconstruct.
- **FT.INFO:** reports the storage mode and whether returned values are exact.

### RDB / AOF / replication

With level 1, nothing changes. With level 2, the index's persisted state must include what's needed to reconstruct, and AOF and replication must carry the written value (full precision), not the reconstruction. Details depend on the core hook.

### Module API

Level 2 needs a Valkey core API for a module to supply (and be notified of writes to) a field's value, plus JSON module support. To be proposed with the Valkey core team.

### Testing

Round trip within the documented error per compression type; behavior with several indexes on one field; RDB, AOF and replica full sync; refusing the option for index types that can't reconstruct.

## Open questions

1. Is level 1 useful on its own, or is the memory benefit only realized with level 2?
2. With several indexes on one field, which supplies the value: the most precise one, the first one, or is it configured?
3. How is the approximation error documented and bounded per compression type?
4. What does the core hook look like, and does it fit Valkey's keyspace model (DUMP / RESTORE, MIGRATE, notifications)?

## Appendix

- SVS_VAMANA RFC review: valkey-io/valkey-search#1256, thread on `reconstruct_at` (2026-08-26).
- `RAW_VECTOR_STORAGE DROP` for SVS_VAMANA: eshenayo/valkey-search#14.
- SVS C API: `svs_index_reconstruct` (`bindings/c/include/svs/c/svs_c.h`).
- `VectorRegistry`: valkey-io/valkey-search#1316.
