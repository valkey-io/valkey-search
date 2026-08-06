# HNSW Duplicate-Label Follow-up

Follow-up work for PR [#1283](https://github.com/valkey-io/valkey-search/pull/1283)
("Fix duplicate label handling"). #1283 makes legacy RDBs with duplicate labels
loadable and prevents new duplicates from being created on modifies. This
document covers the two issues that remain once such an RDB is loaded and the
fix that resolves both.

## Background

`allow-replace-deleted` lets HNSW recycle tombstoned slots on insert. Prior to
#1283 a race between `ModifyRecordImpl` and `RemoveRecord` could leave two slots
carrying the same external label: one live, one tombstoned. That state persists
across save/load. #1283 fixes the modify race and lets loads reconstruct
`label_lookup_` deterministically (pointing at the live slot), but the in-memory
representation still has two failure modes downstream.

Slot geometry (unchanged by #1283):

- Each HNSW slot holds a raw `char*` `data_ptr` (in level-0 memory at
  `offsetData_`) pointing into a valkey-search-owned interned buffer. The slot
  also stores its external label at `label_offset_`.
- `tracked_vectors_` (in `VectorHNSW`) is the owning map:
  `label → InternedStringPtr`. It holds the **only** reference count keeping
  each vector buffer alive; the slot's `data_ptr` is a non-owning view into that
  buffer.
- The interned string store is **content-addressed**: interning identical bytes
  returns the same buffer and bumps a refcount
  (`string_interning.cc: InternImpl`).
- `deleted_elements` (in hnswlib) is the pool of tombstoned slots eligible for
  reuse.
- Deleted/tombstoned slots still participate in graph routing
  (`searchBaseLayer`, neighbor selection in `addPoint`), so their `data_ptr`
  must stay valid for the life of the index. This is why `UnTrackVector` is a
  no-op for HNSW — a tombstone's `tracked_vectors_` entry must survive.

## Remaining issues

### Issue 1: dangling vector data on the intern-race loser

On load, hnswlib streams each slot and calls
`vector_tracker->TrackVector(label, bytes, len)`
(`hnswalg.h` load loop). `VectorBase::TrackVector` interns the bytes and writes
`tracked_vectors_[label] = interned`, returning the interned `char*` which is
stored as the slot's `data_ptr`. With duplicate labels, the second slot's call
overwrites `tracked_vectors_[label]`; if the two slots hold **different** bytes,
the first buffer's refcount drops to zero and it is freed. The slot that loaded
first now has a `data_ptr` into freed heap.

(If the two dup slots hold **identical** bytes, content-addressing makes both
`TrackVector` calls return the same buffer with refcount ≥ 2, and the overwrite
is idempotent — no free. Issue 1 therefore bites specifically when the dup slots
differ, which is the realistic modify-race case, since a modify usually changes
the value.)

Downstream impact when the bytes do differ:

- `FT.SEARCH ... RETURN <vector_field>` on either the HASH or JSON attribute
  path resolves the returned vector via
  `search.cc: vector_index->GetValue(external_id)` →
  `VectorBase::GetValue` → `VectorHNSW::GetValueImpl` →
  `algo_->getPoint(label)` → `getDataByInternalId` → reads the slot's
  `data_ptr`. The JSON path only differs in formatting (`StringFormatVector`).
- `searchBaseLayer` and neighbor-selection in `addPoint` also dereference the
  slot's `data_ptr` on tombstones (tombstones participate in graph routing).

The freed slot is whichever of the two loaded **first** (lower index),
independent of live/tombstone. So the loser can be the **live** slot: freed-heap
bytes returned in a `RETURN` response (or a crash if the arena was released), or
silent distance-calc corruption if the loser is a tombstone used in routing.

### Issue 2: label_lookup_ corruption on the next insert

Below the load loop, the tombstone-reuse branch of the outer
`addPoint(replace_deleted=true)` is:

```cpp
labeltype label_replaced = getExternalLabel(internal_id_replaced);
setExternalLabel(internal_id_replaced, label);

std::unique_lock<std::mutex> lock_table(label_lookup_lock);
label_lookup_.erase(label_replaced);          // unconditional
label_lookup_[label] = internal_id_replaced;
```

`erase(label_replaced)` assumes the label read off the recycled slot is mapped
to that same slot in `label_lookup_`. With a duplicate-label RDB the load routes
the label to the *live* slot, and the tombstoned dup carries the same label.
Trace after loading a dup-label RDB (slot 0 live/label 1, slot 1
tombstone/label 1):

1. `label_lookup_ = {1 → 0}`, `deleted_elements = {1}`.
2. `HSET doc:2 ...` → `addPoint(label=2, replace_deleted=true)`. Slot 1 is
   popped from `deleted_elements`. `label_replaced = getExternalLabel(1) = 1`.
3. `label_lookup_.erase(1)` — wipes the live doc's only mapping.
4. `label_lookup_ = {2 → 1}`. `doc:1` is still live at slot 0 but unmapped.
5. `FT.SEARCH` still returns `doc:1` (found via traversal, resolved via
   `key_by_internal_id_`).
6. `DEL doc:1` → `markDelete(label=1)` → `label_lookup_.find(1) == end()` →
   throws "Label not found", `hnsw_remove_exceptions_cnt` increments. `doc:1`
   is un-deletable ("ghost doc").

This contradicts the premise the PR relies on ("one live slot and any number of
tombstoned slots all carrying the same label"). It is benign only while the
tombstone is never popped for reuse — but legacy RDBs are exactly the population
that runs `allow-replace-deleted`, so it will be popped on the next insert.

## The invariant we want

Make `tracked_vectors_`, level-0 memory, and `label_lookup_` a strict **1:1**
mapping after load: every slot has a unique external label, one owning
`tracked_vectors_` entry, and one `label_lookup_` entry.

- 1:1 `tracked_vectors_` ⇒ every slot's buffer has its own owning reference ⇒
  no `data_ptr` is ever freed out from under a slot. Fixes Issue 1 for both the
  live slot and the tombstones.
- 1:1 `label_lookup_` ⇒ each slot's label is unique ⇒ the reuse branch's
  `erase(getExternalLabel(popped))` only removes that slot's own self-entry and
  never touches the live doc's mapping. Fixes Issue 2.

`tracked_vectors_` is keyed by label (it must stay so — `IsVectorMatch` looks it
up by label on updates), so "1:1 `tracked_vectors_`" and "unique label per slot"
are the same requirement. For the dup tombstones we synthesize new unique
labels.

## Fix: stage-then-commit with in-loop relabeling

The premature free in Issue 1 happens **inside the streaming loop**, so it
cannot be repaired by a post-pass — by the time a post-pass runs, the loser's
buffer is already gone. The fix splits what `TrackVector` does today into two
phases, both driven from `HierarchicalNSW::LoadIndex`.

### Phase 1 — stream and stage (existing load loop)

For each slot `i`, intern the bytes and hold the owning `InternedStringPtr` in a
temporary staging store **keyed by slot index `i`**, then set the slot's
`data_ptr` from it. Do **not** write `tracked_vectors_` here. Because staging is
keyed by slot, no two slots collide, every buffer keeps its own reference, and
nothing is freed — the 1:1 invariant is established by construction. Also
accumulate `max_label` over every label read, to source synthetic labels from
above the real range.

### Phase 2 — commit under final labels (new loop after keeper selection)

#1283's dup-detection loop is left untouched: it rebuilds `label_lookup_` so that
each label points at its **keeper** — the live slot if one exists, else the first
tombstone seen. That already resolves the "keeper" of every label independent of
slot ordering, so a separate commit loop can lean on it rather than redoing the
live/tombstone reasoning inline (which would need a delicate relocate-before-
overwrite in the tombstone-before-live case).

The commit loop then walks every slot once. Slot `i` is its label's keeper iff
`label_lookup_[L] == i`; any other slot sharing `L` is a duplicate:

- **Keeper** (`label_lookup_.find(L)->second == i`): keep `L`; commit staged[i]
  under `L`.
- **Duplicate** (mapping points elsewhere): assign a synthetic label
  `S = ++max_label`; `setExternalLabel(i, S)`; `label_lookup_[S] = i`; commit
  staged[i] under `S`.

Because commit is keyed by slot and every final label is unique, no
`tracked_vectors_` entry is ever overwritten and nothing is freed. After the loop
the staging store is cleared (its refs are now held by `tracked_vectors_`).

Traced against all occurrence patterns — unique label; tombstone-before-live;
live-before-tombstone; N tombstones + 1 live in any order; all-tombstone — every
slot ends with exactly one unique label, one `tracked_vectors_` entry, and one
`label_lookup_` entry, and any live slot keeps the real label.

### Interface change

`hnswlib::VectorTracker` currently exposes one method:

```cpp
virtual char *TrackVector(uint64_t internal_id, char *vector, size_t len) = 0;
```

`TrackVector` is retained for the bruteforce load path (no tombstones, no dup
problem). Add two methods for the HNSW two-phase load:

```cpp
// Phase 1: intern + hold an owning ref keyed by slot; return the data pointer.
virtual char *StageVector(uint64_t slot, char *vector, size_t len) = 0;
// Phase 2: move the staged ref under the slot's final (possibly synthetic) label.
virtual void CommitStagedVector(uint64_t slot, uint64_t label) = 0;
// Release the staging store after all commits.
virtual void ClearStagedVectors() = 0;
```

`VectorBase` implements all three; the staging store is an
`absl::flat_hash_map<uint64_t, InternedStringPtr>` (slot → owning ref) on
`VectorBase`. `CommitStagedVector` reuses the existing virtual
`TrackVector(uint64_t, const InternedStringPtr&)` so `VectorHNSW`/`VectorFlat`
need no new members. The only other implementer is the test tracker in
`vector_test.cc`.

### Why this fixes both issues

Issue 1: every slot has an independent `tracked_vectors_` entry keyed on a
unique label; no overwrite, no dangling `char*`, no freed-heap read from
`GetValue` or from graph traversal. Holds for the live slot and every tombstone.

Issue 2: `getExternalLabel(dup_tombstone) = S`, so the reuse branch does
`label_lookup_.erase(S)` on a self-reference. The real live mapping
(`label_lookup_[real_label] → live_slot`) is untouched. Subsequent
`DEL <real_key>` reaches `markDelete(real_label)`, finds the live slot, and
removes it normally.

### RDB persistence

`SaveIndex` writes `getExternalLabel(slot)` verbatim, so synthetic labels persist
as ordinary, unique tombstone labels. The next load sees no duplicates: the dup
branch never fires and no further synthesis happens. Self-healing across
restarts. (An optional `SaveIndex`-time pass could skip synthetic-labeled dup
tombstones to reclaim space; not required for correctness.)

## Costs and caveats

- **Memory:** one `tracked_vectors_` and one `label_lookup_` entry per duplicate
  tombstone, alive until the slot is reused via `deleted_elements` or the index
  is dropped — the same lifetime as any tombstone. Bounded by the number of
  legacy dups. Staging adds a transient `InternedStringPtr` per slot during
  load only.
- **Reused-tombstone entry retention:** when any tombstone (synthetic-labeled or
  not) is later reused, its old `tracked_vectors_` entry is not reclaimed
  (`UnTrackVector` is a no-op by design). This is pre-existing behavior, not
  introduced here.
- **inc_id_ headroom:** synthetic labels are drawn from `max_label + 1` upward;
  `inc_id_ = GetMaxInternalLabel() + 1` after load then lands above them, so new
  `HSET`s cannot collide. Assumes `labeltype` (a `uint64_t`/`size_t`) never
  approaches its limit — fine for any realistic index lifetime.
- **Interaction with the ingestion-side follow-up:** the "force same-slot reuse
  on updates" change referenced in #1283 prevents *new* duplicates. This fix
  cleans up *existing* duplicates that RDBs from 1.0 / 1.1 / 1.2 already carry.
  Complementary.

## Alternatives considered

- **Erase-guard only** (AI review suggestion): change the reuse-branch erase to
  `if (it != end() && it->second == internal_id_replaced) erase(it)`. Fixes
  Issue 2 but not Issue 1 — leaves the `GetValue` freed-heap read live for any
  RETURN on legacy RDBs. (Note the guard is a reasonable additional hardening,
  but is subsumed here: with unique per-slot labels the erase is already a
  self-reference.)
- **Drop dup tombstones from `deleted_elements` at load**: prevents Issue 2 (the
  tombstone can never be popped) but leaves Issue 1's dangling `char*` on the
  losing slot.
- **Vector-ownership moves into the HNSW slot**: reduce `tracked_vectors_` to a
  dedup index and have the slot hold an owning reference. Cleanest long-term
  design, largest surface area. Worth doing separately; orthogonal to this fix.

## Test coverage

Extend `integration/test_hnsw_allow_replace_deleted.py`:

1. Load the existing `hnsw_duplicate_label.rdb` fixture.
2. `HSET doc:2 vector <bytes>` (fresh key, forces `deleted_elements` reuse).
3. Assert `hnsw_remove_exceptions_count == 0` after `DEL doc:1`.
4. Assert `FT.SEARCH ... RETURN vector` on the live doc returns the same bytes
   originally written (byte-for-byte equality against the source vector).
5. Assert `num_docs` drops to 0 after `DEL doc:1` and `DEL doc:2`.

Add a C++ unit test in `testing/vector_test.cc` covering the `LoadIndex` path
directly:

- Build a golden `ChunkStream` with two slots carrying the same label
  (one tombstoned) and **distinct** bytes.
- Load with `allow_replace_deleted = true`.
- Verify: `label_lookup_.size() == 2`, both entries have distinct keys, the live
  entry maps to the live slot, the dup entry maps to the tombstoned slot with a
  synthetic label, and each slot's `data_ptr` reads back its own original bytes
  (no cross-contamination, no freed read).
- Call `addPoint` for a fresh label with `replace_deleted=true`; verify the live
  entry's `label_lookup_` mapping is intact and its data pointer still reads back
  the original bytes.
