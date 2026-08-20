# Test-coverage review: main-thread state machine for shared vectors

Branch: `memory_sharing` → `main`. Scope: the main-thread state machine that keeps
`VectorRegistry` in sync with the keys in the database —
`IndexSchema::ProcessKeyspaceNotification` → `TrackRecord` →
`VectorRegistry::Track` / erase / `ShareWithValkeyHash`, and the untrack side
(`UntrackIfUnused` / `BatchUntrackIfUnused` → `DetachFromValkeyHash`).

## Summary

The state machine is effectively untested on the branch.
`testing/vector_registry_test.cc` has 18 tests, but 17 of them call
`VectorRegistry::Track()` directly and therefore never exercise the code that
decides *whether* to call it. Exactly one test drives a keyspace notification end
to end, and it is HASH-only, vector-only schema, valid→valid only, with sharing
explicitly disabled.

This review adds two suites and one debug affordance:

- `testing/vector_registry_state_machine_test.cc` — drives real keyspace
  notifications against a fake keyspace across
  `{HASH, JSON} × {vector-only, vector+numeric+tag} × {sharing on, sharing off}`.
  **136 cases: 104 pass, 14 skip (combinations that do not apply), 10 fail.** All
  10 failures are on the HASH path and reproduce product defects. A further
  defect is a hard process abort, expressed as a death test (passing = the abort
  is confirmed). The rest of `indexes_test` is unaffected.
- `integration/test_vector_registry_lifecycle.py` — reproduces every
  failure mode against a real valkey-server with the module at its default
  configuration, and covers ingestion, backfill and RDB load. **13 tests: 7
  fail** deterministically across repeated runs — one per failure mode — and 6
  pass. It complements the branch's existing
  `integration/test_vector_registry.py`, which covers steady-state sharing and
  stays green (11 tests).
- `FT._DEBUG VECTOR_SHARED_COUNT <key> <field>` — makes the engine-side sharing state
  of a single field directly assertable (section 4.1).

Six failure modes are described in section 3. **FM-1 should block the merge.**

## 1. Why the existing suite does not catch these

- **The suite runs the feature off.** `ValkeySearchTest::SetUp`
  (`testing/common.h:389`) sets `enable-vector-sharing = false` before
  constructing the registry, in *every* test. Production defaults to `true`
  (`src/valkey_search_options.cc:151`). The default test configuration is the
  inverse of the default production configuration.
- **The one end-to-end test also turns it off.**
  `HnswVectorIndexReferenceCountOnIngestionAndMutation`
  (`testing/vector_registry_test.cc:518`) is the only test that goes through
  `OnKeyspaceNotification`, and line 521 calls
  `SetHashRegistrationSupported(registry, false)`.
- **`IndexSchemaRDBTest` forces it off too** (`testing/index_schema_test.cc:1191`),
  so the RDB ↔ registry interaction is untested at the production default.
- **The only mixed vector + non-vector test bypasses the state machine.**
  `InvalidDataDropsKey` (`testing/index_schema_test.cc:1993`) builds
  `MutatedAttributes` by hand and calls `SyncProcessMutation`, so `TrackRecord`
  never runs.
- **No test drives JSON through the state machine.** The only JSON reference is a
  direct `Track(..., ATTRIBUTE_DATA_TYPE_JSON, ...)` call
  (`testing/vector_registry_test.cc:211`) that hand-feeds binary bytes the real
  JSON path never produces.

At integration level the branch does ship `integration/test_vector_registry.py`
(11 tests, all passing). It covers steady-state sharing well — HNSW and FLAT,
overwrite with same versus different vectors, deletion, `hash_sharing_errors`,
`COPY`, and memory deltas. What it does not exercise is the state machine's
edges: no wrong-size vector, no absent vector field, no key whose type changes,
no JSON index, no backfill, and no RDB reload. Every failure mode in section 3
sits in one of those gaps, which is why that suite stays green against all of
them.

## 2. Unit-level scenarios

Fixture: `VectorRegistryStateMachineTest`, parameterised over
`{HASH, JSON} × {vector-only, vector+numeric+tag} × {sharing on, off}` (8
combinations). A fixture-owned fake keyspace backs `OpenKey`, `KeyType`,
`HashGet`, `HashSet`, `HashHasStringRef`, `HashSetStringRef` and the JSON shared
API, and records every string reference the engine is handed so the sharing side
is directly observable. `HashHasStringRef` is modelled faithfully as the stateful
predicate the engine implements — `1` when the field already holds a shared
reference, `0` when it holds a plain value, `VALKEYMODULE_ERR` when the key is not
a hash — which is why `ShareWithValkeyHash` (proceeds on `0`: the field is not yet
shared) and `DetachFromValkeyHash` (proceeds on nonzero: the field is shared) test
it in opposite directions.

Every case asserts an observable effect: registry contents and payload bytes,
`entry_cnt` / `hash_sharing_hits`, the index's own view via `IsTracked` /
`GetVectorDuringSearch`, and the exact buffer pointer passed to
`HashSetStringRef`.

Field states: **valid**, **invalid** (wrong dimension count), **absent**.

| Test | Transition |
|---|---|
| `CreateWithValidVector` | key created, vector valid |
| `CreateWithInvalidVector` | key created, vector present but unusable |
| `CreateWithAbsentVector` | key created, vector field missing |
| `OverwriteValidWithDifferentValid` | valid → different valid |
| `OverwriteValidWithIdenticalValid` | valid → byte-identical valid |
| `OverwriteValidWithInvalid` | valid → invalid |
| `OverwriteValidWithAbsent` | valid → field deleted, key survives |
| `OverwriteInvalidWithValid` | invalid → valid |
| `OverwriteAbsentWithValid` | absent → valid |
| `DeleteKeyRemovesRegistryEntry` | whole key deleted |
| `DeleteKeyLeavesOtherKeysTracked` | deletion isolation between keys |
| `NonVectorFieldChangeLeavesVectorAlone` | only a numeric/tag field mutates |
| `InvalidNonVectorFieldWithValidVector` | valid vector + invalid numeric field |
| `KeyReplacedByWrongTypeIsUntracked` | key's stored type changes underneath the index |
| `DroppingIndexUntracksAndDetaches` | index destroyed while keys are tracked |
| `UntrackFromWorkerThreadOnFlatIndex` | untrack reached from a mutation worker thread (death test) |

JSON cases assert that the key is **not** registered — see FM-5 for why that is
the correct behaviour — while still asserting the index-side effect, so they
remain substantive rather than vacuous.

## 3. Failure modes

### FM-1 — Server abort: `DetachFromValkeyHash` runs on a mutation worker thread

**Severity: blocking.** `Check failed: IsMainThread()` — the process aborts.

**Stimulus.** A FLAT vector index over HASH keys, `enable-vector-sharing` at its
default (`true`), mutations flowing through the writer thread pool. Create a key
with a valid vector; then overwrite the vector field with a value of the wrong
size (e.g. a 127-float payload against a 128-dimension index).

**Mechanism.** The overwrite makes `VectorBase::ModifyRecord` return
`kInvalidData`, which calls `RemoveRecordDueToError`, which calls
`VectorRegistry::UntrackIfUnused` (`src/indexes/vector_base.cc:312`). That runs on
the writer thread, because `ProcessAttributeMutation` is reached from
`ProcessSingleMutationAsync`. `LockFreeUntrackIfUnused` sees `use_count() == 1`
(FLAT's `removePoint` releases the index's reference synchronously) and, because
`hash_vector_sharing_` is true (`src/vector_registry.cc:215`), calls
`DetachFromValkeyHash`. The first thing that does is
`vmsdk::MakeUniqueValkeyOpenKey`, which asserts `VerifyMainThread()`.

**Observed.** `F0000 ... utils.h:57] Check failed: IsMainThread()`, followed by a
valkey crash report whose backtrace contains `vmsdk::ThreadPool::WorkerThread`,
and process death. In production this is a crash of the server on ordinary client
input — an `HSET` with a mis-sized vector.

**Why it is not caught elsewhere.** `ShareWithValkeyHash` has an explicit
`vmsdk::VerifyMainThread()` (`src/vector_registry.cc:108`); `DetachFromValkeyHash`
has none, and its callers reach it from both threads. HNSW masks the crash by
accident: its delete is a soft `markDelete` that keeps the element (and its
`shared_ptr`) alive, so `use_count()` never reaches 1 and the detach is skipped.
FLAT does not. The same path is reachable from `~VectorBase` →
`BatchUntrackIfUnused` (`src/indexes/vector_base.cc:583`) whenever the last
reference to an index schema is dropped by a worker thread rather than the main
thread.

**Tests.** `UntrackFromWorkerThreadOnFlatIndex` (unit, `VMSDK_EXPECT_DEATH`);
`test_fm1_invalid_overwrite_on_flat_index_must_not_crash_server` (integration).

### FM-2 — Overwriting a valid vector with an invalid one leaves a stale registry entry

**Severity: high** (memory retention, wrong INFO, and a permanently unshareable
key).

**Stimulus.** HNSW index over HASH keys. `HSET key vec <valid payload>`, then
`HSET key vec <wrong-size payload>`. Both sharing on and sharing off.

**Mechanism.** `TrackRecord` returns early at `src/index_schema.cc:598` when
`IsValidSizeVector` fails, so `VectorRegistry::Track` is never called and nothing
untracks the previous entry. The index does drop the key (via `ModifyRecord` →
`kInvalidData` → `RemoveRecordDueToError`), and that path *does* call
`UntrackIfUnused` — but HNSW's soft delete still holds a reference, so
`use_count() == 1` is false at `src/vector_registry.cc:211` and the entry
survives.

**Observed.** `IsTracked` is correctly `false` and `FT.SEARCH` correctly returns 0
hits, but `LookupRecord` still returns the **superseded payload** and `entry_cnt`
is `1` instead of `0`. It clears only when the key is deleted outright.

**Follow-on.** Setting the field back to its original value re-indexes the key but
never re-shares it: `Track` finds a byte-identical stale entry, takes the dedup
branch, and leaves `needs_sharing` false (`src/vector_registry.cc:93`), so
`ShareWithValkeyHash` is never called. `FT._DEBUG VECTOR_SHARED_COUNT` reports `0` where
a normally indexed key reports `2`.

**Tests.** `OverwriteValidWithInvalid/Hash_{VectorOnly,Mixed}_Sharing{On,Off}`
(unit, 4); `test_fm2_invalid_overwrite_leaves_stale_registry_entry` and
`test_fm2b_key_restored_to_old_value_is_never_shared_again` (integration).

### FM-3 — A key whose stored type changes is never untracked

**Severity: high** — a leaked registry entry on every engine build, and a
search-correctness failure on some.

**Stimulus.** A hash key with a tracked vector is replaced in place by a value of
another type — `RENAME other key`, `RESTORE key ... REPLACE`, or
`COPY src key REPLACE` — none of which emit an intervening `del` notification for
the destination.

**Mechanism.** `ProcessKeyspaceNotification` bails out at
`src/index_schema.cc:619` when `IsProperType(key_obj)` fails, *before* the
per-attribute loop that would call `TrackRecord(..., nullptr, ...)` and erase the
entry. Neither the index nor the registry is updated.

**Observed.** `entry_cnt` still counts a key that is not a hash, and it stays
counted until the key is deleted outright. (The engine-side reference is not at
risk — replacing the value destroys the old hash object and the references into
it.)

Whether the key also stays **searchable** depends on the engine build. On valkey
`a7d49535` `FT.SEARCH` keeps returning it indefinitely — a search-correctness
failure. On `c2b8c17a` it is dropped from the index but the registry entry still
leaks. The leaked entry is the invariant across both, so that is what the test
asserts; it reports the observed hit count alongside, so a reviewer can see which
symptom their engine exhibits.

**Tests.** `KeyReplacedByWrongTypeIsUntracked/Hash_*` (unit, 4);
`test_fm3_key_replaced_by_other_type_is_removed_from_index` (integration).

### FM-4 — Rewriting a hash field with the identical value silently loses sharing

**Severity: medium** (the feature stops delivering its benefit, silently).

**Stimulus.** Sharing on, HASH key with a tracked and shared vector. Re-issue
`HSET key vec <the same bytes>` — a no-op from the client's point of view, but one
that still fires a `hset` notification.

**Mechanism.** The engine replaces the field's value object, which drops the
string reference: `entrySetValueSds` (valkey `src/entry.c`) clears the
`ENTRY_HAS_STRING_REF` aux bit whenever a new sds value is stored, and
`hashTypeSet` does not short-circuit on an equal value. `Track` then finds a
byte-identical entry, takes the dedup branch, and leaves `needs_sharing == false`
(`src/vector_registry.cc:83–95`), so `ShareWithValkeyHash` is never re-invoked.

**Observed.** `FT._DEBUG VECTOR_SHARED_COUNT` drops from `2` to `0` and never recovers;
`MEMORY USAGE` of the key rises from 208 to 832 bytes for a 128-dimension vector,
matching a never-indexed control. `hash_sharing_hits` does not move, so no
existing counter reflects the loss. The saving returns only if the value is
*changed*, which re-enters the `needs_sharing` branch.

**Blast radius.** This is not confined to one key. Re-ingesting a set of
unchanged documents — a periodic refresh job writing the same values back —
unshares every key it touches, whether they were indexed by notification or by
backfill.

**Tests.** `OverwriteValidWithIdenticalValid/Hash_*_SharingOn` (unit, 2);
`test_fm4_identical_rewrite_keeps_sharing` (integration). The branch's
`test_vector_registry_advanced_coverage` already covers the same rewrite from
the counter side — asserting `hash_sharing_hits` does not move — and passes;
what it cannot see is that the field ends up unshared, which is what this test
adds.

### FM-5 — RDB load registers JSON vectors; ingestion does not

**Severity: medium**, and it makes FM-1 reachable for JSON indexes.

**Background — the ingestion behaviour is correct.** JSON vectors are deliberately
never registered. The registry is keyed by `{db_num, key, attribute_identifier}`
(`src/vector_registry.h:92`), so it performs no cross-key deduplication, and its
only reader is `GetOrConstructVectorRecord` (`src/indexes/vector_base.cc:157`),
whose purpose is to make the buffer the engine points at and the buffer the index
holds be the same object. A JSON document stores the vector as text, so there is
no engine-side buffer to unify with; registering a JSON vector would only move an
allocation from the writer pool onto the main thread and pin a `shared_ptr` for
the life of the key. `TrackRecord` receives the record *before*
`NormalizeStringRecord` runs, so raw JSON text (`[1,2,3,4]`) fails
`IsValidSizeVector` and is skipped — the right outcome.

**The defect is the asymmetry.** `VectorBase::LoadTrackedKeys` normalizes first
(`src/indexes/vector_base.cc:456`) and then calls `Track` unconditionally
(line 459), so a JSON index restored from RDB *does* get registry entries.

**Stimulus.** Create a JSON index, ingest documents, `DEBUG RELOAD`.

**Observed.** `entry_cnt` is `0` after ingestion (correct) and jumps to the
document count after the reload.

**Consequences.** Those entries are never refreshed by subsequent ingestion
(`TrackRecord` keeps skipping JSON), so they go stale on the first update and pin
the pre-restart vector until the key is deleted; `vector_registry_entry_cnt`
over-reports. And `LockFreeUntrackIfUnused` gates the detach on
`hash_vector_sharing_` rather than on the attribute data type
(`src/vector_registry.cc:215`), so those JSON entries *do* enter
`DetachFromValkeyHash`. It bails at `KeyType != HASH` — but only after
`MakeUniqueValkeyOpenKey`, where FM-1's `VerifyMainThread()` assert lives.

**Suggested fix.** Gate the `Track` in `LoadTrackedKeys` on the same predicate
`TrackRecord` uses, so both paths agree that JSON is not registered.

**Test.** `test_fm5_json_vectors_stay_unregistered_across_reload` (integration).
Not expressible in the unit suite, which does not drive an RDB round trip.

### FM-6 — An RDB load restores the registry entry but never re-shares

**Severity: high.** Every restart or reload silently doubles vector memory, and
it does not self-heal.

**Stimulus.** A HASH index holding a valid, shared vector. `DEBUG RELOAD` (or any
restart from RDB).

**Mechanism.** `VectorBase::LoadTrackedKeys` re-registers each tracked vector via
`VectorRegistry::Track` (`src/indexes/vector_base.cc:459`), which sets
`needs_sharing` and calls `ShareWithValkeyHash`. That call bails out early — it
increments neither `hash_sharing_hits` nor `hash_sharing_errors`, so the exit is
one of the guard returns in `src/vector_registry.cc:109–139` rather than a failed
`HashSetStringRef`. The registry ends up holding the record while the engine
keeps its own private copy of the same bytes.

**Observed.** After the reload the index is intact — `FT.SEARCH` returns the same
results and `entry_cnt` is restored — but `FT._DEBUG VECTOR_SHARED_COUNT` reports
`0` where it reported `2` before, and `hash_sharing_hits` does not move.
Reproduced at both 4 and 128 dimensions, so it is not an artefact of listpack vs
hashtable encoding.

**It does not recover.** An identical `HSET` afterwards hits the dedup branch
(FM-4) and leaves it unshared; only writing a *different* value re-enters the
`needs_sharing` branch and restores sharing. In practice the memory saving is
gone for the life of every key that is not subsequently modified.

**Test.** `test_fm6_rdb_reload_reshares_hash_vectors` (integration). Not
expressible in the unit suite, which does not drive an RDB round trip.

## 4. Integration-level visibility

Every failure mode is observable from a client.
`integration/test_vector_registry_lifecycle.py` covers all of them, alongside the
existing `integration/test_vector_registry.py` rather than replacing it: that
file covers steady-state sharing, this one covers the transitions. It uses the
same `ValkeySearchTestCaseDebugMode` base, whose per-test server fixture matters
here because FM-1 crashes the server it runs against.

**13 tests: 7 fail** identically across repeated runs — exactly one per failure
mode — and 6 pass. The file uses the suite's shared `Index` / `Vector` /
`Numeric` / `Tag` helpers from `indexes.py` and `waiters` for synchronisation,
so it adds no test infrastructure of its own.

Ingestion:

| Test | Failure mode | Probe | Expected | Observed |
|---|---|---|---|---|
| `test_fm1_invalid_overwrite_on_flat_index_must_not_crash_server` | FM-1 | server liveness | `PING` succeeds | `ConnectionError: Connection closed by server`; log shows `Check failed: IsMainThread()` and a crash report with a `vmsdk::ThreadPool::WorkerThread` frame |
| `test_fm2_invalid_overwrite_leaves_stale_registry_entry` | FM-2 | `entry_cnt` | `0` | `1` — while `FT.SEARCH` correctly returns 0 hits |
| `test_fm2b_key_restored_to_old_value_is_never_shared_again` | FM-2 follow-on | `VECTOR_SHARED_COUNT` | `2` | `0` |
| `test_fm3_key_replaced_by_other_type_is_removed_from_index` | FM-3 | `entry_cnt`, with `FT.SEARCH` hits reported | `0` | `1` — a key that is not a hash is still counted |
| `test_fm4_identical_rewrite_keeps_sharing` | FM-4 | `VECTOR_SHARED_COUNT` | `2` | `0` |

Backfill — keys that already existed when the index was created. Structurally
backfill can only insert a key the index has not seen, or re-read one whose
current value a notification already indexed; it always reads the live value, so
it can never disagree with the keyspace. Parameterised over vector-only and
vector+numeric+tag schemas, each covering all three vector-field states.

| Test | Probe | Result |
|---|---|---|
| `test_backfill_hash[vector_only]`, `[mixed]` | share count per field state, `entry_cnt`, KNN hits, indexing failures, numeric/tag searches | **pass** — valid `2`, invalid `0`, absent `-1` |
| `test_backfill_json[vector_only]`, `[mixed]` | KNN hits, `entry_cnt`, numeric/tag searches | **pass** — indexed, `entry_cnt` stays `0` |

RDB load with sharing on. Both tests use a mixed schema and all three
vector-field states, so index contents and registry state are checked together:

| Test | Failure mode | Probe | Expected | Observed |
|---|---|---|---|---|
| `test_fm5_json_vectors_stay_unregistered_across_reload` | FM-5 | `entry_cnt` after `DEBUG RELOAD`, plus vector/numeric/tag searches | `0`, searches intact | searches intact, `entry_cnt` populated |
| `test_fm6_rdb_reload_reshares_hash_vectors` | FM-6 | `VECTOR_SHARED_COUNT` and `hash_sharing_hits` after `DEBUG RELOAD`, plus searches and the invalid/absent keys | `2`, hits increase | `0`, hits unchanged — index intact, sharing gone |

The share-count probe itself:

| Test | Probe | Result |
|---|---|---|
| `test_share_count_tracks_number_of_holders` | count with 1 then 2 indexes, then dropping both | **passes** — `2`, `3`, `2`, `0`, and the value survives the detach |
| `test_share_count_distinguishes_absent_from_unshared` | missing field / key / wrong type / after `DEL` | **passes** — `-1` each |

Notes on the probes:

- **`entry_cnt` carries FM-2, FM-5 and the RDB reload cases**, and it already exists
  (`search_vector_registry_entry_cnt`, `src/valkey_search.cc:385`). The tests pair
  it with `FT.SEARCH` so a failure distinguishes "registry is stale" from "index
  is stale".
- **FM-3 needs `entry_cnt`.** Its other symptom — `FT.SEARCH` returning a key
  that is not a hash — is engine-version dependent, so the leaked entry is the
  assertion that holds everywhere.
- **FM-1 needs no instrumentation.** The server dies.
- **FM-6 needs both.** `VECTOR_SHARED_COUNT` shows the field is unshared, and
  `hash_sharing_hits` staying flat shows the share was never attempted rather
  than attempted and refused.

### 4.1 `FT._DEBUG VECTOR_SHARED_COUNT <key> <field>`

FM-4, FM-6 and the FM-2 follow-on are all "a field that should be shared is
not".
Neither moves any existing counter, and inferring sharing from `MEMORY USAGE` is a
poor instrument — it reads the engine's internal representation out of an
allocation size, and that size also counts the key name, so a control key of a
different name length silently shifts the comparison.

`FT._DEBUG VECTOR_SHARED_COUNT` exposes the engine's own predicate instead, paired with
the module's reference count:

| reply | meaning |
|---|---|
| `-1` | no such field — the key is missing, is not a hash, or does not carry the field |
| `0` | the field exists and holds a plain value; nothing is shared |
| `>0` | live module references to the shared buffer: one held by the registry, plus one per index over the field |

Separating `-1` from `0` makes an assertion of "exists but not shared"
self-verifying: it cannot be satisfied by a typo'd key or field name.

Implementation is two pieces:

- `VectorRegistry::GetRecordUseCount` (`src/vector_registry.h`) — returns the
  entry's `use_count()` without copying the `shared_ptr` and without touching
  `lookup_record_hits`/`misses`, so the probe cannot perturb what it observes.
  (`LookupRecord` would do both.)
- `VectorSharedCountCmd` (`src/commands/ft_debug.cc`) — reads the sharing state from
  `ValkeyModule_HashHasStringRef`, the same call the registry uses, then reports
  the count.

Verified against every state on a live server:

| state | `VECTOR_SHARED_COUNT` | `MEMORY USAGE` |
|---|---|---|
| missing key / wrong type / missing field / after `DEL` | `-1` | — |
| hash field exists, key never indexed | `0` | 832 |
| after first `HSET` of a valid vector | `2` (registry + index) | 208 |
| after a byte-identical `HSET` | `0` | 832 |
| after `HSET` of a *changed* value | `2` | 208 |
| second index created over the same field | `3` | 208 |
| after dropping the second index | `2` | 208 |
| after dropping the last index | `0` | 832 |

Returning a count rather than a yes/no is what makes this reusable: reference
counting is the registry's core invariant, and it is otherwise assertable only
from inside a unit test (`HnswVectorIndexReferenceCountOnIngestionAndMutation`
reaches into `use_count()` directly). Tests that need it — multi-index sharing,
leak checks after `FT.DROPINDEX`, backfill, replica behaviour — can now assert it
from a client. `test_share_count_tracks_number_of_holders` pins the count
semantics and `test_share_count_distinguishes_absent_from_unshared` pins the
`-1`/`0` boundary, so later tests can rely on both.

It also sharpens coverage that already existed: the branch's
`test_vector_registry_hnsw_sharing_on` checks that `FT.DROPINDEX` leaves the
hash values readable via `HGET`, which is necessary but not sufficient — a
correct-looking value proves nothing about whether the reference was detached.
`test_share_count_tracks_number_of_holders` asserts the detach itself.

## 5. Transitions still not covered

Worth adding before merge:

- **Backfill and RDB load at unit level.** Both are now covered by integration
  tests, but neither is reachable from the unit suite: `IndexSchemaRDBTest`
  forces sharing off, so the RDB ↔ registry interaction has no unit coverage at
  the production default, and no unit test drives `from_backfill=true`.
- **MULTI/EXEC.** `EnqueueMultiMutation` defers index mutation while `Track` still
  happens at notification time on the main thread; the resulting ordering between
  registry state and index state is untested.
- **Two vector attributes on one key.** `TrackRecord` runs per attribute; only
  single-vector schemas are covered.
- **`db_num > 0`, SWAPDB, FLUSHDB** through the state machine (only direct
  `Track()` calls with differing `db_num` are covered).
- **Expire / evict notifications.**
- **`ForceHashSharingError`** (`src/vector_registry.cc:19`) — the debug control is
  never exercised by any test.

## 6. Sanitizer builds

Both suites were re-run against an ASan build (`./build.sh --asan`, and the same
flags for the integration run against an ASan-instrumented valkey). Results are
identical to the release build:

| Suite | Release | ASan |
|---|---|---|
| `indexes_test` (all) | 326 pass / 10 fail | 326 pass / 10 fail |
| state-machine suite | 104 pass / 14 skip / 10 fail | 104 pass / 14 skip / 10 fail |
| `test_vector_registry_lifecycle.py` | 7 fail / 6 pass | 7 fail / 6 pass |

No `heap-use-after-free`, `heap-buffer-overflow`, or leak reports in either
suite. The death test behaves correctly under ASan — `VMSDK_EXPECT_DEATH` still
matches, and no core files are produced.

Two things worth knowing:

- **FM-1 also trips ASan's own runtime.** When the module aborts from a mutation
  worker thread, ASan hits an internal assertion while handling the abort:
  `AddressSanitizer: CHECK failed: sanitizer_thread_arg_retval.cpp:56`. That is a
  consequence of FM-1, not an independent finding, but it makes the crash noisier
  under ASan and could mask the module's own `Check failed: IsMainThread()`
  message in a CI log.
- **The release allocator is not covered by ASan.** `VectorBase`'s
  `vector_allocator_` is compiled out under `#ifndef SAN_BUILD`
  (`src/indexes/vector_base.h:254`), so an ASan build constructs every
  `VectorRecord` through `::operator new` instead of `FixedSizeAllocator`. The
  allocator that production actually uses is therefore never sanitized — a
  pre-existing gap, not one this review introduces, but relevant when reading a
  green ASan run as evidence about the sharing path.

## 7. Unrelated: a pre-existing flaky test on the branch

`IndexSchemaFriendTest.ConsistencyTest` (`testing/index_schema_test.cc`)
intermittently aborts:

```
F0000 ... vector_hnsw.h:129] Check failed: ptr != nullptr
    Internal ID not found in label_lookup: 1101
```

Measured on the branch with the source changes in section 8 stashed: **3 aborts
in 12 runs**. It is unrelated to this review's changes — the test drives 1000
vectors through the writer pool and never touches the registry probe — but it is
a `CHECK` failure under concurrent mutation, which is the same class of problem
as FM-1: in production this path aborts the server rather than a test binary.
Worth triaging separately before merge, and worth knowing about when reading CI
results, since it makes `indexes_test` fail intermittently for reasons that have
nothing to do with the failures in section 3.

## 8. Files changed

Tests:

- `testing/vector_registry_state_machine_test.cc` — new, 16 parameterised tests
  × 8 combinations.
- `testing/CMakeLists.txt` — added to `INDEXES_TEST_SOURCES`.
- `integration/test_vector_registry_lifecycle.py` — new, 13 tests, built on the
  suite's shared `indexes.py` helpers. Sits beside the existing
  `integration/test_vector_registry.py`, which is unchanged and still passes
  (11 tests).

Source (debug-only and additive; no existing behaviour is touched):

- `src/commands/ft_debug.cc` — adds the `VECTOR_SHARED_COUNT` subcommand (4.1).
- `src/vector_registry.{h,cc}` — adds `GetRecordUseCount`, a non-perturbing read
  of an entry's reference count, used by that subcommand.

Run with:

```
# Build the module and the tests.
./build.sh

# Unit tests: the whole suite, or just the one holding the state machine tests.
./build.sh --run-tests
./build.sh --run-tests=indexes_test

# Integration tests: the lifecycle suite added here, or the whole set.
./build.sh --run-integration-tests=vector_registry_lifecycle
./build.sh --run-integration-tests

# Same, under AddressSanitizer.
./build.sh --asan
./build.sh --asan --run-tests=indexes_test
./build.sh --asan --run-integration-tests=vector_registry_lifecycle
```

`--run-integration-tests=<pattern>` passes the pattern to pytest's `-k`, so it
selects by test-file or test-name substring. It also skips the Abseil-based
suite under `testing/integration`, which is why these tests live in
`integration/` alongside the rest of the pytest suite.
