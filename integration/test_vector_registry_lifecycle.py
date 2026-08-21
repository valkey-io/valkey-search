"""Lifecycle coverage for the vector registry / hash memory-sharing feature.

`test_vector_registry.py` covers the steady state: that sharing engages, that
the counters move, that dropping an index or deleting a key clears the entry,
and the memory delta between sharing on and off. This file covers the state
machine that keeps the registry in sync with the keyspace -- the transitions
that suite does not reach: a vector field that is unusable or absent, a key
whose type changes underneath the index, JSON indexes, backfill of
pre-existing keys, and RDB reload.

Tests named `test_fmN_*` pin a known failure mode and are expected to FAIL
against the `memory_sharing` branch; each carries the mode it covers. The rest
pass and guard behaviour that is currently correct.
"""

import os
import shutil
import time

import pytest
import valkey
from indexes import Index, KeyDataType, Numeric, Tag, Vector, float_to_bytes
from valkey_search_test_case import LOGS_DIR, ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401


DIM = 128
VALID = [float(i) for i in range(DIM)]
OTHER = [float(i + 1000) for i in range(DIM)]
# Too few components to be a DIM-wide vector: rejected by IsValidSizeVector.
SHORT = [1.0, 2.0]


class TestVectorRegistryLifecycle(ValkeySearchTestCaseDebugMode):
    """One server per test -- setup_test is function scoped, which matters
    because test_fm1_* crashes the server it runs against."""

    def get_config_file_lines(self, testdir, port):
        # Match test_vector_registry.py: force hashtable encoding so the
        # engine's stringRef state does not depend on value size.
        lines = super().get_config_file_lines(testdir, port)
        lines.append("hash-max-listpack-entries 0")
        return lines

    # ---- observation -----------------------------------------------------

    def registry_stat(self, name: str) -> int:
        """One `search_vector_registry_*` INFO field."""
        info = self.client.execute_command("INFO", "everything")
        key = f"search_vector_registry_{name}"
        if isinstance(info, dict):
            return int(info[key])
        for line in info.decode().splitlines():
            if line.startswith(key + ":"):
                return int(line.split(":", 1)[1])
        raise KeyError(key)

    def share_count(self, key: str, field: str = "vec") -> int:
        """Module references to the buffer the engine shares for this field.

        -1  no such field (key missing, not a hash, or field absent)
         0  the field exists and holds a plain value -- nothing is shared
        >0  one reference from the registry plus one per index over the field

        The -1 is what makes an assertion of 0 self-verifying: it cannot be
        satisfied by a typo'd key or field name.
        """
        return int(
            self.client.execute_command(
                "FT._DEBUG", "VECTOR_SHARED_COUNT", key, field
            )
        )

    def validate_registry(self) -> list:
        """FT._DEBUG VALIDATE_VECTOR_REGISTRY: every disagreement between the
        keyspace, the indexes, the registry and the engine's shared fields.
        Empty means consistent. Errors if the system is not idle."""
        return [
            x.decode()
            for x in self.client.execute_command(
                "FT._DEBUG", "VALIDATE_VECTOR_REGISTRY"
            )
        ]

    def save_flush_restore(self):
        """Persist, empty the keyspace, and load it back.

        SAVE writes both the keys and the index sections; DEBUG RELOAD then
        empties the keyspace and reads the RDB back. Only safe from a state
        that already validates clean -- an inconsistent one can be unloadable
        (FM-7).
        """
        self.client.execute_command("SAVE")
        self.client.execute_command("DEBUG", "RELOAD")
        self.settle()

    def assert_survives_save_restore(self):
        """A clean state must still be clean after a round trip through an RDB."""
        before = self.registry_stat("entry_cnt")
        self.save_flush_restore()
        problems = self.validate_registry()
        assert problems == [], (
            "registry is out of sync after a save/restore cycle:\n  "
            + "\n  ".join(problems)
        )
        assert (
            self.registry_stat("entry_cnt") == before
        ), "registry entry count changed across a save/restore cycle"

    def assert_registry_consistent(self):
        problems = self.validate_registry()
        assert problems == [], "registry is out of sync:\n  " + "\n  ".join(
            problems
        )

    SHARED_BY_ONE_INDEX = 2
    NOT_SHARED = 0
    NO_SUCH_FIELD = -1

    def assert_sharing_active(self):
        """The feature is a no-op unless the engine exposes the sharing API and
        the option is on; assert it so a green run cannot mean "it was off"."""
        assert (
            self.registry_stat("sharing_active") == 1
        ), "vector sharing is not active; these tests would be vacuous"

    # ---- construction ----------------------------------------------------

    def build_index(
        self,
        key_type: KeyDataType = KeyDataType.HASH,
        algo: str = "HNSW",
        mixed: bool = False,
        name: str = "idx",
    ) -> Index:
        self.assert_sharing_active()
        fields = [Vector("vec", DIM, type=algo, distance="L2")]
        if mixed:
            fields += [Numeric("num"), Tag("tag")]
        return Index(
            name, fields, prefixes=["doc"], type=key_type
        )

    def write_key(self, index: Index, suffix: str, vec, mixed: bool = False):
        """Write one key holding `vec`, or omitting the vector field entirely
        when `vec` is None. Returns the key name."""
        key = f"doc:{suffix}"
        if index.type == KeyDataType.HASH:
            data = {} if vec is None else {"vec": float_to_bytes(vec)}
            if mixed:
                data |= {"num": "10", "tag": "a"}
            if not data:
                data = {"other": "x"}
            self.client.hset(key, mapping=data)
        else:
            parts = [] if vec is None else [f'"vec":{list(vec)}']
            if mixed:
                parts += ['"num":10', '"tag":"a"']
            if not parts:
                parts = ['"other":"x"']
            self.client.execute_command(
                "JSON.SET", key, "$", "{" + ",".join(parts) + "}"
            )
        return key

    def write_all_field_states(self, index: Index, mixed: bool = False):
        """One key per field state. Returns (valid, invalid, missing)."""
        return (
            self.write_key(index, "valid", VALID, mixed),
            self.write_key(index, "invalid", SHORT, mixed),
            self.write_key(index, "missing", None, mixed),
        )

    def settle(self, seconds: float = 1.0):
        """Give the mutation workers time to drain when there is no counter to
        wait on. VALIDATE_VECTOR_REGISTRY refuses to run while work is queued,
        so a premature call fails loudly rather than silently."""
        time.sleep(seconds)

    def wait_for_backfill(self, index: Index, timeout: float = 30.0):
        waiters.wait_for_true(
            lambda: index.backfill_complete(self.client), timeout=timeout
        )

    def wait_for_docs(self, index: Index, count: int):
        waiters.wait_for_equal(lambda: index.info(self.client).num_docs, count)

    def knn_hits(self, index: Index, query=VALID) -> int:
        """Documents returned by a KNN scan. Uses FT.SEARCH directly rather
        than Index.query(), whose reply parser assumes a JSON "$" field."""
        return self.client.execute_command(
            "FT.SEARCH", index.name, "*=>[KNN 100 @vec $q]",
            "PARAMS", "2", "q", float_to_bytes(query), "DIALECT", "2",
        )[0]

    def search_count(self, index: Index, query: str) -> int:
        return self.client.execute_command("FT.SEARCH", index.name, query)[0]

    # ---- backfill --------------------------------------------------------
    #
    # Backfill visits keys that already existed when the index was created.
    # Structurally it can only insert a key the index has not seen, or re-read
    # one whose current value a keyspace notification already indexed -- it
    # always reads the live value, so it can never disagree with the keyspace.
    # (The consequence of a client rewriting an already-backfilled key with
    # unchanged data is FM-4, pinned once below.)

    @pytest.mark.parametrize("mixed", [False, True], ids=["vector_only", "mixed"])
    def test_backfill_hash(self, mixed: bool):
        """Backfill of a HASH index over all three vector-field states.

        With `mixed`, the key also carries numeric and tag fields, which must
        index for every key regardless of whether that key's vector is usable.
        """
        index = self.build_index(mixed=mixed)
        valid, invalid, missing = self.write_all_field_states(index, mixed)
        index.create(self.client, wait_for_backfill=True)

        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX
        assert self.share_count(invalid) == self.NOT_SHARED
        assert self.share_count(missing) == self.NO_SUCH_FIELD
        assert self.registry_stat("entry_cnt") == 1
        assert self.knn_hits(index) == 1, "only the valid key indexes"
        assert index.info(self.client).hash_indexing_failures == 1
        if mixed:
            assert self.search_count(index, "@num:[5 20]") == 3
            assert self.search_count(index, "@tag:{a}") == 3

        self.assert_registry_consistent()

        self.assert_survives_save_restore()

    @pytest.mark.parametrize("mixed", [False, True], ids=["vector_only", "mixed"])
    def test_backfill_json(self, mixed: bool):
        """Backfill of a JSON index over all three vector-field states.

        JSON vectors are not registered (FM-5), so the registry must stay
        empty while the index itself still fills correctly.
        """
        index = self.build_index(key_type=KeyDataType.JSON, mixed=mixed)
        self.write_all_field_states(index, mixed)
        index.create(self.client, wait_for_backfill=True)

        assert self.knn_hits(index) == 1, "only the valid document indexes"
        assert (
            self.registry_stat("entry_cnt") == 0
        ), "JSON vectors cannot be shared and must not be registered"
        if mixed:
            assert self.search_count(index, "@num:[5 20]") == 3
            assert self.search_count(index, "@tag:{a}") == 3

    # ---- ingestion -------------------------------------------------------

        self.assert_registry_consistent()

        self.assert_survives_save_restore()

    def test_fm1_invalid_overwrite_on_flat_index_must_not_crash_server(self):
        """FM-1: untrack reached from a mutation worker thread aborts the server.

        Stimulus: FLAT index over HASH keys; store a valid vector, then
        overwrite the same field with a wrong-size payload. FLAT releases its
        reference to the vector synchronously, so the registry drops to its
        last reference on the writer thread and calls DetachFromValkeyHash ->
        ValkeyModule_OpenKey, which asserts VerifyMainThread().

        Expected: the wrong-size value is rejected as invalid data and the
        server stays up.
        Observed: `Check failed: IsMainThread()` and the server dies.
        """
        index = self.build_index(algo="FLAT")
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)

        try:
            self.client.hset(valid, "vec", float_to_bytes(SHORT))
            waiters.wait_for_equal(
                lambda: index.info(self.client).num_docs, 0, timeout=10
            )
            alive = self.client.ping()
        except valkey.exceptions.ConnectionError as e:
            pytest.fail(f"server died on a wrong-size HSET: {e}")
        assert alive, "server did not survive a wrong-size HSET"

    def test_fm2_invalid_overwrite_leaves_stale_registry_entry(self):
        """FM-2: valid -> invalid overwrite strands the old vector.

        Stimulus: HNSW index over HASH keys; store a valid vector, then
        overwrite it with a wrong-size payload. TrackRecord returns early on
        the size check, so nothing untracks the previous entry, and HNSW's
        soft delete keeps use_count above 1 so UntrackIfUnused is a no-op.

        Expected: the key leaves the index and the registry entry goes with it.
        Observed: FT.SEARCH stops returning the key, but entry_cnt stays at 1
        until the key is deleted outright.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)
        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX

        self.client.hset(valid, "vec", float_to_bytes(SHORT))
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0)

        assert (
            self.registry_stat("entry_cnt") == 0
        ), "registry still holds a vector for a key that is not indexed"

        self.assert_registry_consistent()

    def test_fm2b_key_restored_to_old_value_is_never_shared_again(self):
        """FM-2, follow-on: the stale entry permanently disables sharing.

        Stimulus: as above, then set the field back to its original value.
        Track finds a byte-identical stale entry, takes the dedup branch, and
        leaves needs_sharing false, so ShareWithValkeyHash is never called.

        Expected: the key is indexed and shared again.
        Observed: it is re-indexed but the engine keeps its own full copy of
        the vector.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)
        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX

        self.client.hset(valid, "vec", float_to_bytes(SHORT))
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0)
        self.client.hset(valid, "vec", float_to_bytes(VALID))
        waiters.wait_for_equal(lambda: self.knn_hits(index), 1)

        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX, (
            "the stale registry entry sent Track down the dedup branch, so "
            "the field was never re-shared"
        )

        self.assert_registry_consistent()

    def test_fm3_key_replaced_by_other_type_is_removed_from_index(self):
        """FM-3: a key whose type changes in place is never untracked.

        Stimulus: index a hash key, then RENAME a list onto that same name.
        RENAME replaces the destination in place and emits no `del`
        notification for it, and ProcessKeyspaceNotification bails out at the
        IsProperType check before the per-attribute loop that would untrack.

        Expected: the key is dropped from the index and from the registry.
        Observed: entry_cnt still counts a key that is not a hash.

        Whether the key also stays searchable depends on the engine build --
        valkey a7d49535 keeps returning it from FT.SEARCH indefinitely, while
        c2b8c17a drops it from the index but still leaks the registry entry.
        The leaked entry is the invariant, so that is what is asserted; the
        observed hit count is reported alongside it.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)

        self.client.rpush("src", "a", "b")
        self.client.rename("src", valid)  # the key is a LIST now
        waiters.wait_for_equal(lambda: self.client.type(valid), b"list")

        hits = self.knn_hits(index)
        assert self.registry_stat("entry_cnt") == 0, (
            "registry still holds a vector for a key that is not a hash "
            f"(FT.SEARCH returns {hits} hits for it on this engine build)"
        )
        assert hits == 0, "FT.SEARCH returns a key that no longer holds a vector"

        self.assert_registry_consistent()

    def test_fm4_identical_rewrite_keeps_sharing(self):
        """FM-4: rewriting a field with the same bytes silently unshares it.

        Stimulus: index a hash key, then re-issue the identical HSET. The
        engine replaces the value object -- clearing the stringRef aux bit --
        while Track takes the dedup branch and leaves needs_sharing false, so
        the field is never re-shared.

        test_vector_registry.py::test_vector_registry_advanced_coverage covers
        the same rewrite from the counter side, asserting that
        hash_sharing_hits does not move. This asserts the consequence that
        counter cannot see: the field is no longer shared at all.

        Expected: the field stays shared across a no-op rewrite.
        Observed: it drops to unshared and nothing re-shares it. Re-ingesting a
        set of unchanged documents therefore unshares every key it touches.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)
        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX

        hits_before = self.registry_stat("get_record_hits")
        self.client.hset(valid, "vec", float_to_bytes(VALID))  # identical
        waiters.wait_for_equal(
            lambda: self.registry_stat("get_record_hits") > hits_before, True
        )

        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX, (
            "a no-op HSET dropped the shared reference; the engine now holds "
            "its own copy of the vector and nothing re-shares it"
        )

    # ---- RDB load with sharing on ---------------------------------------

        self.assert_registry_consistent()

    def test_fm5_json_vectors_stay_unregistered_across_reload(self):
        """FM-5: an RDB load registers JSON vectors that ingestion does not.

        Ingestion skips registration for JSON -- a JSON vector is stored as
        text and cannot be shared with the engine -- but LoadTrackedKeys
        normalizes the record first and then calls Track unconditionally.

        Expected: entry_cnt stays 0 across the reload.
        Observed: it jumps to the indexed-document count. Those entries are
        never refreshed by later ingestion, and they make JSON indexes reach
        the DetachFromValkeyHash path behind FM-1.
        """
        index = self.build_index(key_type=KeyDataType.JSON, mixed=True)
        index.create(self.client, wait_for_backfill=True)
        self.write_all_field_states(index, mixed=True)
        self.wait_for_docs(index, 3)
        assert self.registry_stat("entry_cnt") == 0

        self.client.execute_command("DEBUG", "RELOAD")
        waiters.wait_for_equal(
            lambda: index.info(self.client).num_docs, 3
        )

        # The index survives the reload intact.
        assert self.knn_hits(index) == 1
        assert self.search_count(index, "@num:[5 20]") == 3
        assert self.search_count(index, "@tag:{a}") == 3
        assert (
            self.registry_stat("entry_cnt") == 0
        ), "RDB load registered JSON vectors that ingestion never registers"

        self.assert_registry_consistent()

    def test_fm6_rdb_reload_reshares_hash_vectors(self):
        """FM-6: an RDB load restores the registry entry but never re-shares.

        Stimulus: a HASH index holding a valid vector, shared, then
        DEBUG RELOAD. VectorBase::LoadTrackedKeys re-registers the record, so
        entry_cnt comes back, but ShareWithValkeyHash bails out without
        incrementing either hash_sharing_hits or hash_sharing_errors, so the
        engine keeps its own copy of every vector.

        Expected: the field is shared again after the reload.
        Observed: share count 0, and no counter records the loss. The key stays
        unshared -- an identical HSET hits the dedup branch (FM-4) and does not
        recover it; only writing a different value does.
        """
        index = self.build_index(mixed=True)
        index.create(self.client, wait_for_backfill=True)
        valid, invalid, missing = self.write_all_field_states(index, mixed=True)
        self.wait_for_docs(index, 3)
        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX
        shares_before = self.registry_stat("shared_externally_cnt")

        self.client.execute_command("DEBUG", "RELOAD")
        waiters.wait_for_equal(lambda: index.info(self.client).num_docs, 3)

        # The index survives the reload intact, including the non-vector
        # fields and the keys whose vector was unusable or absent.
        assert self.knn_hits(index) == 1
        assert self.search_count(index, "@num:[5 20]") == 3
        assert self.search_count(index, "@tag:{a}") == 3
        assert self.registry_stat("entry_cnt") == 1
        assert self.share_count(invalid) == self.NOT_SHARED
        assert self.share_count(missing) == self.NO_SUCH_FIELD

        assert (
            self.registry_stat("shared_externally_cnt") > shares_before
        ), "no share was attempted for the restored entry"
        assert (
            self.share_count(valid) == self.SHARED_BY_ONE_INDEX
        ), "the engine holds its own copy of every vector after a reload"

    def test_fm7_rdb_load_after_a_type_change_must_not_crash(self):
        """FM-7: an RDB load aborts the server if any tracked key is no longer
        readable as the schema's type.

        FM-3 leaves the index tracking a key whose type has changed. On the
        next save/load VectorBase::LoadTrackedKeys walks the tracked keys and
        asks the attribute data type for each field, then CHECKs the result:

            vector_base.cc:455] Check failed: record.ok()

        Stimulus: index a hash key, RENAME a list onto it, then DEBUG RELOAD.

        Expected: the reload completes.
        Observed: the server aborts. The same load runs on every restart, on a
        replica's full sync, and on any restore from backup, so a single
        RENAME can leave a dataset that cannot be loaded.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        key = self.write_key(index, "1", VALID)
        self.wait_for_docs(index, 1)

        self.client.rpush("src", "a", "b")
        self.client.rename("src", key)  # the key is a LIST now
        waiters.wait_for_equal(lambda: self.client.type(key), b"list")
        self.settle()

        try:
            self.client.execute_command("DEBUG", "RELOAD")
            self.settle()
            alive = self.client.ping()
        except valkey.exceptions.ConnectionError as e:
            pytest.fail(f"server died loading an RDB after a type change: {e}")
        assert alive, "server did not survive the reload"

    # ---- whole-keyspace operations ---------------------------------------
    #
    # Each of these moves or removes keys behind the index's back. The registry
    # has to follow, and VALIDATE_VECTOR_REGISTRY is what says whether it did.

    def test_multi_exec_batch_leaves_registry_consistent(self):
        """Mutations issued inside MULTI/EXEC are queued and drained lazily.

        IndexSchema::EnqueueMultiMutation defers the index work while Track
        still runs at notification time on the main thread, so the two halves
        of the update are separated in a way ordinary writes never are.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)

        pipe = self.client.pipeline(transaction=True)
        for i in range(20):
            pipe.hset(f"doc:{i}", "vec", float_to_bytes(VALID))
        pipe.execute()
        self.wait_for_docs(index, 20)
        assert self.registry_stat("entry_cnt") == 20

        # A second transaction that rewrites some keys with a different vector
        # and deletes others.
        pipe = self.client.pipeline(transaction=True)
        for i in range(0, 10):
            pipe.hset(f"doc:{i}", "vec", float_to_bytes(OTHER))
        for i in range(10, 20):
            pipe.delete(f"doc:{i}")
        pipe.execute()
        self.wait_for_docs(index, 10)
        assert self.registry_stat("entry_cnt") == 10

        self.assert_survives_save_restore()

    def test_flushdb_clears_the_registry(self):
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        for i in range(10):
            self.write_key(index, str(i), VALID)
        self.wait_for_docs(index, 10)
        assert self.registry_stat("entry_cnt") == 10

        self.client.flushdb()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        assert self.client.dbsize() == 0

        self.assert_survives_save_restore()

    def test_flushall_clears_the_registry(self):
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        for i in range(10):
            self.write_key(index, str(i), VALID)
        self.wait_for_docs(index, 10)
        assert self.registry_stat("entry_cnt") == 10

        self.client.flushall()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)

        self.assert_survives_save_restore()

    def test_swapdb_moves_keys_out_from_under_the_index(self):
        """SWAPDB exchanges two keyspaces without touching any key.

        The registry is keyed by db number, so every entry for the indexed db
        now refers to keys that live somewhere else.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        for i in range(5):
            self.write_key(index, str(i), VALID)
        self.wait_for_docs(index, 5)
        assert self.registry_stat("entry_cnt") == 5

        self.client.execute_command("SWAPDB", 0, 1)
        waiters.wait_for_equal(lambda: self.client.dbsize(), 0)
        self.settle()

        self.assert_survives_save_restore()

    def test_rename_indexed_key_to_a_new_name(self):
        """RENAME of an indexed key to an unused name inside the prefix.

        The old name must leave the registry and the new one must enter it.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        old = self.write_key(index, "old", VALID)
        self.wait_for_docs(index, 1)
        assert self.share_count(old) == self.SHARED_BY_ONE_INDEX

        self.client.rename(old, "doc:new")
        waiters.wait_for_equal(lambda: self.client.exists(old), 0)
        self.settle()

        assert self.share_count(old) == self.NO_SUCH_FIELD
        assert self.registry_stat("entry_cnt") == 1

        self.assert_survives_save_restore()

    def test_rename_indexed_key_out_of_the_prefix(self):
        """RENAME an indexed key to a name the index does not cover."""
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        old = self.write_key(index, "old", VALID)
        self.wait_for_docs(index, 1)

        self.client.rename(old, "outside:1")
        waiters.wait_for_equal(lambda: self.client.exists(old), 0)
        self.settle()

        assert self.knn_hits(index) == 0, "the key left the index's prefix"
        assert self.registry_stat("entry_cnt") == 0

        self.assert_survives_save_restore()

    # ---- one field, two indexes, only one of them holding the key --------
    #
    # Backfill is per index, not per key, so a key can be indexed by one index
    # and absent from another that covers the very same field. SKIPINITIALSCAN
    # produces exactly that state deterministically and leaves the system idle,
    # so the registry can be validated while it holds. Both indexes share a
    # single record per {db, key, identifier}, which makes this the one shape
    # that exercises reference counting above two.

    def _create_skipscan_index(self, name: str) -> Index:
        """An index over the same prefix and field, created without scanning."""
        self.client.execute_command(
            "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "doc",
            "SKIPINITIALSCAN",
            "SCHEMA", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        return Index(
            name,
            [Vector("vec", DIM, type="HNSW", distance="L2")],
            prefixes=["doc"],
            type=KeyDataType.HASH,
        )

    def _one_index_ahead(self):
        """Index `a` holds the key; index `b` covers the same field but was
        created with SKIPINITIALSCAN, so it never saw the key."""
        first = self.build_index(name="a")
        first.create(self.client, wait_for_backfill=True)
        key = self.write_key(first, "1", VALID)
        self.wait_for_docs(first, 1)
        assert self.share_count(key) == 2, "registry + index a"

        second = self._create_skipscan_index("b")
        self.wait_for_backfill(second)
        self.settle()

        assert second.info(self.client).num_docs == 0, "b should not have scanned"
        assert self.share_count(key) == 2, "b holds no reference to the record"
        # The asymmetry is legitimate, so the registry is consistent in it.
        self.assert_registry_consistent()
        return first, second, key

    @pytest.mark.parametrize(
        "operation",
        ["same", "different", "invalid", "missing", "delete", "reload"],
    )
    def test_two_indexes_one_skipped_the_key(self, operation: str):
        """Apply one operation while the two indexes disagree about the key.

        A mutation is delivered to every matching index, so any write pulls the
        skipped index into line; a reload does not, because the skipped index
        has no tracked keys to restore. Either way the registry must end up
        describing exactly what the indexes hold.
        """
        first, second, key = self._one_index_ahead()

        if operation == "same":
            self.client.hset(key, "vec", float_to_bytes(VALID))
        elif operation == "different":
            self.client.hset(key, "vec", float_to_bytes(OTHER))
        elif operation == "invalid":
            self.client.hset(key, "vec", float_to_bytes(SHORT))
        elif operation == "missing":
            self.client.hdel(key, "vec")
        elif operation == "delete":
            self.client.delete(key)
        elif operation == "reload":
            self.client.execute_command("DEBUG", "RELOAD")
        self.settle()

        # The validator goes first: it names the precise disagreement, where
        # the assertions below only say a number came out wrong. It is also
        # what checks the reference count -- share_count reports 0 for a field
        # that is not shared at all, which would otherwise mask a bad count.
        self.assert_registry_consistent()

        if operation in ("same", "different"):
            # The write reached both indexes; they now share one record.
            assert self.registry_stat("entry_cnt") == 1
            assert self.knn_hits(first) == 1
            assert self.knn_hits(second) == 1
            assert self.share_count(key) == 3, (
                "registry + two indexes should share one record"
            )
        elif operation == "reload":
            # Nothing pulled b into line, so the asymmetry survives the reload.
            assert self.registry_stat("entry_cnt") == 1
            assert self.knn_hits(first) == 1
            assert self.knn_hits(second) == 0
            assert self.share_count(key) == 2, "registry + index a only"
        else:
            assert self.registry_stat("entry_cnt") == 0, (
                "no index holds the key, so nothing should remain registered"
            )
            assert self.knn_hits(first) == 0
            assert self.knn_hits(second) == 0

    def test_two_indexes_one_skipped_the_key_drop_the_holder(self):
        """Dropping the index that holds the key leaves nothing referencing it,
        even though a second index still covers the field."""
        first, second, key = self._one_index_ahead()

        first.drop(self.client)
        self.settle()

        self.assert_registry_consistent()
        assert self.registry_stat("entry_cnt") == 0
        assert self.knn_hits(second) == 0

    # ---- the share-count probe itself ------------------------------------

        self.assert_registry_consistent()

        self.assert_survives_save_restore()

    def test_share_count_tracks_number_of_holders(self):
        """Pins what the count means, so later tests can rely on it.

        The count is the number of live module references to the shared
        buffer: one held by the registry plus one per index over the field.
        """
        first = self.build_index(name="idx")
        first.create(self.client, wait_for_backfill=True)
        valid = self.write_key(first, "valid", VALID)
        self.wait_for_docs(first, 1)
        assert self.share_count(valid) == 2, "registry + one index"

        second = self.build_index(name="idx2")
        second.create(self.client, wait_for_backfill=True)
        waiters.wait_for_equal(lambda: self.share_count(valid), 3)

        second.drop(self.client)
        waiters.wait_for_equal(lambda: self.share_count(valid), 2)

        # Dropping the last index must also hand the field back to the engine
        # as a plain value.
        first.drop(self.client)
        waiters.wait_for_equal(lambda: self.share_count(valid), self.NOT_SHARED)
        assert self.client.hget(valid, "vec") == float_to_bytes(VALID)

        self.assert_registry_consistent()

        self.assert_survives_save_restore()

    def test_share_count_distinguishes_absent_from_unshared(self):
        """Pins the -1 / 0 boundary.

        The absent cases must report -1 rather than 0, so that an assertion of
        "exists but not shared" cannot be satisfied by a typo'd key or field
        name. Establishing existence is also a safety guard: the engine's
        HashHasStringRef must not be called for a field that does not exist.
        """
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)
        assert self.share_count(valid) == self.SHARED_BY_ONE_INDEX

        assert self.share_count(valid, "nosuchfield") == self.NO_SUCH_FIELD
        assert self.share_count("doc:nosuchkey") == self.NO_SUCH_FIELD
        self.client.rpush("alist", "x")
        assert self.share_count("alist") == self.NO_SUCH_FIELD
        self.client.delete(valid)
        assert self.share_count(valid) == self.NO_SUCH_FIELD
        assert self.client.ping()

        self.assert_registry_consistent()

        self.assert_survives_save_restore()


class TestVectorRegistryPersistence(ValkeySearchTestCaseDebugMode):
    """Save on one server, load on another.

    The lifecycle tests above reload in place with DEBUG RELOAD, which keeps
    the module's state across the load. These start a second process on the
    same RDB, which is what a restart actually does, and lets the sharing
    setting differ between the writer and the reader.
    """

    DIM = 8
    # Deliberately unnormalized, and spread out enough that the three metrics
    # order them differently.
    CORPUS = [[float((i * 7 + j * 3) % 11) + 0.5 for j in range(8)] for i in range(10)]
    QUERY = [1.5, 0.25, 3.0, 2.0, 0.5, 4.0, 1.0, 2.5]

    def _start(self, testdir: str, sharing: bool):
        """A server with an explicit sharing setting, rooted at `testdir`."""
        port = self.get_bind_port()
        os.makedirs(testdir, exist_ok=True)
        flag = "yes" if sharing else "no"
        lines = [
            "enable-debug-command yes",
            "hash-max-listpack-entries 0",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"dir {testdir}",
            f"loadmodule {os.getenv('MODULE_PATH')} --debug-mode yes "
            f"--info-developer-visible yes --enable-vector-sharing {flag}",
        ]
        conf_file = f"{testdir}/valkey_{port}.conf"
        with open(conf_file, "w+") as f:
            f.write("\n".join(lines) + "\n")
        logfile = f"{testdir}/logfile-{port}.log"
        server, client = self.create_server(
            testdir=testdir,
            server_path=os.getenv("VALKEY_SERVER_PATH"),
            # `args` are appended after the config file, so this is what
            # actually decides where the RDB is read from and written to.
            # `args` are appended after the config file, so these decide where
            # the RDB is read from and written to. Both must be pinned: the
            # framework otherwise picks a per-server directory and filename,
            # and the reader would never find what the writer saved.
            args={"logfile": logfile, "dir": testdir, "dbfilename": "dump.rdb"},
            port=port,
            conf_file=conf_file,
        )
        self.wait_for_logfile(logfile, "Ready to accept connections")
        client.ping()
        assert int(self._stat(client, "sharing_active")) == (1 if sharing else 0)
        return server, client

    @staticmethod
    def _stat(client, name: str) -> int:
        info = client.execute_command("INFO", "search")
        key = f"search_vector_registry_{name}"
        if isinstance(info, dict):
            return int(info[key])
        for line in info.decode().splitlines():
            if line.startswith(key + ":"):
                return int(line.split(":", 1)[1])
        raise KeyError(key)

    @staticmethod
    def _validate(client) -> list:
        return [
            x.decode()
            for x in client.execute_command(
                "FT._DEBUG", "VALIDATE_VECTOR_REGISTRY"
            )
        ]

    def _create_and_fill(self, client, metric: str):
        index = Index(
            "idx",
            [Vector("vec", self.DIM, type="HNSW", distance=metric)],
            prefixes=["doc"],
            type=KeyDataType.HASH,
        )
        index.create(client, wait_for_backfill=True)
        for i, vec in enumerate(self.CORPUS):
            client.hset(f"doc:{i}", "vec", float_to_bytes(vec))
        waiters.wait_for_equal(lambda: index.info(client).num_docs, len(self.CORPUS))
        return index

    def _knn(self, client) -> dict:
        """{key: distance} for the whole corpus, as the engine computed it."""
        reply = client.execute_command(
            "FT.SEARCH", "idx",
            f"*=>[KNN {len(self.CORPUS)} @vec $q AS score]",
            "PARAMS", "2", "q", float_to_bytes(self.QUERY),
            "RETURN", "1", "score", "DIALECT", "2",
        )
        out = {}
        for i in range(1, len(reply), 2):
            fields = reply[i + 1]
            score = dict(zip(fields[::2], fields[1::2]))[b"score"]
            out[reply[i].decode()] = float(score)
        assert len(out) == len(self.CORPUS), f"expected the whole corpus, got {out}"
        return out

    def _save_and_hand_off(self, client, dst_dir: str):
        """Persist, then place the RDB where the next server will look.

        The working directory is read back from the server rather than assumed:
        the test framework supplies its own `dir`, which overrides the config
        file.
        """
        client.execute_command("SAVE")
        cfg = client.execute_command("CONFIG", "GET", "dir")
        src_dir = (cfg[1].decode() if isinstance(cfg[1], bytes) else cfg[1])
        dbfile = client.execute_command("CONFIG", "GET", "dbfilename")
        name = (dbfile[1].decode() if isinstance(dbfile[1], bytes) else dbfile[1])
        os.makedirs(dst_dir, exist_ok=True)
        shutil.copyfile(f"{src_dir}/{name}", f"{dst_dir}/{name}")

    # ---- sharing setting differs between writer and reader ---------------

    @pytest.mark.parametrize(
        "writer_sharing,reader_sharing",
        [(True, False), (False, True)],
        ids=["written_shared_read_unshared", "written_unshared_read_shared"],
    )
    def test_rdb_moves_between_sharing_configurations(
        self, writer_sharing: bool, reader_sharing: bool
    ):
        """An RDB written by one configuration must load in the other.

        Sharing is a property of the running process, not of the data: the
        index section stores vectors inline and the keys carry their own copy,
        so neither side of the RDB should depend on whether the writer was
        sharing.
        """
        base = f"{LOGS_DIR}/{self.test_name}"
        writer_dir, reader_dir = f"{base}/writer", f"{base}/reader"

        server, client = self._start(writer_dir, writer_sharing)
        index = self._create_and_fill(client, "L2")
        expected = self._knn(client)
        assert self._stat(client, "entry_cnt") == len(self.CORPUS)
        self._save_and_hand_off(client, reader_dir)
        server.exit()

        server2, client2 = self._start(reader_dir, reader_sharing)
        assert index.info(client2).num_docs == len(self.CORPUS), (
            "the reader lost documents"
        )
        assert self._knn(client2) == expected, (
            "distances changed when the RDB moved between sharing settings"
        )
        assert self._stat(client2, "entry_cnt") == len(self.CORPUS)
        assert self._validate(client2) == []
        # The reader shares according to its own setting, not the writer's.
        shared = client2.execute_command(
            "FT._DEBUG", "VECTOR_SHARED_COUNT", "doc:0", "vec"
        )
        assert shared == (2 if reader_sharing else 0), (
            f"reader sharing={reader_sharing} but share count is {shared}"
        )
        server2.exit()

    # ---- distances survive persistence and a sharing toggle --------------

    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_distances_agree_across_restart_and_sharing_toggle(self, metric: str):
        """The same query must return the same distances in three places.

        Ten unnormalized vectors are indexed and queried; the corpus is then
        persisted and reloaded by a second process with sharing unchanged, and
        by a third with sharing flipped. COSINE is the interesting one, since
        the module normalizes internally and the index section stores the
        normalized form while the keys keep the raw bytes.
        """
        base = f"{LOGS_DIR}/{self.test_name}"
        first_dir = f"{base}/first"
        same_dir = f"{base}/same_sharing"
        toggled_dir = f"{base}/toggled_sharing"

        server, client = self._start(first_dir, True)
        self._create_and_fill(client, metric)
        first = self._knn(client)
        assert self._validate(client) == []
        self._save_and_hand_off(client, same_dir)
        self._save_and_hand_off(client, toggled_dir)
        server.exit()

        server2, client2 = self._start(same_dir, True)
        second = self._knn(client2)
        assert self._validate(client2) == []
        server2.exit()

        server3, client3 = self._start(toggled_dir, False)
        third = self._knn(client3)
        assert self._validate(client3) == []
        server3.exit()

        assert second == first, (
            f"{metric}: distances changed across save/restore\n"
            f"  before: {first}\n  after:  {second}"
        )
        assert third == first, (
            f"{metric}: distances changed when sharing was turned off\n"
            f"  sharing on:  {first}\n  sharing off: {third}"
        )
