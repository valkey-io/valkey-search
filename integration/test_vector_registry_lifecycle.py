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
import pytest
import valkey
from valkey import Valkey
from indexes import Index, KeyDataType, Numeric, Tag, Vector, float_to_bytes
from valkey_search_test_case import LOGS_DIR, ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401


DIM = 128
VALID = [float(i) for i in range(DIM)]
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

    def assert_sharing_active(self):
        """The feature is a no-op unless the engine exposes the sharing API and
        the option is on; assert it so a green run cannot mean "it was off"."""
        assert (
            self.registry_stat("sharing_active") == 1
        ), "vector sharing is not active; these tests would be vacuous"

    def save_flush_restore(self):
        """Persist, empty the keyspace, and load it back."""
        self.client.execute_command("SAVE")
        self.client.execute_command("DEBUG", "RELOAD")

    def assert_survives_save_restore(self, index=None):
        """A clean state must still be clean after a round trip through an RDB."""
        before = self.registry_stat("entry_cnt")
        self.save_flush_restore()
        if index:
            waiters.wait_for_equal(lambda: index.info(self.client).num_docs, before)
        assert (
            self.registry_stat("entry_cnt") == before
        ), "registry entry count changed across a save/restore cycle"

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

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    @pytest.mark.parametrize("mixed", [False, True], ids=["vector_only", "mixed"])
    def test_backfill_hash(self, algo: str, mixed: bool):
        """Backfill of a HASH index over all three vector-field states.

        With `mixed`, the key also carries numeric and tag fields, which must
        index for every key regardless of whether that key's vector is usable.
        """
        index = self.build_index(algo=algo, mixed=mixed)
        valid, invalid, missing = self.write_all_field_states(index, mixed)
        index.create(self.client, wait_for_backfill=True)

        assert self.registry_stat("entry_cnt") == 1
        assert self.registry_stat("shared_externally_cnt") >= 1
        assert self.knn_hits(index) == 1, "only the valid key indexes"
        assert index.info(self.client).hash_indexing_failures == 1
        if mixed:
            assert self.search_count(index, "@num:[5 20]") == 3
            assert self.search_count(index, "@tag:{a}") == 3

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    @pytest.mark.parametrize("mixed", [False, True], ids=["vector_only", "mixed"])
    def test_backfill_json(self, algo: str, mixed: bool):
        """Backfill of a JSON index over all three vector-field states.

        JSON vectors are registered and deduplicated in VectorRegistry,
        while engine hash sharing is skipped.
        """
        index = self.build_index(key_type=KeyDataType.JSON, algo=algo, mixed=mixed)
        self.write_all_field_states(index, mixed)
        index.create(self.client, wait_for_backfill=True)

        assert self.knn_hits(index) == 1, "only the valid document indexes"
        assert (
            self.registry_stat("entry_cnt") == 1
        ), "JSON valid vector is tracked and deduplicated in VectorRegistry"
        if mixed:
            assert self.search_count(index, "@num:[5 20]") == 3
            assert self.search_count(index, "@tag:{a}") == 3



    # ---- ingestion -------------------------------------------------------

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
                lambda: self.knn_hits(index), 0, timeout=10
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
        self.client.hset(valid, "vec", float_to_bytes(SHORT))
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0)

        assert (
            self.registry_stat("entry_cnt") == 0
        ), "registry still holds a vector for a key that is not indexed"

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

        self.client.hset(valid, "vec", float_to_bytes(SHORT))
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0)
        self.client.hset(valid, "vec", float_to_bytes(VALID))
        waiters.wait_for_equal(lambda: self.knn_hits(index), 1)

        assert self.registry_stat("entry_cnt") == 1

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_fm3_key_replaced_by_other_type_is_removed_from_index(self, algo: str):
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
        index = self.build_index(algo=algo)
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

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_fm4_identical_rewrite_keeps_sharing(self, algo: str):
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
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "valid", VALID)
        self.wait_for_docs(index, 1)
        assert self.registry_stat("entry_cnt") == 1

        hits_before = self.registry_stat("shared_externally_cnt")
        self.client.hset(valid, "vec", float_to_bytes(VALID))  # identical
        waiters.wait_for_equal(
            lambda: self.registry_stat("shared_externally_cnt") > hits_before, True
        )
        assert self.registry_stat("entry_cnt") == 1

    # ---- RDB load with sharing on ---------------------------------------

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_json_vectors_registered_and_retained_across_reload(self, algo: str):
        """JSON vectors are registered and deduplicated in VectorRegistry,
        and their registration is preserved across RDB reload without engine sharing.
        """
        index = self.build_index(key_type=KeyDataType.JSON, algo=algo, mixed=True)
        index.create(self.client, wait_for_backfill=True)
        self.write_all_field_states(index, mixed=True)
        self.wait_for_docs(index, 3)
        assert self.registry_stat("entry_cnt") == 1

        self.client.execute_command("DEBUG", "RELOAD")
        waiters.wait_for_equal(
            lambda: index.info(self.client).num_docs, 3
        )

        # The index survives the reload intact and stays registered.
        assert self.knn_hits(index) == 1
        assert self.search_count(index, "@num:[5 20]") == 3
        assert self.search_count(index, "@tag:{a}") == 3
        assert (
            self.registry_stat("entry_cnt") == 1
        ), "RDB load registered JSON vector safely in VectorRegistry"

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_fm6_rdb_reload_reshares_hash_vectors(self, algo: str):
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
        index = self.build_index(algo=algo, mixed=True)
        index.create(self.client, wait_for_backfill=True)
        valid, invalid, missing = self.write_all_field_states(index, mixed=True)
        self.wait_for_docs(index, 3)
        assert self.registry_stat("entry_cnt") == 1
        shares_before = self.registry_stat("shared_externally_cnt")

        self.client.execute_command("DEBUG", "RELOAD")
        waiters.wait_for_equal(lambda: index.info(self.client).num_docs, 3)

        # The index survives the reload intact, including the non-vector
        # fields and the keys whose vector was unusable or absent.
        assert self.knn_hits(index) == 1
        assert self.search_count(index, "@num:[5 20]") == 3
        assert self.search_count(index, "@tag:{a}") == 3
        assert self.registry_stat("entry_cnt") == 1

        assert (
            self.registry_stat("shared_externally_cnt") > shares_before
        ), "no share was attempted for the restored entry"

    # ---- multi-index tracking --------------------------------------------

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_index_tracking_shares_registry_entry(self, algo: str):
        """When multiple indexes track the same field, the registry entry is shared."""
        first = self.build_index(name="idx", algo=algo)
        first.create(self.client, wait_for_backfill=True)
        valid = self.write_key(first, "valid", VALID)
        self.wait_for_docs(first, 1)
        assert self.registry_stat("entry_cnt") == 1

        second = self.build_index(name="idx2", algo=algo)
        second.create(self.client, wait_for_backfill=True)
        self.wait_for_docs(second, 1)
        assert self.knn_hits(second) == 1
        assert self.registry_stat("entry_cnt") == 1

        second.drop(self.client)
        assert self.registry_stat("entry_cnt") == 1

        self.client.delete(valid)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        first.drop(self.client)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_fm7_rdb_load_after_a_type_change_must_not_crash(self, algo: str):
        """FM-7: an RDB load aborts the server if any tracked key is no longer
        readable as the schema's type.
        """
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        key = self.write_key(index, "1", VALID)
        self.wait_for_docs(index, 1)

        self.client.rpush("src", "a", "b")
        self.client.rename("src", key)  # the key is a LIST now
        waiters.wait_for_equal(lambda: self.client.type(key), b"list")

        try:
            self.client.execute_command("DEBUG", "RELOAD")
            alive = self.client.ping()
        except valkey.exceptions.ConnectionError as e:
            pytest.fail(f"server died loading an RDB after a type change: {e}")
        assert alive, "server did not survive the reload"

    # ---- whole-keyspace operations ---------------------------------------

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_exec_batch_leaves_registry_consistent(self, algo: str):
        """Mutations issued inside MULTI/EXEC are queued and drained lazily."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)

        pipe = self.client.pipeline(transaction=True)
        for i in range(20):
            pipe.hset(f"doc:{i}", "vec", float_to_bytes(VALID))
        pipe.execute()
        self.wait_for_docs(index, 20)
        assert self.registry_stat("entry_cnt") == 20

        # A second transaction that rewrites some keys with a different vector
        # and deletes others.
        OTHER = [float(i + 1000) for i in range(DIM)]
        pipe = self.client.pipeline(transaction=True)
        for i in range(0, 10):
            pipe.hset(f"doc:{i}", "vec", float_to_bytes(OTHER))
        for i in range(10, 20):
            pipe.delete(f"doc:{i}")
        pipe.execute()
        self.wait_for_docs(index, 10)
        assert self.registry_stat("entry_cnt") == 10

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_flushdb_clears_the_registry(self, algo: str):
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        for i in range(10):
            self.write_key(index, str(i), VALID)
        self.wait_for_docs(index, 10)
        assert self.registry_stat("entry_cnt") == 10

        self.client.flushdb()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        assert self.client.dbsize() == 0

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_flushdb_preserves_other_database_entries(self, algo: str):
        """Verify that FLUSHDB on db 0 does not erase registry entries in db 1."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx1 = self.build_index(name="idx_db1", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)
        idx1.create(client_db1, wait_for_backfill=True)

        for i in range(5):
            self.write_key(idx0, str(i), VALID)
            client_db1.hset(f"doc:{i}", mapping={"vec": float_to_bytes(VALID)})

        self.wait_for_docs(idx0, 5)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, 5)
        assert self.registry_stat("entry_cnt") == 10

        # Flush DB 0 only
        client_db0.flushdb()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 5)
        res1 = client_db1.execute_command(
            "FT.SEARCH", idx1.name, "*=>[KNN 100 @vec $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2"
        )
        assert res1[0] == 5

        # Flush DB 1
        client_db1.flushdb()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_flushall_clears_the_registry(self, algo: str):
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        for i in range(10):
            self.write_key(index, str(i), VALID)
        self.wait_for_docs(index, 10)
        assert self.registry_stat("entry_cnt") == 10

        self.client.flushall()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_swapdb_moves_keys_out_from_under_the_index(self, algo: str):
        """SWAPDB exchanges two keyspaces without touching any key."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        for i in range(5):
            self.write_key(index, str(i), VALID)
        self.wait_for_docs(index, 5)
        assert self.registry_stat("entry_cnt") == 5

        self.client.swapdb(0, 1)
        waiters.wait_for_equal(lambda: self.client.dbsize(), 0)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_rename_indexed_key_to_a_new_name(self, algo: str):
        """RENAME an indexed key to another name within the index's prefix."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        old = self.write_key(index, "old", VALID)
        self.wait_for_docs(index, 1)
        assert self.registry_stat("entry_cnt") == 1

        self.client.rename(old, "doc:new")
        waiters.wait_for_equal(lambda: self.client.exists(old), 0)
        waiters.wait_for_equal(lambda: self.client.exists("doc:new"), 1)
        waiters.wait_for_equal(lambda: self.knn_hits(index), 1)
        assert self.registry_stat("entry_cnt") == 1
        assert self.client.hget("doc:new", "vec") == float_to_bytes(VALID)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_rename_indexed_key_out_of_the_prefix(self, algo: str):
        """RENAME an indexed key to a name the index does not cover."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        old = self.write_key(index, "old", VALID)
        self.wait_for_docs(index, 1)

        self.client.rename(old, "outside:1")
        waiters.wait_for_equal(lambda: self.client.exists(old), 0)
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        assert self.client.hget("outside:1", "vec") == float_to_bytes(VALID)

    # ---- cross-database move ---------------------------------------------

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_move_key_between_compatible_indexes_across_databases(self, algo: str):
        """MOVE an indexed key to another DB where a matching schema indexes it."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx1 = self.build_index(name="idx_db1", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)
        idx1.create(client_db1, wait_for_backfill=True)

        key = self.write_key(idx0, "1", VALID)
        self.wait_for_docs(idx0, 1)
        assert self.registry_stat("entry_cnt") == 1
        assert self.knn_hits(idx0) == 1

        # Move key from DB 0 to DB 1
        assert client_db0.move(key, 1) == 1

        # DB 0: key no longer exists and index drops it
        waiters.wait_for_equal(lambda: client_db0.exists(key), 0)
        waiters.wait_for_equal(lambda: self.knn_hits(idx0), 0)

        # DB 1: key exists, indexed, and searchable
        waiters.wait_for_equal(lambda: client_db1.exists(key), 1)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, 1)
        res1 = client_db1.execute_command(
            "FT.SEARCH", idx1.name, "*=>[KNN 100 @vec $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2"
        )
        assert res1[0] == 1
        assert res1[1].decode() == key

        # Vector registry entry count stays 1 (migrated from DB 0 to DB 1)
        assert self.registry_stat("entry_cnt") == 1
        assert client_db1.hget(key, "vec") == float_to_bytes(VALID)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_move_key_to_database_without_index_unshares_vector(self, algo: str):
        """MOVE an indexed key to a DB with no vector index unshares the vector safely."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)

        key = self.write_key(idx0, "1", VALID)
        self.wait_for_docs(idx0, 1)
        assert self.registry_stat("entry_cnt") == 1

        # Move key to DB 1 (which has no index)
        assert client_db0.move(key, 1) == 1

        waiters.wait_for_equal(lambda: client_db0.exists(key), 0)
        waiters.wait_for_equal(lambda: self.knn_hits(idx0), 0)

        # Vector is unshared in DB 1, so registry drops to 0
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        waiters.wait_for_equal(lambda: client_db1.exists(key), 1)

        # In DB 1, the vector field is still valid and intact (materialized as standard string)
        assert client_db1.hget(key, "vec") == float_to_bytes(VALID)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_move_key_to_database_with_incompatible_dimensions(self, algo: str):
        """MOVE an indexed key to a DB whose schema expects different dimensions."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        # idx0 expects DIM 128
        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)

        # idx1 in DB 1 expects DIM 256
        client_db1.execute_command(
            "FT.CREATE", "idx_db1", "ON", "HASH", "PREFIX", "1", "doc",
            "SCHEMA", "vec", "VECTOR", algo, "6",
            "TYPE", "FLOAT32", "DIM", "256", "DISTANCE_METRIC", "L2",
        )

        key = self.write_key(idx0, "1", VALID)
        self.wait_for_docs(idx0, 1)
        assert self.registry_stat("entry_cnt") == 1

        # Move key (128-dim vector) to DB 1
        assert client_db0.move(key, 1) == 1

        waiters.wait_for_equal(lambda: client_db0.exists(key), 0)
        waiters.wait_for_equal(lambda: self.knn_hits(idx0), 0)

        # DB 1 schema rejects the 128-dim vector, so vector is unshared
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        waiters.wait_for_equal(lambda: client_db1.exists(key), 1)
        assert client_db1.hget(key, "vec") == float_to_bytes(VALID)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_move_key_round_trip_across_databases(self, algo: str):
        """MOVE a key from DB 0 to DB 1 and back to DB 0."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx1 = self.build_index(name="idx_db1", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)
        idx1.create(client_db1, wait_for_backfill=True)

        key = self.write_key(idx0, "1", VALID)
        self.wait_for_docs(idx0, 1)
        assert self.registry_stat("entry_cnt") == 1

        # Move 0 -> 1
        assert client_db0.move(key, 1) == 1
        waiters.wait_for_equal(lambda: client_db0.exists(key), 0)
        waiters.wait_for_equal(lambda: client_db1.exists(key), 1)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, 1)
        assert self.registry_stat("entry_cnt") == 1

        # Move 1 -> 0
        assert client_db1.move(key, 0) == 1
        waiters.wait_for_equal(lambda: client_db1.exists(key), 0)
        waiters.wait_for_equal(lambda: client_db0.exists(key), 1)
        waiters.wait_for_equal(lambda: self.knn_hits(idx0), 1)
        assert self.registry_stat("entry_cnt") == 1

    # ---- advanced rename transitions -------------------------------------

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_rename_overwriting_another_indexed_key(self, algo: str):
        """RENAME an indexed key onto an already existing indexed key."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)

        OTHER = [float(i + 100) for i in range(DIM)]
        src = self.write_key(index, "src", VALID)
        dst = self.write_key(index, "dst", OTHER)
        self.wait_for_docs(index, 2)
        assert self.registry_stat("entry_cnt") == 2

        # Rename src onto dst (atomically overwrites dst)
        self.client.rename(src, dst)
        waiters.wait_for_equal(lambda: self.client.exists(src), 0)
        waiters.wait_for_equal(lambda: self.client.exists(dst), 1)
        waiters.wait_for_equal(lambda: index.info(self.client).num_docs, 1)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1)

        # dst now holds VALID vector, not OTHER
        assert self.client.hget(dst, "vec") == float_to_bytes(VALID)
        res = self.client.execute_command(
            "FT.SEARCH", index.name, "*=>[KNN 1 @vec $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2"
        )
        assert res[0] == 1
        assert res[1].decode() == dst

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_rename_unindexed_hash_over_indexed_key(self, algo: str):
        """RENAME an unindexed hash over an indexed key untracks the old vector."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)

        indexed = self.write_key(index, "target", VALID)
        self.wait_for_docs(index, 1)
        assert self.registry_stat("entry_cnt") == 1

        # Create an unindexed hash
        self.client.hset("outside:src", "other", "data")

        self.client.rename("outside:src", indexed)
        waiters.wait_for_equal(lambda: self.client.exists("outside:src"), 0)
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_rename_between_schemas_with_different_attribute_names(self, algo: str):
        """RENAME a key to a prefix whose schema indexes a different vector attribute."""
        self.client.execute_command(
            "FT.CREATE", "idx_user", "ON", "HASH", "PREFIX", "1", "user",
            "SCHEMA", "user_vec", "VECTOR", algo, "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        self.client.execute_command(
            "FT.CREATE", "idx_item", "ON", "HASH", "PREFIX", "1", "item",
            "SCHEMA", "item_vec", "VECTOR", algo, "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )

        self.client.hset("user:1", "user_vec", float_to_bytes(VALID))
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1)

        # Rename user:1 -> item:1. item:1 matches idx_item prefix, but has user_vec (not item_vec).
        self.client.rename("user:1", "item:1")
        waiters.wait_for_equal(lambda: self.client.exists("user:1"), 0)
        waiters.wait_for_equal(lambda: self.client.exists("item:1"), 1)

        # idx_item does not index user_vec, so vector is unshared and registry drops to 0
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        assert self.client.hget("item:1", "user_vec") == float_to_bytes(VALID)

        self.client.execute_command("FT.DROPINDEX", "idx_user")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        self.client.execute_command("FT.DROPINDEX", "idx_item")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)

    def test_rename_multi_vector_document(self):
        """RENAME a document holding multiple vectors within and outside the index prefix."""
        self.assert_sharing_active()
        index = Index(
            "multi_vec_rename",
            [
                Vector("vec_a", DIM, type="HNSW", distance="L2"),
                Vector("vec_b", DIM, type="HNSW", distance="L2"),
            ],
            prefixes=["doc"],
            type=KeyDataType.HASH,
        )
        index.create(self.client, wait_for_backfill=True)

        OTHER = [float(i + 10) for i in range(DIM)]
        self.client.hset(
            "doc:multi1",
            mapping={
                "vec_a": float_to_bytes(VALID),
                "vec_b": float_to_bytes(OTHER),
            },
        )
        self.wait_for_docs(index, 1)
        assert self.registry_stat("entry_cnt") == 2

        # Rename within prefix
        self.client.rename("doc:multi1", "doc:multi2")
        waiters.wait_for_equal(lambda: self.client.exists("doc:multi1"), 0)
        waiters.wait_for_equal(lambda: self.client.exists("doc:multi2"), 1)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 2)

        # Both vectors are searchable under doc:multi2
        res_a = self.client.execute_command(
            "FT.SEARCH", index.name, "*=>[KNN 1 @vec_a $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2"
        )
        assert res_a[0] == 1
        assert res_a[1].decode() == "doc:multi2"

        res_b = self.client.execute_command(
            "FT.SEARCH", index.name, "*=>[KNN 1 @vec_b $q]",
            "PARAMS", "2", "q", float_to_bytes(OTHER), "DIALECT", "2"
        )
        assert res_b[0] == 1
        assert res_b[1].decode() == "doc:multi2"

        # Rename outside prefix
        self.client.rename("doc:multi2", "outside:multi")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        assert self.client.hget("outside:multi", "vec_a") == float_to_bytes(VALID)
        assert self.client.hget("outside:multi", "vec_b") == float_to_bytes(OTHER)

        index.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_rdb_reload_after_cross_database_move(self, algo: str):
        """Verify that keys moved across databases survive RDB reload cleanly."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx1 = self.build_index(name="idx_db1", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)
        idx1.create(client_db1, wait_for_backfill=True)

        for i in range(5):
            self.write_key(idx0, str(i), VALID)
        self.wait_for_docs(idx0, 5)

        # Move 2 keys to DB 1
        assert client_db0.move("doc:0", 1) == 1
        assert client_db0.move("doc:1", 1) == 1

        waiters.wait_for_equal(lambda: idx0.info(client_db0).num_docs, 3)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, 2)
        assert self.registry_stat("entry_cnt") == 5

        # Reload RDB
        self.client.execute_command("SAVE")
        self.client.execute_command("DEBUG", "RELOAD")

        # Reconnect client_db1 after reload
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        waiters.wait_for_equal(lambda: idx0.info(self.client).num_docs, 3)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, 2)
        assert self.registry_stat("entry_cnt") == 5
        assert self.knn_hits(idx0) == 3
        res1 = client_db1.execute_command(
            "FT.SEARCH", idx1.name, "*=>[KNN 100 @vec $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2"
        )
        assert res1[0] == 2

    # ---- one field, two indexes, only one of them holding the key --------

    def _create_skipscan_index(self, name: str, algo: str = "HNSW") -> Index:
        """An index over the same prefix and field, created without scanning."""
        self.client.execute_command(
            "FT.CREATE", name, "ON", "HASH", "PREFIX", "1", "doc",
            "SKIPINITIALSCAN",
            "SCHEMA", "vec", "VECTOR", algo, "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        )
        return Index(
            name,
            [Vector("vec", DIM, type=algo, distance="L2")],
            prefixes=["doc"],
            type=KeyDataType.HASH,
        )

    def _one_index_ahead(self, algo: str = "HNSW"):
        """Index `a` holds the key; index `b` covers the same field but was
        created with SKIPINITIALSCAN, so it never saw the key."""
        first = self.build_index(name="a", algo=algo)
        first.create(self.client, wait_for_backfill=True)
        key = self.write_key(first, "1", VALID)
        self.wait_for_docs(first, 1)
        assert self.registry_stat("entry_cnt") == 1

        second = self._create_skipscan_index("b", algo=algo)
        assert second.info(self.client).num_docs == 0, "b should not have scanned"
        assert self.registry_stat("entry_cnt") == 1
        return first, second, key

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    @pytest.mark.parametrize(
        "operation",
        ["same", "different", "invalid", "missing", "delete", "reload"],
    )
    def test_two_indexes_one_skipped_the_key(self, algo: str, operation: str):
        """Apply one operation while the two indexes disagree about the key."""
        first, second, key = self._one_index_ahead(algo=algo)

        if operation == "same":
            self.client.hset(key, "vec", float_to_bytes(VALID))
        elif operation == "different":
            OTHER = [float(i + 1000) for i in range(DIM)]
            self.client.hset(key, "vec", float_to_bytes(OTHER))
        elif operation == "invalid":
            self.client.hset(key, "vec", float_to_bytes(SHORT))
        elif operation == "missing":
            self.client.hdel(key, "vec")
        elif operation == "delete":
            self.client.delete(key)
        elif operation == "reload":
            self.client.execute_command("DEBUG", "RELOAD")

        if operation in ("same", "different"):
            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1)
            waiters.wait_for_equal(lambda: self.knn_hits(first), 1)
            waiters.wait_for_equal(lambda: self.knn_hits(second), 1)
        elif operation == "reload":
            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1)
            waiters.wait_for_equal(lambda: self.knn_hits(first), 1)
            waiters.wait_for_equal(lambda: self.knn_hits(second), 0)
        else:
            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
            waiters.wait_for_equal(lambda: self.knn_hits(first), 0)
            waiters.wait_for_equal(lambda: self.knn_hits(second), 0)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_two_indexes_one_skipped_the_key_drop_the_holder(self, algo: str):
        """Dropping the index that holds the key leaves nothing referencing it."""
        first, second, key = self._one_index_ahead(algo=algo)
        first.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0)
        assert self.knn_hits(second) == 0

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_slot_reuse_and_leak_detection(self, algo: str):
        """Verify that soft-deleted slots and overwritten vectors recycle properly
        without memory leaks or registry corruption."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)

        num_initial = 20
        for i in range(num_initial):
            vec = [float(i + j) for j in range(DIM)]
            self.client.hset(f"doc:{i}", "vec", float_to_bytes(vec))
        self.wait_for_docs(index, num_initial)
        assert self.registry_stat("entry_cnt") == num_initial

        # Soft delete half the vectors (HDEL)
        for i in range(num_initial // 2):
            self.client.hdel(f"doc:{i}", "vec")
        waiters.wait_for_equal(lambda: index.info(self.client).num_docs, num_initial // 2)
        assert self.registry_stat("entry_cnt") == num_initial // 2

        # Ingest new vectors to trigger slot recycling
        for i in range(num_initial // 2):
            vec = [float(i + j + 500) for j in range(DIM)]
            self.client.hset(f"doc:new_{i}", "vec", float_to_bytes(vec))
        self.wait_for_docs(index, num_initial)
        assert self.registry_stat("entry_cnt") == num_initial

        # Verify that all live documents return correct KNN hits and deleted documents are gone
        for i in range(num_initial // 2):
            query = [float(i + j) for j in range(DIM)]
            res = self.client.execute_command(
                "FT.SEARCH", index.name, "*=>[KNN 1 @vec $q]",
                "PARAMS", "2", "q", float_to_bytes(query), "DIALECT", "2",
            )
            if res[0] > 0:
                assert res[1] != f"doc:{i}"

    def test_multi_vector_fields_lifecycle(self):
        """Verify lifecycle and isolation when a schema contains multiple vector attributes."""
        self.assert_sharing_active()
        index = Index(
            "multi_vec_idx",
            [
                Vector("text_vec", DIM, type="HNSW", distance="L2"),
                Vector("img_vec", DIM, type="HNSW", distance="L2"),
            ],
            prefixes=["doc"],
            type=KeyDataType.HASH,
        )
        index.create(self.client, wait_for_backfill=True)

        # Ingest a document with both vectors valid
        key = "doc:multi"
        self.client.hset(
            key,
            mapping={
                "text_vec": float_to_bytes(VALID),
                "img_vec": float_to_bytes(VALID),
            },
        )
        self.wait_for_docs(index, 1)
        assert self.registry_stat("entry_cnt") == 2
        assert self.registry_stat("shared_externally_cnt") >= 2

        # Overwrite only text_vec with an invalid (wrong dimension) payload
        self.client.hset(key, "text_vec", float_to_bytes(SHORT))
        # Wait for worker thread to process and drop invalid vector
        waiters.wait_for_equal(
            lambda: self.client.execute_command(
                "FT.SEARCH", index.name, "*=>[KNN 1 @text_vec $q]",
                "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2",
            )[0],
            0,
            timeout=10,
        )
        # Verify img_vec remains intact, searchable, and shared
        res_img = self.client.execute_command(
            "FT.SEARCH", index.name, "*=>[KNN 1 @img_vec $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2",
        )
        assert res_img[0] == 1
        assert res_img[1].decode() == key

        # Delete key and verify both entries are untracked
        self.client.delete(key)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["FLAT", "HNSW"])
    def test_ttl_expiration_untracks_shared_vector(self, algo: str):
        """Verify that TTL key expiration unregisters shared vectors without crashing."""
        index = self.build_index(algo=algo)
        index.create(self.client, wait_for_backfill=True)
        valid = self.write_key(index, "expiring", VALID)
        self.wait_for_docs(index, 1)
        assert self.registry_stat("entry_cnt") == 1

        # Set 50ms TTL
        self.client.pexpire(valid, 50)
        waiters.wait_for_equal(lambda: self.client.exists(valid), 0, timeout=10)
        waiters.wait_for_equal(lambda: self.knn_hits(index), 0, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        assert self.client.ping(), "server died on key expiration"

    def test_restore_replace_and_copy_replace_untracks_vector(self):
        """Verify that RESTORE ... REPLACE and COPY ... REPLACE untrack old vectors."""
        index = self.build_index()
        index.create(self.client, wait_for_backfill=True)
        doc1 = self.write_key(index, "1", VALID)
        doc2 = self.write_key(index, "2", VALID)
        self.wait_for_docs(index, 2)
        assert self.registry_stat("entry_cnt") == 2

        # Prepare a string key dump
        self.client.set("str_source", "plain string value")
        dump = self.client.dump("str_source")

        # RESTORE REPLACE over doc:1
        self.client.restore(doc1, 0, dump, replace=True)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # COPY REPLACE over doc:2
        self.client.copy("str_source", doc2, replace=True)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_drop_index_partial_prefix_overlap(self, algo: str):
        """Verify that dropping a broad prefix index unshares only keys that do not
        match a narrower surviving index."""
        idx_broad = self.build_index(name="idx_broad", algo=algo)
        idx_narrow = Index(
            "idx_narrow",
            [Vector("vec", DIM, type=algo, distance="L2")],
            prefixes=["doc:sub:"],
            type=KeyDataType.HASH,
        )
        idx_broad.create(self.client, wait_for_backfill=True)
        idx_narrow.create(self.client, wait_for_backfill=True)

        self.client.hset("doc:1", mapping={"vec": float_to_bytes(VALID)})
        self.client.hset("doc:sub:1", mapping={"vec": float_to_bytes(VALID)})

        self.wait_for_docs(idx_broad, 2)
        self.wait_for_docs(idx_narrow, 1)
        assert self.registry_stat("entry_cnt") == 2

        # Drop the broad index. 'doc:1' has no surviving index and is unshared/erased.
        # 'doc:sub:1' still matches idx_narrow and must remain tracked.
        idx_broad.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        self.wait_for_docs(idx_narrow, 1)
        assert self.knn_hits(idx_narrow) == 1
        assert self.client.hget("doc:1", "vec") == float_to_bytes(VALID)
        assert self.client.hget("doc:sub:1", "vec") == float_to_bytes(VALID)

        # Drop the narrow index. Now 'doc:sub:1' has no surviving index and is unshared/erased.
        idx_narrow.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        assert self.client.hget("doc:sub:1", "vec") == float_to_bytes(VALID)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_drop_index_multi_vector_field_isolation(self, algo: str):
        """Verify that dropping an index for one vector attribute identifier unshares
        only that field and leaves other vector fields on the same document intact."""
        idx_a = Index(
            "idx_field_a",
            [Vector("vec_a", DIM, type=algo, distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        idx_b = Index(
            "idx_field_b",
            [Vector("vec_b", DIM, type=algo, distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        idx_a.create(self.client, wait_for_backfill=True)
        idx_b.create(self.client, wait_for_backfill=True)

        OTHER = [float(i + 500) for i in range(DIM)]
        self.client.hset("doc:1", mapping={"vec_a": float_to_bytes(VALID), "vec_b": float_to_bytes(OTHER)})
        self.wait_for_docs(idx_a, 1)
        self.wait_for_docs(idx_b, 1)
        assert self.registry_stat("entry_cnt") == 2

        # Drop idx_a. Only vec_a should be erased from registry.
        idx_a.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        self.wait_for_docs(idx_b, 1)
        assert self.client.execute_command(
            "FT.SEARCH", idx_b.name, "*=>[KNN 1 @vec_b $q]",
            "PARAMS", "2", "q", float_to_bytes(OTHER), "DIALECT", "2"
        )[0] == 1
        assert self.client.hget("doc:1", "vec_a") == float_to_bytes(VALID)
        assert self.client.hget("doc:1", "vec_b") == float_to_bytes(OTHER)

        # Drop idx_b. Remaining entry should be erased.
        idx_b.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        assert self.client.hget("doc:1", "vec_b") == float_to_bytes(OTHER)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_drop_index_different_dimensions_discrimination(self, algo: str):
        """Verify that two indexes on the same prefix and field name but different dimensions
        discriminate properly when one index is dropped."""
        import struct
        idx_64 = Index(
            "idx_dim_64",
            [Vector("vec", 64, type=algo, distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        idx_128 = Index(
            "idx_dim_128",
            [Vector("vec", 128, type=algo, distance="L2")],
            prefixes=["doc:"],
            type=KeyDataType.HASH,
        )
        idx_64.create(self.client, wait_for_backfill=True)
        idx_128.create(self.client, wait_for_backfill=True)

        vec64 = struct.pack("64f", *[0.1] * 64)
        vec128 = struct.pack("128f", *[0.2] * 128)
        self.client.hset("doc:64", mapping={"vec": vec64})
        self.client.hset("doc:128", mapping={"vec": vec128})

        self.wait_for_docs(idx_64, 2)
        self.wait_for_docs(idx_128, 2)
        assert self.knn_hits(idx_64, query=[0.1] * 64) == 1
        assert self.knn_hits(idx_128, query=[0.2] * 128) == 1
        assert self.registry_stat("entry_cnt") == 2

        # Drop idx_64. 'doc:64' is unshared and erased because idx_128 has dimension 128 != 64.
        idx_64.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        assert self.knn_hits(idx_128, query=[0.2] * 128) == 1
        assert self.client.hget("doc:64", "vec") == vec64
        assert self.client.hget("doc:128", "vec") == vec128

        # Drop idx_128. 'doc:128' is unshared and erased.
        idx_128.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        assert self.client.hget("doc:128", "vec") == vec128

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_drop_index_cross_database_isolation(self, algo: str):
        """Verify that dropping an index in DB 0 does not erase or unshare matching
        entries in DB 1."""
        client_db0 = self.client
        client_db1 = self.server.get_new_client()
        client_db1.select(1)

        idx0 = self.build_index(name="idx_db0", algo=algo)
        idx1 = self.build_index(name="idx_db1", algo=algo)
        idx0.create(client_db0, wait_for_backfill=True)
        idx1.create(client_db1, wait_for_backfill=True)

        count = 10
        for i in range(count):
            vec_bytes = float_to_bytes([float(i + j) for j in range(DIM)])
            client_db0.hset(f"doc:{i}", mapping={"vec": vec_bytes})
            client_db1.hset(f"doc:{i}", mapping={"vec": vec_bytes})

        self.wait_for_docs(idx0, count)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, count)
        assert self.registry_stat("entry_cnt") == count * 2

        # Drop idx0 in DB 0. DB 0 keys must be unshared/erased, but DB 1 keys must remain tracked.
        idx0.drop(client_db0)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), count, timeout=10)
        waiters.wait_for_equal(lambda: idx1.info(client_db1).num_docs, count)
        assert client_db1.execute_command(
            "FT.SEARCH", idx1.name, "*=>[KNN 100 @vec $q]",
            "PARAMS", "2", "q", float_to_bytes(VALID), "DIALECT", "2",
        )[0] > 0

        # Drop idx1 in DB 1. All remaining entries must be unshared/erased.
        idx1.drop(client_db1)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_index_sharing_and_drop_one_by_one(self, algo: str):
        """Verify that multiple indexes tracking the same vector field share entries
        and dropping indexes sequentially cleans up only on the last drop."""
        idx_a = self.build_index(name="idx_a", algo=algo)
        idx_b = self.build_index(name="idx_b", algo=algo)
        idx_a.create(self.client, wait_for_backfill=True)
        idx_b.create(self.client, wait_for_backfill=True)

        count = 20
        for i in range(count):
            self.write_key(idx_a, str(i), [float(i + j) for j in range(DIM)])

        self.wait_for_docs(idx_a, count)
        self.wait_for_docs(idx_b, count)
        assert self.registry_stat("entry_cnt") == count

        # Drop the first index; second index is still referencing all entries
        idx_a.drop(self.client)
        # Entry count in registry must remain intact because idx_b is still active
        assert self.registry_stat("entry_cnt") == count
        self.wait_for_docs(idx_b, count)
        assert self.knn_hits(idx_b) > 0

        # Drop the second index
        idx_b.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_index_flushdb_cleanup(self, algo: str):
        """Verify that FLUSHDB completely cleans up registry entries across multiple indexes."""
        idx_a = self.build_index(name="flush_a", algo=algo)
        idx_b = self.build_index(name="flush_b", algo=algo)
        idx_a.create(self.client, wait_for_backfill=True)
        idx_b.create(self.client, wait_for_backfill=True)

        count = 15
        for i in range(count):
            self.write_key(idx_a, str(i), [float(i + j) for j in range(DIM)])

        self.wait_for_docs(idx_a, count)
        self.wait_for_docs(idx_b, count)
        assert self.registry_stat("entry_cnt") == count

        self.client.flushdb()
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        assert len(self.client.execute_command("FT._LIST")) == 0

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_index_concurrent_ingest_and_drop(self, algo: str):
        """Verify thread safety during concurrent ingestion while dropping an index."""
        import threading

        idx_a = self.build_index(name="conc_a", algo=algo)
        idx_b = self.build_index(name="conc_b", algo=algo)
        idx_a.create(self.client, wait_for_backfill=True)
        idx_b.create(self.client, wait_for_backfill=True)

        errors = []

        def worker_ingest():
            try:
                for i in range(50):
                    vec = [float(i + j) for j in range(DIM)]
                    self.client.hset(f"doc:conc_{i}", "vec", float_to_bytes(vec))
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=worker_ingest)
        t.start()

        # Concurrently drop index A while worker is writing
        idx_a.drop(self.client)

        t.join()
        assert not errors, f"Ingestion worker encountered errors: {errors}"

        # Wait for index B to finish indexing all 50 keys
        self.wait_for_docs(idx_b, 50)
        assert self.registry_stat("entry_cnt") == 50

        # Drop index B
        idx_b.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_index_json_sharing_and_drop_one_by_one(self, algo: str):
        """Verify that multiple JSON indexes tracking the same vector field share entries
        and dropping indexes sequentially cleans up only on the last drop."""
        idx_a = self.build_index(name="json_a", key_type=KeyDataType.JSON, algo=algo)
        idx_b = self.build_index(name="json_b", key_type=KeyDataType.JSON, algo=algo)
        idx_a.create(self.client, wait_for_backfill=True)
        idx_b.create(self.client, wait_for_backfill=True)

        initial_hits = self.registry_stat("shared_externally_cnt")
        count = 10
        for i in range(count):
            self.write_key(idx_a, str(i), [float(i + j) for j in range(DIM)])

        self.wait_for_docs(idx_a, count)
        self.wait_for_docs(idx_b, count)
        assert self.registry_stat("entry_cnt") == count
        # Engine hash sharing must not be invoked for JSON
        assert self.registry_stat("shared_externally_cnt") == initial_hits

        # Drop first index; second index still active
        idx_a.drop(self.client)
        assert self.registry_stat("entry_cnt") == count
        self.wait_for_docs(idx_b, count)
        assert self.knn_hits(idx_b) > 0

        # Drop second index
        idx_b.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_multi_index_payload_modification_lifecycle(self, algo: str):
        """Verify that updating a vector payload preserves tracking across multiple schemas."""
        idx_a = self.build_index(name="mod_a", algo=algo)
        idx_b = self.build_index(name="mod_b", algo=algo)
        idx_a.create(self.client, wait_for_backfill=True)
        idx_b.create(self.client, wait_for_backfill=True)

        count = 10
        for i in range(count):
            self.write_key(idx_a, str(i), [float(i + j) for j in range(DIM)])
        self.wait_for_docs(idx_a, count)
        self.wait_for_docs(idx_b, count)
        assert self.registry_stat("entry_cnt") == count

        # Overwrite payload with new vector values
        for i in range(count):
            self.write_key(idx_a, str(i), [float((i + j) * 2) for j in range(DIM)])

        # Drop index A; index B must still retain all entries
        idx_a.drop(self.client)
        assert self.registry_stat("entry_cnt") == count
        self.wait_for_docs(idx_b, count)
        assert self.knn_hits(idx_b) > 0

        # Drop index B
        idx_b.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_hash_vs_json_sharing_hits_isolation(self, algo: str):
        """Verify that HASH indexes use engine StringRef sharing while JSON indexes do not,
        and both register in VectorRegistry."""
        idx_hash = self.build_index(name="hash_iso", key_type=KeyDataType.HASH, algo=algo)
        idx_json = self.build_index(name="json_iso", key_type=KeyDataType.JSON, algo=algo)
        idx_hash.create(self.client, wait_for_backfill=True)
        idx_json.create(self.client, wait_for_backfill=True)

        count = 10
        for i in range(count):
            self.write_key(idx_hash, f"hash_{i}", [float(i + j) for j in range(DIM)])
        self.wait_for_docs(idx_hash, count)
        assert self.registry_stat("entry_cnt") == count
        hits_after_hash = self.registry_stat("shared_externally_cnt")
        assert hits_after_hash > 0, "HASH indexes must share strings with the engine"

        for i in range(count):
            self.write_key(idx_json, f"json_{i}", [float(i + j) for j in range(DIM)])
        self.wait_for_docs(idx_json, count)
        assert self.registry_stat("entry_cnt") == count * 2
        # Engine hash sharing hits must not change on JSON ingestion
        assert self.registry_stat("shared_externally_cnt") == hits_after_hash

        # Drop indexes
        idx_hash.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), count, timeout=10)
        idx_json.drop(self.client)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    def test_random_chaos_lifecycle_and_registry_drain(self):
        """Perform randomized mutations, invalid payloads, non-vector updates,
        deletions, and concurrent index create/drop cycles, then verify all registry
        entries drain to 0 on teardown."""
        import random
        import threading

        rng = random.Random(42)
        active_indexes = {}
        index_counter = 0
        key_pool = [f"chaos_key_{i}" for i in range(25)]
        algos = ["HNSW", "FLAT"]

        # Start with two initial HASH indexes
        for _ in range(2):
            name = f"chaos_idx_{index_counter}"
            idx = self.build_index(
                name=name,
                key_type=KeyDataType.HASH,
                algo=rng.choice(algos)
            )
            idx.create(self.client, wait_for_backfill=True)
            active_indexes[name] = idx
            index_counter += 1

        stop_event = threading.Event()
        errors = []

        def worker_mutations():
            try:
                cl = self.client
                while not stop_event.is_set():
                    k = rng.choice(key_pool)
                    action = rng.choice(["valid", "invalid", "modify", "del", "other"])
                    if action == "valid":
                        vec = [rng.random() for _ in range(DIM)]
                        cl.hset(f"doc:{k}", "vec", float_to_bytes(vec))
                    elif action == "invalid":
                        cl.hset(f"doc:{k}", "vec", b"invalid_short_payload")
                    elif action == "modify":
                        vec = [rng.random() * 2.0 for _ in range(DIM)]
                        cl.hset(f"doc:{k}", "vec", float_to_bytes(vec))
                    elif action == "del":
                        cl.delete(f"doc:{k}")
                    elif action == "other":
                        cl.hset(f"doc:{k}", "tag", f"val_{rng.randint(0, 10)}")
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=worker_mutations)
        t.start()

        # Main thread performs schema creations and drops concurrently
        for _ in range(12):
            if len(active_indexes) > 1 and rng.random() < 0.45:
                drop_name, drop_idx = active_indexes.popitem()
                drop_idx.drop(self.client)
            elif len(active_indexes) < 4:
                name = f"chaos_idx_{index_counter}"
                idx = self.build_index(
                    name=name,
                    key_type=KeyDataType.HASH,
                    algo=rng.choice(algos)
                )
                idx.create(self.client, wait_for_backfill=False)
                active_indexes[name] = idx
                index_counter += 1

        stop_event.set()
        t.join()
        assert not errors, f"Worker thread encountered errors: {errors}"

        # Teardown: Drop all remaining index schemas
        for name, idx in list(active_indexes.items()):
            idx.drop(self.client)
            del active_indexes[name]

        # Verify VectorRegistry drains to 0 upon dropping all indexes
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

        self.client.flushdb()
        # Verify VectorRegistry drains to 0
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        assert len(self.client.execute_command("FT._LIST")) == 0

    def test_registry_ownership_collision(self):
        """
        Test that an incompatible schema does not erase the registry entry
        of a compatible schema.
        """
        import struct
        import time
        from valkeytestframework.util import waiters
        r = self.client

        # Create a vector of 128 dimensions (512 bytes)
        vec128 = struct.pack('128f', *[0.1]*128)
        r.hset('doc1', mapping={'vec': vec128})

        # Create Schema A which perfectly matches the vector
        r.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2')
        
        # Wait for background indexer
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # Create Schema B which expects 256 dimensions
        r.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '256', 'DISTANCE_METRIC', 'L2')

        # Wait for Schema B's background indexer to finish scanning the keyspace
        from utils import IndexingTestHelper
        IndexingTestHelper.wait_for_backfill_complete_on_node(r, 'idx2')

        # If the bug exists, Schema B's background scan erased the registry entry.
        # So we verify it remains safely tracked for Schema A.
        assert self.registry_stat("entry_cnt") == 1
        
        r.execute_command('FT.DROPINDEX', 'idx1')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        r.execute_command('FT.DROPINDEX', 'idx2')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    def test_registry_vector_modification_single_schema(self):
        """
        1. Single schema: Modifying a vector from valid to invalid size, and back to valid size.
        """
        import struct
        from valkeytestframework.util import waiters
        r = self.client

        r.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2')

        vec128 = struct.pack('128f', *[0.1]*128)
        vec256 = struct.pack('256f', *[0.1]*256)
        
        def hits1():
            return r.execute_command('FT.SEARCH', 'idx1', '*=>[KNN 100 @vec $q]', 'PARAMS', '2', 'q', vec128, 'DIALECT', '2')[0]

        # Valid size
        r.hset('doc1', mapping={'vec': vec128})
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        waiters.wait_for_equal(hits1, 1, timeout=10)

        # Modify to invalid size
        r.hset('doc1', mapping={'vec': vec256})
        waiters.wait_for_equal(hits1, 0, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

        # Modify back to valid size
        r.hset('doc1', mapping={'vec': vec128})
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        waiters.wait_for_equal(hits1, 1, timeout=10)

        # Cleanup
        r.execute_command('FT.DROPINDEX', 'idx1')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    def test_registry_vector_modification_different_dims(self):
        """
        2. Two schemas, different dimensions: Initially indexed by one, then modified to fit the other.
        """
        import struct
        from valkeytestframework.util import waiters
        r = self.client

        r.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2')
        r.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '256', 'DISTANCE_METRIC', 'L2')

        vec128 = struct.pack('128f', *[0.1]*128)
        vec256 = struct.pack('256f', *[0.2]*256)
        
        def hits1():
            return r.execute_command('FT.SEARCH', 'idx1', '*=>[KNN 100 @vec $q]', 'PARAMS', '2', 'q', vec128, 'DIALECT', '2')[0]
        def hits2():
            return r.execute_command('FT.SEARCH', 'idx2', '*=>[KNN 100 @vec $q]', 'PARAMS', '2', 'q', vec256, 'DIALECT', '2')[0]

        # Initially 128 (indexed by idx1, rejected by idx2)
        r.hset('doc1', mapping={'vec': vec128})
        waiters.wait_for_equal(hits1, 1, timeout=10)
        waiters.wait_for_equal(hits2, 0, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # Modify to 256 (rejected by idx1, indexed by idx2)
        r.hset('doc1', mapping={'vec': vec256})
        waiters.wait_for_equal(hits2, 1, timeout=10)
        waiters.wait_for_equal(hits1, 0, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # Cleanup
        r.execute_command('FT.DROPINDEX', 'idx1')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        r.execute_command('FT.DROPINDEX', 'idx2')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    def test_registry_vector_modification_same_dims(self):
        """
        3. Two schemas, same dimensions: Modifying vector from valid to invalid size and back.
        """
        import struct
        from valkeytestframework.util import waiters
        r = self.client

        r.execute_command('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2')
        r.execute_command('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'IP')

        vec128 = struct.pack('128f', *[0.1]*128)
        vec256 = struct.pack('256f', *[0.2]*256)

        def hits1():
            return r.execute_command('FT.SEARCH', 'idx1', '*=>[KNN 100 @vec $q]', 'PARAMS', '2', 'q', vec128, 'DIALECT', '2')[0]
        def hits2():
            return r.execute_command('FT.SEARCH', 'idx2', '*=>[KNN 100 @vec $q]', 'PARAMS', '2', 'q', vec128, 'DIALECT', '2')[0]

        # Valid size
        r.hset('doc1', mapping={'vec': vec128})
        waiters.wait_for_equal(hits1, 1, timeout=10)
        waiters.wait_for_equal(hits2, 1, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # Modify to invalid size
        r.hset('doc1', mapping={'vec': vec256})
        waiters.wait_for_equal(hits1, 0, timeout=10)
        waiters.wait_for_equal(hits2, 0, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

        # Modify back to valid size
        r.hset('doc1', mapping={'vec': vec128})
        waiters.wait_for_equal(hits1, 1, timeout=10)
        waiters.wait_for_equal(hits2, 1, timeout=10)
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # Cleanup
        r.execute_command('FT.DROPINDEX', 'idx1')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)
        r.execute_command('FT.DROPINDEX', 'idx2')
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    def test_drop_index_rate_limited_drain(self):
        """Verifies that vector unsharing on FT.DROPINDEX respects the batch size
        limit, queues pending unshares, and drains completely via server cron."""
        r = self.client
        num_docs = 25
        batch_size = 5
        # Configure small batch size
        r.execute_command("CONFIG", "SET", "search.vector-unshare-batch-size", str(batch_size))
        try:
            r.execute_command(
                "FT.CREATE", "idx_rate_limit", "ON", "HASH", "PREFIX", "1", "rate_doc:",
                "SCHEMA", "vec", "VECTOR", "FLAT", "6",
                "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
            )
            vec_raw = float_to_bytes(VALID)
            for i in range(num_docs):
                r.hset(f"rate_doc:{i}", mapping={"vec": vec_raw})

            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), num_docs, timeout=10)

            # Drop the index. The drop command synchronously processes at most `batch_size` keys.
            # The rest are placed in pending_unshares and drained by server cron.
            r.execute_command("FT.DROPINDEX", "idx_rate_limit")

            # Verify that pending unshares and entry_cnt eventually drain to 0 via cron
            waiters.wait_for_equal(lambda: self.registry_stat("pending_unshare_cnt"), 0, timeout=10)
            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

            # Verify that the underlying hash data in Valkey is intact and unshared
            for i in range(num_docs):
                val = r.hget(f"rate_doc:{i}", "vec")
                assert val == vec_raw, f"Key rate_doc:{i} lost or corrupted its vector after unshare"
        finally:
            r.execute_command("CONFIG", "SET", "search.vector-unshare-batch-size", "10240")

    def test_reinsert_key_while_pending_unshare_queued(self):
        """Verifies that if a key is re-inserted or updated while queued in pending_unshares,
        CancelPendingUnshare cancels the unshare and retains the vector."""
        r = self.client
        num_docs = 50
        # Set batch size to 1 so FT.DROPINDEX only processes 1 key synchronously,
        # leaving 49 keys in pending_unshares.
        r.execute_command("CONFIG", "SET", "search.vector-unshare-batch-size", "1")
        try:
            r.execute_command(
                "FT.CREATE", "idx_cancel_drop", "ON", "HASH", "PREFIX", "1", "cancel_doc:",
                "SCHEMA", "vec", "VECTOR", "FLAT", "6",
                "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
            )
            vec_raw = float_to_bytes(VALID)
            for i in range(num_docs):
                r.hset(f"cancel_doc:{i}", mapping={"vec": vec_raw})

            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), num_docs, timeout=10)

            # Drop the index. 1 key is processed synchronously; remaining keys are queued.
            r.execute_command("FT.DROPINDEX", "idx_cancel_drop")

            # Immediately create a new index covering cancel_doc:49 and re-insert it.
            # This triggers CancelPendingUnshare for cancel_doc:49, while the remaining 49
            # pending unshare entries drain and get untracked.
            r.execute_command(
                "FT.CREATE", "idx_cancel_new", "ON", "HASH", "PREFIX", "1", "cancel_doc:49",
                "SCHEMA", "vec", "VECTOR", "FLAT", "6",
                "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
            )
            r.hset("cancel_doc:49", mapping={"vec": vec_raw})

            # Wait for all pending unshares to drain
            waiters.wait_for_equal(lambda: self.registry_stat("pending_unshare_cnt"), 0, timeout=10)

            # Since cancel_doc:49 was re-inserted and cancelled from pending unshares,
            # it should still be in entry_cnt (at least 1 entry) and searchable.
            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

            res = r.execute_command(
                "FT.SEARCH", "idx_cancel_new", "*=>[KNN 10 @vec $q]",
                "PARAMS", "2", "q", vec_raw, "DIALECT", "2"
            )
            assert res[0] == 1
            assert res[1] == b"cancel_doc:49"

            # Cleanup
            r.execute_command("FT.DROPINDEX", "idx_cancel_new")
            waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)
        finally:
            r.execute_command("CONFIG", "SET", "search.vector-unshare-batch-size", "10240")

    @pytest.mark.parametrize("algo", ["HNSW", "FLAT"])
    def test_swapdb_full_index_and_registry_lifecycle(self, algo: str):
        """Verifies that SWAPDB correctly transfers VectorRegistry tracked entries,
        index schema db numbers, search query execution, and post-swap drop lifecycle."""
        r = self.client
        vec_raw = float_to_bytes(VALID)

        # 1. Setup on DB 0
        r.execute_command("SELECT", 0)
        r.execute_command(
            "FT.CREATE", "idx_swap", "ON", "HASH", "PREFIX", "1", "swap_doc:",
            "SCHEMA", "vec", "VECTOR", algo, "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
        )
        r.hset("swap_doc:0", mapping={"vec": vec_raw})
        r.hset("swap_doc:1", mapping={"vec": vec_raw})

        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 2, timeout=10)

        # Search on DB 0 finds both documents
        res0 = r.execute_command(
            "FT.SEARCH", "idx_swap", "*=>[KNN 10 @vec $q]",
            "PARAMS", "2", "q", vec_raw, "DIALECT", "2"
        )
        assert res0[0] == 2

        # 2. Perform SWAPDB 0 1
        r.execute_command("SWAPDB", 0, 1)

        # On DB 0: index should no longer exist
        r.execute_command("SELECT", 0)
        with pytest.raises(valkey.exceptions.ResponseError, match=r"(?i)(no such index|not found)"):
            r.execute_command("FT.SEARCH", "idx_swap", "*")

        # On DB 1: index exists and returns results
        r.execute_command("SELECT", 1)
        res1 = r.execute_command(
            "FT.SEARCH", "idx_swap", "*=>[KNN 10 @vec $q]",
            "PARAMS", "2", "q", vec_raw, "DIALECT", "2"
        )
        assert res1[0] == 2

        # Ingest another document into DB 1
        r.hset("swap_doc:2", mapping={"vec": vec_raw})
        waiters.wait_for_equal(
            lambda: r.execute_command(
                "FT.SEARCH", "idx_swap", "*=>[KNN 10 @vec $q]",
                "PARAMS", "2", "q", vec_raw, "DIALECT", "2"
            )[0],
            3,
            timeout=10,
        )
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 3, timeout=10)

        # Drop index on DB 1
        r.execute_command("FT.DROPINDEX", "idx_swap")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

        # Clean DB 1 and return to DB 0
        r.execute_command("FLUSHDB")
        r.execute_command("SELECT", 0)

    def test_drop_hash_index_retains_json_index_with_same_field(self):
        """Verifies that HasMatchingVectorIndex properly discriminates by data type
        (JSON vs HASH) when an index on one type is dropped while an index on another
        type shares the same vector attribute name."""
        r = self.client
        vec_raw = float_to_bytes(VALID)

        # Create both a JSON index and a HASH index using the same vector attribute name 'vec'
        r.execute_command(
            "FT.CREATE", "idx_json_same", "ON", "JSON", "PREFIX", "1", "item_json:",
            "SCHEMA", "$.vec", "AS", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
        )
        r.execute_command(
            "FT.CREATE", "idx_hash_same", "ON", "HASH", "PREFIX", "1", "item_hash:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
        )

        # Insert a JSON item and a HASH item
        r.execute_command("JSON.SET", "item_json:1", "$", f'{{"vec": {VALID}}}')
        r.hset("item_hash:1", mapping={"vec": vec_raw})

        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 2, timeout=10)

        # Drop the HASH index
        r.execute_command("FT.DROPINDEX", "idx_hash_same")

        # The HASH item vector is unshared, but the JSON item vector must remain intact
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # JSON search still works and finds item_json:1
        res = r.execute_command(
            "FT.SEARCH", "idx_json_same", "*=>[KNN 10 @vec $q]",
            "PARAMS", "2", "q", vec_raw, "DIALECT", "2"
        )
        assert res[0] == 1
        assert res[1] == b"item_json:1"

        # Drop the JSON index
        r.execute_command("FT.DROPINDEX", "idx_json_same")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

    def test_move_key_preserves_client_selected_db(self):
        """Verifies that MOVE notification processing with ValkeySelectDbGuard
        does not corrupt or alter the client's currently selected database."""
        r = self.client
        vec_raw = float_to_bytes(VALID)

        # Select DB 3 and create an index
        r.execute_command("SELECT", 3)
        r.execute_command(
            "FT.CREATE", "idx_move_src", "ON", "HASH", "PREFIX", "1", "move_doc:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2"
        )
        r.hset("move_doc:1", mapping={"vec": vec_raw})
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 1, timeout=10)

        # Move key to DB 5
        res = r.execute_command("MOVE", "move_doc:1", 5)
        assert res == 1

        # Verify client connection is still on DB 3
        client_info = r.execute_command("CLIENT", "INFO")
        if isinstance(client_info, bytes):
            client_info = client_info.decode()
        assert "db=3" in client_info

        # Key no longer exists on DB 3
        assert r.exists("move_doc:1") == 0

        # Vector registry tracked entry for DB 3 should be removed
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt"), 0, timeout=10)

        # Cleanup DB 3 and DB 5 and return to DB 0
        r.execute_command("FT.DROPINDEX", "idx_move_src")
        r.execute_command("SELECT", 5)
        r.execute_command("FLUSHDB")
        r.execute_command("SELECT", 0)

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

    def get_config_file_lines(self, testdir, port) -> list[str]:
        sharing_flag = "yes" if getattr(self, "_sharing_enabled", True) else "no"
        return [
            "enable-debug-command yes",
            "hash-max-listpack-entries 0",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"dir {testdir}",
            f"loadmodule {os.getenv('MODULE_PATH')} --debug-mode yes "
            f"--info-developer-visible yes --enable-vector-sharing {sharing_flag}",
        ]

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        if hasattr(self, "_current_testdir"):
            args["dir"] = self._current_testdir
            args["dbfilename"] = "dump.rdb"
        return args

    def _start(self, test_suffix: str, sharing: bool) -> tuple[object, Valkey]:
        """A server with an explicit sharing setting, rooted at a suffix under LOGS_DIR."""
        self._sharing_enabled = sharing
        self._current_testdir = f"{LOGS_DIR}/{self.test_name}_{test_suffix}"
        server, client, _ = self.start_server(
            port=self.get_bind_port(),
            test_name=f"{self.test_name}_{test_suffix}",
            cluster_enabled=False,
            is_primary=True,
        )
        assert int(self.registry_stat("sharing_active", client)) == (1 if sharing else 0)
        return server, client

    def registry_stat(self, name: str, client: Valkey | None = None) -> int:
        """One `search_vector_registry_*` INFO field."""
        cl = client if client is not None else self.client
        info = cl.execute_command("INFO", "everything")
        key = f"search_vector_registry_{name}"
        if isinstance(info, dict):
            return int(info[key])
        for line in info.decode().splitlines():
            if line.startswith(key + ":"):
                return int(line.split(":", 1)[1])
        raise KeyError(key)

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

    def _save_and_hand_off(self, client, dst_suffix: str):
        """Persist, then place the RDB where the next server will look."""
        client.execute_command("SAVE")
        cfg = client.execute_command("CONFIG", "GET", "dir")
        src_dir = (cfg[1].decode() if isinstance(cfg[1], bytes) else cfg[1])
        dbfile = client.execute_command("CONFIG", "GET", "dbfilename")
        name = (dbfile[1].decode() if isinstance(dbfile[1], bytes) else dbfile[1])
        dst_dir = f"{LOGS_DIR}/{self.test_name}_{dst_suffix}"
        os.makedirs(dst_dir, exist_ok=True)
        shutil.copyfile(f"{src_dir}/{name}", f"{dst_dir}/{name}")

    @pytest.mark.parametrize(
        "writer_sharing,reader_sharing",
        [(True, False), (False, True)],
        ids=["written_shared_read_unshared", "written_unshared_read_shared"],
    )
    def test_rdb_moves_between_sharing_configurations(
        self, writer_sharing: bool, reader_sharing: bool
    ):
        server, client = self._start("writer", writer_sharing)
        index = self._create_and_fill(client, "L2")
        expected = self._knn(client)
        expected_writer_entries = len(self.CORPUS)
        assert self.registry_stat("entry_cnt", client) == expected_writer_entries
        self._save_and_hand_off(client, "reader")
        server.exit()

        server2, client2 = self._start("reader", reader_sharing)
        waiters.wait_for_equal(
            lambda: index.info(client2).num_docs, len(self.CORPUS)
        )
        assert self._knn(client2) == expected, (
            "distances changed when the RDB moved between sharing settings"
        )
        expected_reader_entries = len(self.CORPUS)
        assert self.registry_stat("entry_cnt", client2) == expected_reader_entries
        if reader_sharing:
            assert self.registry_stat("shared_externally_cnt", client2) > 0
        else:
            assert self.registry_stat("shared_externally_cnt", client2) == 0
        server2.exit()

    @pytest.mark.parametrize("metric", ["L2", "IP", "COSINE"])
    def test_distances_agree_across_restart_and_sharing_toggle(self, metric: str):
        server, client = self._start("first", True)
        self._create_and_fill(client, metric)
        first = self._knn(client)
        self._save_and_hand_off(client, "same_sharing")
        self._save_and_hand_off(client, "toggled_sharing")
        server.exit()

        server2, client2 = self._start("same_sharing", True)
        second = self._knn(client2)
        server2.exit()

        server3, client3 = self._start("toggled_sharing", False)
        third = self._knn(client3)
        server3.exit()

        assert second == first, (
            f"{metric}: distances changed across save/restore\n"
            f"  before: {first}\n  after:  {second}"
        )
        assert third == first, (
            f"{metric}: distances changed when sharing was turned off\n"
            f"  before: {first}\n  after:  {third}"
        )

    def test_deduplication_lifecycle_with_sharing_disabled(self):
        """Verifies that vector deduplication across multiple indexes continues to function
        when hash vector sharing (--enable-vector-sharing no) is disabled."""
        server, client = self._start("dedup_unshared", False)
        assert int(self.registry_stat("sharing_active", client)) == 0

        # Create two indexes sharing the same prefix and field name
        client.execute_command(
            "FT.CREATE", "idx_unshared1", "ON", "HASH", "PREFIX", "1", "shared_doc:",
            "SCHEMA", "vec", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", str(self.DIM), "DISTANCE_METRIC", "L2"
        )
        client.execute_command(
            "FT.CREATE", "idx_unshared2", "ON", "HASH", "PREFIX", "1", "shared_doc:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(self.DIM), "DISTANCE_METRIC", "COSINE"
        )

        vec_bytes = float_to_bytes(self.CORPUS[0])
        client.hset("shared_doc:1", mapping={"vec": vec_bytes})

        # Both indexes index the same key and attribute.
        # Deduplication should record only 1 entry in the registry, while external sharing count is 0.
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt", client), 1, timeout=10)
        assert self.registry_stat("shared_externally_cnt", client) == 0

        # Both indexes should return the hit
        hits1 = client.execute_command(
            "FT.SEARCH", "idx_unshared1", "*=>[KNN 5 @vec $q]",
            "PARAMS", "2", "q", vec_bytes, "DIALECT", "2"
        )[0]
        hits2 = client.execute_command(
            "FT.SEARCH", "idx_unshared2", "*=>[KNN 5 @vec $q]",
            "PARAMS", "2", "q", vec_bytes, "DIALECT", "2"
        )[0]
        assert hits1 == 1
        assert hits2 == 1

        # Drop first index: entry is retained because idx_unshared2 still covers it
        client.execute_command("FT.DROPINDEX", "idx_unshared1")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt", client), 1, timeout=10)

        # Drop second index: entry count drops to 0
        client.execute_command("FT.DROPINDEX", "idx_unshared2")
        waiters.wait_for_equal(lambda: self.registry_stat("entry_cnt", client), 0, timeout=10)

        # The vector data in hash is still readable
        assert client.hget("shared_doc:1", "vec") == vec_bytes
        server.exit()