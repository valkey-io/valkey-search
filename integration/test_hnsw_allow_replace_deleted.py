import struct
import os
import shutil
import pytest

from valkey.client import Valkey
from valkey_search_test_case import (
    LOGS_DIR,
    ValkeySearchTestCaseCommon,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector
from util import waiters


class TestHNSWAllowReplaceDeleted(ValkeySearchTestCaseDebugMode):
    """
    Verify HNSW allow_replace_deleted works correctly with both yes/no settings.
    """

    @pytest.mark.parametrize("allow_replace_deleted", ["yes", "no"])
    def test_allow_replace_deleted_after_rdb_reload(self, allow_replace_deleted):
        """
        After RDB reload, inserting new vectors must succeed without inc_id_
        collisions regardless of allow_replace_deleted setting.
        """
        client: Valkey = self.server.get_new_client()

        client.config_set("search.hnsw-allow-replace-deleted",
                          allow_replace_deleted)

        hnsw_index = Index(
            "test_rdb_idx",
            [Vector("vector", 4, type="HNSW", distance="L2")],
            prefixes=["rdoc:"]
        )
        hnsw_index.create(client)

        # Add 10 vectors (labels 0-9)
        num_vecs = 10
        for i in range(num_vecs):
            vec = struct.pack('<4f', *[float(i) + 0.1 * d for d in range(4)])
            client.hset(f"rdoc:{i}", mapping={"vector": vec})

        # Wait for backfill/indexing to complete
        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            num_vecs
        )

        # Delete highest-labeled vectors (8 and 9)
        num_deleted = 2
        for i in range(num_vecs - num_deleted, num_vecs):
            client.delete(f"rdoc:{i}")

        surviving = num_vecs - num_deleted

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            surviving
        )

        # RDB save + reload
        client.execute_command("SAVE")

        info_before = client.info("SEARCH")
        exc_before = int(info_before.get("search_hnsw_add_exceptions_count", 0))

        os.environ.pop("SKIPLOGCLEAN", None)
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()

        # Wait for index to be loaded
        waiters.wait_for_true(
            lambda: hnsw_index.backfill_complete(client)
        )

        # Add 5 new vectors after reload
        num_new = 5
        for i in range(num_new):
            vec = struct.pack('<4f', *[100.0 + i + 0.1 * d for d in range(4)])
            client.hset(f"rdoc:new{i}", mapping={"vector": vec})

        expected_total = surviving + num_new  # 8 + 5 = 13

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            expected_total
        )

        info_after = client.info("SEARCH")
        exc_after = int(info_after.get("search_hnsw_add_exceptions_count", 0))

        assert exc_after - exc_before == 0, \
            (f"[allow_replace_deleted={allow_replace_deleted}] "
             f"Expected 0 add exceptions after RDB reload, "
             f"got {exc_after - exc_before}")

        ft_info = hnsw_index.info(client)
        assert ft_info.num_docs == expected_total, \
            (f"[allow_replace_deleted={allow_replace_deleted}] "
             f"Expected {expected_total} docs ({surviving} surviving + "
             f"{num_new} new), got {ft_info.num_docs}")

        # Verify all vectors are searchable via KNN
        query_vec = struct.pack('<4f', *[50.0, 50.1, 50.2, 50.3])
        search_result = client.execute_command(
            "FT.SEARCH", "test_rdb_idx",
            f"*=>[KNN {expected_total} @vector $q]",
            "PARAMS", "2", "q", query_vec,
        )
        search_count = search_result[0]
        assert search_count == expected_total, \
            (f"[allow_replace_deleted={allow_replace_deleted}] "
             f"KNN search returned {search_count}, expected {expected_total}")

        # Cleanup
        client.execute_command("FT.DROPINDEX", "test_rdb_idx")

class TestReplaceDeletedOnLoad(ValkeySearchTestCaseDebugMode):
    """
    Test that deleted element slots are reusable after RDB load.
    """

    def append_startup_args(self, args):
        args["search.hnsw-allow-replace-deleted"] = "yes"
        return args

    def test_index_add_after_rdb_load_with_deleted_elements(self):
        """
        When allow_replace_deleted is enabled, deleted element slots should be
        reusable after RDB load. This verifies that the deleted_elements set is
        correctly populated during LoadIndex so that new inserts can reclaim
        deleted slots when the index is at capacity.
        """
        client: Valkey = self.server.get_new_client()

        # Create HNSW index with small INITIAL_CAP to hit capacity quickly
        hnsw_index = Index(
            "idx",
            [Vector("vector", 4, type="HNSW", distance="L2", initialcap=4)],
            prefixes=["doc:"]
        )
        hnsw_index.create(client)

        # Fill the index to capacity
        for i in range(4):
            vec = struct.pack('<4f', *[float(i) + 0.1 * d for d in range(4)])
            client.hset(f"doc:{i}", mapping={"vector": vec})

        # Delete some vectors so num_deleted_ > 0 after reload
        for i in range(2):
            client.delete(f"doc:{i}")

        # Verify KNN search returns 2 indexes after delete
        query_vec = struct.pack('<4f', *[100.0, 100.1, 100.2, 100.3])
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 2 @vector $q]",
            "PARAMS", "2", "q", query_vec,
        )
        assert search_result[0] == 2, \
            f"KNN search after delete returned {search_result[0]}, expected 2"

        # RDB save + reload
        client.execute_command("SAVE")
        os.environ.pop("SKIPLOGCLEAN", None)
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()

        waiters.wait_for_true(
            lambda: hnsw_index.backfill_complete(client)
        )

        # Add new vectors — these should reuse deleted slots
        for i in range(2):
            vec = struct.pack('<4f', *[200.0 + i + 0.1 * d for d in range(4)])
            client.hset(f"doc:new{i}", mapping={"vector": vec})

        # Verify KNN search returns expected results
        query_vec = struct.pack('<4f', *[100.0, 100.1, 100.2, 100.3])
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 4 @vector $q]",
            "PARAMS", "2", "q", query_vec,
        )
        assert search_result[0] == 4, \
            f"KNN search returned {search_result[0]}, expected 4"

        client.execute_command("FT.DROPINDEX", "idx")


class TestHNSWDuplicateLabelRDBLoad(ValkeySearchTestCaseCommon):
    """
    Regression test for loading an RDB that carries a duplicate label.

    Previously ModifyRecordImpl first called markDelete() on the record to
    tombstone the existing slot with the label and then called addPoint() to add
    the new vector with the exact same label. In the normal case, the same slot
    was updated and there were no duplicate labels. The problem occurred when
    another document was deleted between markDelete() and addPoint(), which put
    another tombstoned slot at the front of deleted_elements. The addPoint()
    for the modified record reused it, adding the label to a new slot while still
    keeping the same label on the old slot it had just tombstoned.
    """

    RDB_FILENAME = "hnsw_duplicate_label.rdb"
    RDB_FIXTURE = f"rdbs/{RDB_FILENAME}"
    LIVE_VEC = struct.pack('<4f', 10.0, 20.0, 30.0, 40.0)

    # TODO: Make functionality common once https://github.com/valkey-io/valkey-search/pull/1172 is merged
    def _start_server(self, test_name, search_module_args=""):
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir = f"{LOGS_DIR}/{test_name}"
        port = self.get_bind_port()

        os.makedirs(testdir, exist_ok=True)
        shutil.copy(
            os.path.join(os.path.dirname(__file__), self.RDB_FIXTURE),
            os.path.join(testdir, self.RDB_FILENAME),
        )

        lines = [
            "enable-debug-command yes",
            f"dbfilename {self.RDB_FILENAME}",
            f"dir {testdir}",
            f"port {port}",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"loadmodule {os.getenv('MODULE_PATH')} {search_module_args}",
        ]
        conf_file = os.path.join(testdir, f"valkey_{port}.conf")
        with open(conf_file, "w") as f:
            f.write("\n".join(lines) + "\n")

        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            port=port,
            conf_file=conf_file,
            args={"logfile": f"logfile_{port}", "dbfilename": self.RDB_FILENAME},
        )
        return server, client, os.path.join(testdir, f"logfile_{port}")

    def test_label_lookup_reconstructed(self):
        '''
        The HNSW index in the RDB has two slots with the same label.
        The first slot has the live vector. Previously the label lookup
        would always point to the largest slot with the label.
        '''
        server, client, logfile = self._start_server(
            "hnsw_dup_label_rdb",
            search_module_args="--debug-mode yes")
        client.config_set("search.info-developer-visible", "yes")

        hnsw_index = Index(
            "idx",
            [Vector("vector", 4, type="HNSW", distance="L2")],
            prefixes=["doc:"]
        )
        waiters.wait_for_true(
            lambda: hnsw_index.backfill_complete(client), timeout=10
        )

        dup_on_load = int(client.info("SEARCH").get(
            "search_hnsw_duplicate_label_on_load_count"))
        assert dup_on_load == 1, \
            f"Expected a duplicate label on load, got {dup_on_load}"

        ft_info = hnsw_index.info(client)
        assert ft_info.num_docs == 1, \
            f"Expected 1 doc after load, got {ft_info.num_docs}"

        # The survivor should be doc:1 carrying the live vector.
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 2 @vector $q]",
            "PARAMS", "2", "q", self.LIVE_VEC,
            "RETURN", "1", "vector",
        )
        assert search_result[0] == 1, \
            f"Expected 1 search result, got {search_result[0]}"
        assert search_result[1] == b"doc:1", \
            f"Expected doc:1 to survive, got {search_result[1]}"
        returned_fields = search_result[2]
        vector_value = returned_fields[returned_fields.index(b"vector") + 1]
        assert vector_value == self.LIVE_VEC, \
            f"Expected doc:1 to carry the updated vector, got {vector_value!r}"

        # Deleting the survivor must actually remove it, proving label_lookup_
        # resolves the label to the live slot rather than a tombstone.
        client.delete("doc:1")
        assert hnsw_index.info(client).num_docs == 0
        assert int(client.info("SEARCH").get(
            "search_hnsw_remove_exceptions_count", 0)) == 0
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 2 @vector $q]",
            "PARAMS", "2", "q", self.LIVE_VEC,
        )
        assert search_result[0] == 0, \
            f"Expected doc:1 to be deleted, got {search_result[0]} results"


    def test_labels_are_unique(self):
        '''
        The vectors in HNSW are owned by tracked_vectors_ at the module level
        which maps labels to vectors. The HNSW library holds a raw pointer to them.
        This pattern enforces that there can only be at most one vector alive per
        label. The HNSW index in the RDB has two slots with the same label. Both
        alive and tombstoned slots are traversed during searches and need valid
        vectors, but previously every one with a duplicate label except the last
        would be left with a dangling pointer. In our case, that would be slot 0
        with the live vector. To be absolutely certain that we would be hitting the
        dangling pointer, we will overwrite the tombstoned slot which erases the
        label from tracked_vectors_, so that live slot is guaranteed to have a
        dangling pointer.

        In the end, the fix is all the same. Ensure that every slot corresponds with
        a unique label on restore.
        '''
        server, client, logfile = self._start_server(
            "hnsw_dup_label_rdb",
            search_module_args="--debug-mode yes --hnsw-allow-replace-deleted yes")
        client.config_set("search.info-developer-visible", "yes")

        hnsw_index = Index(
            "idx",
            [Vector("vector", 4, type="HNSW", distance="L2")],
            prefixes=["doc:"]
        )
        waiters.wait_for_true(
            lambda: hnsw_index.backfill_complete(client), timeout=10
        )

        # Confirm the fixture actually carried a duplicate label into this load.
        assert int(client.info("SEARCH").get(
            "search_hnsw_duplicate_label_on_load_count", 0)) == 1

        # Fresh key reuses the dup tombstone's slot.
        new_vec = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        client.hset("doc:2", mapping={"vector": new_vec})
        waiters.wait_for_equal(lambda: hnsw_index.info(client).num_docs, 2)

        # The survivor's RETURN bytes must still be its own.
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 1 @vector $q]",
            "PARAMS", "2", "q", self.LIVE_VEC,
            "RETURN", "1", "vector",
        )
        returned_fields = search_result[2]
        vector_value = returned_fields[returned_fields.index(b"vector") + 1]
        assert vector_value == self.LIVE_VEC, \
            f"Expected survivor to carry its vector, got {vector_value!r}"

