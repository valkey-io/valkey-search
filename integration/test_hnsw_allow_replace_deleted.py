import struct
import os
import pytest

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector
from ft_info_parser import FTInfoParser
from util import waiters
from utils import pausepoint_hit, run_in_thread


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

        os.environ["SKIPLOGCLEAN"] = "1"
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


class TestHNSWDuplicateLabelRace(ValkeySearchTestCaseDebugMode):
    """
    Reproduce the duplicate-label RDB corruption from a modify/delete race.

    ModifyRecordImpl is not atomic. It first calls markDelete() on the record to
    tombstone the existing slot with the label and then calls addPoint() to add
    the new vector with the exact same label. Theoretically,
    std::unordered_set<tableint> deleted_elements makes no promises about which
    slot deleted_elements.begin() points to. In practice though, it's always
    the last added slot. In the normal case, the same slot will be updated and
    there will be no duplicate labels. The problem occurs when another document
    is deleted between markDelete() and addPoint(). It will put another
    tombstoned slot at the front of deleted_elements and the addPoint() for the
    modified record will use it, adding the label to a new slot and still keeping
    the same label in the old slot it just tombstoned.

    The pausepoint "hnsw_modify_between_delete_and_add" sits between those two
    calls, so the test can sequence the race deterministically rather than
    relying on timing.
    """

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.hnsw-allow-replace-deleted"] = "yes"
        # Two writer threads so the parked modify and the delete run
        # concurrently on separate workers.
        args["search.writer-threads"] = "2"
        return args

    def test_modify_steals_deleted_slot_causing_duplicate_label(self):
        client: Valkey = self.server.get_new_client()

        hnsw_index = Index(
            "idx",
            [Vector("vector", 4, type="HNSW", distance="L2")],
            prefixes=["doc:"]
        )
        hnsw_index.create(client)

        vec0 = struct.pack('<4f', 1.0, 2.0, 3.0, 4.0)
        vec1 = struct.pack('<4f', 5.0, 6.0, 7.0, 8.0)
        client.hset("doc:0", mapping={"vector": vec0})
        client.hset("doc:1", mapping={"vector": vec1})
        assert hnsw_index.info(client).num_docs == 2

        # Pause the next modify between its markDelete and addPoint.
        assert client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET",
            "hnsw_modify_between_delete_and_add") == b"OK"

        # Modify doc:1. markDelete runs on its slot, and then the thread parks
        # at the pausepoint before addPoint.
        vec1_new = struct.pack('<4f', 10.0, 20.0, 30.0, 40.0)
        update_thread, _, update_err = run_in_thread(
            lambda: client.hset("doc:1", mapping={"vector": vec1_new})
        )
        waiters.wait_for_true(
            lambda: pausepoint_hit(
                client, "hnsw_modify_between_delete_and_add")
        )

        # Delete doc:0 while the modify is parked. This adds doc:0's slot to
        # deleted_elements.
        client2 = self.server.get_new_client()
        client2.delete("doc:0")
        waiters.wait_for_equal(
            lambda: hnsw_index.info(client2).num_docs, 1
        )

        # Resume the modify. Its addPoint pops a slot off deleted_elements to
        # reuse. In practice we expect that to be doc:0's slot, so doc:1's live
        # record mapped in label_lookup_ lands on slot 0 and its tombstone on slot 1.
        assert client2.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET",
            "hnsw_modify_between_delete_and_add") == b"OK"
        update_thread.join()
        assert update_err[0] is None, \
            f"Modify HSET raised in its thread: {update_err[0]!r}"

        # Reloading from the RDB rebuilds label_lookup_ from the on-disk
        # labels. There should be no validation error hit.
        assert client2.execute_command("SAVE")
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        waiters.wait_for_true(
            lambda: hnsw_index.backfill_complete(client)
        )
        ft_info = hnsw_index.info(client)
        assert ft_info.num_docs == 1, \
            f"Expected 1 doc after update+delete, got {ft_info.num_docs}"

        # The survivor should be doc:1 carrying its updated vector.
        search_result = client.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 2 @vector $q]",
            "PARAMS", "2", "q", vec1_new,
            "RETURN", "1", "vector",
        )
        assert search_result[0] == 1, \
            f"Expected 1 search result, got {search_result[0]}"
        assert search_result[1] == b"doc:1", \
            f"Expected doc:1 to survive, got {search_result[1]}"
        returned_fields = search_result[2]
        vector_value = returned_fields[returned_fields.index(b"vector") + 1]
        assert vector_value == vec1_new, \
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
            "PARAMS", "2", "q", vec1_new,
        )
        assert search_result[0] == 0, \
            f"Expected doc:1 to be deleted, got {search_result[0]} results"
