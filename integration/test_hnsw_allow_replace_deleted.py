import struct
import time
import os
import pytest

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector
from ft_info_parser import FTInfoParser
from util import waiters


class TestHNSWAllowReplaceDeleted(ValkeySearchTestCaseBase):
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
            num_vecs,
            timeout=10
        )

        # Delete highest-labeled vectors (8 and 9)
        num_deleted = 2
        for i in range(num_vecs - num_deleted, num_vecs):
            client.delete(f"rdoc:{i}")

        surviving = num_vecs - num_deleted

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            surviving,
            timeout=10
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
            lambda: hnsw_index.backfill_complete(client),
            timeout=30
        )

        # Add 5 new vectors after reload
        num_new = 5
        for i in range(num_new):
            vec = struct.pack('<4f', *[100.0 + i + 0.1 * d for d in range(4)])
            client.hset(f"rdoc:new{i}", mapping={"vector": vec})

        expected_total = surviving + num_new  # 8 + 5 = 13

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            expected_total,
            timeout=10
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
